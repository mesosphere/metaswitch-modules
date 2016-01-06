#!/usr/bin/env python

# Copyright 2015 Metaswitch Networks
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import subprocess
import threading
import time
import math
from random import randint
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
import sys
from calico_utils import _setup_logging
from tasks import (TaskUpdateError,
                   SleepTask,
                   PingTask)
from constants import LOGFILE, TASK_CPUS, TASK_MEM, \
    BAD_TASK_STATES, TEST_TIMEOUT

from fnmatch import fnmatch
import os
from unittest.loader import VALID_MODULE_NAME

_log = _setup_logging(LOGFILE)
NEXT_AVAILABLE_TASK_ID = 0

# Global driver set during Framework initialization by start() and used
# throughout to start and stop the framework, and handle offer actions and
# other control needs.


class TestState(object):
    Unstarted, Running, Complete = range(0,3)


class TestCase(object):
    def __init__(self, tasks, name):
        self.name = name
        """
        Nicename to identify this test.
        """

        self.state = TestState.Unstarted

        self.tasks = tasks
        """
        Tasks for this test.
        """

        self.timeout = None
        """
        If None, this test is not being tracked for timeout.

        Otherwise, this number will be seconds-since-epoch of the last
        checkpoint.

        For an unstarted test, this is set if/when
        this test refuses to start while no other tests are running.
        If time expires in this state, that means the mesos cluster was
        unable to provide enough offers to launch this test when no other tests
        were running.

        For a started test, this value is reset at each task update, and
        represents the last time
        this Test heard from one of its tasks.
        """

        self.additional_info = None
        """
        Test-wide information which will be provided during the test output.
        Useful for explaining a test-scope failure (vs. a task failure).
        """

        self.killed = False
        """
        Flag, when set to True, will indicate that this test is being shutdown.
        """

        # Add a backwards reference from each Task to its parent test
        for task in tasks:
            task.test = self

    def __repr__(self):
        s_repr = "Test(%s" % self.name
        if self.timeout is not None:
            s_repr += (", timeout=%d" % (time.time() - self.timeout))
        s_repr += ")"

        return s_repr

    def can_run_on(self, offers):
        """
        Checks if this test could run on the offers passed in.

        :return: A dictionary with key: slave_id, value: offer, specifiying
         which offers this test would like to reserve. Will be empty if this
         test doesn't see enough offers to run the test in full.
        """
        anywhere_tasks = []
        tasks_by_slave = {}
        for task in self.tasks:
            if task.slave is None:
                anywhere_tasks.append(task)
            else:
                tasks_by_slave.setdefault(task.slave, []).append(task)

        # Sort those groups by most tasks to least tasks
        tasks_by_slave = sorted(tasks_by_slave.values(),
                                lambda x, y: len(y) - len(x))

        # Quit early if there isn't enough unique slaves
        if len(tasks_by_slave) > len(offers):
            return {}

        # Sort the offers by resources
        offers_by_tasks_launchable = sorted(offers.values(), lambda x, y:
                                            cmp(y.num_tasks_launchable(),
                                                x.num_tasks_launchable()))

        tasks_for_offer = {}

        # We now have the offers sorted by largest to smallest, and the
        # tasks-per-slave grouped from largest to smallest.
        reserved_offers_by_slave_id = {}
        try:
            for offer, tasks in zip(offers_by_tasks_launchable,
                                    tasks_by_slave):
                if offer.num_tasks_launchable() < len(tasks):
                    raise NotEnoughResources(
                        "Need a larger offer to meet slave-id specifications")

                # We can fit the tasks for this slave on this offer.
                for task in tasks:
                    _log.debug("\t\tPlanning %s for %s", task, offer)
                    # Claim the slave
                    task.slave_id = offer.offer.slave_id.value
                    reserved_offers_by_slave_id[offer.slave_id] = offer

                    # Track tasks added to offer (for anywhere_tasks later)
                    tasks_for_offer.setdefault(offer, [])
                    tasks_for_offer[offer].append(task)


            # Try to place anywhere tasks.
            for offer in offers.values():
                while anywhere_tasks and \
                                len(tasks_for_offer) < offer.num_tasks_launchable():
                    tasks_for_offer.setdefault(offer, [])
                    anywhere_task = anywhere_tasks.pop()
                    anywhere_task.slave_id = offer.offer.slave_id.value
                    tasks_for_offer[offer].append(anywhere_task)
                    reserved_offers_by_slave_id[offer.slave_id] = offer

            # Check that all anywhere_tasks were assigned
            if anywhere_tasks:
                raise NotEnoughResources(
                    "Matched all slave-specific tasks, but "
                    "not enough remaining resources to launch "
                    "the remaining %d anywhere-tasks" % len(anywhere_tasks))

        except NotEnoughResources as e:
            # Rollback all reservations
            _log.debug("\t\tNot Launching: %s" % e)
            for task in self.tasks:
                task.slave_id = None
            return {}

        _log.info("\t\tAll Tasks Planned. Starting Test.")
        return reserved_offers_by_slave_id

    def print_report(self):
        """
        Print this test's results.
        """
        failed_tasks = []
        unstarted_tasks = []
        for task in self.tasks:
            if task.state in BAD_TASK_STATES:
                failed_tasks.append(task)
            elif task.state is None:
                unstarted_tasks.append(task)

        print "|--- %s ---|" % self.name
        if self.killed:
            print "Test Status: KILLED"
            print "Reason: ", self.additional_info
        elif failed_tasks:
            print "Test Status: FAIL"
        elif unstarted_tasks:
            print "Test Status: UNLAUNCHED"
            print "Reason: ", self.additional_info
        else:
            print "Test Status: PASS"

        print "Task Statuses:"
        for task in self.tasks:
            if task.state is None:
                state = "UNLAUNCHED"
            else:
                state = mesos_pb2.TaskState.Name(task.state)
            print "\t%s: %s" % (task, state)
            if type(task) == PingTask:
                print "\t\tTest Results: %s" % task.ping_status_data

        print "-----------------------------------"

    def launch(self, offers_by_slave_id):
        """
        Launches the test on the provided offers.
        This assumes each task has already picked a task.slave_id.
        """
        operations_by_offer = {}
        for task in self.tasks:
            if task.state != None:
                # Task has already been lauched
                continue

            if not task.dependencies_are_met or \
                    task.slave_id not in offers_by_slave_id:
                _log.debug("\t\tNot yet ready to launch %s", task)
                continue

            # Proceed with launch
            offer = offers_by_slave_id[task.slave_id]

            try:
                operation = operations_by_offer[offer]
            except KeyError:
                operation = mesos_pb2.Offer.Operation()
                operation.type = mesos_pb2.Offer.Operation.LAUNCH
                operations_by_offer[offer] = operation

            global NEXT_AVAILABLE_TASK_ID
            task.task_id = str(NEXT_AVAILABLE_TASK_ID)
            NEXT_AVAILABLE_TASK_ID += 1

            task.state = mesos_pb2.TASK_STAGING
            task.port = offer.port

            _log.info("\t\tLaunching %s Using %s", task, offer)

            operation.launch.task_infos.extend([task.as_new_mesos_task()])

        for offer in offers_by_slave_id.values():
            # If there's an entry for it, we loaded tasks on, so launch it
            if offer in operations_by_offer:
                operation = operations_by_offer[offer]
                driver.acceptOffers([offer.offer.id], [operation])
            else:
                # Otherwise, we need to decline, since we didn't have any tasks
                # ready for it yet
                _log.info("\t\tNot ready to launch more tasks. Declining offer")
                driver.declineOffer(offer.offer.id)

    def timed_out(self):
        """
        Returns true if the test timed out.
        :param start: Start the timer if it isn't currently running
        """
        if self.timeout is None:
            raise Exception("Timeout hasn't started on test")
        return (time.time() - self.timeout) > TEST_TIMEOUT

    def restart_timeout(self):
        self.timeout = time.time()

    def remove_timeout(self):
        self.timeout = None

    def start_timeout_if_not_currently_started(self):
        if self.timeout is None:
            self.restart_timeout()


class Offer(object):
    """
    Provides encapsulation around the mesos offer for quick access to mesos
    settings.
    """
    def __init__(self, offer):
        self.offer = offer
        self.cpus = 0.0
        self.mem = 0.0
        self.slave_id = offer.slave_id.value
        self.offer_id = str(self.offer.id.value)
        self.port = None
        for resource in self.offer.resources:
            if resource.name == "cpus":
                self.cpus += resource.scalar.value
            elif resource.name == "mem":
                self.mem += resource.scalar.value
            elif resource.name == "ports":
                port_range = resource.ranges.range[0]
                self.port = randint(port_range.begin, port_range.end)

    def num_tasks_launchable(self):
        return int(math.floor(min(self.cpus / TASK_CPUS, self.mem / TASK_MEM)))

    def __repr__(self):
        return "Offer(id=%s, tasks=%d, slave=%s)" % (
            self.offer_id[-5:], self.num_tasks_launchable(),
            self.offer.slave_id.value[-5:])


class TestScheduler(mesos.interface.Scheduler):
    def __init__(self, implicit_acknowledgements):
        self.implicitAcknowledgements = implicit_acknowledgements
        """
        Flag to disable the requirement that the Executor responds to ACK
        messages
        """

        self.tests = []
        """
        A collection of TestCases this scheduler will run through should deploy.
        """

        self.test_by_slave_id = {}
        """
        Dictionary which specifies which test has reserved which slave.
        Key is the slave_id. Value is the test.
        """

        self.unreserved_offers_by_slave_id = {}
        """
        Dictionary of offers by slave ID.  Offers in this dictionary are from
        slaves that are not running any tests.
        """

    def all_tasks(self):
        """
        Get all tasks across all tests.
        """
        for test in self.tests:
            for task in test.tasks:
                yield task

    def kill_test(self, test, msg=None):
        """
        Marks a test as 'killed', and removes Slave reservations.
        """
        test.state = TestState.Complete
        test.killed = True
        test.additional_info = msg
        if msg:
            _log.error("Test Killed: %s. Reason: %s", test, msg)

        # Unreserve any offers reserved by this test
        for slave_id, reserving_test in self.test_by_slave_id.copy().iteritems():
            if test == reserving_test:
                _log.debug("\tRemoving slave reservation")
                del(self.test_by_slave_id[slave_id])

    def registered(self, driver, frameworkId, masterInfo):
        """
        Callback used when the framework is succesfully registered.
        """
        _log.info("REGISTERED: with framework ID %s", frameworkId.value)

    def resourceOffers(self, driver, offers):
        """
        Triggered when the framework is offered resources by mesos.
        """

        # Send offer to reserved test or unreserved pool
        for offer in offers:
            new_offer = Offer(offer)
            try:
                self.test_by_slave_id[new_offer.slave_id].launch({new_offer.slave_id: new_offer})
                _log.info("New offer %s Sent to reserved slave", new_offer)
            except KeyError:
                self.unreserved_offers_by_slave_id[
                    new_offer.slave_id] = new_offer
                _log.info("New offer %s Moved to unreserved pool", new_offer)

        # Loop through unreserved offers and offer to unstarted tests
        if self.unreserved_offers_by_slave_id:
            _log.info("Offering %d unreserved offers to Unstarted Tests",
                      len(self.unreserved_offers_by_slave_id))
            for test in self.tests:
                # Skip all running/complete tests
                if test.state is not TestState.Unstarted:
                    continue

                _log.info("\t%s", test)
                reserved_offers_by_slave_id = test.can_run_on(
                    self.unreserved_offers_by_slave_id)
                if reserved_offers_by_slave_id:
                    test.state = TestState.Running
                    test.restart_timeout()

                    for slave_id in reserved_offers_by_slave_id:
                        # Save slave reservations for the future
                        self.test_by_slave_id[slave_id] = test

                        # Remove from unreserved pool
                        del(self.unreserved_offers_by_slave_id[slave_id])

                    test.launch(reserved_offers_by_slave_id)

    def stop_driver_and_report_results(self, error=None):
        if error:
            _log.error("KILLING FRAMEWORK: %s", error)
        driver.stop()
        for test in self.tests:
            test.print_report()
            print "\n"

    def statusUpdate(self, driver, update):
        """
        Triggered when the Framework receives a task Status Update from the
        Executor
        """
        # Find the task which corresponds to the status update
        try:
            calico_task = next(task for task in self.all_tasks() if
                               task.task_id == update.task_id.value)
        except StopIteration:
            _log.error(
                "FATAL: Received Task Update from Unidentified TaskID: %s",
                update.task_id.value)
            driver.abort()
            return

        try:
            calico_task.process_update(update)
        except TaskUpdateError as e:
            self.kill_test(calico_task.test, str(e))


        _log.info("TASK_UPDATE - %s: %s",
                  mesos_pb2.TaskState.Name(calico_task.state),
                  calico_task)

        if calico_task.state in BAD_TASK_STATES:
            _log.error(
                "\t%s is in unexpected state %s with message '%s'",
                calico_task,
                mesos_pb2.TaskState.Name(update.state),
                update.message)
            _log.error("\tData:  %s", repr(str(update.data)))
            _log.error("\tSent by: %s",
                   mesos_pb2.TaskStatus.Source.Name(update.source))
            _log.error("\tReason: %s",
                   mesos_pb2.TaskStatus.Reason.Name(update.reason))
            _log.error("\tMessage: %s", update.message)
            _log.error("\tHealthy: %s", update.healthy)

            self.kill_test(calico_task.test, update.message)
            return


        # Check for good update
        if update.state == mesos_pb2.TASK_FINISHED:
            # If its a sleep task, check that its pingers have finished as well
            if type(calico_task) == SleepTask:
                calico_task_targeters = [task for task in calico_task.test.tasks
                                         if type(task) == PingTask and
                                         calico_task in (
                                         task.can_ping_targets + task.cant_ping_targets)]

                for calico_task_targeter in calico_task_targeters:
                    if calico_task_targeter.state is not mesos_pb2.TASK_FINISHED:
                        self.kill_test(calico_task.test,
                                       "A Sleep task finished before "
                                       "its Pinger did.")
                        return

        # Check for test completion, or reset timeout
        for task in calico_task.test.tasks:
            if task.state != mesos_pb2.TASK_FINISHED:
                calico_task.test.restart_timeout()
                break
        else:
            _log.info("\tTEST_COMPLETE: %s", calico_task.test)
            calico_task.test.state = TestState.Complete
            calico_task.test.timeout = None
            # Need to remove the entry in test_by_slave_id
            for slave_id, test in list(self.test_by_slave_id.iteritems()):
                if test == calico_task.test:
                    del(self.test_by_slave_id[slave_id])

        # Explicitly acknowledge the update if implicit acknowledgements
        # are not being used.
        if not self.implicitAcknowledgements:
            driver.acknowledgeStatusUpdate(update)

    def offerRescinded(self, driver, offerId):
        _log.error("Offer %s was rescinded.", offerId)
        # We expect this to be rare, so a linear walk through retained offers
        # is fine.  Alternative would be a second dict, which adds too much
        # complexity.
        for slave_id, offer in self.unreserved_offers_by_slave_id.items():
            if offer.offer_id == offerId:
                del(self.unreserved_offers_by_slave_id[slave_id])
                break

    def kill_timed_out_tests(self):
        """
        Checks test health, and removes tests from the queue (kill) if
        they appear unhealthy. Also check if all tests are complete, and quit if they are.
         Tests are unhealthy on two conditions:

        1. A Started test is unhealthy if it has been greater than TEST_TIMEOUT
        seconds since any of its tasks last reported a status. This usually is
        triggered by tasks stuck in TASK_STAGING.

        2. An Unstarted test is unhealthy if no other tests have been running for TEST_TIMEOUT
        seconds and this test still refuses to start. This is condition is usually met
        when the mesos cluster does not have enough resources available or does not meet
        the minimum host conditions required by the test.

        :return:
        """
        a_test_is_running = False
        for test in self.tests:
            if test.state is TestState.Running:
                a_test_is_running = True
                break

        # No running tests?
        if not a_test_is_running:
            for test in self.tests:
                # Kill timed out tests and start timers
                if test.state is TestState.Unstarted:
                    test.start_timeout_if_not_currently_started()
                    if test.timed_out():
                        self.kill_test(test, "Timed out waiting for enough "
                                             "offers to start test")
        else:
            # There is a running test.
            for test in self.tests:
                # Remove unstarted test timers
                if test.state is TestState.Unstarted:
                    test.remove_timeout()
                # Check running test timeouts
                elif test.state is TestState.Running:
                    test.start_timeout_if_not_currently_started()
                    if test.timed_out():
                        self.kill_test(test, "Timed out waiting for task "
                                             "status update")


def get_host_ip():
    ip = subprocess.Popen('ip route get 8.8.8.8 | head -1 | cut -d\' \' -f8',
                          shell=True, stdout=subprocess.PIPE).stdout.read()
    return ip.strip()


class NotEnoughResources(Exception):
    pass


def _get_name_from_path(path):
    path = os.path.splitext(os.path.normpath(path))[0]

    _relpath = os.path.relpath(path)
    assert not os.path.isabs(_relpath), "Path must be within the project"
    assert not _relpath.startswith('..'), "Path must be within the project"

    name = _relpath.replace(os.path.sep, '.')
    return name

def find_tests():
    start_dir = "/framework/tests"
    pattern  = "test_*.py"
    paths = os.listdir(start_dir)

    tests = []
    for path in paths:
        full_path = os.path.join(start_dir, path)
        if os.path.isfile(full_path):
            if not VALID_MODULE_NAME.match(path):
                # valid Python identifiers only
                continue
            if not fnmatch(path, pattern):
                continue
            # if the test file matches, load it
            name = _get_name_from_path(full_path)
            try:
                __import__(name)
                module = sys.modules[name]
            except:
                raise Exception("Couldn't import test: %s" % name)
            else:
                for name in dir(module):
                    obj = getattr(module, name)
                    if name.startswith("test_") and hasattr(obj, "__call__"):
                        tests.append(obj())
    return tests

driver = None
if __name__ == '__main__':
    """
    Initializes framework by loading supplied tests into the scheduler and
    starting the driver.
    :param tests: Collection of TestCases.
    :return: Will sys.exit with relevant return code.
    """
    tests = find_tests()
    print tests

    master_ip = get_host_ip() + ":5050"

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "Test Framework (Python)"
    framework.principal = "test-framework-python"

    scheduler = TestScheduler(0)
    scheduler.tests = tests

    _log.info("Launching")

    driver = mesos.native.MesosSchedulerDriver(scheduler,
                                               framework,
                                               master_ip,
                                               0)
    driver.start()

    def healthchecks():
        while True:
            scheduler.kill_timed_out_tests()

            # Check if all tests are now complete
            for test in scheduler.tests:
                if test.state != TestState.Complete:
                    # At least one test is incomplete.
                    time.sleep(5)
                    break
            else:
                _log.info("All tests complete.")
                scheduler.stop_driver_and_report_results()
                return

    # Start the healthcheck thread, then wait for the driver to finish
    # (or for healthchecks to kill it)
    thread = threading.Thread(target=healthchecks)
    thread.start()
    driver.join()
    thread.join()

    if [task for task in scheduler.all_tasks() if
        task.state in BAD_TASK_STATES] == []:
        sys.exit(0)
    else:
        sys.exit(1)