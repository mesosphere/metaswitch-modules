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

import os
import sys
import errno
import logging
import logging.handlers
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native


TASK_CPUS = 1
TASK_MEM = 128
LOGFILE = '/var/log/calico/test_framework.log'

BAD_TASK_STATES = [mesos_pb2.TASK_LOST, mesos_pb2.TASK_KILLED, mesos_pb2.TASK_FAILED]


_log = logging.getLogger("TestFramework")

def _setup_logging(logfile):
    # Ensure directory exists.
    try:
        os.makedirs(os.path.dirname(LOGFILE))
    except OSError as oserr:
        if oserr.errno != errno.EEXIST:
            raise

    _log.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] %(name)s %(lineno)d: %(message)s')
    handler = logging.handlers.TimedRotatingFileHandler(logfile,
                                                        when='D',
                                                        backupCount=10)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    _log.addHandler(handler)

    # Create Console Logger
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    _log.addHandler(handler)


class Task(object):
    """
    Base Class for storing data about a Mesos task.
    Implementations of tasks should superclass this class.
    """
    def __init__(self, ip=None, netgroup="default"):
        self.ip = ip
        self.netgroup = netgroup
        self.state = None
        self.task_id = None
        self.executor_id = None
        self.task_id = None

    def as_new_mesos_task(self):
        """
        Create a new mesos task populated by the data currently stored in
        this Task.
        """
        assert self.task_id, "Calico task must be assigned a task_id"
        assert self.slave_id, "Calico task must be assigned a slave_id"

        def _generate_executor(self):
            """
            Helper method to generate a new mesos executor.
            Each executor will run in its own namespace.
            """
            # Get a new executor for this task
            executor = mesos_pb2.ExecutorInfo()
            executor.executor_id.value = "execute Task %s" % self.task_id
            executor.command.value = "python /framework/test_executor.py"
            executor.name = "Test Executor for Task %s" % self.task_id
            executor.source = "python_test"

            return executor

        task = mesos_pb2.TaskInfo()
        task.name = "unnamed task"
        task.task_id.value = self.task_id
        task.slave_id.value = self.slave_id

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM

        # Netgroup label
        netgroup_label = task.labels.labels.add()
        netgroup_label.key = "network_isolator.netgroups"
        netgroup_label.value = self.netgroup

        # Isolator IP label
        if self.ip:
            ip_label = task.labels.labels.add()
            ip_label.key = "network_isolator.ip"
            ip_label.value = self.ip

        # create the executor
        task.executor.MergeFrom(_generate_executor(self))
        self.executor_id = task.executor.executor_id.value

        return task


class PingTask(Task):
    """
    Subclass of Task which attempts to ping a target.
    This Task will report a failure if it cannot ping the target.
    """
    def __init__(self, target, *args, **kwargs):
        super(PingTask, self).__init__(*args, **kwargs)
        self.target = target

    def as_new_mesos_task(self):
        """
        Extends the basic mesos task settings by adding the target field,
        as well as a custom label called "task_type" which the executor will
        read to identify the task type.
        """
        task = super(PingTask, self).as_new_mesos_task()
        task.name = repr(self)

        task_type_label = task.labels.labels.add()
        task_type_label.key = "task_type"
        task_type_label.value = "ping"

        target_label = task.labels.labels.add()
        target_label.key = "target"
        target_label.value = self.target.ip

        return task


    def __repr__(self):
        """
        Give a nice-name to identify the task
        """
        return "PingTask(from=%s, to=%s)" % (self.ip, self.target.ip)


class CantPingTask(Task):
    """
    Subclass of Task which will succeed if it _cannot_ ping the target.
    This Task asserts that targets which should be unreachable are in fact
    unreachable.
    """
    def __init__(self, target, *args, **kwargs):
        super(CantPingTask, self).__init__(*args, **kwargs)
        self.target = target

    def as_new_mesos_task(self):
        """
        Extends the basic mesos task settings by adding the target field,
        as well as a custom label called "task_type" which the executor will
        read to identify the task type.
        """
        task = super(CantPingTask, self).as_new_mesos_task()
        task.name = repr(self)

        task_type_label = task.labels.labels.add()
        task_type_label.key = "task_type"
        task_type_label.value = "cant_ping"

        target_label = task.labels.labels.add()
        target_label.key = "target"
        target_label.value = self.target.ip

        return task

    def __repr__(self):
        """
        Give a nice-name to identify the task
        """
        return "CantPing(from=%s, to=%s)" % (self.ip, self.target.ip)


class SleepTask(Task):
    def as_new_mesos_task(self):
        """
        Extends the basic mesos task settings by adding  a custom label called "task_type" which the executor will
        read to identify the task type.
        """
        task = super(SleepTask, self).as_new_mesos_task()
        task.name = repr(self)

        task_type_label = task.labels.labels.add()
        task_type_label.key = "task_type"
        task_type_label.value = "sleep"

        return task

    def __repr__(self):
        """
        Give a nice-name to identify the task
        """
        return "ListenTask(ip=%s)" % self.ip


class TestScheduler(mesos.interface.Scheduler):
    """
    This sample Scheduler implements a Mesos Framework powered by Calico.
    It performs the following steps:

    1. Launch all SleepTasks
    2. Wait for SleepTasks to report as RUNNING
    3. Launch all Cant/PingTasks
    4. Wait for all Cant/PingTasks to FINISH

    It will report failures during each step appropriately.
    """
    def __init__(self, implicitAcknowledgements):
        self.implicitAcknowledgements = implicitAcknowledgements
        """
        Flag to disable the requirement that the Executor responds to ACK messages
        """

        self.tasks_launched = 0
        """
        Counter to track number of tasks launched
        """

        self.framework_acks_sent = 0
        """
        Running total of how many acks have been sent to the executor
        """

        self.framework_acks_received = 0
        """
        Running total of how many acks have been received by the executor
        """

        sleep_task = SleepTask()
        self.tasks = [sleep_task,
                      PingTask(target=sleep_task),
                      PingTask(ip="192.168.1.2", target=sleep_task),
                      CantPingTask(ip="192.168.1.3", netgroup="netgroup_b", target=sleep_task)]
        """
        The source-of-truth for task information. Whenever the framework receives
        an update or modifies configuration of tasks in mesos in any way, it should
        immediately update the information stored here.
        """

    @property
    def num_tasks_finished(self):
        """
        Counter to track number of completed tasks.
        """
        return len([task for task in self.tasks if task.state is mesos_pb2.TASK_FINISHED])

    def registered(self, driver, frameworkId, masterInfo):
        """
        Callback used when the framework is succesfully registered.
        """
        _log.info("REGISTERED: with framework ID %s", frameworkId.value)

    def _calculate_offer(self, offer):
        """
        Calculates how much cpu / memory is available in an offer
        """
        availableCpus = 0
        availableMem = 0
        offerCpus = 0
        offerMem = 0
        for resource in offer.resources:
            if resource.name == "cpus":
                offerCpus += resource.scalar.value
            elif resource.name == "mem":
                offerMem += resource.scalar.value

        _log.debug("\tReceived Offer %s with cpus: %s and mem: %s",
                   offer.id.value, offerCpus, offerMem)

        availableCpus += offerCpus
        availableMem += offerMem
        return availableCpus, availableMem

    def _get_next_launch_task(self):
        """
        Returns the next task that is ready for launch depending on the
        framework's current state, as well as the tasks' states.
        """
        pending_sleep_tasks = [calico_task for calico_task in self.tasks if \
                               type(calico_task) is SleepTask and \
                               calico_task.state is None]

        pending_ping_tasks = [calico_task for calico_task in self.tasks if \
                              type(calico_task) in [PingTask, CantPingTask] and \
                              calico_task.target.state is mesos_pb2.TASK_RUNNING and \
                              calico_task.state is None]

        launch_tasks = pending_sleep_tasks + pending_ping_tasks
        if launch_tasks:
            return launch_tasks.pop()
        else:
            return None

    def resourceOffers(self, driver, offers):
        """
        Triggered when the framework is offered resources by mesos.
        This launches pending tasks, which are determined by
        the current self.state of the framework.
        """
        _log.info("RECEIVED_OFFER")

        # Check if there's any tasks queued for launch
        if not self._get_next_launch_task():
            _log.info("No tasks queued. Rejecting offer")
            for offer in offers:
                driver.declineOffer(offer.id)
        else:
            prepared_tasks = []
            for offer in offers:
                if not self._get_next_launch_task():
                    _log.info("Declining offer: %s", offer.id.value)
                    driver.declineOffer(offer.id)
                else:
                    operation = mesos_pb2.Offer.Operation()
                    operation.type = mesos_pb2.Offer.Operation.LAUNCH

                    offer_cpus_remaining, offer_mem_remaining = self._calculate_offer(offer)
                    while offer_cpus_remaining >= TASK_CPUS and offer_mem_remaining >= TASK_MEM and \
                            self._get_next_launch_task():
                        calico_task = self._get_next_launch_task()
                        # Get the slave_id from one of the offers, they should all be the same
                        slave_id = offer.slave_id.value
                        # loop through calico_tasks, prepare as many as possible for launch
                        # as mesos_tasks until we run out of resources within these offers

                        self.tasks_launched += 1
                        calico_task.task_id = str(self.tasks_launched)
                        calico_task.slave_id = slave_id
                        calico_task.state = mesos_pb2.TASK_STAGING

                        _log.info("\tLaunching Task %s (%s)", calico_task.task_id, calico_task)
                        _log.debug("\t Using offer %s", offer.id.value)

                        operation.launch.task_infos.extend([calico_task.as_new_mesos_task()])

                        offer_cpus_remaining -= TASK_CPUS
                        offer_mem_remaining -= TASK_MEM

                    driver.acceptOffers([offer.id], [operation])

    def statusUpdate(self, driver, update):
        """
        Triggered when the Framework receives a task Status Update from the Executor

        First, we'll check the data payload to ensure the update came from the
        executor in tact.
        Then, we'll update the corresponding task in self.tasks
        Then we'll take appropriate action based on the type of update received, as
        well as the current state of the framework.
        Lastly, we do general all-case actions, such as Executor ACKS
        """
        # Find the task which corresponds to the status update
        task_search = [task for task in self.tasks if task.task_id == update.task_id.value]
        if len(task_search) == 1:
            calico_task = task_search.pop()
            calico_task.state = update.state
            if calico_task.ip:
                # An update came from a task which already has an IP.
                # Confirm that the IP is as expected
                if calico_task.ip != str(update.data):
                    _log.error("Task came up with unexpected IP")
                    driver.abort()
            else:
                # An IPAM'd task is running, and is providing its IP
                calico_task.ip = str(update.data)
        else:
            _log.error("Received Task Update from Unidentified TaskID: %s", update.task_id.value)
            driver.abort()

        # Report the update
        _log.info("TASK_UPDATE: Task %s (%s) is in state %s", \
            calico_task.task_id,
             calico_task,
             mesos_pb2.TaskState.Name(calico_task.state))

        # Check for bad update
        if update.state in BAD_TASK_STATES:
            _log.error("Task %s (%s) is in unexpected state %s with message '%s'",
                       update.task_id.value,
                       calico_task,
                       mesos_pb2.TaskState.Name(update.state),
                       update.message)
            driver.abort()

        # On each TASK_FINISHED, send an ACK to the framework
        if update.state == mesos_pb2.TASK_FINISHED:
            if self.num_tasks_finished == len(self.tasks):
                _log.info("All tasks finished!")

            self.framework_acks_sent += 1
            _log.info("Sending ACK to Executor")
            driver.sendFrameworkMessage(
                mesos_pb2.ExecutorID(value=calico_task.executor_id),
                mesos_pb2.SlaveID(value=calico_task.slave_id),
                'data with a \0 byte')

        # Explicitly acknowledge the update if implicit acknowledgements
        # are not being used.
        if not self.implicitAcknowledgements:
            driver.acknowledgeStatusUpdate(update)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        _log.info("Received ACK from Executor")
        self.framework_acks_received += 1

        # The message bounced back as expected.
        if message != "data with a \0 byte":
            _log.error("Executor ACK contains unexpected data")
            sys.exit(1)

        if self.framework_acks_received == len(self.tasks):
            if self.framework_acks_received != self.framework_acks_sent:
                _log.info("Sent %s", self.framework_acks_sent)
                _log.info("but received %s", self.framework_acks_received)
                driver.abort()
            _log.info("All tasks done, and all messages received, exiting")
            driver.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    _setup_logging(LOGFILE)

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "Test Framework (Python)"

    # TODO(vinod): Make checkpointing the default when it is default
    # on the slave.
    if os.getenv("MESOS_CHECKPOINT"):
        _log.info("Enabling checkpoint for the framework")
        framework.checkpoint = True

    implicitAcknowledgements = 1
    if os.getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS"):
        _log.info("Enabling explicit status update acknowledgements")
        implicitAcknowledgements = 0

    if os.getenv("MESOS_AUTHENTICATE"):
        _log.info("Enabling authentication for the framework")

        if not os.getenv("DEFAULT_PRINCIPAL"):
            _log.info("Expecting authentication principal in the environment")
            sys.exit(1)

        credential = mesos_pb2.Credential()
        credential.principal = os.getenv("DEFAULT_PRINCIPAL")

        if os.getenv("DEFAULT_SECRET"):
            credential.secret = os.getenv("DEFAULT_SECRET")

        framework.principal = os.getenv("DEFAULT_PRINCIPAL")

        driver = mesos.native.MesosSchedulerDriver(
            TestScheduler(implicitAcknowledgements),
            framework,
            sys.argv[1],
            implicitAcknowledgements,
            credential)
    else:
        framework.principal = "test-framework-python"

        driver = mesos.native.MesosSchedulerDriver(
            TestScheduler(implicitAcknowledgements),
            framework,
            sys.argv[1],
            implicitAcknowledgements)

    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    # Ensure that the driver process terminates.
    driver.stop();

    sys.exit(status)
