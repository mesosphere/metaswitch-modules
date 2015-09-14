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
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

TASK_CPUS = 1
TASK_MEM = 128


class PingTask(object):
    def __init__(self, target, ip=None, state=None, id=None):
        self.target = target
        self.ip = ip
        self.state = state
        self.id = id


class SleepTask(object):
    def __init__(self, ip=None, state=None, id=None):
        self.ip = ip
        self.state = state
        self.id = id


class State():
    LaunchSleepTasks, LaunchPingTasks = range(0,2)


class TestScheduler(mesos.interface.Scheduler):
    def __init__(self, implicitAcknowledgements):
        self.implicitAcknowledgements = implicitAcknowledgements
        self.taskData = {}
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.messagesSent = 0
        self.messagesReceived = 0
        self.tasks = [SleepTask(ip="192.168.1.0", state=None, id=None),
                      PingTask(ip="192.168.1.1", target="192.168.1.0", state=None, id=None),
                      PingTask(ip="192.168.1.2", target="192.168.1.0", state=None, id=None)]
        self.state = State.LaunchSleepTasks


    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value


    def _generate_executor(self, tid):
        """
        Helper method to generate a new mesos executor.
        Each executor will run in its own namespace.
        """
        # Get a new executor for this task
        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = "execute Task %s" % tid
        executor.command.value = "python /framework/test_executor.py"
        executor.name = "Test Executor for Task %s" % tid
        executor.source = "python_test"

        return executor


    def _generate_task(self, tid, slave_id, calico_task):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(tid)
        calico_task.id = task.task_id.value
        task.slave_id.value = slave_id


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
        netgroup_label.value = "the_only_netgroup"

        # Isolator IP label
        ip_label = task.labels.labels.add()
        ip_label.key = "network_isolator.ip"
        ip_label.value = calico_task.ip

        if type(calico_task) == SleepTask:
            task.name = "sleeping at %s" % calico_task.ip
            task_type_label = task.labels.labels.add()
            task_type_label.key = "task_type"
            task_type_label.value = "sleep"
        elif type(calico_task) == PingTask:
            task.name = "pinging %s from %s" % (calico_task.target, calico_task.ip)
            task_type_label = task.labels.labels.add()
            task_type_label.key = "task_type"
            task_type_label.value = "ping"

            # Add the target label
            target_label = task.labels.labels.add()
            target_label.key = "target"
            target_label.value = calico_task.target

        return task


    def resourceOffers(self, driver, offers):
        """
        Fill up offers with all sleep tasks
        """
        # Calculate how juicy these offers are
        availableCpus = 0
        availableMem = 0
        for offer in offers:
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value

            print "Received offer %s with cpus: %s and mem: %s" \
                  % (offer.id.value, offerCpus, offerMem)

            availableCpus += offerCpus
            availableMem += offerMem

        if self.state == State.LaunchSleepTasks:
            print "Got offer during sleep task phase"
            launch_type = SleepTask
        elif self.state == State.LaunchPingTasks:
            print "Got offer during ping task phase"
            launch_type = PingTask

        # Get all tasks pending launch
        launch_tasks = [calico_task for calico_task in self.tasks if \
                        type(calico_task) == launch_type and \
                        calico_task.state is None]
        # If there's not task pending launch, reject the offers - we're waiting for them to come up
        if not launch_tasks:
            print "All tasks launched. Rejecting offer"
            for offer in offers:
                driver.declineOffer(offer.id)
            return
        else:
            prepared_tasks = []
            for calico_task in launch_tasks:
                if availableCpus >= TASK_CPUS and availableMem >= TASK_MEM:
                    tid = self.tasksLaunched
                    self.tasksLaunched += 1

                    executor = self._generate_executor("execute %s" % tid)

                    # TODO: not sure if this is coshire
                    calico_task.state = mesos_pb2.TASK_STAGING

                    print "Launching SLEEP task %d using offer %s" \
                              % (tid, offer.id.value)

                    mesos_task = self._generate_task(tid=tid,
                                              slave_id=offer.slave_id.value,
                                              calico_task = calico_task)

                    mesos_task.executor.MergeFrom(executor)

                    prepared_tasks.append(mesos_task)

                    # TODO: deprecate this
                    self.taskData[mesos_task.task_id.value] = (
                        offer.slave_id, mesos_task.executor.executor_id)

                    availableCpus -= TASK_CPUS
                    availableMem -= TASK_MEM
            if prepared_tasks:
                operation = mesos_pb2.Offer.Operation()
                operation.type = mesos_pb2.Offer.Operation.LAUNCH
                operation.launch.task_infos.extend(prepared_tasks)
                # driver.acceptOffers([offer.id for offer in offers], [operation])
                driver.acceptOffers([offers.pop().id], [operation])


    def statusUpdate(self, driver, update):
        """
        Run when the Executor sends a status update back to the Framework
        """
        task_id = update.task_id.value
        state = update.state

        print "Task %s is in state %s" % \
            (update.task_id.value, mesos_pb2.TaskState.Name(update.state))

        # Find the task which corresponds to the status update

        for task in self.tasks:
            if task.id == task_id:
                task.state = state
                break


        if self.state == State.LaunchSleepTasks:
            if task.state == mesos_pb2.TASK_RUNNING:
                # Received a RUNNING update from one of the launching SleepTasks
                # Check if all sleep tasks are running
                unfinished_sleep_tasks = [task for task in self.tasks if type(task) == SleepTask and task.state != mesos_pb2.TASK_RUNNING]
                if unfinished_sleep_tasks:
                    print 'Waiting for the remaining %s' % len(unfinished_sleep_tasks)
                else:
                    print 'All sleep tasks running. Moving on to Ping Launch'
                    # Move all ping tasks into the task queue
                    self.task_queue = [task for task in self.tasks if type(task) == PingTask]
                    self.state = State.LaunchPingTasks
            else:
                print 'Killing framework' % state
                driver.abort()

        elif self.state == State.LaunchPingTasks:
            if task.state == mesos_pb2.TASK_RUNNING:
                pass
            elif task.state == mesos_pb2.TASK_FAILED:
                print "Ping from %s to %s failed" % (task.ip, task.target)
                driver.abort()
            elif task.state == mesos_pb2.TASK_FINISHED:
                print "Ping task complete!"
                # Check if all ping tasks are complete
                unfinished_ping_tasks = [task for task in self.tasks if type(task) == PingTask and task.state != mesos_pb2.TASK_RUNNING]
                if unfinished_ping_tasks:
                    print 'Waiting for the remaining %s' % len(unfinished_ping_tasks)
                else:
                    print 'All ping tests finished succesfully. SUCCCESS!!!!'
            else:
                print 'unexpected state: %s' % state
                driver.abort()


        print "Task %s is in state %s" % \
            (update.task_id.value, mesos_pb2.TaskState.Name(update.state))

        # Ensure the binary data came through.
        if update.data != "data with a \0 byte":
            print "The update data did not match!"
            print "  Expected: 'data with a \\x00 byte'"
            print "  Actual:  ", repr(str(update.data))
            sys.exit(1)

        if update.state == mesos_pb2.TASK_FINISHED:
            self.tasksFinished += 1
            if self.tasksFinished == len(self.tasks):
                print "All %s tasks done, waiting for final framework message" % self.tasksFinished

            slave_id, executor_id = self.taskData[update.task_id.value]

            self.messagesSent += 1
            driver.sendFrameworkMessage(
                executor_id,
                slave_id,
                'data with a \0 byte')

        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_FAILED:
            print "Aborting because task %s is in unexpected state %s with message '%s'" \
                % (update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message)
            driver.abort()

        # Explicitly acknowledge the update if implicit acknowledgements
        # are not being used.
        if not self.implicitAcknowledgements:
            driver.acknowledgeStatusUpdate(update)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.messagesReceived += 1

        # The message bounced back as expected.
        if message != "data with a \0 byte":
            print "The returned message data did not match!"
            print "  Expected: 'data with a \\x00 byte'"
            print "  Actual:  ", repr(str(message))
            sys.exit(1)
        print "Received message:", repr(str(message))

        if self.messagesReceived == len(self.tasks):
            if self.messagesReceived != self.messagesSent:
                print "Sent", self.messagesSent,
                print "but received", self.messagesReceived
                sys.exit(1)
            print "All tasks done, and all messages received, exiting"
            driver.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "Test Framework (Python)"

    # TODO(vinod): Make checkpointing the default when it is default
    # on the slave.
    if os.getenv("MESOS_CHECKPOINT"):
        print "Enabling checkpoint for the framework"
        framework.checkpoint = True

    implicitAcknowledgements = 1
    if os.getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS"):
        print "Enabling explicit status update acknowledgements"
        implicitAcknowledgements = 0

    if os.getenv("MESOS_AUTHENTICATE"):
        print "Enabling authentication for the framework"

        if not os.getenv("DEFAULT_PRINCIPAL"):
            print "Expecting authentication principal in the environment"
            sys.exit(1);

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
