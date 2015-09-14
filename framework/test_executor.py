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

import sys
import threading
import subprocess

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

class MyExecutor(mesos.interface.Executor):
    def launchTask(self, driver, task):
        # Create a thread to run the task
        def run_task():
            print "Running task %s" % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            update.data = 'data with a \0 byte'
            driver.sendStatusUpdate(update)

            # This is where one would perform the requested task.
            labels = {label.key: label.value for label in task.labels.labels}
            task_type = labels["task_type"]

            if task_type == 'ping':
                target = labels["target"]
                try:
                    # TODO: Wait for framework to report that the target is up before
                    # firing the ping
                    print subprocess.check_output(["ifconfig"])
                    print "Pinging %s" % target
                    subprocess.check_call(["ping", "-c", "1", target])
                except subprocess.CalledProcessError:
                    print "Couldn't ping %s" % target
                    print "Sending FAIL update..."
                    update = mesos_pb2.TaskStatus()
                    update.task_id.value = task.task_id.value
                    update.state = mesos_pb2.TASK_FAILED
                    update.message = "I can't even"
                    update.data = 'data with a \0 byte'
                    driver.sendStatusUpdate(update)
                    print "Sent status update"
                else:
                    # Send success
                    print "Sending SUCCESS update..."
                    update = mesos_pb2.TaskStatus()
                    update.task_id.value = task.task_id.value
                    update.state = mesos_pb2.TASK_FINISHED
                    update.data = 'data with a \0 byte'
                    driver.sendStatusUpdate(update)
                    print "Sent status update"


            elif task_type == 'sleep':
                try:
                    # TODO: Wait for framework to report that the target is up before
                    # firing the ping
                    print subprocess.check_output(["ifconfig"])
                    subprocess.check_call(["sleep", "10"])
                except subprocess.CalledProcessError:
                    print "Sending FAIL update..."
                    update = mesos_pb2.TaskStatus()
                    update.task_id.value = task.task_id.value
                    update.state = mesos_pb2.TASK_FINISHED
                    update.message = "I can't even"
                    update.data = 'data with a \0 byte'
                    driver.sendStatusUpdate(update)
                    print "Sent status update"
                else:
                    # Send success
                    print "Sending SUCCESS update..."
                    update = mesos_pb2.TaskStatus()
                    update.task_id.value = task.task_id.value
                    update.state = mesos_pb2.TASK_FINISHED
                    update.data = 'data with a \0 byte'
                    driver.sendStatusUpdate(update)
                    print "Sent status update"

        thread = threading.Thread(target=run_task)
        thread.start()

    def frameworkMessage(self, driver, message):
        """
        Respond to messages sent from the Framework.

        In this case, we'll just echo the message back.
        """
        driver.sendFrameworkMessage(message)

if __name__ == "__main__":
    print "Starting executor"
    driver = mesos.native.MesosExecutorDriver(MyExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
