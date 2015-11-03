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
import json
import os
import time
import logging
import logging.handlers
import errno
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native



def _setup_logging(logfile):
    _log = logging.getLogger(__name__)

    # Ensure directory exists.
    try:
        os.makedirs(os.path.dirname(logfile))
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

    return _log


_log = _setup_logging('/var/log/calico/calico_executor.log')


class ExecutorTask(object):
    def __init__(self, task):
        self.id = task.task_id.value
        self.labels = {label.key: label.value for label in task.labels.labels}
        for resource in task.resources:
            if resource.name == "ports":
                port_range = resource.ranges.range[:1].pop()
                self.port = port_range.begin

    def send_update(self, state, message=None, data=None):
        update = mesos_pb2.TaskStatus()
        update.task_id.value = self.id
        update.state = state
        update.message = message or ''
        update.data = data or ''
        if state in [mesos_pb2.TASK_FAILED,
                     mesos_pb2.TASK_ERROR,
                     mesos_pb2.TASK_LOST]:
            update.healthy = False
        else:
            update.healthy = True
        driver.sendStatusUpdate(update)

    def run_pre_task(self):
        print "Running task %s" % self.id
        self.send_update(mesos_pb2.TASK_RUNNING)

        print subprocess.check_output(["ip", "addr"])

    def run_task(self):
        raise NotImplementedError

    def start(self):
        try:
            self.run_pre_task()
        except Exception as e:
            self.send_update(mesos_pb2.TASK_ERROR, message=str(e))
            return
        else:
            try:
                data = self.run_task()
            except Exception as e:
                self.send_update(mesos_pb2.TASK_FAILED, message=str(e))
            else:
                self.send_update(mesos_pb2.TASK_FINISHED, data=data)


class ExecutorPingTask(ExecutorTask):
    def run_task(self):
        results = {}
        can_ping_targets = self.labels["can_ping"]
        if can_ping_targets:
            targets = can_ping_targets.split(",")
            for target in targets:
                command = "ping -c 1 %s" % target
                try:
                    subprocess.check_call(command, shell=True)
                except subprocess.CalledProcessError:
                    results[target] = False
                else:
                    results[target] = True

        cant_ping_targets = self.labels["cant_ping"]
        if cant_ping_targets:
            targets = cant_ping_targets.split(",")
            for target in targets:
                command = "! ping -c 1 -w 1 %s" % target
                try:
                    subprocess.check_call(command, shell=True)
                except subprocess.CalledProcessError:
                    results[target] = False
                else:
                    results[target] = True

        return json.dumps(results)


class ExecutorSleepTask(ExecutorTask):
    def run_task(self):
        command = ["sleep", "25"]
        subprocess.check_call(command)


class ExecutorNetcatListener(ExecutorTask):
    def run_task(self):
        command = ["nc", "-l", "0.0.0.0", str(self.port)]
        print command
        p = subprocess.Popen(command)
        time.sleep(25)
        results = p.poll()
        if results == 1:
            raise Exception("Tak error")
        elif results == 0:
            pass
        else:
            raise Exception("Never received connection")


class ExecutorNetcatSender(ExecutorTask):
    def run_task(self):
        results = {}
        can_cat_targets = self.labels["can_cat"]
        if can_cat_targets:
            targets = can_cat_targets.split(",")
            for target in targets:
                command = "printf hi | nc %s" % target
                print command
                try:
                    subprocess.check_call(command, shell=True)
                except subprocess.CalledProcessError:
                    results[target] = False
                else:
                    results[target] = True

        return json.dumps(results)


class Executor(mesos.interface.Executor):
    def __init__(self, task_executor):
        self.task_executor = task_executor
        super(Executor, self).__init__()

    def launchTask(self, driver, task):
        # Create a thread to run the task
        # print task['resources']

        thread = threading.Thread(target=self.task_executor(task).start)
        thread.start()

    def frameworkMessage(self, driver, message):
        """
        Respond to messages sent from the Framework.

        In this case, we'll just echo the message back.
        """
        driver.sendFrameworkMessage(message)


if __name__ == "__main__":
    print "Starting executor for %s" % sys.argv[1]

    task_type = sys.argv[1]
    if task_type == 'ping_task':
        task_executor = ExecutorPingTask
    elif task_type == 'sleep_task':
        task_executor = ExecutorSleepTask
    elif task_type == 'netcat_listen':
        task_executor = ExecutorNetcatListener
    elif task_type == 'netcat_send':
        task_executor = ExecutorNetcatSender

    driver = mesos.native.MesosExecutorDriver(Executor(task_executor))
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
