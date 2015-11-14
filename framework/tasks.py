import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from constants import BAD_TASK_STATES, UNFINISHED_TASK_STATES
from constants import TASK_CPUS, TASK_MEM


class TaskUpdateError(Exception):
    """
    There was a problem with a TaskUpdate.
    """
    pass


class Task(object):
    def __init__(self,
                 requested_ips=None,
                 netgroups=None,
                 slave=None,
                 calico=True,
                 default_executor=False,
                 *args, **kwargs):
        if requested_ips:
            assert calico, "Must use Calico Networking if spawning task " \
                           "with specific IPs"
            self.requested_ips = requested_ips
        else:
            self.requested_ips = []

        if not calico:
            assert "auto_ipv4" not in kwargs, "Must use Calico Networking if" \
                                              " spawning task with " \
                                              "per-container IPs."
            assert "auto_ipv6" not in kwargs, "Must use Calico Networking if" \
                                              " spawning task with " \
                                              "per-container IPs."
            self.auto_ipv6 = 0
            self.auto_ipv4 = 0
        else:
            # Default to a single IPv4 address if not explicitly specified.
            self.auto_ipv4 = kwargs.get("auto_ipv4", 1)
            self.auto_ipv6 = kwargs.get("auto_ipv6", 0)

        if netgroups:
            assert calico, "Can't specify netgroups unless " \
                           "using Calico Networking"
            assert type(netgroups) == list, "Must specify a list of netgroups"
            self.netgroups = netgroups
        else:
            self.netgroups = []

        self.slave = slave
        self.state = None
        self.task_id = None
        self.executor_id = None
        self.slave_id = None
        self.calico = calico
        self.default_executor = default_executor
        self.ip_addresses = []

    def as_new_mesos_task(self):
        """
        Take the information stored in this Task object and fill a
        mesos task.
        """
        assert self.task_id, "Calico task must be assigned a task_id"
        assert self.slave_id, "Calico task must be assigned a slave_id"

        task = mesos_pb2.TaskInfo()
        task.name = repr(self)
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

        if not self.default_executor:
            executor = mesos_pb2.ExecutorInfo()
            executor.executor_id.value = "execute Task %s" % self.task_id
            executor.command.value = "python %s" % self.executor_script
            executor.name = "Test Executor for Task %s" % self.task_id
            executor.source = "python_test"
            executor.container.type = mesos_pb2.ContainerInfo.MESOS
            task.executor.MergeFrom(executor)
            self.executor_id = executor.executor_id.value
        else:
            task.container.type = mesos_pb2.ContainerInfo.MESOS

        if self.calico:
            if not self.default_executor:
                network_info = task.executor.container.network_infos.add()
            else:
                network_info = task.container.network_infos.add()

            for netgroup in self.netgroups:
                network_info.groups.append(netgroup)

            for ip in self.requested_ips:
                network_info.ip_addresses.add().ip_address = ip

            for _ in range(self.auto_ipv4):
                network_info.ip_addresses.add().protocol = \
                    mesos_pb2.NetworkInfo.IPv4
            for _ in range(self.auto_ipv6):
                network_info.ip_addresses.add().protocol = \
                    mesos_pb2.NetworkInfo.IPv4

        return task

    def process_update(self, update):

        # Update the calico_task with info from TaskInfo
        self.state = update.state

        assert len(update.container_status.network_infos) == 1
        network_info = update.container_status.network_infos[0]
        actual_ips = [ipa.ip_address for ipa in network_info.ip_addresses]

        # Check requested IPs.
        for ip in self.requested_ips:
            if ip not in actual_ips:
                raise TaskUpdateError("Requested IP %s wasn't assigned." % ip)

        # Check total number of IPs.
        if self.calico:
            expected = (len(self.requested_ips) +
                        self.auto_ipv4 +
                        self.auto_ipv6)
            if len(actual_ips) != expected:
                raise TaskUpdateError(
                    "Expected %d static, %d IPv4, and %d IPv6 but got %s." %
                    (len(self.requested_ips),
                     self.auto_ipv4,
                     self.auto_ipv6,
                     actual_ips))

        # If we already have IPs, check they haven't changed.
        if self.ip_addresses and self.ip_addresses != actual_ips:
            raise TaskUpdateError("Previously got %s IPs, not have %s IPs." %
                                  (self.ip_addresses, actual_ips))

        # If we make it this far, no error.
        self.ip_addresses = actual_ips

    @property
    def dependencies_are_met(self):
        raise NotImplementedError

    @property
    def ip(self):
        if self.ip_addresses:
            return self.ip_addresses[0]
        else:
            return None


class PingTask(Task):
    """
    Subclass of Task which attempts to ping a target.

    Pass in a collection of can_ping_targets or cant_ping_targets, and
    this PingTask will fail accordingly.

    Since it can target multiple tasks, results reported by the Executor
    are stored in ping_status_data, so individual pings can be checked.

    """
    executor_script = "/framework/calico_executor.py ping_task"

    def __init__(self, can_ping_targets=[], cant_ping_targets=[],
                 *args, **kwargs):
        """
        Initializer for a Ping task.

        :param can_ping_targets: A list of tasks which should be reachable
        :param cant_ping_targets: A list of tasks which should be unreachable
        """
        super(PingTask, self).__init__(*args, **kwargs)
        assert can_ping_targets or cant_ping_targets, "Must provide can/t " \
                                                      "ping targets."
        self.can_ping_targets = can_ping_targets
        self.cant_ping_targets = cant_ping_targets

        self.ping_status_data = {}
        """
        A dictionary to keep track of individual target results, where the Key
        is the targeted IP, and the Value is boolean representing success or
        failure.

        'True' indicates a success, which either means a can_ping_task was
        pingable, or a cant_ping_task wasn't.
        """

    def __repr__(self):
        task_description = "PingTask(from=%s" % self.ip
        if self.can_ping_targets:
            task_description += ", can_ping=%s" % self.can_ping_targets
        if self.cant_ping_targets:
            task_description += ", cant_ping=%s" % self.cant_ping_targets
        if self.netgroups:
            task_description += ", netgroups=%s" % self.netgroups

        return task_description + ")"

    def as_new_mesos_task(self):
        task = super(PingTask, self).as_new_mesos_task()
        ping_ips = []
        for target in self.can_ping_targets:
            ping_ips.extend(target.ip_addresses)
        cant_ping_ips = []
        for target in self.cant_ping_targets:
            cant_ping_ips.extend(target.ip_addresses)
        if not self.default_executor:
            task_type_label = task.labels.labels.add()
            task_type_label.key = "task_type"
            task_type_label.value = "ping"

            can_ping_label = task.labels.labels.add()
            can_ping_label.key = "can_ping"
            can_ping_label.value = ",".join(ping_ips)

            cant_ping_label = task.labels.labels.add()
            cant_ping_label.key = "cant_ping"
            cant_ping_label.value = ",".join(cant_ping_ips)
        else:
            command = " && ".join(["ping -c 1 %s" % ip for ip in ping_ips])
            task.command.value = command
        return task

    @property
    def dependencies_are_met(self):
        """
        Checks if all this tasks' ping targets are up and running
        :return:
        """
        if self.state is not None:
            raise Exception("PingTask has already been started")
        for task in self.can_ping_targets + self.cant_ping_targets:
            if task.state != mesos_pb2.TASK_RUNNING:
                return False
        return True


class NetcatListenTask(Task):
    executor_script = "/framework/calico_executor.py netcat_listen"

    def __init__(self, *args, **kwargs):
        super(NetcatListenTask, self).__init__(calico=False, *args, **kwargs)
        self.port = None

    def __repr__(self):
        """
        Give a nice-name to identify the task
        """
        return "NetcatSleepTask(id=%s, port=%s)" % (self.task_id, self.port)

    def as_new_mesos_task(self):
        assert self.port, "netcat listen must be assigned an address before launching"
        task = super(NetcatListenTask, self).as_new_mesos_task()

        port_ranges = task.resources.add()
        port_ranges.name = "ports"
        port_ranges.type = mesos_pb2.Value.RANGES
        port_range = port_ranges.ranges.range.add()
        port_range.begin = self.port
        port_range.end = self.port

        if self.default_executor:
            task.command.value = "nc -l 0.0.0.0 %s & sleep 10; ! kill -0 $!" % self.port

        return task

    @property
    def dependencies_are_met(self):
        return self.state is None


class NetcatSendTask(Task):
    executor_script = "/framework/calico_executor.py netcat_send"

    def __init__(self, can_cat_targets=[], *args, **kwargs):
        super(NetcatSendTask, self).__init__(calico=False, *args, **kwargs)
        assert can_cat_targets, "Must provide can/t cat targets."
        self.can_cat_targets = can_cat_targets

        self.ping_status_data = {}

    def __repr__(self):
        task_description = "NetcatSend(from=%s" % self.ip
        if self.can_cat_targets:
            task_description += ", can_ping=%s" % self.can_cat_targets

        return task_description + ")"

    def as_new_mesos_task(self):
        """
        Extends the basic mesos task settings by adding  a custom label called
        "task_type" which the executor will read to identify the task type.
        """
        assert self.port, "netcat listen must be assigned a port " \
                          "before launching"
        task = super(NetcatSendTask, self).as_new_mesos_task()

        can_cat_label = task.labels.labels.add()
        can_cat_label.key = "can_cat"
        target_strs = []
        for target in self.can_cat_targets:
            for ip in target.ip_addresses:
                target_strs.append(" ".join([ip, str(target.port)]))
        can_cat_label.value = ",".join(target_strs)

        if self.default_executor:
            shell_cmds = ["printf hi | nc %s" % target_str
                          for target_str in target_strs]
            task.command.value = " && ".join(shell_cmds)
        return task

    @property
    def dependencies_are_met(self):
        if self.state is not None:
            return False
        for task in self.can_cat_targets:
            if task.state != mesos_pb2.TASK_RUNNING:
                return False
        return True


class SleepTask(Task):
    executor_script = "/framework/calico_executor.py sleep_task"

    def __repr__(self):
        task_description = "ListenTask(id=%s, ip=%s" % (self.task_id, self.ip)

        if self.netgroups:
            task_description += ", netgroups=%s" % self.netgroups
        return task_description + ")"

    def as_new_mesos_task(self):
        """
        Extends the basic mesos task settings by adding  a custom label called "task_type" which the executor will
        read to identify the task type.
        """
        task = super(SleepTask, self).as_new_mesos_task()

        if not self.default_executor:
            task_type_label = task.labels.labels.add()
            task_type_label.key = "task_type"
            task_type_label.value = "sleep"
        else:
            task.command.value = "sleep 20"

        return task

    @property
    def dependencies_are_met(self):
        return self.state is None
