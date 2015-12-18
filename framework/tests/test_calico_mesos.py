from calico_framework import TestCase
from tasks import (SleepTask,
                   PingTask,
                   NetcatListenTask,
                   NetcatSendTask)

def test_same_host_netgroups_can_ping():
    test_name = "Same-Host Same-Netgroups Can Ping"
    sleep_task = SleepTask(netgroups=['netgroup_a'], slave=0)
    ping_task = PingTask(netgroups=['netgroup_a'], slave=0,
                         can_ping_targets=[sleep_task])
    return TestCase([sleep_task, ping_task], name=test_name)

def test_sameHost_different_netgroups_cant_ping():
    test_name = "Same-Host Different-Netgroups Can't Ping"
    sleep_task = SleepTask(netgroups=['netgroup_a'], slave=0)
    ping_task = PingTask(netgroups=['netgroup_b'], slave=0,
                         cant_ping_targets=[sleep_task])
    return TestCase([sleep_task, ping_task], name=test_name)


def test_same_netgroups_can_ping():
    test_name = "Different-Host Same-Netgroups Can Ping (Default Executor)"
    sleep_task = SleepTask(netgroups=['netgroup_a'], slave=0,
                           default_executor=True)
    ping_task = PingTask(netgroups=['netgroup_a'], slave=1,
                         default_executor=True,
                         can_ping_targets=[sleep_task])
    return TestCase([sleep_task, ping_task], name=test_name)

    # test_name = "Tasks that Opt-out of Calico can Communicate"
    # sleep_task = NetcatListenTask()
    # cat_task = NetcatSendTask(can_cat_targets=[sleep_task])
    # tests.append(TestCase([sleep_task, cat_task], name=test_name))
    #
    # test_name = "Tasks that Opt-out of Calico can Communicate (Default Executor)"
    # sleep_task = NetcatListenTask(default_executor=True)
    # cat_task = NetcatSendTask(can_cat_targets=[sleep_task], default_executor=True)
    # tests.append(TestCase([sleep_task, cat_task], name=test_name))
    #
    # test_name = "Multiple Netgroup Task Can Ping Each"
    # sleep_task_1 = SleepTask(netgroups=['netgroup_a'])
    # sleep_task_2 = SleepTask(netgroups=['netgroup_b'])
    # ping_task = PingTask(netgroups=['netgroup_a', 'netgroup_b'],
    #                      can_ping_targets=[sleep_task_1, sleep_task_2])
    # test = TestCase([sleep_task_1, sleep_task_2, ping_task], name=test_name)
    # tests.append(test)
    #
    # test_name = "Netgroup Mesh"
    # sleep_task_a_b = SleepTask(netgroups=['netgroup_a', 'netgroup_b'])
    # sleep_task_b = SleepTask(netgroups=['netgroup_b'])
    # sleep_task_a = SleepTask(netgroups=['netgroup_a'])
    # ping_task_a_b = PingTask(netgroups=['netgroup_a', 'netgroup_b'],
    #                          can_ping_targets=[sleep_task_a, sleep_task_b,
    #                                            sleep_task_a_b])
    # ping_task_a = PingTask(netgroups=['netgroup_a'],
    #                        can_ping_targets=[sleep_task_a, sleep_task_a_b],
    #                        cant_ping_targets=[sleep_task_b])
    # ping_task_b = PingTask(netgroups=['netgroup_b'],
    #                        can_ping_targets=[sleep_task_b, sleep_task_a_b],
    #                        cant_ping_targets=[sleep_task_a])
    # test = TestCase([sleep_task_a,
    #                  sleep_task_b,
    #                  sleep_task_a_b,
    #                  ping_task_a,
    #                  ping_task_b,
    #                  ping_task_a_b],
    #                 name=test_name)
    # tests.append(test)
    #
    # test_name = "Multiple IPs Can Ping"
    # sleep_task = SleepTask(netgroups=['A'], auto_ipv4=2)
    # ping_task = PingTask(netgroups=['A', 'D'],
    #                      can_ping_targets=[sleep_task],
    #                      auto_ipv4=3)
    # test = TestCase([sleep_task, ping_task], name=test_name)
    # tests.append(test)
    #
    # test_name = "Static IPs"
    # sleep_task = SleepTask(requested_ips=["192.168.28.23"],
    #                        netgroups=['A'],
    #                        auto_ipv4=2)
    # ping_task = PingTask(requested_ips=["192.168.28.34"],
    #                      netgroups=['A', 'D'],
    #                      can_ping_targets=[sleep_task])
    # test = TestCase([sleep_task, ping_task], name=test_name)
    # tests.append(test)
    #
    # test_name = "Mix static and assigned IPs"
    # sleep_task = SleepTask(requested_ips=["192.168.27.23",
    #                                       "192.168.27.34"],
    #                        netgroups=['A'],
    #                        auto_ipv4=2)
    # ping_task = PingTask(netgroups=['A', 'D'],
    #                      can_ping_targets=[sleep_task])
    # test = TestCase([sleep_task, ping_task], name=test_name)
    # tests.append(test)
    #
    # # Same IPs fail
    # # TODO: fail test individually on isolator error
    # # sleep_task_a = SleepTask(ip="192.168.254.1")
    # # sleep_task_b = SleepTask(ip="192.168.254.1")
    # # test_e = TestCase([sleep_task_a, sleep_task_b], "Test Same IP Fails")
    # # tests.append(test_e)
    #
    # calico_framework.start(tests)
