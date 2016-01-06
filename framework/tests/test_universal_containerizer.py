from tasks import SleepTask
from tasks import PingTask
from calico_framework import TestCase

__author__ = 'dano'

class TestYadda():
    def test_yadda_yadda(self):
        test_name = "Different-Host Same-Netgroups Can Ping (Default Executor)"
        sleep_task = SleepTask(netgroups=['netgroup_a'], slave=0,
                               default_executor=True)
        ping_task = PingTask(netgroups=['netgroup_a'], slave=1,
                             default_executor=True,
                             can_ping_targets=[sleep_task])
        return TestCase([sleep_task, ping_task], name=test_name)