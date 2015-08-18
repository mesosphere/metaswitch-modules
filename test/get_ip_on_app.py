"""
Receives an app name, then determines what the calico assigned IP of the app is.

This script relies on a "state.json" file to exist in the same
directory, which is a dump of a GET request to the mesos-master state.json file.

The only script param is the name of the targeted app.

Example:
$ python get_ip_on_app.py sample-flask-app
 > 192.168.0.2
"""

import sys
import json


def get_slave_id(args, slave_name):
    for slave in data['slaves']:
        if slave_name in slave['hostname']:
            return slave['id']
    raise Exception("Couldn't find a slave with name: %s" % slave_name)

def get_ip_of_slave(args, slave_id):
    for task in args['frameworks'][0]['tasks']:
        if task['slave_id'] == slave_id:
            return task['statuses'][0]['labels'][0]['value']


if __name__ == '__main__':
    slave_name = sys.argv[1]
    json_blob = open('state.json').readline()

    data = json.loads(json_blob)

    # Get Slave 1's ID
    slave_id = get_slave_id(data, slave_name)
    # Get the app on slave1 whose slave_id matches slave_id
    task_name = get_ip_of_slave(data, slave_id)

    print task_name
