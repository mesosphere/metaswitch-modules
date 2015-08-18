"""
Receives a slave name, then determines which app is running on it.

This script relies on a "state.json" file to exist in the same
directory, which is a dump of a GET request to the mesos-master state.json file.

The only script param is the name of the targeted slave.

Example:
$ python get_app_on_slave.py slave1
 > sample-flask-app
"""

import sys
import json


def get_slave_id(args, slave_name):
    for slave in data['slaves']:
        if slave_name in slave['hostname']:
            return slave['id']
    raise Exception("Couldn't find a slave with name: %s" % slave_name)

def get_app_with_slave_id(args, slave_id):
    for task in args['frameworks'][0]['tasks']:
        if task['slave_id'] == slave_id:
            return task['name']


if __name__ == '__main__':
    slave_name = sys.argv[1]
    json_blob = open('state.json').readline()

    data = json.loads(json_blob)

    # Get Slave 1's ID
    slave_id = get_slave_id(data, slave_name)
    # Get the app on slave1 whose slave_id matches slave_id
    task_name = get_app_with_slave_id(data, slave_id)

    print task_name
