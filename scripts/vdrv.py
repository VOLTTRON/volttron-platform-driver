#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
import argcomplete
import argparse
import logging
import os
import sys

from pprint import pprint

from volttron.client.commands.connection import ControlConnection
from volttron.utils import get_address, parse_json_config

_log = logging.getLogger(os.path.basename(sys.argv[0]) if __name__ == "__main__" else __name__)

connection = ControlConnection(get_address(), peer='platform.driver')


# TODO: How to supress space after topic-part is completed?
def topic_completer(prefix, **kwargs):
    topic = prefix if prefix else None
    return connection.call('list_topics', topic=topic)

def driver_get_func(args):
    result, errors = connection.call('get', topic=args.topic, tag=args.tag, regex=args.regex)
    return (result, errors) if not errors and not result else result if not errors else result

def driver_set_func(args):
    result, errors = connection.call('set',  value=args.value, topic=args.topic, tag=args.tag, regex=args.regex,
                                     confirm_values=args.confirm_values, map_points=args.map_points)
    return (result, errors) if not errors and not result else result if not errors else result

def driver_last_func(args):
    return connection.call('last', topic=args.topic, tag=args.tag, regex=args.regex,
                           value=args.value, updated=args.updated)

def driver_revert_func(args):
    return connection.call('revert', topic=args.topic, tag=args.tag, regex=args.regex)

def driver_start_func(args):
    return connection.call('start', topic=args.topic, tag=args.tag, regex=args.regex)

def driver_stop_func(args):
    return connection.call('stop', topic=args.topic, tag=args.tag, regex=args.regex)

def driver_enable_func(args):
    return connection.call('enable', topic=args.topic, tag=args.tag, regex=args.regex)

def driver_disable_func(args):
    return connection.call('disable', topic=args.topic, tag=args.tag, regex=args.regex)

def driver_status_func(args):
    return connection.call('status', topic=args.topic, tag=args.tag, regex=args.regex)

def driver_node_add_func(args):
    if not args.config and args.config_path:
        with open(args.config_path) as f:
            config = parse_json_config(f)
    else:
        config = args.config
    return connection.call('add_node', node_topic=args.topic, config=config, update_schedule=args.update_schedule)

def driver_node_remove_func(args):
    return connection.call('remove_node', topic=args.topic)

def driver_interface_install_func(args):
    return connection.call('add_interface', interface_name=args.name)

def driver_interface_list_func(args):
    return connection.call('list_interfaces')

def driver_interface_remove_func(args):
    return connection.call('remove_interface', interface_name=args.name)

def driver_list_topics_func(args):
    return connection.call('list_topics', topic=args.topic, tag=args.tag, regex=args.regex)

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()
parser_driver = subparsers.add_parser('driver', help='Driver Commands')
driver_subparsers = parser_driver.add_subparsers()

# TODO: How to allow empty topic, but still have it be positional?
# TODO: How to suppress optional arguments from suggestions?
parser_driver_get = driver_subparsers.add_parser('get', help='Query data from equipment.')
parser_driver_get.add_argument('topic', help='Topic or pattern to select equipment.').completer = topic_completer
parser_driver_get.add_argument('-r', '--regex', help='Regex pattern to modify equipment selection.')
parser_driver_get.add_argument('-t', '--tag', help='Tagging expression to select equipment')
parser_driver_get.set_defaults(func=driver_get_func)

parser_driver_set = driver_subparsers.add_parser('set', help='Set data on equipment.')
parser_driver_set.add_argument('topic', help='Topic or pattern to select equipment.').completer = topic_completer
parser_driver_set.add_argument('value', help='Value to which to set the selected points.')
parser_driver_set.add_argument('-r', '--regex', help='Regex pattern to modify equipment selection.')
parser_driver_set.add_argument('-t', '--tag', help='Tagging expression to select equipment.')
parser_driver_set.add_argument('--confirm-values', help='Query points after setting to confirm the value was set.')
parser_driver_set.add_argument('--map-points', help='Take value to be a dictionary of {point: value} mappings.')
parser_driver_set.set_defaults(func=driver_set_func)

parser_driver_last = driver_subparsers.add_parser('last', help='Query last known values and/or the corresponding'
                                                               ' datetime of points on equipment.')
parser_driver_last.add_argument('topic', help='Topic or pattern to select equipment.').completer = topic_completer
parser_driver_last.add_argument('-r', '--regex', help='Regex pattern to modify equipment selection.')
parser_driver_last.add_argument('-t', '--tag', help='Tagging expression to select equipment')
parser_driver_last.add_argument('-v', '--value', default=True, help='Should the value be returned?')
parser_driver_last.add_argument('-u', '--updated', default=True, help='Should the datetime of the last known value be returned?')
parser_driver_last.set_defaults(func=driver_last_func)

parser_driver_revert = driver_subparsers.add_parser('revert', help='Reset any commands set on equipment.')
parser_driver_revert.add_argument('topic', help='Topic or pattern to select equipment.').completer = topic_completer
parser_driver_revert.add_argument('-r', '--regex', help='Regex pattern to modify equipment selection.')
parser_driver_revert.add_argument('-t', '--tag', help='Tagging expression to select equipment')
parser_driver_revert.add_argument('-c', '--confirm-values', default=False, help='Query points after reversion to confirm values were reset.')
parser_driver_revert.set_defaults(func=driver_revert_func)

parser_driver_start = driver_subparsers.add_parser('start', help='Start polling for a given topic.')
parser_driver_start.add_argument('topic', help='Topic or pattern to select equipment.').completer = topic_completer
parser_driver_start.add_argument('-r', '--regex', help='Regex pattern to modify equipment selection.')
parser_driver_start.add_argument('-t', '--tag', help='Tagging expression to select equipment')
parser_driver_start.set_defaults(func=driver_start_func)

parser_driver_stop = driver_subparsers.add_parser('stop', help='Stop polling for a given topic.')
parser_driver_stop.add_argument('topic', help='Topic or pattern to select equipment.').completer = topic_completer
parser_driver_stop.add_argument('-r', '--regex', help='Regex pattern to modify equipment selection.')
parser_driver_stop.add_argument('-t', '--tag', help='Tagging expression to select equipment')
parser_driver_stop.set_defaults(func=driver_stop_func)

parser_driver_enable = driver_subparsers.add_parser('enable', help='Set polling to start automatically for topic.')
parser_driver_enable.add_argument('topic', help='Topic or pattern to select equipment.').completer = topic_completer
parser_driver_enable.add_argument('-r', '--regex', help='Regex pattern to modify equipment selection.')
parser_driver_enable.add_argument('-t', '--tag', help='Tagging expression to select equipment')
parser_driver_enable.set_defaults(func=driver_enable_func)

parser_driver_disable = driver_subparsers.add_parser('disable', help='Set polling not to start automatically for topic.')
parser_driver_disable.add_argument('topic', help='Topic or pattern to select equipment.').completer = topic_completer
parser_driver_disable.add_argument('-r', '--regex', help='Regex pattern to modify equipment selection.')
parser_driver_disable.add_argument('-t', '--tag', help='Tagging expression to select equipment')
parser_driver_disable.set_defaults(func=driver_disable_func)

parser_driver_status = driver_subparsers.add_parser('status', help='Provide status of running drivers.')
parser_driver_status.add_argument('topic', help='Topic or pattern to select equipment.').completer = topic_completer
parser_driver_status.add_argument('-r', '--regex', help='Regex pattern to modify equipment selection.')
parser_driver_status.add_argument('-t', '--tag', help='Tagging expression to select equipment')
parser_driver_status.set_defaults(func=driver_status_func)

parser_driver_node = driver_subparsers.add_parser('node', help='Add and remove equipment from Equipment Tree.')
driver_node_subparsers = parser_driver_node.add_subparsers()

parser_driver_node_add = driver_node_subparsers.add_parser('add', help='Add a node to the Equipment Tree.')
parser_driver_node_add.add_argument('node_topic', help='Topic for the new equipment node.')
parser_driver_node_add.add_argument('-c', '--config', help='Configuration for the new equipment node.')
parser_driver_node_add.add_argument('-p', '--config-path', help='Path of configuration file for the new equipment node.')
parser_driver_node_add.add_argument('-u', '--update-schedule', default=True, help='Should the Poll Scheduler be run after adding the node?')
parser_driver_node_add.set_defaults(func=driver_node_add_func)

parser_driver_node_remove = driver_node_subparsers.add_parser('remove', help='Remove a node from the Equipment Tree.')
parser_driver_node_add.add_argument('topic', help='Topic of the equipment node to remove.')
parser_driver_node_remove.set_defaults(func=driver_node_remove_func)

parser_driver_interface = driver_subparsers.add_parser('interface', help='Add and remove interfaces.')
driver_interface_subparsers = parser_driver_interface.add_subparsers()

parser_driver_interface_install = driver_interface_subparsers.add_parser('install', help='Install a new driver interface.')
parser_driver_interface_install.add_argument('name', help='Name of the driver interface to install.')
parser_driver_interface_install.set_defaults(func=driver_interface_install_func)

parser_driver_interface_list = driver_interface_subparsers.add_parser('list', help='List installed driver interfaces.')
parser_driver_interface_list.set_defaults(func=driver_interface_list_func)

parser_driver_interface_remove = driver_interface_subparsers.add_parser('remove', help='Remove an interface from the driver.')
parser_driver_interface_remove.add_argument('name', help='Name of the driver interface to uninstall.')
parser_driver_interface_remove.set_defaults(func=driver_interface_remove_func)

parser_driver_topics = driver_subparsers.add_parser('list', help='List configured equipment topics.')
parser_driver_topics.add_argument('topic', default='devices', help='Topic or pattern to select equipment.').completer = topic_completer
parser_driver_topics.add_argument('-r', '--regex', help='Regex pattern to modify equipment selection.')
parser_driver_topics.add_argument('-t', '--tag', help='Tagging expression to select equipment')
parser_driver_topics.add_argument('-a', '--active', default=False, help='Exclude topics which are not actively polled.')
parser_driver_topics.add_argument('-e', '--enabled', default=False, help='Exclude topics are not enabled for polling.')
parser_driver_topics.set_defaults(func=driver_list_topics_func)

argcomplete.autocomplete(parser)
opts = parser.parse_args()
pprint(opts.func(opts))

