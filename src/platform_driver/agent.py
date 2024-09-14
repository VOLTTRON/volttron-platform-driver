# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Installable Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2022 Battelle Memorial Institute
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# ===----------------------------------------------------------------------===
# }}}

import gevent
import importlib
import logging
import os
import subprocess
import sys

from collections import defaultdict
from datetime import datetime
from pkgutil import iter_modules
from pydantic import ValidationError
from typing import Iterable, Sequence, Set
from weakref import WeakValueDictionary

from volttron.client.commands.install_agents import InstallRuntimeError
from volttron.client.known_identities import PLATFORM_DRIVER
from volttron.client.messaging.health import STATUS_BAD
from volttron.client.messaging.utils import normtopic
from volttron.client.vip.agent import Agent, Core
from volttron.client.vip.agent.subsystems.rpc import RPC
from volttron.driver.base.driver import BaseInterface, DriverAgent, RemoteConfig
from volttron.driver.base.driver_locks import configure_publish_lock, setup_socket_lock
from volttron.driver.base.config import DeviceConfig, EquipmentConfig
from volttron.driver import interfaces
from volttron.utils import format_timestamp, get_aware_utc_now, load_config, setup_logging, vip_main
from volttron.utils.jsonrpc import RemoteError

from .config import latest_config_version, PlatformDriverConfig, PlatformDriverConfigV1, PlatformDriverConfigV2
from .constants import *
from .equipment import EquipmentTree, PointNode
from .overrides import OverrideManager
from .reservations import ReservationManager
from .scalability_testing import ScalabilityTester

setup_logging()
_log = logging.getLogger(__name__)
__version__ = '4.0'


class PlatformDriverAgent(Agent):

    def __init__(self, **kwargs):
        config_path = kwargs.pop('config_path', None)
        super(PlatformDriverAgent, self).__init__(**kwargs)
        self.config: PlatformDriverConfig = self._load_versioned_config(load_config(config_path) if config_path else {})

        # Initialize internal data structures:
        self.remotes = WeakValueDictionary()
        self.equipment_tree = EquipmentTree(self)
        self.interface_classes = {}

        # Set up locations for helper objects:
        self.heartbeat_greenlet = None
        self.override_manager = None  # TODO: Should this initialize object here and call a load method on config?
        self.poll_scheduler = None  # TODO: Should this use a default poll scheduler?
        self.reservation_manager = None  # TODO: Should this use a default reservation manager?
        self.scalability_test = None

        self.vip.config.set_default("config", self.config.dict())
        self.vip.config.subscribe(self.configure_main, actions=['NEW', 'UPDATE', 'DELETE'], pattern='config')

        self.equipment_config_lock = True  # Set equipment_config_lock until after on_configure is complete.
        _log.debug('########### SETTING LOCK IN __INIT__()')
        _log.debug('########### SUBSCRIBING TO NEW AND UPDATE ON "devices/*"')
        self.vip.config.subscribe(self.update_equipment, actions=['NEW', 'UPDATE'], pattern='devices/*')
        self.vip.config.subscribe(self.remove_equipment, actions='DELETE', pattern='devices/*')

    #########################
    # Configuration & Startup
    #########################

    def _load_versioned_config(self, config: dict):
        if not config: # There is no configuration yet, just loading defaults. No need to warn about versions.
            return PlatformDriverConfigV1()
        config_version = config.get('config_version', 1)
        try:
            if config_version < latest_config_version:
                _log.warning(f'Deprecation Warning: Platform Driver Agent has been configured with an agent'
                             f' configuration which is either unversioned or less than {latest_config_version}.'
                             f' Please see readthedocs for information regarding update of configuration style'
                             f' to a version {latest_config_version}. Support for'
                             f' configuration version style {config_version} may be removed in a future release.')
                return PlatformDriverConfigV1(**config)
            else:
                return PlatformDriverConfigV2(**config)
        except ValidationError as e:
            _log.warning(f'Validation of platform driver configuration file failed. Using default values.'
                         f' Errors: {str(e)}')
            if self.core.connected:  # TODO: Is this a valid way to make sure we are ready to call subsystems?
                self.vip.health.set_status(STATUS_BAD, "Error processing configuration: {e}")
            return PlatformDriverConfigV2()

    def configure_main(self, _, action: str, contents: dict):
        _log.debug("############# STARTING CONFIGURE_MAIN")
        old_config = self.config.copy()
        new_config = self._load_versioned_config(contents)
        _log.debug(self.config)
        if action == "NEW":
            self.config = new_config
            try:
                setup_socket_lock(self.config.max_open_sockets)
                configure_publish_lock(int(self.config.max_concurrent_publishes))
                self.scalability_test = (ScalabilityTester(self.config.scalability_test_iterations)
                                         if self.config.scalability_test else None)
            except ValueError as e:
                _log.error(
                    "ERROR PROCESSING STARTUP CRITICAL CONFIGURATION SETTINGS: {}".format(e))
                _log.error("Platform driver SHUTTING DOWN")
                sys.exit(1)

        else:
            # Some settings cannot be changed while running. Warn and replace these with the old ones until restart.
            _log.info('Updated configuration received for Platform Driver.')
            if new_config.max_open_sockets != old_config['max_open_sockets']:
                new_config.max_open_sockets = old_config['max_open_sockets']
                _log.info('Restart Platform Driver for changes to the max_open_sockets setting to take effect')

            if new_config.max_concurrent_publishes != old_config['max_concurrent_publishes']:
                new_config.max_concurrent_publishes = old_config['max_concurrent_publishes']
                _log.info('Restart Platform Driver for changes to the max_concurrent_publishes setting to take effect')

            if new_config.scalability_test != old_config['scalability_test']:
                new_config.scalability_test = old_config['scalability_test']
                if not old_config.scalability_test:
                    _log.info('Restart Platform Driver with scalability_test set to true in order to run a test.')
                if old_config.scalability_test:
                    _log.info("A scalability test may not be interrupted. Restart the driver to stop the test.")
            try:
                if new_config.scalability_test_iterations != old_config['scalability_test_iterations'] and \
                        old_config.scalability_test:
                    new_config.scalability_test_iterations = old_config['scalability_test_iterations']
                    _log.info('The scalability_test_iterations setting cannot be changed without restarting the agent.')
            except ValueError:
                pass
            if old_config.scalability_test:
                _log.info("Running scalability test. Settings may not be changed without restart.")
                return
            self.config = new_config

        if self.override_manager is None:
            self.override_manager = OverrideManager(self)

        # Set up Poll Scheduler:
        poll_scheduler_module = importlib.import_module(self.config.poll_scheduler_module_name)
        poll_scheduler_class = getattr(poll_scheduler_module, self.config.poll_scheduler_class_name)
        self.poll_scheduler = poll_scheduler_class(self, **self.config.poll_scheduler_configs)

        # Set up Reservation Manager:
        if self.reservation_manager is None:
            now = get_aware_utc_now()
            self.reservation_manager = ReservationManager(self, self.config.reservation_preempt_grace_time, now)
            self.reservation_manager.update(now)
        else:
            self.reservation_manager.set_grace_period(self.config.reservation_preempt_grace_time)

        # Set up heartbeat to devices:
        # TODO: Should this be globally uniform (here), by device (in remote), or globally scheduled (in poll scheduler)?
        # Only restart the heartbeat if it changes.
        if (self.config.remote_heartbeat_interval != old_config.remote_heartbeat_interval
                or action == "NEW" or self.heartbeat_greenlet is None):
            if self.heartbeat_greenlet is not None:
                self.heartbeat_greenlet.kill()
            self.heartbeat_greenlet = self.core.periodic(self.config.remote_heartbeat_interval, self.heart_beat)

        # Start subscriptions:
        current_subscriptions = {topic: subscribed for _, topic, subscribed in self.vip.pubsub.list('pubsub').get()}
        for topic, callback in [
            (GET_TOPIC, self.handle_get),
            (SET_TOPIC, self.handle_set),
            (RESERVATION_REQUEST_TOPIC, self.handle_reservation_request),
            (REVERT_POINT_TOPIC, self.handle_revert_point),
            (REVERT_DEVICE_TOPIC, self.handle_revert_device)
        ]:
            if not current_subscriptions.get(topic):
                self.vip.pubsub.subscribe('pubsub', topic, callback)

        # Load Equipment Tree:
        for c in self.vip.config.list():
            if 'devices/' in c[:8]:
                equipment_config = self.vip.config.get(c)
                _log.debug('GOT EQUIPMENT CONFIG: ')
                _log.debug(equipment_config)
#                registry_location = equipment_config['registry_config']#[len('config://'):]
 #               _log.debug(f'############ ATTEMPTING TO RETRIEVE REGISTRY CONFIG FROM: {registry_location}')
  #              equipment_config['registry_config'] = self.vip.config.get(registry_location)
                self._configure_new_equipment(c, 'NEW', equipment_config, schedule_now=False)
        # Schedule Polling
        self.poll_scheduler.schedule()
        _log.debug("############ ENDING CONFIGURE_MAIN")

    @Core.receiver('onstart')
    def on_start(self, _):
        # Remove the equipment_config_lock after the on configure event has completed. Initial configuration of all
        #  equipment should be complete. We can now allow update events to run.
        _log.error(f"########## RUNNING ON_START (REMOVING LOCK).")
        self.equipment_config_lock = False

    def _configure_new_equipment(self, equipment_name: str, _, contents: dict, schedule_now: bool = True):
        # Separate remote_config and make adjustments for possible config version 1:
        remote_config = contents.pop('remote_config', contents.pop('driver_config', {}))
        remote_config['driver_type'] = remote_config.get('driver_type', contents.pop('driver_type', None))
        # TODO: Where to put heart_beat_point? Is that remote or equipment specific?
        remote_config = RemoteConfig(**remote_config)

        if self.equipment_tree.get_node(equipment_name):
            _log.warning(f'Received a NEW configuration for equipment which already exists: {equipment_name}.'
                         f'New configuration has been ignored.')
            return
        if remote_config.driver_type:
            # Received new device node.
            registry_config = contents.pop('registry_config', [])
            dev_config = DeviceConfig(**contents)
            try:
                driver = self._get_or_create_remote(equipment_name, remote_config, dev_config.allow_duplicate_remotes)
            except ValueError as e:
                _log.warning(f'Skipping configuration of equipment: {equipment_name} after encountering error --- {e}')
                return
            device_node = self.equipment_tree.add_device(device_topic=equipment_name, config=dev_config,
                                                         driver_agent=driver, registry_config=registry_config)
            driver.add_equipment(device_node)
        else: # Received new or updated segment node.
            equipment_config = EquipmentConfig(**contents)
            self.equipment_tree.add_segment(equipment_name, equipment_config)
        if schedule_now:
            self.poll_scheduler.schedule()

    def _get_or_create_remote(self, equipment_name: str, remote_config: RemoteConfig, allow_duplicate_remotes):
        interface = self.interface_classes.get(remote_config.driver_type)
        if not interface:
            try:
                module = remote_config.module
                interface = BaseInterface.get_interface_subclass(remote_config.driver_type, module)
            except (AttributeError, ModuleNotFoundError, ValueError) as e:
                raise ValueError(f'Unable to configure driver for equipment: "{equipment_name}"'
                                 f' with interface: {remote_config.driver_type}.'
                                 f' This interface type is currently unknown or not installed.'
                                 f' Received exception: {e}')
            self.interface_classes[remote_config.driver_type] = interface

        allow_duplicate_remotes = True if (allow_duplicate_remotes or self.config.allow_duplicate_remotes) else False
        if not allow_duplicate_remotes:
            unique_remote_id = interface.unique_remote_id(equipment_name, remote_config)
        else:
            unique_remote_id = BaseInterface.unique_remote_id(equipment_name, remote_config)

        driver_agent = self.remotes.get(unique_remote_id)
        if not driver_agent:
            driver_agent = DriverAgent(remote_config, self.equipment_tree, self. scalability_test, self.config.timezone,
                                       unique_remote_id, self.vip)
            gevent.spawn(driver_agent.core.run)
            # TODO: Were the right number spawned? Need more debug code to ascertain this is working correctly.
            self.remotes[unique_remote_id] = driver_agent
        return driver_agent

    def update_equipment(self, config_name: str, action: str, contents: dict):
        """Callback for updating equipment configuration."""
        if self.equipment_config_lock:
            _log.debug(f'############# {action} ACTION RAN! THE LOCK IS SET! ##############')
            return
        # TODO: Implement UPDATE callback for /devices.
        _log.debug(f'############ {action} ACTION RAN! UH OH, NO LOCK!!! ##############')
        self.poll_scheduler.check_for_reschedule()

    def remove_equipment(self, config_name: str, _, __):
        """Callback to remove equipment configuration."""
        self.equipment_tree.remove_segment(config_name)
        # TODO: Implement override handling.
        # self._update_override_state(config_name, 'remove')
        self.poll_scheduler.check_for_reschedule()


    ###############
    # Query Backend
    ###############

    def resolve_tags(self, tags):
        """ Resolve tags from tagging service. """
        try:
            tag_list = self.vip.rpc.call('platform.tagging', 'get_topics_by_tags', tags).get(timeout=5)
            return tag_list if tag_list else []
        except gevent.Timeout as e:
            _log.warning(f'Tagging Service timed out: {e.exception}')
            return []

    def build_query_plan(self, topic: str | Sequence[str] | Set[str] = None, regex: str = None, tag: str = None
                         ) -> dict[DriverAgent, set[PointNode]]:
        """ Find points to be queried and organize by remote."""
        exact_matches, topic = (topic, None) if isinstance(topic, list) else ([], topic)
        if tag:
            exact_matches.extend(self.resolve_tags(tag))
        query_plan = defaultdict(set)
        for p in self.equipment_tree.find_points(topic, regex, tag):
            query_plan[self.equipment_tree.get_remote(p.identifier)].add(p)
        return query_plan

    ###############
    # RPC Interface
    ###############
    # TODO: semantic_get

    @RPC.export
    def get(self, topic: str | Sequence[str] | Set[str] = None, tag: str = None, regex: str = None) -> (dict, dict):
        _log.debug("############ IN GET")
        results = {}
        errors = {}
        # Find set of points to query and organize by remote:
        query_plan = self.build_query_plan(topic, regex, tag)
        # Make query for selected points on each remote:
        for (remote, point_set) in query_plan.items():
            q_return_values, q_return_errors = remote.get_multiple_points([p.identifier for p in point_set])
            _log.debug(f'GOT BACK FROM GET_MULTIPLE_POINTS WITH: {q_return_values}')
            _log.debug(f'ERRORS FROM GET_MULTIPLE_POINTS: {q_return_errors}')
            for topic, val in q_return_values.items():
                node = self.equipment_tree.get_node(topic)
                if node:
                    node.last_value(val)
            results.update(q_return_values)
            errors.update(q_return_errors)
        return results, errors

    @RPC.export
    def set(self, value: any, topic: str | Sequence[str] | Set[str] = None, tag: str = None, regex: str = None,
            confirm_values: bool = False, map_points=False) -> (dict, dict):
        results = {}
        errors = {}
        # Find set of points to query and organize by remote:
        query_plan = self.build_query_plan(topic, regex, tag)
        # Set selected points on each remote:
        for (remote, point_set) in query_plan.items():
            # TODO: The DriverAgent isn't currently expecting full topics.
            point_value_tuples = list(value.items()) if map_points else [(p.identifier, value) for p in point_set]
            query_return_errors = remote.set_multiple_points(point_value_tuples)
            errors.update(query_return_errors)
            if confirm_values:
                # TODO: Should results contain the values read back from the device, or Booleans for success?
                results.update(remote.get_multiple_points([p.identifier for p in point_set]))

    @RPC.export
    def revert(self, topic: str | Sequence[str] | Set[str] = None, tag: str = None, regex: str = None,
              confirm_values: bool = False) -> dict:
        query_plan = self.build_query_plan(topic, regex, tag)
        # Set selected points on each remote:
        for (remote, point_set) in query_plan.items():
            # TODO: How to handle all/single/multiple reverts? Detect devices, and othewise use revert_point? Add revert_multiple?
            pass
        # TODO: What to return for this? The current methods return None.

    @RPC.export
    def last(self, topic: str | Sequence[str] | Set[str] = None, tag: str = None, regex: str = None,
             value: bool = True, updated: bool = True) -> dict:
        points = self.equipment_tree.find_points(topic, regex, tag)
        if value:
            if updated:
                return_dict = {p.topic: {'value': p.last_value, 'updated': p.last_updated} for p in points}
            else:
                return_dict = {p.topic: p.last_value for p in points}
        else:
            return_dict = {p.topic: p.last_updated for p in points}
        return return_dict

    #-----------
    # UI Support
    #-----------
    @RPC.export
    def start(self, topic: str | Sequence[str] | Set[str] = None, tag: str = None, regex: str = None) -> None:
        points = self.equipment_tree.find_points(topic, regex, tag)
        for p in points:
            if p.active:
                return
            else:
                p.active = True
                if self.config.allow_reschedule:
                    self.poll_scheduler.schedule()
                else:
                    self.poll_scheduler.add_to_schedule(p)

    @RPC.export
    def stop(self, topic: str | Sequence[str] | Set[str] = None, tag: str = None, regex: str = None) -> None:
        points = self.equipment_tree.find_points(topic, regex, tag)
        for p in points:
            if not p.active:
                return
            else:
                p.active = False
                if self.config.allow_reschedule:
                    self.poll_scheduler.schedule()
                else:
                    self.poll_scheduler.remove_from_schedule(p)

    @RPC.export
    def enable(self, topic: str | Sequence[str] | Set[str] = None, tag: str = None, regex: str = None) -> None:
        nodes = self.equipment_tree.find_points(topic, regex, tag)
        for node in nodes:
            node.config.active = True
            if not node.is_point:
                # TODO: Make sure this doesn't trigger UPDATE.
                self.vip.config.set(node.topic, node.config, trigger_callback=False)
            else:
                self.equipment_tree.update_stored_registry_config(node.identifier)

    @RPC.export
    def disable(self, topic: str | Sequence[str] | Set[str] = None, tag: str = None, regex: str = None) -> None:
        nodes = self.equipment_tree.find_points(topic, regex, tag)
        for node in nodes:
            node.config.active = False
            if not node.is_point:
                self.vip.config.set(node.topic, node.config, trigger_callback=False)
            else:
                self.equipment_tree.update_stored_registry_config(node.identifier)

    @RPC.export
    def status(self, topic: str | Sequence[str] | Set[str] = None, regex: str = None) -> dict:
        nodes = self.equipment_tree.find_points(topic, regex)
        return self._status(nodes)

    @RPC.export
    def status(self, topic: str | Sequence[str] | Set[str] = None, tag: str = None, regex: str = None) -> dict:
        # TODO: Implement status()
        nodes = self.equipment_tree.find_points(topic, regex, tag)
        pass

    @RPC.export
    def add_node(self, node_topic: str, config: dict, update_schedule: bool = True) -> dict|None:
        self._configure_new_equipment(node_topic, 'NEW', contents=config, schedule_now=update_schedule)
        # TODO: What should this return? If error_dict, how to get this?

    @RPC.export
    def remove_node(self, node_topic: str) -> dict|None:
        self.equipment_tree.remove_segment(node_topic)
        # TODO: What should this return? If error_dict, how to get this?

    @RPC.export
    def add_interface(self, driver_name: str, local_path: str = None) -> dict|None:
        # TODO: Implement add_interface()
        pass

    @RPC.export
    def list_interfaces(self) -> list[str]:
        """Return list of all installed driver interfaces."""
        return [i.name for i in iter_modules(interfaces.__path__)]

    @RPC.export
    def remove_interface(self, interface_name: str) -> dict | None:
        interface_package = self._interface_package_from_short_name(interface_name)
        subprocess.run([sys.executable, '-m', 'pip', 'uninstall', interface_package])
        # TODO: What should this be returning?  If error_dict, how to get this?

    @RPC.export
    def list_topics(self, topic: str, tag: str = None, regex: str = None,
                    active: bool = False, enable: bool = False) -> list[str]:
        # TODO: Fix issue with topics coming back from Remote.
        # TODO: Handle regex and tags.
        # TODO: Handle active and enable (exclude non-active and exclude non-enabled) flags.
        topic = topic.strip('/') if topic and topic.startswith(self.equipment_tree.root) else self.equipment_tree.root
        parent = topic if self.equipment_tree.get_node(topic) else topic.rsplit('/', 1)[0]
        children = [c.identifier for c in self.equipment_tree.children(parent)]
        return children

    #-------------
    # Reservations
    #-------------
    @RPC.export
    def new_reservation(self, task_id: str, priority: str, requests: list) -> dict|None:
        """
        Reserve one or more blocks on time on one or more device.

        :param task_id: An identifier for this reservation.
        :param priority: Priority of the task. Must be either "HIGH", "LOW",
        or "LOW_PREEMPT"
        :param requests: A list of time slot requests in the format
        described in `Device Schedule`_.
        """
        rpc_peer = self.vip.rpc.context.vip_message.peer
        # TODO: Is new_reservation the same as new_task?
        return self.reservation_manager.new_reservation(rpc_peer, task_id, priority, requests, publish_result=False)

    @RPC.export
    def cancel_reservation(self, task_id: str) -> dict|None:
        """
        Requests the cancellation of the specified task id.
        :param task_id: Task name.
        """
        rpc_peer = self.vip.rpc.context.vip_message.peer
        # TODO: Is cancel_reservation the same as new_task?
        return self.reservation_manager.cancel_reservation(rpc_peer, task_id, publish_result=False)

    #----------
    # Overrides
    #----------
    @RPC.export
    def set_override_on(self, pattern: str, duration: float = 0.0,
                        failsafe_revert: bool = True, staggered_revert: bool = False):
        """RPC method

        Turn on override condition on all the devices matching the pattern.
        :param pattern: Override pattern to be applied. For example,
            If pattern is campus/building1/* - Override condition is applied for all the devices under
            campus/building1/.
            If pattern is campus/building1/ahu1 - Override condition is applied for only campus/building1/ahu1
            The pattern matching is based on bash style filename matching semantics.
        :type pattern: str
        :param duration: Time duration for the override in seconds. If duration <= 0.0, it implies as indefinite
        duration.
        :type duration: float
        :param failsafe_revert: Flag to indicate if all the devices falling under the override condition has to be set
         to its default state/value immediately.
        :type failsafe_revert: boolean
        :param staggered_revert: If this flag is set, reverting of devices will be staggered.
        :type staggered_revert: boolean
        """
        self.override_manager.set_on(pattern, duration, failsafe_revert, staggered_revert)

    @RPC.export
    def set_override_off(self, pattern: str):
        """RPC method

        Turn off override condition on all the devices matching the pattern. The pattern matching is based on bash style
        filename matching semantics.
        :param pattern: Pattern on which override condition has to be removed.
        :type pattern: str
        """
        return self.override_manager.set_off(pattern)

    # Get a list of all the devices with override condition.
    @RPC.export
    def get_override_devices(self):
        """RPC method

        Get a list of all the devices with override condition.
        """
        return list(self.override_manager.devices)

    @RPC.export
    def clear_overrides(self):
        """RPC method

        Clear all overrides.
        """
        self.override_manager.clear()

    @RPC.export
    def get_override_patterns(self):
        """RPC method

        Get a list of all the override patterns.
        """
        return list(self.override_manager.patterns)

    #-------------------
    # Legacy RPC Methods
    #-------------------
    @RPC.export
    def get_point(self, path: str = None, point_name: str = None, **kwargs) -> any:
        """
        RPC method

        Gets up-to-date value of a specific point on a device.
        Does not require the device be scheduled.

        :param path: The topic of the point to grab in the
                      format <device topic>/<point name>

                      Only the <device topic> if point is specified.
        :param point_name: Point on the device. Assumes topic includes point name if omitted.
        :param kwargs: Any driver specific parameters
        :type path: str
        :returns: point value
        :rtype: any base python type"""

        # Support for old-actuator-style keyword arguments.
        path = path if path else kwargs.get('topic', None)
        point_name = point_name if point_name else kwargs.get('point', None)
        if path is None:
            # DEPRECATED: Only allows topic to be None to permit use of old-actuator-style keyword argument "topic".
            raise TypeError('Argument "path" is required.')

        point_name = self._equipment_id(path, point_name)
        node = self.equipment_tree.get_node(point_name)
        if not node:
            raise ValueError(f'No equipment found for topic: {point_name}')
        remote = self.equipment_tree.get_remote(node.identifier)
        if not remote:
            raise ValueError(f'No remote found for topic: {point_name}')
        return remote.get_point(point_name, **kwargs)

    @RPC.export
    def set_point(self, path: str, point_name: str | None, value: any, *args, **kwargs) -> any:
        """RPC method

        Sets the value of a specific point on a device.
        Requires the device be scheduled by the calling agent.

        :param path: The topic of the point to set in the
                      format <device topic>/<point name>
                      Only the <device topic> if point is specified.
        :param value: Value to set point to.
        :param point_name: Point on the device.
        :param kwargs: Any driver specific parameters
        :type path: str
        :type value: any basic python type
        :type point_name: str
        :returns: value point was actually set to. Usually invalid values
                cause an error but some drivers (MODBUS) will return a
                different
                value with what the value was actually set to.
        :rtype: any base python type

        .. warning:: Calling will raise a ReservationLockError if another agent has already scheduled
        this device for the present time."""

        sender = self.vip.rpc.context.vip_message.peer

        # Support for old-actuator-style arguments.
        topic = kwargs.get('topic')
        if topic:
            path = topic
        elif path == sender or len(args) > 0:
            # Function was likely called with actuator-style positional arguments. Reassign variables to match.
            _log.debug('Deprecated actuator-style positional arguments detected in set_point().'
                       ' Please consider converting code to use set() method.')
            path, point_name = (point_name, args[0]) if len(args) >= 1 else point_name, None
        point_name = point_name if point_name else kwargs.get('point', None)

        point_name = self._equipment_id(path, point_name)
        node = self.equipment_tree.get_node(self._equipment_id(path, point_name))
        if not node:
            raise ValueError(f'No equipment found for topic: {point_name}')
        self.equipment_tree.raise_on_locks(node, sender)
        remote = self.equipment_tree.get_remote(node.identifier)
        if not remote:
            raise ValueError(f'No remote found for topic: {point_name}')
        result = remote.set_point(point_name, value, **kwargs)
        headers = self._get_headers(sender)
        self._push_result_topic_pair(WRITE_ATTEMPT_PREFIX, topic, headers, value)
        self._push_result_topic_pair(VALUE_RESPONSE_PREFIX, topic, headers, result)
        return result

    @RPC.export
    def scrape_all(self, topic: str) -> dict:
        """RPC method

        Get all points from a device.

        :param topic: Device topic
        :returns: Dictionary of points to values
        """
        path = self._equipment_id(topic, None)
        return self.get(topic=path)

    @RPC.export
    def get_multiple_points(self, path: str | Sequence[str | Sequence] = None, point_names = None,
                            **kwargs) -> (dict, dict):
        """RPC method

        Get multiple points on multiple devices. Makes a single
        RPC call to the platform driver per device.

        :param path: A topic (with or without point names), a list of full topics (with point names),
         or a list of [device, point] pairs.
        :param point_names: A Sequence of point names associated with the given path.
        :param kwargs: Any driver specific parameters

        :returns: Dictionary of points to values and dictionary of points to errors

        .. warning:: This method does not require that all points be returned
                     successfully. Check that the error dictionary is empty.
        """
        # Support for actuator-style keyword arguments.
        topics = path if path else kwargs.get('topics', None)
        if topics is None:
            # path is allowed to be None to permit use of old-actuator-style keyword argument "topics".
            raise TypeError('Argument "path" is required.')

        errors = {}
        devices = set()
        if isinstance(topics, str):
            if not point_names:
                devices.add(topics)
            else:
                for point in point_names:
                    devices.add(self._equipment_id(topics, point))
        elif isinstance(topics, Sequence):
            for topic in topics:
                if isinstance(topic, str):
                    devices.add(self._equipment_id(topic))
                elif isinstance(topic, Sequence) and len(topic) == 2:
                    devices.add(self._equipment_id(*topic))
                else:
                    e = ValueError("Invalid topic: {}".format(topic))
                    errors[repr(topic)] = repr(e)

        results, query_errors = self.get(devices)
        errors.update(query_errors)
        return results, errors

    @RPC.export
    def set_multiple_points(self, path: str, point_names_values: list[tuple[str, any]], **kwargs) -> dict:
        """RPC method

        Set values on multiple set points at once. If global override is condition is set,raise OverrideError exception.
        :param path: device path
        :type path: str
        :param point_names_values: list of points and corresponding values
        :type point_names_values: list of tuples
        :param kwargs: additional arguments for the device
        :type kwargs: arguments pointer
        """
        errors = {}
        topic_value_map = {}
        sender = self.vip.rpc.context.vip_message.peer
        # Support for old-actuator-style positional arguments so long as sender matches rpc peer.
        topics_values = kwargs.get('topics_values')
        if path == sender or topics_values is not None:  # Method was called with old-actuator-style arguments.
            topics_values = topics_values if topics_values else point_names_values
            for topic, value in topics_values:
                if isinstance(topic, str):
                    topic_value_map[self._equipment_id(topic, None)] = value
                elif isinstance(topic, Sequence) and len(topic) == 1:
                    topic_value_map[self._equipment_id(*topic)] = value
                else:
                    e = ValueError("Invalid topic: {}".format(topic))
                    errors[str(topic)] = repr(e)
        else:  # Assume method was called with old-driver-style arguments.
            for point, value in point_names_values:
                topic_value_map[self._equipment_id(path, point)] = value

        _, ret_errors = self.set(topic_value_map, map_points=True, **kwargs)
        errors.update(ret_errors)
        return errors

    @RPC.export
    def heart_beat(self):
        """RPC method

        Sends heartbeat to all devices
        """
        # TODO: Make sure this is being called with the full topic.
        # TODO: Should this still be exposed if the actuator agent no longer needs to send to this?
        _log.debug("sending heartbeat")
        for remote in self.remotes.values():
            remote.heart_beat()

    @RPC.export
    def revert_point(self, path: str, point_name: str, **kwargs):
        """RPC method

        Revert the set point to default state/value.
        If global override is condition is set, raise OverrideError exception.
        If topic has been reserved by another user
        or if it is not reserved but reservations are required,
         raise ReservationLockError exception.
        :
        param path: device path
        :type path: str
        :param point_name: set point to revert
        :type point_name: str
        :param kwargs: additional arguments for the device
        :type kwargs: arguments pointer
        """
        sender = self.vip.rpc.context.vip_message.peer

        # Support for old-actuator-style arguments.
        topic = kwargs.get('topic')
        if topic:
            path, point_name = topic, None
        elif path == sender:
            # Function was likely called with actuator-style positional arguments. Reassign variables to match.
            _log.debug('Deprecated actuator-style positional arguments detected in revert_point().'
                       ' Please consider converting code to use revert() method.')
            path, point_name = point_name, None

        point_name = self._equipment_id(path, point_name)
        node = self.equipment_tree.get_node(self._equipment_id(path, point_name))
        if not node:
            raise ValueError(f'No equipment found for topic: {point_name}')
        self.equipment_tree.raise_on_locks(node, sender)
        remote = self.equipment_tree.get_remote(node.identifier)
        remote.revert_point(point_name, **kwargs)

        headers = self._get_headers(sender)
        self._push_result_topic_pair(REVERT_POINT_RESPONSE_PREFIX, topic, headers, None)

    @RPC.export
    def revert_device(self, path: str, *args, **kwargs):
        """RPC method

        Revert all the set point values of the device to default state/values. If global override is condition is set,
        raise OverrideError exception.
        :param path: device path
        :type path: str
        :param kwargs: additional arguments for the device
        :type kwargs: arguments pointer
        """
        sender = self.vip.rpc.context.vip_message.peer

        # Support for old-actuator-style arguments.
        topic = kwargs.get('topic')
        if topic:
            path = topic
        elif path == sender and len(args) > 0:
            # Function was likely called with actuator-style positional arguments. Reassign variables to match.
            _log.debug('Deprecated actuator-style positional arguments detected in revert_device().'
                       ' Please consider converting code to use revert() method.')
            path = args[0]

        node = self.equipment_tree.get_node(self._equipment_id(path, None))
        if not node:
            raise ValueError(f'No equipment found for topic: {path}')
        self.equipment_tree.raise_on_locks(node, sender)
        remote = self.equipment_tree.get_remote(node.identifier)
        remote.revert_all(**kwargs)

        headers = self._get_headers(sender)
        self._push_result_topic_pair(REVERT_DEVICE_RESPONSE_PREFIX, topic, headers, None)


    @RPC.export
    def request_new_schedule(self, requester_id: str, task_id: str, priority: str,
                             requests: list[list[str]] | list[str]) -> dict:
        """
        RPC method

        Requests one or more blocks on time on one or more device.

        :param requester_id: Ignored, VIP Identity used internally
        :param task_id: Task name.
        :param priority: Priority of the task. Must be either "HIGH", "LOW",
        or "LOW_PREEMPT"
        :param requests: A list of time slot requests in the format
        described in `Device Schedule`_.

        :type requester_id: str
        :type task_id: str
        :type priority: str
        :returns: Request result
        :rtype: dict

        :Return Values:

            The return values are described in `New Task Response`_.
        """
        _log.debug('Call to deprecated RPC method "request_new_schedule. '
                   'This method provides compatability with the actuator API, but has been superseded '
                   'by "new_reservation". Please update to the newer method.')
        rpc_peer = self.vip.rpc.context.vip_message.peer
        return self.reservation_manager.new_task(rpc_peer, task_id, priority, requests)

    @RPC.export
    def request_cancel_schedule(self, requester_id: str, task_id: str) -> dict:
        """RPC method

        Requests the cancellation of the specified task id.

        :param requester_id: Ignored, VIP Identity used internally
        :param task_id: Task name.

        :type requester_id: str
        :type task_id: str
        :returns: Request result
        :rtype: dict

        :Return Values:

        The return values are described in `Cancel Task Response`_.

        """
        _log.debug('Call to deprecated RPC method "request_cancel_schedule. '
                   'This method provides compatability with the actuator API, but has been superseded '
                   'by "cancel_reservation". Please update to the newer method.')
        rpc_peer = self.vip.rpc.context.vip_message.peer
        return self.reservation_manager.cancel_reservation(rpc_peer, task_id, publish_result=False)

    ##################
    # PubSub Interface
    ##################

    def handle_get(self, _, sender: str, __, topic: str, ___, ____):
        """
        Requests up-to-date value of a point.

        To request a value publish a message to the following topic:

        ``devices/actuators/get/<device path>/<actuation point>``

        with the fallowing header:

        .. code-block:: python

            {
                'requesterID': <Ignored, VIP Identity used internally>
            }

        The ActuatorAgent will reply on the **value** topic
        for the actuator:

        ``devices/actuators/value/<full device path>/<actuation point>``

        with the message set to the value the point.

        """
        point = topic.replace(GET_TOPIC + '/', '', 1)
        headers = self._get_headers(sender)
        try:
            value = self.get_point(point)
            self._push_result_topic_pair(VALUE_RESPONSE_PREFIX, point, headers, value)
        except Exception as ex:
            self._handle_error(ex, point, headers)


    def handle_set(self, _, sender: str, __, topic: str, ___, message: any):
        """
        Set the value of a point.

        To set a value publish a message to the following topic:

        ``devices/actuators/set/<device path>/<actuation point>``

        with the fallowing header:

        .. code-block:: python

            {
                'requesterID': <Ignored, VIP Identity used internally>
            }

        The ActuatorAgent will reply on the **value** topic
        for the actuator:

        ``devices/actuators/value/<full device path>/<actuation point>``

        with the message set to the value the point.

        Errors will be published on

        ``devices/actuators/error/<full device path>/<actuation point>``

        with the same header as the request.

        """
        point = topic.replace(SET_TOPIC + '/', '', 1)
        headers = self._get_headers(sender)
        if not message:
            error = {'type': 'ValueError', 'value': 'missing argument'}
            _log.debug('ValueError: ' + str(error))
            self._push_result_topic_pair(ERROR_RESPONSE_PREFIX, point, headers, error)
            return

        try:
            self.set_point(point, None, message)
        except Exception as ex:
            self._handle_error(ex, point, headers)

    def handle_revert_point(self, _, sender: str, __, topic: str, ___, ____):
        """
        Revert the value of a point.

        To revert a value publish a message to the following topic:

        ``actuators/revert/point/<device path>/<actuation point>``

        with the fallowing header:

        .. code-block:: python

            {
                'requesterID': <Ignored, VIP Identity used internally>
            }

        The ActuatorAgent will reply on

        ``devices/actuators/reverted/point/<full device path>/<actuation
        point>``

        This is to indicate that a point was reverted.

        Errors will be published on

        ``devices/actuators/error/<full device path>/<actuation point>``

        with the same header as the request.
        """
        topic = self._equipment_id(topic.replace(REVERT_POINT_TOPIC + '/', '', 1), None)
        headers = self._get_headers(sender)

        try:
            node = self.equipment_tree.get_node(topic)
            self.equipment_tree.raise_on_locks(node, sender)
            remote = self.equipment_tree.get_remote(node.identifier)
            remote.revert_point(topic)

            self._push_result_topic_pair(REVERT_POINT_RESPONSE_PREFIX, topic, headers, None)
        except Exception as ex:
            self._handle_error(ex, topic, headers)

    def handle_revert_device(self, _, sender: str, __, topic: str, ___, ____):
        """
        Revert all the writable values on a device.

        To revert a device publish a message to the following topic:

        ``devices/actuators/revert/device/<device path>``

        with the fallowing header:

        .. code-block:: python

            {
                'requesterID': <Ignored, VIP Identity used internally>
            }

        The ActuatorAgent will reply on the **value** topic
        for the actuator:

        ``devices/actuators/reverted/device/<full device path>``

        to indicate that a point was reverted.

        Errors will be published on

        ``devices/actuators/error/<full device path>/<actuation point>``

        with the same header as the request.
        """
        topic = self._equipment_id(topic.replace(REVERT_DEVICE_TOPIC + '/', '', 1), None)
        headers = self._get_headers(sender)
        try:
            node = self.equipment_tree.get_node(topic)
            self.equipment_tree.raise_on_locks(node, sender)
            remote = self.equipment_tree.get_remote(node.identifier)
            remote.revert_all()

            self._push_result_topic_pair(REVERT_DEVICE_RESPONSE_PREFIX, topic, headers, None)

        except Exception as ex:
            self._handle_error(ex, topic, headers)

    def handle_reservation_request(self, _, sender: str, __, topic: str, headers: dict,
                                   message: list[list[str]] | list[str]):
        """
        Schedule request pub/sub handler

        An agent can request a task schedule by publishing to the
        ``devices/actuators/schedule/request`` topic with the following header:

        .. code-block:: python

            {
                'type': 'NEW_SCHEDULE',
                'requesterID': <Ignored, VIP Identity used internally>,
                'taskID': <unique task ID>, #The desired task ID for this
                task. It must be unique among all scheduled tasks.
                'priority': <task priority>, #The desired task priority,
                must be 'HIGH', 'LOW', or 'LOW_PREEMPT'
            }

        The message must describe the blocks of time using the format
        described in `Device Schedule`_.

        A task may be canceled by publishing to the
        ``devices/actuators/schedule/request`` topic with the following header:

        .. code-block:: python

            {
                'type': 'CANCEL_SCHEDULE',
                'requesterID': <Ignored, VIP Identity used internally>,
                'taskID': <unique task ID>, #The task ID for the canceled Task.
            }

        requesterID
            The name of the requesting agent. Automatically replaced with VIP id.
        taskID
            The desired task ID for this task. It must be unique among all
            scheduled tasks.
        priority
            The desired task priority, must be 'HIGH', 'LOW', or 'LOW_PREEMPT'

        No message is requires to cancel a schedule.

        """
        request_type = headers.get('type')
        _log.debug(f'handle_schedule_request: {topic}, {headers}, {message}')

        task_id = headers.get('taskID')
        priority = headers.get('priority')

        now = get_aware_utc_now()
        if request_type == RESERVATION_ACTION_NEW:
            try:
                requests = message[0] if len(message) == 1 else message
                headers = self._get_headers(sender, now, task_id, RESERVATION_ACTION_NEW)
                result = self.reservation_manager.new_task(sender, task_id, requests, priority, now)
            except Exception as ex:
                return self._handle_unknown_reservation_error(ex, headers, message)
            # Dealing with success and other first world problems.
            if result.success:
                for preempted_task in result.data:
                    preempt_headers = self._get_headers(preempted_task[0], task_id=preempted_task[1],
                                                        action_type=RESERVATION_ACTION_CANCEL)
                    self.vip.pubsub.publish('pubsub',
                                            topic=RESERVATION_RESULT_TOPIC,
                                            headers=preempt_headers,
                                            message={
                                                'result': RESERVATION_CANCEL_PREEMPTED,
                                                'info': '',
                                                'data': {
                                                    'agentID': sender,
                                                    'taskID': task_id
                                                }
                                            })
            results = {'result': (RESERVATION_RESPONSE_SUCCESS if result.success else RESERVATION_RESPONSE_FAILURE),
                       'data': (result.data if not result.success else {}),
                       'info': result.info_string}
            self.vip.pubsub.publish('pubsub', topic=RESERVATION_RESULT_TOPIC, headers=headers, message=results)


        elif request_type == RESERVATION_ACTION_CANCEL:
            try:
                result = self.reservation_manager.cancel_reservation(sender, task_id)
                message = {
                    'result': (RESERVATION_RESPONSE_SUCCESS if result.success else RESERVATION_RESPONSE_FAILURE),
                    'info': result.info_string,
                    'data': {}
                }
                topic = RESERVATION_RESULT_TOPIC
                headers = self._get_headers(sender, now, task_id, RESERVATION_ACTION_CANCEL)
                self.vip.pubsub.publish('pubsub', topic, headers=headers, message=message)

            except Exception as ex:
                return self._handle_unknown_reservation_error(ex, headers, message)
        else:
            _log.debug('handle-schedule_request, invalid request type')
            self.vip.pubsub.publish('pubsub', RESERVATION_RESULT_TOPIC, headers, {
                'result': RESERVATION_RESPONSE_FAILURE,
                'info': 'INVALID_REQUEST_TYPE',
                'data': {}
            })

    ################
    # Helper Methods
    ################

    def _equipment_id(self, path: str, point: str = None) -> str:
        """Convert (path, point) pair to full devices/.../point format."""
        path = path.strip('/')
        if point is not None:
            path = '/'.join([path, point])
        if not path.startswith(self.equipment_tree.root):
            path = '/'.join([self.equipment_tree.root, path])
        return path

    @staticmethod
    def _get_headers(requester: str, time: datetime = None, task_id: str = None, action_type: str = None):
        headers = {'time': format_timestamp(time) if time else format_timestamp(get_aware_utc_now())}
        if requester is not None:
            headers['requesterID'] = requester
        if task_id is not None:
            headers['taskID'] = task_id
        if type is not None:
            headers['type'] = action_type
        return headers

    def _handle_error(self, ex: BaseException, point: str, headers: dict):
        if isinstance(ex, RemoteError):
            try:
                exc_type = ex.exc_info['exc_type']
                exc_args = ex.exc_info['exc_args']
            except KeyError:
                exc_type = "RemoteError"
                exc_args = ex.message
            error = {'type': exc_type, 'value': str(exc_args)}
        else:
            error = {'type': ex.__class__.__name__, 'value': str(ex)}
        self._push_result_topic_pair(ERROR_RESPONSE_PREFIX, point, headers, error)
        _log.warning('Error handling subscription: ' + str(error))

    def _handle_unknown_reservation_error(self, ex: BaseException, headers: dict, message: list[list[str]] | list[str]):
        _log.warning(f'bad request: {headers}, {message}, {str(ex)}')
        results = {
            'result': "FAILURE",
            'data': {},
            'info': 'MALFORMED_REQUEST: ' + ex.__class__.__name__ + ': ' + str(ex)
        }
        self.vip.pubsub.publish('pubsub', RESERVATION_RESULT_TOPIC, headers=headers, message=results)
        return results

    @staticmethod
    def _interface_package_from_short_name(interface_name):
        if interface_name.startswith('volttron-lib-') and interface_name.endswith('-driver'):
            return interface_name
        else:
            return f'volttron-lib-{interface_name}-driver'

    def _push_result_topic_pair(self, prefix: str, point: str, headers: dict, value: any):
        topic = normtopic('/'.join([prefix, point]))
        self.vip.pubsub.publish('pubsub', topic, headers, message=value)

    def _split_topic(self, topic: str, point: str = None) -> (str, str):
        """Convert actuator-style optional point names to (path, point) pair."""
        topic = topic.strip('/')
        if not topic.startswith(self.equipment_tree.root):
            topic = '/'.join([self.equipment_tree.root, topic])
        path, point_name = (topic, point) if point is not None else topic.rsplit('/', 1)
        return path, point_name


def main():
    """Main method called to start the agent."""
    vip_main(PlatformDriverAgent, identity=PLATFORM_DRIVER, version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
