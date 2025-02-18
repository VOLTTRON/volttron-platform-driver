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

import fnmatch
import logging

from datetime import timedelta

from volttron.driver.base.interfaces import DriverInterfaceError
from volttron.utils import format_timestamp, get_aware_utc_now, parse_timestamp_string
from volttron.utils.jsonapi import dumps, loads


_log = logging.getLogger(__name__)


class OverrideError(DriverInterfaceError):
    """Error raised when the user tries to set/revert point when global override is set."""
    pass

# TODO: Rework the logic in this class to use new data structures instead of self.instances.
class OverrideManager:
    def __init__(self, parent):
        self.devices = set()
        self.interval_events = {}
        self.agent = parent
        self.patterns = set()

        try:
            values = loads(self.agent.vip.config.get("_override_patterns"))
            if isinstance(values, dict):
                for pattern, end_time in values.items():
                    now = get_aware_utc_now()
                    if end_time == "0.0":   # If end time is indefinite, set override with indefinite duration
                        self.set_on(pattern, 0.0, from_config_store=True)
                    else:
                        end_time = parse_timestamp_string(end_time)
                        if end_time > now:  # If end time > current time, set override with new duration
                            self.set_on(pattern, (end_time - now).total_seconds(), from_config_store=True)
        except KeyError:
            self.patterns = set()
        except ValueError:
            _log.error("Override patterns is not set correctly in config store")
            self.patterns = set()

    def set_on(self,
               pattern,
               duration=0.0,
               failsafe_revert=True,
               staggered_revert=False,
               from_config_store=False):
        """
        Turn on override condition on all devices matching the pattern. It schedules
        an event to keep track of the duration over which override has to be applied.

        :param pattern: Override pattern (wildcard) to be applied, e.g. "devices/some_device/*".
        :param duration: Time duration for the override in seconds. If <= 0.0, it's indefinite.
        :param failsafe_revert: If True, revert points to their default state.
        :param staggered_revert: If True, revert calls are staggered over time.
        :param from_config_store: If True, indicates this method was called from a config store callback.
        """
        _log.debug(f"Setting override on pattern='{pattern}', duration={duration}, "
                   f"failsafe_revert={failsafe_revert}, staggered_revert={staggered_revert}")

        stagger_interval = 0.05  # seconds between staggered reverts

        # Add this pattern to our set of known override patterns
        self.patterns.add(pattern)

        i = 0
        # Loop over each device from the equipment tree
        # 'devices()' might return strings or DeviceNodes
        for device_obj in self.agent.equipment_tree.devices():
            i += 1

            # Convert DeviceNode -> string, if needed
            if hasattr(device_obj, "identifier"):
                device_name = device_obj.identifier  # e.g. "devices/singletestfake"
            else:
                device_name = device_obj  # it might already be a string

            # Match the device_name against the override pattern
            if fnmatch.fnmatch(device_name, pattern):
                _log.debug(f"Override matched device='{device_name}' with pattern='{pattern}'")

                # If failsafe_revert is requested, revert the device
                if failsafe_revert:
                    if staggered_revert:
                        # Stagger this revert call
                        self.agent.core.spawn_later(
                            i * stagger_interval, self.agent.revert, device_name
                        )
                    else:
                        # Immediately revert
                        self.agent.core.spawn(self.agent.revert, device_name)

                # Mark this device as overridden
                self.devices.add(device_name)

        # Set a timer for the override duration (if > 0)
        config_update = self._update_override_interval(duration, pattern)

        # If we changed the override intervals and it wasn't a config store callback,
        # then update our stored override patterns in config store
        if config_update and not from_config_store:
            patterns_dict = {}
            for pat in self.patterns:
                if self.interval_events.get(pat) is None:
                    patterns_dict[pat] = "0.0"
                else:
                    evt, end_time = self.interval_events[pat]
                    patterns_dict[pat] = format_timestamp(end_time)

            self.agent.vip.config.set("_override_patterns", dumps(patterns_dict))

    def set_off(self, pattern):
        """Turn off override condition on all devices matching the pattern. It removes the pattern from the override
        patterns set, clears the list of overridden devices  and reevaluates the state of devices. It then cancels the
        pending override event and removes pattern from the config store.
        :param pattern: Override pattern to be removed.
        :type pattern: str
        """
        # If pattern exactly matches
        if pattern in self.patterns:
            self.patterns.discard(pattern)
            # Cancel any pending override events
            self._cancel_override_events(pattern)
            self.devices.clear()
            patterns = dict()
            # Build override devices list again
            for pat in self.patterns:
                for device in self.agent.equipment_tree.devices():
                    if fnmatch.fnmatch(device, pat):
                        self.devices.add(device)

                if self.interval_events[pat] is None:
                    patterns[pat] = str(0.0)
                else:
                    evt, end_time = self.interval_events[pat]
                    patterns[pat] = format_timestamp(end_time)

            self.agent.vip.config.set("_override_patterns", dumps(patterns))
        else:
            _log.error("Override Pattern did not match!")
            raise OverrideError(
                "Pattern {} does not exist in list of override patterns".format(pattern))

    def clear(self):
        """RPC method

        Clear all overrides.
        """
        # Cancel all pending override timer events
        for pattern, evt in self.interval_events.items():
            if evt is not None:
                evt[0].cancel()
        self.interval_events.clear()
        self.devices.clear()
        self.patterns.clear()
        self.agent.vip.config.set("_override_patterns", {})

    def _update_override_interval(self, interval, pattern):
        """Schedules a new override event for the specified interval and pattern. If the pattern already exists and new
        end time is greater than old one, the event is cancelled and new event is scheduled.

        :param interval override duration. If interval is <= 0.0, implies indefinite duration
        :type pattern: float
        :param pattern: Override pattern.
        :type pattern: str
        :return Flag to indicate if update is done or not.
        """
        if interval <= 0.0:    # indicative of indefinite duration
            if pattern in self.interval_events:
                # If override duration is indefinite, do nothing
                if self.interval_events[pattern] is None:
                    return False
                else:
                    # Cancel the old event
                    evt = self.interval_events.pop(pattern)
                    evt[0].cancel()
            self.interval_events[pattern] = None
            return True
        else:
            override_start = get_aware_utc_now()
            override_end = override_start + timedelta(seconds=interval)
            if pattern in self.interval_events:
                evt = self.interval_events[pattern]
                # If event is indefinite or greater than new end time, do nothing
                if evt is None or override_end < evt[1]:
                    return False
                else:
                    evt = self.interval_events.pop(pattern)
                    evt[0].cancel()
            # Schedule new override event
            event = self.agent.core.schedule(override_end, self.set_off, pattern)
            self.interval_events[pattern] = (event, override_end)
            return True

    def _cancel_override_events(self, pattern):
        """
        Cancel override event matching the pattern
        :param pattern: override pattern
        :type pattern: str
        """
        if pattern in self.interval_events:
            # Cancel the override cancellation timer event
            evt = self.interval_events.pop(pattern, None)
            if evt is not None:
                evt[0].cancel()

    def _update_override_state(self, device, state):
        """
        If a new device is added, it is checked to see if the device is part of the list of overridden patterns. If so,
        it is added to the list of overridden devices. Similarly, if a device is being removed, it is also removed
        from list of overridden devices (if exists).
        :param device: device to be removed
        :type device: str
        :param state: 'add' or 'remove'
        :type state: str
        """
        device = device.lower()

        if state == 'add':
            # If device falls under the existing overridden patterns, then add it to list of overridden devices.
            for pattern in self.patterns:
                if fnmatch.fnmatch(device, pattern):
                    self.devices.add(device)
                    return
        else:
            # If device is in list of overridden devices, remove it.
            if device in self.devices:
                self.devices.remove(device)
