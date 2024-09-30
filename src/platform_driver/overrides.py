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
        self.parent = parent
        self.patterns = set()

        try:
            values = loads(self.parent.vip.config.get("_override_patterns"))
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
        """Turn on override condition on all devices matching the pattern. It schedules an event to keep track of
        the duration over which override has to be applied. New override patterns and corresponding end times are
        stored in config store.
        :param pattern: Override pattern to be applied. For example,
        :type pattern: str
        :param duration: Time duration for the override in seconds. If duration <= 0.0, it implies as indefinite
        duration.
        :type duration: float
        :param failsafe_revert: Flag to indicate if revert is required
        :type failsafe_revert: boolean
        :param staggered_revert: Flag to indicate if staggering of reverts is needed.
        :type staggered_revert: boolean
        :param from_config_store: Flag to indicate if this function is called from config store callback
        :type from_config_store: boolean
        """
        stagger_interval = 0.05    # sec
        # Add to override patterns set
        self.patterns.add(pattern)
        i = 0
        for name in self.instances.keys():
            i += 1
            if fnmatch.fnmatch(name, pattern):
                # If revert to default state is needed
                if failsafe_revert:
                    if staggered_revert:
                        self.parent.core.spawn_later(i * stagger_interval, self.instances[name].revert_all())
                    else:
                        self.parent.core.spawn(self.instances[name].revert_all())
                # Set override
                self.devices.add(name)
        # Set timer for interval of override condition
        config_update = self._update_override_interval(duration, pattern)
        if config_update and not from_config_store:
            # Update config store
            patterns = dict()
            for pat in self.patterns:
                if self.interval_events[pat] is None:
                    patterns[pat] = str(0.0)
                else:
                    evt, end_time = self.interval_events[pat]
                    patterns[pat] = format_timestamp(end_time)

            self.parent.vip.config.set("_override_patterns", dumps(patterns))

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
                for device in self.instances:
                    if fnmatch.fnmatch(device, pat):
                        self.devices.add(device)

                if self.interval_events[pat] is None:
                    patterns[pat] = str(0.0)
                else:
                    evt, end_time = self.interval_events[pat]
                    patterns[pat] = format_timestamp(end_time)

            self.parent.vip.config.set("_override_patterns", dumps(patterns))
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
        self.parent.vip.config.set("_override_patterns", {})

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
            event = self.parent.core.schedule(override_end, self.set_off, pattern)
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
