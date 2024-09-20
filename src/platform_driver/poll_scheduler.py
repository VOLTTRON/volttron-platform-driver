import abc
import gevent
import importlib  # TODO: Look into using "get_module", "get_class", "get_subclasses" from volttron.utils.dynamic_helper
import logging

from collections import defaultdict
from datetime import datetime, timedelta
from math import floor, gcd, lcm
from weakref import WeakKeyDictionary, WeakValueDictionary, WeakSet

from volttron.utils import get_aware_utc_now, setup_logging
from volttron.utils.scheduling import periodic

from .config import GroupConfig
from .equipment import EquipmentTree

setup_logging()
_log = logging.getLogger(__name__)


class PollScheduler:
    interval_dicts: dict[str, WeakKeyDictionary] = defaultdict(WeakKeyDictionary)  # Class variable TODO: Needed?

    def __init__(self, data_model, group, group_config, **kwargs):
        self.data_model: EquipmentTree = data_model
        self.group = group
        self.group_config: GroupConfig = group_config

        self.start_all_datetime = get_aware_utc_now()
        self.pollers = {}

    def schedule(self):
        _log.debug(f'@@@@@@@@@ IN SCHEDULE OF POLL_SCHEDULER FOR GROUP: {self.group}')
        self._prepare_to_schedule()
        self._schedule_polling()
        self._start_all_publishes()

    @classmethod
    def setup(cls, data_model, group_configs):
        # TODO: Make remotes and ET both be part of "data_model".
        """
        Sort points from each of the remote's EquipmentNodes by interval:
            Build interval_dict for each group as: {remote: {interval: WeakSet(points)}}}
        """
        # TODO: Instantiate a poll scheduler instance for each group based on the group_configs.
        #  These will need to each call _find_slots() before or at the start of scheduling.
        _log.debug('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ IN P_S.SETUP @@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        _log.debug(f'@@@@@ GROUP CONFIGS IS: {group_configs}')
        for remote in data_model.remotes.values():
            _log.debug(f'@@@@@ REMOTE IS: {remote}')
            interval_dict = defaultdict(lambda: defaultdict(WeakSet))
            groups = set()
            for point in remote.point_set:
                _log.debug(f'@@@@@@ POINT IS: {point}')
                # TODO: get_group needs to handle getting the default group if there is not a group set in the config.
                if data_model.is_active(point.identifier):
                    _log.debug('@@@@@@ ACTIVE @@@@@@@')
                    group = data_model.get_group(point.identifier)
                    _log.debug(f'@@@@@@ GROUP: {group}')
                    interval = data_model.get_polling_interval(point.identifier)
                    _log.debug(f'@@@@@@ INTERVAL: {interval}')
                    interval_dict[group][interval].add(point)
                    groups.add(group)

            for group in groups:
                # TODO: This should now be handled by the default_value of self.interval_dicts. Remove if it works.
                # if not cls.interval_dicts.get(group):
                #     cls.interval_dicts[group] = {}

                cls.interval_dicts[group][remote] = interval_dict[group]
                _log.debug(f'@@@@@@@@@ INTERVAL_DICT IN LOOP IS: {cls.interval_dicts}')
        poll_schedulers = {}
        _log.debug(f'@@@@@@ INTERVAL_DICT OUTSIDE OF LOOP IS: {cls.interval_dicts}')
        for i, group in enumerate(cls.interval_dicts):
            group_config = group_configs.get(group)
            if group_config is None:
                # Create a config for the group based off the default and mimic the old offset multiplier behavior.
                group_config = group_configs['default'].copy()
                group_config.start_offset = group_config.start_offset * i
            poll_scheduler_module = importlib.import_module(group_config.poll_scheduler_module)
            poll_scheduler_class = getattr(poll_scheduler_module, group_config.poll_scheduler_class_name)
            poll_schedulers[group] = poll_scheduler_class(data_model, group, group_config)
        _log.debug(f'@@@@@@@@@@@@@@@@@@@@@@ RETURNING POLL SCHEDULERS AS: {poll_schedulers}')
        return poll_schedulers

    def _build_publish_setup(self, points):
        publish_setup = {
            'single_depth': set(),
            'single_breadth': set(),
            'multi_depth': defaultdict(set),
            'multi_breadth': defaultdict(set) #,
            # 'all_depth': set(),
            # 'all_breadth': set()
        }
        for p in points:
            point_depth, point_breadth = self.data_model.get_point_topics(p.identifier)
            device_depth, device_breadth = self.data_model.get_device_topics(p.identifier)
            if self.data_model.is_published_single_depth(p.identifier):
                publish_setup['single_depth'].add(point_depth)
            if self.data_model.is_published_single_breadth(p.identifier):
                publish_setup['single_breadth'].add((point_depth, point_breadth))
            if self.data_model.is_published_multi_depth(p.identifier):
                publish_setup['multi_depth'][device_depth].add(point_depth)
            if self.data_model.is_published_multi_breadth(p.identifier):
                publish_setup['multi_breadth'][device_breadth].add(p.identifier)
            # TODO: Uncomment if we are going to allow all-publishes on every poll.
            # if self.data_model.is_published_all_depth(device.identifier):
            #     publish_setup['all_depth'].add(device.identifier)
            # if self.data_model.is_published_all_breadth(device.identifier):
            #     publish_setup['all_breadth'].add(self.data_model.get_device_topics(p.identifier))
        return publish_setup

    def _start_all_publishes(self):
        for remote in self.data_model.remotes.values():
            _log.debug(f'@@@@@@@@@@ IN START_ALL_PUBLISHES, REMOTE IS: {remote}')
            for device in remote.equipment:  # TODO: Can we just schedule and let the stale property work its magic?
                _log.debug('@@@@@@@@@@@@ SETTING UP ALL PUBLISHES:')
                if (device.all_publish_interval
                        and self.data_model.is_published_all_depth(device.identifier)
                        or self.data_model.is_published_all_breadth(device.identifier)):
                    _log.debug(f'@@@@@@@@@@@@@@@@ FOUND ONE: {device.identifier}')
                    # TODO: Base Interface and/or remote needs a timeout config. BACnet has this, but not universal.
                    # last_start + timeout should guarantee that the first polls have been made of all points.
                    # TODO: Is this calculation still necessary after adding stale property to points?
                    timeout = timedelta(seconds=30)  # TODO: This seems really long, but it is the default on BACnet.
                    remote.publishers[device] = remote.core.schedule(
                        periodic(device.all_publish_interval, start=self.start_all_datetime+timeout),
                        remote.all_publish, device
                    )

    @staticmethod
    def find_starting_datetime(now: datetime, interval: timedelta, group_delay: timedelta = None):
        group_delay = timedelta(seconds=0.0) if not isinstance(group_delay, timedelta) else group_delay
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_from_midnight = (now - midnight)  # .total_seconds()
        offset = seconds_from_midnight % interval
        if not offset:
            return now
        next_from_midnight = seconds_from_midnight - offset + interval
        return midnight + next_from_midnight + group_delay

    @abc.abstractmethod
    def add_to_schedule(self, point):
        # Add a poll to the schedule, without complete rescheduling if possible.
        pass

    @abc.abstractmethod
    def check_for_reschedule(self):
        # Check whether it is necessary to reschedule after a change.
        pass

    @abc.abstractmethod
    def remove_from_schedule(self, point):
        # Remove a poll from the schedule without rescheduling.
        pass

    @abc.abstractmethod
    def _prepare_to_schedule(self):
        pass

    @abc.abstractmethod
    def _schedule_polling(self):
        pass

    @abc.abstractmethod
    def get_schedule(self):
        pass


class StaticCyclicPollScheduler(PollScheduler):
    def __init__(self, *args, **kwargs):
        super(StaticCyclicPollScheduler, self).__init__(*args, **kwargs)
        # Poll sets has: {remote: {hyperperiod: {slot: WeakSet(points)}}}
        self.poll_sets = []

    def add_to_schedule(self, point):
        # TODO: Implement add_to_schedule.
        pass

    def check_for_reschedule(self):
        # TODO: Implement check_for_reschedule.
        pass

    def remove_from_schedule(self, point):
        # TODO: Implement remove_from_schedule.
        pass
        # OLD DRIVER HAD THIS:
        # bisect.insort(self.freed_time_slots[driver.group], driver.time_slot)
        # self.group_counts[driver.group] -= 1

    def get_schedule(self):
        return_dict = defaultdict(lambda: defaultdict(dict))
        for poll_set in self.poll_sets:
            for hyperperiod, slot_plan in poll_set.items():
                for slot, points in slot_plan.items():
                    return_dict[str(hyperperiod)][str(slot)] = {'remote': points['remote'].unique_id,
                                                                'points': list(points['points'].keys())}
        return return_dict

    @staticmethod
    def calculate_hyper_period(intervals, minimum_polling_interval):
        return lcm(*[floor(i / minimum_polling_interval) for i in intervals]) * minimum_polling_interval
        # Usage: hyper_period = self.calculate_hyper_period(self.interval_dict.keys(), group_config.minimum_polling_interval)

    @staticmethod
    def _separate_coprimes(intervals):
        separated = []
        unseparated = list(intervals)
        unseparated.sort(reverse=True)
        while len(unseparated) > 0:
            non_coprime, coprime = [], []
            first = unseparated.pop(0)
            non_coprime.append(first)
            for i in unseparated:
                if gcd(first, i) == 1 and first != 1 and i != 1:
                    coprime.append(i)
                else:
                    non_coprime.append(i)
            unseparated = coprime
            separated.append(non_coprime)
        return separated

    def _find_slots(self, input_dict):
        coprime_interval_sets = self._separate_coprimes(input_dict.keys())
        slot_plan = defaultdict(lambda: defaultdict(dict))
        for interval_set in coprime_interval_sets:
            hyper_period = self.calculate_hyper_period(interval_set, min(interval_set))
            for interval in interval_set:
                s_count = int(hyper_period / interval)
                for slot, remote in [((interval * i) + (self.group_config.minimum_polling_interval * r), remote)
                                     for i in range(s_count) for r, remote in enumerate(input_dict[interval].keys())]:
                    slot_plan[timedelta(seconds=hyper_period)][timedelta(seconds=slot)] = {
                        # TODO: Does this update work with sub-dictionaries or is this overwriting things?
                        'remote': remote,
                        'points': WeakValueDictionary({p.identifier: p for p in input_dict[interval][remote]}),
                        'publish_setup': self._build_publish_setup(input_dict[interval][remote])
                    }
        return slot_plan

    @staticmethod
    def get_poll_generator(hyperperiod_start, hyperperiod, slot_plan):
        def get_polls(start_time):
            return ((start_time + k, v['points'], v['publish_setup'], v['remote']) for k, v in slot_plan.items())

        polls = get_polls(hyperperiod_start)
        while True:
            try:
                p = next(polls)
            except StopIteration:
                hyperperiod_start += hyperperiod
                polls = get_polls(hyperperiod_start)
                p = next(polls)
            yield p

    def _prepare_to_schedule(self):
        interval_dicts = self.interval_dicts[self.group]
        if self.group_config.parallel_subgroups:
            for remote, interval_dict in interval_dicts.items():
                input_dict = defaultdict(lambda: defaultdict(WeakSet))
                for interval, point_set in interval_dict.items():
                    input_dict[interval][remote] = point_set
                self.poll_sets.append(self._find_slots(input_dict))
        else:
            input_dict = defaultdict(lambda: defaultdict(WeakSet))
            for remote, interval_dict in interval_dicts.items():
                for interval, point_set in interval_dict.items():
                    input_dict[interval][remote] |= point_set
            self.poll_sets.append(self._find_slots(input_dict))

    def _schedule_polling(self):
        # TODO: How to ensure minimum_polling_interval? Commented code works but isn't very useful. It is simple
        #  to ensure delayed initial_start times, but that doesn't prevent collisions between individual polls in
        #  separate schedules. Is it worth keeping these apart if it requires a check for each slot at schedule time?
        #  Just for one remote? For the whole platform?
        #  Alternately, could create a global lock that oscillates at the minimum_polling_interval and check it at poll
        #  time for the next allowed time to start.
        #minimum_polling_interval = timedelta(seconds=group_config.minimum_polling_interval)
        #_log.debug(f'@@@@@@@@@@ MINIMUM POLLING INTERVAL: {minimum_polling_interval} @@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        #next_available_start_time = get_aware_utc_now()
        for poll_set in self.poll_sets: # TODO: Is it a problem that hyperperiods would run in parallel within groups?
            for hyperperiod, slot_plan in poll_set.items():
                initial_start = self.find_starting_datetime(get_aware_utc_now(), hyperperiod,  # Replace get_aware_utc_now with next_available_start_time?
                                                            self.group_config.start_offset)
     #           next_available_start_time = max(initial_start + minimum_polling_interval, get_aware_utc_now())
                self.start_all_datetime = max(self.start_all_datetime, initial_start)
                poll_generator = self.get_poll_generator(initial_start, hyperperiod, slot_plan)
                start, points, publish_setup, remote = next(poll_generator)
                self.pollers[hyperperiod] = remote.core.schedule(start, self._operate_polling, hyperperiod,
                                                                 poll_generator, points, publish_setup, remote)

    def _operate_polling(self, poller_id, poll_generator, current_points, current_publish_setup, current_remote):
        next_start, next_points, next_publish_setup, next_remote = next(poll_generator)

        # Find the current and next polls where the next poll is the first to still be in the future
        #  (This assures that if the host has gone to sleep, the poll will still be the most up to date):
        now = get_aware_utc_now()
        # TODO: If this takes too long for long pauses,
        #  break get_poll_generator into its own function so it can be called again.
        while next_start <= now:
            current_points = next_points
            next_start, next_points, next_publish_setup, next_remote = next(poll_generator)

        # Schedule next poll:
        self.pollers[poller_id] = next_remote.core.schedule(next_start, self._operate_polling, poller_id, poll_generator,
                                                            next_points, next_publish_setup, next_remote)
        current_remote.poll_data(current_points, current_publish_setup)


class SerialPollScheduler(PollScheduler):
    def __init__(self, agent, sleep_duration, **kwargs):
        super(SerialPollScheduler, self).__init__(agent, **kwargs)
        self.sleep_duration = sleep_duration

        self.status = {}

    def add_to_schedule(self, point):
        # TODO: Implement add_to_schedule.
        pass

    def check_for_reschedule(self):
        # TODO: Implement check_for_reschedule.
        pass

    def remove_from_schedule(self, point):
        # TODO: Implement remove_from_schedule.
        pass
        # OLD DRIVER HAD THIS:
        # bisect.insort(self.freed_time_slots[driver.group], driver.time_slot)
        # self.group_counts[driver.group] -= 1

    # TODO: Serial Poll Scheduler (schedule a single job to poll each item of poll set after the return or failure
    #  of the previous):
    #  Create timeouts such that enough time is available to address each item in series before the next cycle.
    def _schedule_polling(self):
        pass

    # TODO: If there is not sufficient difference in the prepare and schedule methods,
    #  this could be a separate operate method in the StaticCyclicPollScheduler.
    def _operate_polling(self,  remote, poller_id, poll_generator):
        while True:  # TODO: This should probably check whether agent is stopping.
            start, points = next(poll_generator)
            poll = gevent.spawn(remote.poll_data, points)
            # Wait for poll to finish.
            while not poll.ready():
                gevent.sleep(self.sleep_duration)
            # Track whether this poller_id has been successful.
            # TODO: Would it be more helpful if the poll_data method returned the time (when it is successful) or None?
            self.status[poller_id] = poll.get(timeout=1)
