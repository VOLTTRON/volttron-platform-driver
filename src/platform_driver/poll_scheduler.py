import abc
import gevent
import importlib  # TODO: Look into using "get_module", "get_class", "get_subclasses" from volttron.utils.dynamic_helper
import logging

from collections import defaultdict
from datetime import datetime, timedelta
from functools import reduce
from math import floor, gcd, lcm
from weakref import WeakKeyDictionary, WeakValueDictionary

from volttron.client.vip.agent.core import ScheduledEvent
from volttron.utils import get_aware_utc_now

from platform_driver.config import GroupConfig
from platform_driver.equipment import EquipmentTree, PointNode


_log = logging.getLogger(__name__)


class PollScheduler:
    interval_dicts: dict[str, WeakKeyDictionary] = defaultdict(WeakKeyDictionary)

    def __init__(self, data_model: EquipmentTree, group: str, group_config: GroupConfig, **kwargs):
        self.data_model: EquipmentTree = data_model
        self.group: str = group
        self.group_config: GroupConfig = group_config

        self.start_all_datetime: datetime = get_aware_utc_now()
        self.pollers: dict[str, ScheduledEvent] = {}

    def schedule(self):
        self._prepare_to_schedule()
        self._schedule_polling()

    @classmethod
    def setup(cls, data_model: EquipmentTree, group_configs: dict[str, GroupConfig]):
        """
        Sort points from each of the remote's EquipmentNodes by interval:
            Build cls.interval_dict  as: {group: {remote: {interval: WeakSet(points)}}}}
        """
        cls._build_interval_dict(data_model)
        poll_schedulers = cls.create_poll_schedulers(data_model, group_configs)
        return poll_schedulers

    @classmethod
    def create_poll_schedulers(cls, data_model: EquipmentTree, group_configs,
                               specific_groups: list[str] = None, existing_group_count: int = 0):
        poll_schedulers = {}
        groups = specific_groups if specific_groups else cls.interval_dicts
        for i, group in enumerate(groups):
            group_config = group_configs.get(group)
            if group_config is None:
                # Create a config for the group with default settings and mimic the old offset multiplier behavior.
                group_config: GroupConfig = GroupConfig()
                # TODO: Should start_offset instead be either a default offset * i or the specified start_offset if it is there?
                group_config.start_offset = group_config.start_offset * (i + existing_group_count)
                group_configs[group] = group_config  # Add this new config back to the agent settings.
                # TODO: Save the agent settings afterwards so this group gets the same config next time?
            poll_scheduler_module = importlib.import_module(group_config.poll_scheduler_module)
            poll_scheduler_class = getattr(poll_scheduler_module, group_config.poll_scheduler_class_name)
            poll_schedulers[group] = poll_scheduler_class(data_model, group, group_config)
        return poll_schedulers

    @classmethod
    def _build_interval_dict(cls, data_model: EquipmentTree):
        for remote in data_model.remotes.values():
            interval_dict = defaultdict(lambda: defaultdict(dict))
            groups = set()
            for point in remote.point_set:
                if data_model.is_active(point.identifier):
                    group = data_model.get_group(point.identifier)
                    interval = data_model.get_polling_interval(point.identifier)
                    if 'points' not in interval_dict[group][interval]:
                        interval_dict[group][interval]['points'] = WeakValueDictionary()
                    interval_dict[group][interval]['points'][point.identifier] = point
                    # noinspection PyTypeChecker
                    if 'publish_setup' not in interval_dict[group][interval]:
                        interval_dict[group][interval]['publish_setup'] = cls._setup_publish(point, data_model, None)
                    cls._setup_publish(point, data_model, interval_dict[group][interval]['publish_setup'])
                    groups.add(group)

            for group in groups:
                # Remote level is assigned separately because we don't have the option for a default-WeakKeyDictionary.
                cls.interval_dicts[group][remote] = interval_dict[group]

    @classmethod
    def _setup_publish(cls, point: PointNode, data_model: EquipmentTree, publish_setup: dict = None):
        if publish_setup is None:
            publish_setup = {
                'single_depth': set(),
                'single_breadth': set(),
                'multi_depth': defaultdict(set),
                'multi_breadth': defaultdict(set)
            }
        point_depth, point_breadth = data_model.get_point_topics(point.identifier)
        device_depth, device_breadth = data_model.get_device_topics(point.identifier)
        if data_model.is_published_single_depth(point.identifier):
            publish_setup['single_depth'].add(point_depth)
        if data_model.is_published_single_breadth(point.identifier):
            publish_setup['single_breadth'].add((point_depth, point_breadth))
        if data_model.is_published_multi_depth(point.identifier):
            publish_setup['multi_depth'][device_depth].add(point_depth)
        if data_model.is_published_multi_breadth(point.identifier):
            publish_setup['multi_breadth'][device_breadth].add(point.identifier)
        return publish_setup

    @staticmethod
    def find_starting_datetime(now: datetime, interval: timedelta, group_delay: timedelta = None):
        group_delay = timedelta(seconds=0.0) if not isinstance(group_delay, timedelta) else group_delay
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_from_midnight = (now - midnight)
        offset = seconds_from_midnight % interval
        if not offset:
            return now + interval + group_delay
        next_from_midnight = seconds_from_midnight - offset + interval
        return midnight + next_from_midnight + group_delay

    @classmethod
    def add_to_schedule(cls, point: PointNode, data_model: EquipmentTree):
        """Add a poll to the schedule, without complete rescheduling if possible."""
        group = data_model.get_group(point.identifier)
        remote = data_model.get_remote(point.identifier)
        interval = data_model.get_polling_interval(point.identifier)
        reschedule_required = (group not in cls.interval_dicts
                               or remote not in cls.interval_dicts[group]
                               or interval not in cls.interval_dicts[group][remote])
        cls.interval_dicts[group][remote][interval][point.identifier] = point
        return reschedule_required

    @classmethod
    def remove_from_schedule(cls, point: PointNode, data_model: EquipmentTree):
        """Remove a poll from the schedule without rescheduling."""
        group = data_model.get_group(point.identifier)
        remote = data_model.get_remote(point.identifier)
        interval = data_model.get_polling_interval(point.identifier)
        success = cls.interval_dicts[group][remote][interval].pop(point.identifier, None)
        cls._prune_interval_dict(group, interval, remote)
        return True if success else False

    @classmethod
    def _prune_interval_dict(cls, group, interval, remote):
        if not cls.interval_dicts[group][remote][interval]:
            cls.interval_dicts[group][remote].pop('interval')
            if not cls.interval_dicts[group][remote]:
                cls.interval_dicts[group].pop(remote)
                if not cls.interval_dicts[group]:
                    cls.interval_dicts.pop(group)

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

    def get_schedule(self):
        """Return the calculated schedules to the user."""
        return_dict = defaultdict(lambda: defaultdict(dict))
        for poll_set in self.poll_sets:
            for hyperperiod, slot_plan in poll_set.items():
                for slot, points in slot_plan.items():
                    remote = str(points['remote'].unique_id)
                    return_dict[str(hyperperiod)][str(slot)][remote] = [p.split("/")[-1] for p in points['points'].keys()]
        return return_dict

    @staticmethod
    def calculate_hyperperiod(intervals, minimum_polling_interval):
        return lcm(*[floor(i / minimum_polling_interval) for i in intervals]) * minimum_polling_interval
        

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

    def _find_slots(self, input_dict, parallel_remote_index: int = 0):
        coprime_interval_sets = self._separate_coprimes(input_dict.keys())
        slot_plan = defaultdict(lambda: defaultdict(defaultdict))
        parallel_offset = parallel_remote_index * self.group_config.minimum_polling_interval
        min_spread = self.group_config.minimum_polling_interval
        all_remotes = {k for i in input_dict for k in input_dict[i].keys()}
        min_interval = min(input_dict.keys())
        min_remote_offset = min_interval / len(all_remotes)
        if self.group_config.parallel_subgroups and min_remote_offset < min_spread:
            _log.warning(f'There are {len(all_remotes)} scheduled sequentially with a smallest interval of'
                         f' {min_interval}. This only allows {min_remote_offset} between polls --- less than'
                         f' the group {self.group} minimum_polling_interval of {min_spread}. The resulting schedule is'
                         f' likely to result in unexpected behavior and potential loss of data if these remotes share'
                         f' a collision domain. If the minimum polling interval cannot be lowered, consider polling'
                         f' less frequently.')
        remote_offsets = {r: i * min_remote_offset for i, r in enumerate(all_remotes)}
        for interval_set in coprime_interval_sets:
            hyperperiod = self.calculate_hyperperiod(interval_set, min(interval_set))
            for interval in interval_set:
                s_count = int(hyperperiod / interval)
                remote_spread = interval / len(input_dict[interval].keys())
                spread = min_spread if self.group_config.parallel_subgroups else max(min_spread, remote_spread)
                for slot, remote in [((interval * i) + (spread * r) + remote_offsets[remote] + parallel_offset , remote)
                                     for i in range(s_count) for r, remote in enumerate(input_dict[interval].keys())]:
                    plan = slot_plan[timedelta(seconds=hyperperiod)][timedelta(seconds=slot)]
                    if not plan.get('points'):
                        plan['points'] = []
                    if not plan.get('publish_setup'):
                        plan['publish_setup'] = []
                    plan['remote'] = remote
                    plan['points'].extend([x['points'] for x in input_dict[interval][remote]])
                    plan['publish_setup'].extend([x['publish_setup'] for x in input_dict[interval][remote]])
        return {hyperperiod: dict(sorted(sp.items())) for hyperperiod, sp in slot_plan.items()}

    @staticmethod
    def get_poll_generator(hyperperiod_start, hyperperiod, slot_plan):
        def get_polls(start_time):
            # Union of points and publish_setups is here to get any changes to the interval_dict at start of hyperperiod.
            return ((start_time + k,
                     reduce(lambda d1, d2: d1 | d2, v['points']),
                     reduce(lambda d1, d2: d1 | d2, v['publish_setup']),
                     v['remote']
                     ) for k, v in slot_plan.items())
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
            for parallel_index, (remote, interval_dict) in enumerate(interval_dicts.items()):
                input_dict = defaultdict(lambda: defaultdict(list))
                for interval, point_set in interval_dict.items():  # TODO: point_set is a bad name. This is now a dict of dicts: {'points' {}, 'publish_setup': {}}
                    input_dict[interval][remote].append(point_set)
                self.poll_sets.append(self._find_slots(input_dict, parallel_index))
        else:
            input_dict = defaultdict(lambda: defaultdict(list))
            for remote, interval_dict in interval_dicts.items():
                for interval, point_set in interval_dict.items():
                    input_dict[interval][remote].append(point_set)
            self.poll_sets.append(self._find_slots(input_dict))

    def _schedule_polling(self):
        # TODO: How to fully ensure min_polling_interval? Nothing yet prevents collisions between individual polls in
        #  separate schedules. Is it worth keeping these apart if it requires a check for each slot at schedule time?
        #  Or, create global lock oscillating at min_poll_interval - check on poll for the next allowed start time?
        _log.debug('In _schedule_polling, poll sets is: ')
        _log.debug(self.poll_sets)
        for poll_set in self.poll_sets:
            for hyperperiod, slot_plan in poll_set.items():
                initial_start = self.find_starting_datetime(get_aware_utc_now(), hyperperiod,
                                                            self.group_config.start_offset)
                self.start_all_datetime = max(self.start_all_datetime, initial_start + hyperperiod)
                poll_generator = self.get_poll_generator(initial_start, hyperperiod, slot_plan)
                start, points, publish_setup, remote = next(poll_generator)
                _log.info(f'Scheduled polling for {self.group}--{hyperperiod} starts at {start.time()}')
                self.pollers[hyperperiod] = remote.core.schedule(start, self._operate_polling, hyperperiod,
                                                                 poll_generator, points, publish_setup, remote)

    def _operate_polling(self, poller_id, poll_generator, current_points, current_publish_setup, current_remote):
        next_start, next_points, next_publish_setup, next_remote = next(poll_generator)

        # Find the current and next polls where the next poll is the first to still be in the future
        #  (This assures that if the host has gone to sleep, the poll will still be the most up to date):
        now = get_aware_utc_now()
        while next_start <= now:
            # TODO: If this takes too long for long pauses, call get_poll_generator again, instead.
            _log.warning(f'Skipping polls from {next_start} to {now} to catch up to the current time.')
            current_points = next_points
            next_start, next_points, next_publish_setup, next_remote = next(poll_generator)

        # Schedule next poll:
        if next_points:
            self.pollers[poller_id] = next_remote.core.schedule(next_start, self._operate_polling, poller_id,
                                                                poll_generator, next_points, next_publish_setup,
                                                                next_remote)
        else:
            _log.info(f'Stopping polling loop of {poller_id} points on {next_remote.unique_id}.'
                      f' There are no points in this request set to poll.')
        current_remote.poll_data(current_points, current_publish_setup)


class SerialPollScheduler(PollScheduler):
    def get_schedule(self):
        pass

    def _prepare_to_schedule(self):
        pass

    def __init__(self, agent, sleep_duration, **kwargs):
        super(SerialPollScheduler, self).__init__(agent, **kwargs)
        self.sleep_duration = sleep_duration

        self.status = {}

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
