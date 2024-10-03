import abc
import gevent
import importlib  # TODO: Look into using "get_module", "get_class", "get_subclasses" from volttron.utils.dynamic_helper
import logging

from collections import defaultdict
from datetime import datetime, timedelta
from math import floor, gcd, lcm
from weakref import WeakKeyDictionary, WeakValueDictionary, WeakSet

from volttron.client.vip.agent.core import ScheduledEvent
from volttron.utils import get_aware_utc_now

from .config import GroupConfig
from .equipment import EquipmentTree


_log = logging.getLogger(__name__)


class PollScheduler:
    interval_dicts: dict[str, WeakKeyDictionary] = defaultdict(WeakKeyDictionary)  # Class variable TODO: Needed?

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
        cls.build_interval_dict(data_model)
        poll_schedulers = cls._create_poll_schedulers(data_model, group_configs)
        return poll_schedulers

    @classmethod
    def _create_poll_schedulers(cls, data_model, group_configs):
        poll_schedulers = {}
        for i, group in enumerate(cls.interval_dicts):
            group_config = group_configs.get(group)
            if group_config is None:
                # Create a config for the group with default settings and mimic the old offset multiplier behavior.
                group_config: GroupConfig = GroupConfig()
                group_config.start_offset = group_config.start_offset * i
                group_configs[group] = group_config  # Add this new config back to the agent settings.
                # TODO: Save the agent settings afterwards so this group gets the same config next time?
            poll_scheduler_module = importlib.import_module(group_config.poll_scheduler_module)
            poll_scheduler_class = getattr(poll_scheduler_module, group_config.poll_scheduler_class_name)
            poll_schedulers[group] = poll_scheduler_class(data_model, group, group_config)
        return poll_schedulers

    @classmethod
    def build_interval_dict(cls, data_model: EquipmentTree):
        for remote in data_model.remotes.values():
            interval_dict = defaultdict(lambda: defaultdict(WeakSet))
            groups = set()
            for point in remote.point_set:
                if data_model.is_active(point.identifier):
                    group = data_model.get_group(point.identifier)
                    interval = data_model.get_polling_interval(point.identifier)
                    interval_dict[group][interval].add(point)
                    groups.add(group)

            for group in groups:
                # TODO: Was there a reason this isn't just assigned this way three lines above?
                cls.interval_dicts[group][remote] = interval_dict[group]

    def _setup_publish(self, points, publish_setup=None):
        if publish_setup is None:
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

    @staticmethod
    def find_starting_datetime(now: datetime, interval: timedelta, group_delay: timedelta = None):
        group_delay = timedelta(seconds=0.0) if not isinstance(group_delay, timedelta) else group_delay
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_from_midnight = (now - midnight)  # .total_seconds()
        offset = seconds_from_midnight % interval
        if not offset:
            return now + interval + group_delay
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
                        plan['points'] = WeakValueDictionary()
                    plan['remote'] = remote
                    plan['points'].update({p.identifier: p for p in input_dict[interval][remote]})
                    plan['publish_setup'] = self._setup_publish(input_dict[interval][remote], plan.get('publish_setup'))
        return {hyperperiod: dict(sorted(sp.items())) for hyperperiod, sp in slot_plan.items()}

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
            for parallel_index, (remote, interval_dict) in enumerate(interval_dicts.items()):
                input_dict = defaultdict(lambda: defaultdict(WeakSet))
                for interval, point_set in interval_dict.items():
                    input_dict[interval][remote] = point_set
                self.poll_sets.append(self._find_slots(input_dict, parallel_index))
        else:
            input_dict = defaultdict(lambda: defaultdict(WeakSet))
            for remote, interval_dict in interval_dicts.items():
                for interval, point_set in interval_dict.items():
                    input_dict[interval][remote] |= point_set
            self.poll_sets.append(self._find_slots(input_dict))

    def _schedule_polling(self):
        # TODO: How to fully ensure min_polling_interval? Nothing yet prevents collisions between individual polls in
        #  separate schedules. Is it worth keeping these apart if it requires a check for each slot at schedule time?
        #  Or, create global lock oscillating at min_poll_interval - check on poll for the next allowed start time?
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
        self.pollers[poller_id] = next_remote.core.schedule(next_start, self._operate_polling, poller_id, poll_generator,
                                                            next_points, next_publish_setup, next_remote)
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
