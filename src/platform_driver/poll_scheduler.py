import abc
import gevent
import logging

from collections import defaultdict
from datetime import datetime, timedelta
from math import floor, gcd, lcm
from weakref import WeakKeyDictionary, WeakValueDictionary, WeakSet

from volttron.utils import get_aware_utc_now, setup_logging
from volttron.utils.scheduling import periodic

from .agent import PlatformDriverAgent

setup_logging()
_log = logging.getLogger(__name__)


class PollScheduler:
    def __init__(self, agent, **kwargs):
        self.agent: PlatformDriverAgent = agent

    def schedule(self):
        self._prepare_to_schedule()
        self._schedule_polling()

    @staticmethod
    def find_starting_datetime(now: datetime, interval: timedelta, group_delay: float = 0.0):
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_from_midnight = (now - midnight)  # .total_seconds()
        offset = seconds_from_midnight % interval
        if not offset:
            return now
        next_from_midnight = seconds_from_midnight - offset + interval
        return midnight + next_from_midnight + timedelta(seconds=group_delay)

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

class StaticCyclicPollScheduler(PollScheduler):
    def __init__(self, agent, **kwargs):
        super(StaticCyclicPollScheduler, self).__init__(agent, **kwargs)
        # Poll sets has: {remote: {group: {hyperperiod: {slot: WeakSet(points)}}}}
        self.poll_sets = WeakKeyDictionary()

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

    @staticmethod
    def calculate_hyper_period(intervals, minimum_polling_interval):
        return lcm(*[floor(i / minimum_polling_interval) for i in intervals]) * minimum_polling_interval
    # Usage: hyper_period = self.calculate_hyper_period(self.interval_dict.keys(), self.agent.minimum_polling_interval)

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
            point_depth, point_breadth = self.agent.equipment_tree.get_point_topics(p.identifier)
            device_depth, device_breadth = self.agent.equipment_tree.get_device_topics(p.identifier)
            if self.agent.equipment_tree.is_published_single_depth(p.identifier):
                publish_setup['single_depth'].add(point_depth)
            if self.agent.equipment_tree.is_published_single_breadth(p.identifier):
                publish_setup['single_breadth'].add((point_depth, point_breadth))
            if self.agent.equipment_tree.is_published_multi_depth(p.identifier):
                publish_setup['multi_depth'][device_depth].add(point_depth)
            if self.agent.equipment_tree.is_published_multi_breadth(p.identifier):
                publish_setup['multi_breadth'][device_breadth].add(p.identifier)
            # TODO: Uncomment if we are going to allow all-publishes on every poll.
            # if self.agent.equipment_tree.is_published_all_depth(device.identifier):
            #     publish_setup['all_depth'].add(device.identifier)
            # if self.agent.equipment_tree.is_published_all_breadth(device.identifier):
            #     publish_setup['all_breadth'].add(self.agent.equipment_tree.get_device_topics(p.identifier))
        return publish_setup

    def _find_slots(self, interval_dict):
        coprime_interval_sets = self._separate_coprimes(interval_dict.keys())
        slot_plan = defaultdict(lambda: defaultdict(dict))
        for interval_set in coprime_interval_sets:
            hyper_period = self.calculate_hyper_period(interval_set, min(interval_set))
            for interval in interval_set:
                s_count = int(hyper_period / interval)
                if s_count == 1:
                    slots = [0]
                else:
                    slots = [int(hyper_period / s_count * i) for i in range(s_count)]
                for slot in slots:
                    point_dict = WeakValueDictionary({p.identifier: p for p in interval_dict[interval]})
                    slot_plan[timedelta(seconds=hyper_period)][timedelta(seconds=slot)].update({
                        'points': point_dict,
                        'publish_setup': self._build_publish_setup(interval_dict[interval])
                    })  #.add(interval_dict[interval])
        return slot_plan

    def _prepare_to_schedule(self):
        for remote in self.agent.remotes.values():
            # Group points from each of the remote's EquipmentNodes by interval:
            interval_dict = defaultdict(lambda: defaultdict(WeakSet))
            groups = set()
            for point in remote.point_set:
                group = self.agent.equipment_tree.get_group(point.identifier)
                interval_dict[group][self.agent.equipment_tree.get_polling_interval(point.identifier)].add(point)
                groups.add(group)
            # Build poll set for each remote as: {group: {hyperperiod: {slot: WeakSet(points)}}}
            for group in groups:
                if not self.poll_sets.get(remote):
                    self.poll_sets[remote] = {}
                self.poll_sets[remote][group] = self._find_slots(interval_dict[group])

    @staticmethod
    def get_poll_generator(hyperperiod_start, hyperperiod, slot_plan):
        def get_polls(start_time):
            return ((start_time + k, v['points'], v['publish_setup']) for k, v in slot_plan.items())

        polls = get_polls(hyperperiod_start)
        while True:
            try:
                p = next(polls)
            except StopIteration:
                hyperperiod_start += hyperperiod
                polls = get_polls(hyperperiod_start)
                p = next(polls)
            yield p

    def _schedule_polling(self):
        # TODO: How to ensure minimum_polling_interval? Commented code works but isn't very useful. It is simple
        #  to ensure delayed initial_start times, but that doesn't prevent collisions between individual polls in
        #  separate schedules. Is it worth keeping these apart if it requires a check for each slot at schedule time?
        #  Just for one remote? For the whole platform?
        #  Alternately, could create a global lock that oscillates at the minimum_polling_interval and check it at poll
        #  time for the next allowed time to start.
        #minimum_polling_interval = timedelta(seconds=self.agent.config.minimum_polling_interval)
        #_log.debug(f'@@@@@@@@@@ MINIMUM POLLING INTERVAL: {minimum_polling_interval} @@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        #next_available_start_time = get_aware_utc_now()
        for remote, groups in self.poll_sets.items():
            start_times = []
            for group, poll_set in groups.items(): # TODO: Is it a problem that hyperperiods would run in parallel within groups?
                for hyperperiod, slot_plan in poll_set.items():
                    initial_start = self.find_starting_datetime(get_aware_utc_now(), hyperperiod,  # Replace get_aware_utc_now with next_available_start_time?
                                                                group * self.agent.config.group_offset_interval)
         #           next_available_start_time = max(initial_start + minimum_polling_interval, get_aware_utc_now())
                    start_times.append(initial_start)
                    poll_generator = self.get_poll_generator(initial_start, hyperperiod, slot_plan)
                    remote.pollers[hyperperiod] = remote.core.schedule(initial_start, self._operate_polling, remote,
                                                                       hyperperiod, poll_generator)
            last_start = max(start_times)
            for device in remote.equipment:  # TODO: Can we just schedule and let the stale property work its magic?
                _log.debug('@@@@@@@@@@@@ SETTING UP ALL PUBLISHES:')
                if (device.all_publish_interval
                        and self.agent.equipment_tree.is_published_all_depth(device.identifier)
                        or self.agent.equipment_tree.is_published_all_breadth(device.identifier)):
                    _log.debug(f'@@@@@@@@@@@@@@@@ FOUND ONE: {device.identifier}')
                    # TODO: Base Interface and/or remote needs a timeout config. BACnet has this, but not universal.
                    # last_start + timeout should guarantee that the first polls have been made of all points.
                    # TODO: Is this calculation still necessary after adding stale property to points?
                    timeout = timedelta(seconds=30)  # TODO: This seems really long, but it is the default on BACnet.
                    remote.publishers[device] = remote.core.schedule(
                        periodic(device.all_publish_interval, start=last_start+timeout), remote.all_publish, device)

    def _operate_polling(self, remote, poller_id, poll_generator,
                         current_start=None, current_points=None, current_publish_setup=None):
        if current_start is None:
            current_start, current_points, current_publish_setup = next(poll_generator)
        next_start, next_points, next_publish_setup = next(poll_generator)

        # Find the current and next polls where the next poll is the first to still be in the future
        #  (This assures that if the host has gone to sleep, the poll will still be the most up to date):
        now = get_aware_utc_now()
        # TODO: If this takes too long for long pauses,
        #  break get_poll_generator into its own function so it can be called again.
        while next_start <= now:
            current_start, current_points = next_start, next_points
            next_start, next_points, next_publish_setup = next(poll_generator)

        # Schedule next poll:
        remote.pollers[poller_id] = remote.core.schedule(next_start, self._operate_polling, remote, poller_id,
                                                         poll_generator, next_start, next_points, next_publish_setup)
        remote.poll_data(current_points, current_publish_setup)


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

    def _prepare_to_schedule(self):
        # Does it work to organize by group and interval
        pass

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
