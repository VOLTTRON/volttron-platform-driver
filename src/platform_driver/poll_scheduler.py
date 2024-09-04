import abc

from collections import defaultdict, namedtuple
from datetime import timedelta
from math import floor, gcd, lcm
from weakref import WeakKeyDictionary, WeakValueDictionary, WeakSet

from volttron.utils.time import get_aware_utc_now

from .agent import PlatformDriverAgent

slot_schedule = namedtuple('slot_schedule', ('start', 'topics', 'points'))

class PollScheduler:
    def __init__(self, agent, **kwargs):
        self.agent: PlatformDriverAgent = agent
        # Poll sets has: {remote: {hyperperiod: {slot: WeakSet(points)}}}
        self.poll_sets = WeakKeyDictionary()

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

    def schedule(self, **kwargs):
        self._prepare_to_schedule()
        for remote, poll_set in self.poll_sets.items():
            remote.shedule_polling(poll_set)

    @abc.abstractmethod
    def _prepare_to_schedule(self):
        pass

    @staticmethod
    def find_starting_datetime(now, interval, group_delay=0.0):
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_from_midnight = (now - midnight).total_seconds()
        offset = seconds_from_midnight % interval
        if not offset:
            return now
        next_from_midnight = timedelta(seconds=(seconds_from_midnight - offset + interval))
        return midnight + next_from_midnight + timedelta(seconds=group_delay)


class StaticCyclicPollScheduler(PollScheduler):
    def __init__(self, agent, **kwargs):
        super(StaticCyclicPollScheduler, self).__init__(agent, **kwargs)

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

    def schedule(self):
        pass
    # TODO: Implement Schedule:
    #     super(StaticCyclicPollScheduler, self).schedule()
    #     if remote_group.type == RemoteGroupingType['Parallel']:
    #         # TODO: Parallel Regime (schedule each remote’s poll set individually):
    #         #           For each poll set create NSi = NDi + 1 slots.
    #         #           Spread polls for each device evenly through its own Nsi.
    #         pass
    #     elif remote_group.type == RemoteGroupingType['Sequential']:
    #         group_poll_set = self._combine_poll_sets([self.poll_sets[c] for c in remote_group.remotes])
    #         # TODO: Sequential Regime (schedule all polls for all remotes in one sequential poll set):
    #         #           Create NS = ∑ NDi + 1 slots over max interval of all D.
    #         #           Spread polls for each device evenly through NS, starting with highest frequency polls.
    #
    #         pass
    #     elif remote_group.type == RemoteGroupingType['Serial']:
    #         # TODO: Serial Regime (schedule a single job to poll each item of poll set after the return or failure
    #         #  of the previous):
    #         #  Create timeouts such that enough time is available to address each item in series before the next cycle.
    #         #  This may require additional code at the DriverAgent level to poll this way.
    #         pass
    #     else:
    #         pass

    @staticmethod
    def calculate_hyper_period(intervals, minimum_polling_interval):
        return lcm(*[floor(i / minimum_polling_interval) for i in intervals]) * minimum_polling_interval
    # Usage: hyper_period = self.calculate_hyper_period(self.interval_dict.keys(), self.agent.minimum_polling_interval)

    @staticmethod
    def _separate_coprimes(intervals):
        separated = []
        unseparated = intervals.copy()
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

    def _find_slots(self, interval_dict):
        coprime_interval_sets = self._separate_coprimes(interval_dict.keys())
        slot_plan = defaultdict(lambda: defaultdict(WeakValueDictionary)) # TODO: Check change from WeakSet to WVDict works.
        for interval_set in coprime_interval_sets:
            hyper_period = self.calculate_hyper_period(interval_set, min(interval_set))
            for interval in interval_set:
                s_count = int(hyper_period / interval)
                if s_count == 1:
                    slots = [0]
                else:
                    slots = [int(hyper_period / s_count * i) for i in range(s_count)]
                for slot in slots:
                    point_dict = {p.identifier: p for p in interval_dict[interval]}
                    slot_plan[hyper_period][timedelta(seconds=slot)].update(point_dict)  #.add(interval_dict[interval])
        return slot_plan

    def _prepare_to_schedule(self):
        for remote in self.agent.remotes:
            # Group points from each of the remote's EquipmentNodes by interval:
            interval_dict = defaultdict(WeakSet)
            for point in remote.point_set:
                interval_dict[point.polling_interval].add(point)
            # Build poll set for each remote as: {hyperperiod: {slot: WeakSet(points)}}
            self.poll_sets[remote] = self._find_slots(interval_dict)

    # TODO: This or _prepare_to_schedule() should take account of group numbers too. Make extra "groupings" for them?
    # TODO: This can probably be pushed up to base class by calling "hyperperiod" "grouping" or something.
    def schedule_polling(self):
        for remote, poll_set in self.poll_sets.items():
            for hyperperiod, slot_plan in poll_set.items():
                initial_start = self.find_starting_datetime(get_aware_utc_now(), hyperperiod)
                # TODO: This is a sequential regime. Should we have another for serial regimes? Just get_polls part?
                def get_poll_generator(hyperperiod_start):
                    def get_polls(start_time):
                        return ((start_time + k, v) for k, v in slot_plan.items())

                    polls = get_polls(hyperperiod_start)
                    while True:
                        try:
                            p = next(polls)
                        except StopIteration:
                            hyperperiod_start += hyperperiod
                            polls = get_polls(hyperperiod_start)
                            p = next(polls)
                        yield p
                poll_generator = get_poll_generator(initial_start)
                # TODO: Should this instead call a registration function in the remote?
                remote.pollers[hyperperiod] = remote.core.schedule(initial_start, remote.periodic_read,
                                                                   hyperperiod, initial_start, poll_generator)

    # TODO: If this function is useful, needs to be updated to {'remote': {'hyper_period': {'slot': {points}}}}
    # @staticmethod
    # def _combine_poll_sets(poll_set_list):
    #     one = poll_set_list.pop(0)
    #     for another in poll_set_list:
    #         one = {k: one[k].union(another[k]) for k in list(one.keys()) + list(another.keys())}
    #     return one
