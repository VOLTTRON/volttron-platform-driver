import abc

from collections import defaultdict
from math import floor, gcd, lcm
from weakref import WeakKeyDictionary, WeakSet

from volttron.driver.base.driver import DriverAgent

from .agent import PlatformDriverAgent


class PollScheduler:
    def __init__(self, agent, **kwargs):
        self.agent: PlatformDriverAgent = agent

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

    @abc.abstractmethod
    def _prepare_to_schedule(self):
        pass


class StaticCyclicPollScheduler(PollScheduler):
    def __init__(self, agent, **kwargs):
        super(StaticCyclicPollScheduler, self).__init__(agent, **kwargs)
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

    def schedule(self):
        pass
    # TODO: Implement Schedule:
    #     super(StaticCyclicPollScheduler, self).schedule()
    #     if controller_group.type == ControllerGroupingType['Parallel']:
    #         # TODO: Parallel Regime (schedule each controller’s poll set individually):
    #         #           For each poll set create NSi = NDi + 1 slots.
    #         #           Spread polls for each device evenly through its own Nsi.
    #         pass
    #     elif controller_group.type == ControllerGroupingType['Sequential']:
    #         group_poll_set = self._combine_poll_sets([self.poll_sets[c] for c in controller_group.controllers])
    #         # TODO: Sequential Regime (schedule all polls for all controllers in one sequential poll set):
    #         #           Create NS = ∑ NDi + 1 slots over max interval of all D.
    #         #           Spread polls for each device evenly through NS, starting with highest frequency polls.
    #
    #         pass
    #     elif controller_group.type == ControllerGroupingType['Serial']:
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
    def separate_coprimes(intervals):
        separated = []
        unseparated = intervals.copy()
        unseparated.sort(reverse=True)
        while len(unseparated) > 0:
            non_coprime, coprime = [], []
            first = unseparated.pop(0)
            non_coprime.append(first)
            for i in unseparated:
                if gcd(first, i) == 1:
                    coprime.append(i)
                else:
                    non_coprime.append(i)
            unseparated = coprime
            separated.append(non_coprime)
        return separated

    def _prepare_to_schedule(self):
        for controller in self.agent.controllers:
            # Group points from each of the controller's EquipmentNodes by interval:
            interval_dict = defaultdict(WeakSet)
            for point in controller.point_set:
                interval_dict[point.polling_interval].add(point)
            self.poll_sets[controller] = interval_dict

    @staticmethod
    def _combine_poll_sets(poll_set_list):
        one = poll_set_list.pop(0)
        for another in poll_set_list:
            one = {k: one[k].union(another[k]) for k in list(one.keys()) + list(another.keys())}
        return one
