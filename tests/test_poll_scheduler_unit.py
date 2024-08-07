import pytest
from unittest.mock import Mock
from collections import defaultdict
from platform_driver.poll_scheduler import StaticCyclicPollScheduler
from platform_driver.agent import PlatformDriverAgent


class TestStaticCyclicPollScheduler:
    @pytest.fixture
    def scheduler(self):
        agent_mock = Mock(spec=PlatformDriverAgent)
        return StaticCyclicPollScheduler(agent=agent_mock)

    def test_calculate_hyper_period(self, scheduler):
        intervals = [4, 5, 6]
        minimum_polling_interval = 1
        result = scheduler.calculate_hyper_period(intervals, minimum_polling_interval)
        assert result == 60  # LCM of 4, 5, 6 is 60

    def test_prepare_to_schedule(self, scheduler):
        remote_mock = Mock()
        point1 = Mock(polling_interval=5)
        point2 = Mock(polling_interval=10)
        remote_mock.point_set = [point1, point2]
        scheduler.agent.remotes = [remote_mock]

        scheduler._prepare_to_schedule()

        interval_dict = scheduler.poll_sets[remote_mock]

        assert remote_mock in scheduler.poll_sets
        assert 5 in interval_dict and 10 in interval_dict
        assert point1 in interval_dict[5]
        assert point2 in interval_dict[10]

    def test_combine_poll_sets(self, scheduler):
        point1, point2, point3, point4 = Mock(), Mock(), Mock(), Mock()
        poll_set1 = defaultdict(set, {5: {point1, point2}, 10: {point3}})
        poll_set2 = defaultdict(set, {5: {point4}, 15: {Mock()}})
        poll_sets = [poll_set1, poll_set2]

        combined = scheduler._combine_poll_sets(poll_sets)
        assert set(combined.keys()) == {5, 10, 15}
        assert combined[5] == {point1, point2, point4}  # Properly combined sets
        assert combined[10] == {point3}
        assert len(combined[15]) == 1  # One point in poll_set2 for interval 15

    # TODO create tests for separate_coprimes
