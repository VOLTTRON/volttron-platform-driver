from datetime import datetime, timedelta, timezone
from weakref import WeakSet
import pytest
from unittest.mock import MagicMock, Mock, patch, call, ANY
from collections import defaultdict
from weakref import WeakValueDictionary
from volttron.driver.base.driver import DriverAgent
from platform_driver.equipment import EquipmentTree, PointNode
from volttron.utils import get_aware_utc_now

from platform_driver.poll_scheduler import (PollSet, PollScheduler, StaticCyclicPollScheduler,
                                            SerialPollScheduler, GroupConfig, EquipmentTree)


def test_poll_scheduler_init():
    """
    Test that PollScheduler initializes correctly with given parameters.
    """
    data_model = MagicMock(spec=EquipmentTree)
    group = 'test_group'
    group_config = MagicMock(spec=GroupConfig)

    scheduler = PollScheduler(data_model, group, group_config)

    assert scheduler.data_model == data_model
    assert scheduler.group == group
    assert scheduler.group_config == group_config
    assert isinstance(scheduler.start_all_datetime, datetime)
    assert scheduler.pollers == {}


def test_poll_scheduler_schedule():
    """
    Test that schedule method calls the internal scheduling methods.
    """
    scheduler = PollScheduler(MagicMock(), 'test_group', MagicMock())
    scheduler._prepare_to_schedule = MagicMock()
    scheduler._schedule_polling = MagicMock()

    scheduler.schedule()

    scheduler._prepare_to_schedule.assert_called_once()
    scheduler._schedule_polling.assert_called_once()


class TestPollSchedulerSetupPart1:

    @pytest.fixture
    def mock_data_model(self):
        """
        Fixture to create a mock data_model with necessary attributes and methods.
        """
        data_model = MagicMock(spec=EquipmentTree)

        mock_remote1 = MagicMock()
        mock_remote1.point_set = [MagicMock(identifier='point1'), MagicMock(identifier='point2')]

        mock_remote2 = MagicMock()
        mock_remote2.point_set = [MagicMock(identifier='point3'), MagicMock(identifier='point4')]

        data_model.remotes = {'remote1': mock_remote1, 'remote2': mock_remote2}

        def is_active_side_effect(identifier):
            return identifier in ['point1', 'point3']    # Only point1 and point3 are active

        def get_group_side_effect(identifier):
            return 'group1' if identifier == 'point1' else 'group2'

        def get_polling_interval_side_effect(identifier):
            return timedelta(seconds=3600) if identifier == 'point1' else timedelta(seconds=1800)

        data_model.is_active.side_effect = is_active_side_effect
        data_model.get_group.side_effect = get_group_side_effect
        data_model.get_polling_interval.side_effect = get_polling_interval_side_effect

        return data_model

    def test_interval_dict_population(self, mock_data_model):
        """
        Test that the setup method correctly populates interval_dicts based on the data model.
        """
        # Ensure interval_dicts is reset before the test
        StaticCyclicPollScheduler.poll_sets = defaultdict(lambda: defaultdict(WeakSet))

        StaticCyclicPollScheduler.setup(mock_data_model, {})

        interval_dicts = StaticCyclicPollScheduler.poll_sets

        # Ensure group1 and group2 are created
        assert 'group1' in interval_dicts
        assert 'group2' in interval_dicts

        # Verify group1 contains remote1 with the correct interval and point
        group1_remote1 = interval_dicts['group1'][mock_data_model.remotes['remote1']]
        assert timedelta(seconds=3600) in group1_remote1
        assert mock_data_model.remotes['remote1'].point_set[0] in group1_remote1[timedelta(
            seconds=3600)]

        # verify group2 contains remote2 with the correct interval and point
        group2_remote2 = interval_dicts['group2'][mock_data_model.remotes['remote2']]
        assert timedelta(seconds=1800) in group2_remote2
        assert mock_data_model.remotes['remote2'].point_set[0] in group2_remote2[timedelta(
            seconds=1800)]


class TestPollSchedulerSetupPart2:

    @pytest.fixture
    def mock_data_model(self):
        """fixture to create a mock data_model with necessary attributes and methods"""
        data_model = MagicMock()
        return data_model

    @pytest.fixture
    def group_configs(self):
        """Fixture to create a mock group_configs dictionary"""
        return {
            'group1':
            GroupConfig(
                poll_scheduler_module='platform_driver.poll_scheduler',
                poll_scheduler_class_name='StaticCyclicPollScheduler',
                minimum_polling_interval=60,    # seconds
                start_offset=timedelta(seconds=0)),
        # group2 will simulate a missing configuration
        }

    @patch('platform_driver.poll_scheduler.importlib.import_module')
    @patch('platform_driver.poll_scheduler.getattr')
    def test_poll_scheduler_creation(self, mock_getattr, mock_import_module, mock_data_model,
                                     group_configs):
        """Test that the setup method correctly creates poll_schedulers using imported modules and classes"""
        # Reset interval_dicts for testing
        StaticCyclicPollScheduler.poll_sets = {
            'group1': {
                'remote1': {
                    60: 'points1'
                }
            },
            'group2': {
                'remote2': {
                    1800: 'points2'
                }
            }
        }

        mock_module = MagicMock()
        mock_import_module.return_value = mock_module

        mock_poll_scheduler_class = MagicMock()
        mock_getattr.return_value = mock_poll_scheduler_class

        poll_schedulers = StaticCyclicPollScheduler.setup(mock_data_model, group_configs)

        mock_import_module.assert_has_calls([call('platform_driver.poll_scheduler')] * 2)

        assert mock_getattr.call_count == 2
        mock_getattr.assert_any_call(mock_module, 'StaticCyclicPollScheduler')

        assert 'group1' in poll_schedulers
        assert 'group2' in poll_schedulers

        mock_poll_scheduler_class.assert_any_call(mock_data_model, 'group1',
                                                  group_configs['group1'])
        assert 'group2' in group_configs
        mock_poll_scheduler_class.assert_any_call(mock_data_model, 'group2',
                                                  group_configs['group2'])

    @patch('platform_driver.poll_scheduler.importlib.import_module')
    @patch('platform_driver.poll_scheduler.getattr')
    def test_group_config_creation_when_missing(self, mock_getattr, mock_import_module,
                                                mock_data_model):
        """
        Test that a default GroupConfig is created when one is missing from group_configs.
        """
        StaticCyclicPollScheduler.poll_sets = {
            'group1': {
                'remote1': {
                    60: 'points1'
                }
            },
            'group2': {
                'remote2': {
                    1800: 'points2'
                }
            }
        }

        group_configs = {}

        mock_module = MagicMock()
        mock_import_module.return_value = mock_module

        mock_poll_scheduler_class = MagicMock()
        mock_getattr.return_value = mock_poll_scheduler_class

        poll_schedulers = StaticCyclicPollScheduler.setup(mock_data_model, group_configs)

        assert 'group1' in group_configs
        assert 'group2' in group_configs

        assert group_configs['group2'].start_offset == timedelta(
            seconds=0)    # i = 1, so the offset is 0 initially

        # Ensure the scheduler class was called with the correct default GroupConfig
        mock_poll_scheduler_class.assert_any_call(mock_data_model, 'group1',
                                                  group_configs['group1'])
        mock_poll_scheduler_class.assert_any_call(mock_data_model, 'group2',
                                                  group_configs['group2'])


class TestCreatePollSchedulers:

    @pytest.fixture
    def mock_data_model(self):
        """Fixture to create a mock data_model with necessary methods"""
        data_model = MagicMock(spec=EquipmentTree)
        return data_model

    @pytest.fixture
    def mock_group_config(self):
        """Fixture to create a mock GroupConfig with necessary fields"""
        group_config = GroupConfig(
            poll_scheduler_module='scheduler.poll_scheduler',
            poll_scheduler_class_name='StaticCyclicPollScheduler',
            minimum_polling_interval=60,
            start_offset=0.0    # Updated to pass a float instead of timedelta
        )
        return group_config

    @pytest.fixture
    def mock_importlib(self, mocker):
        """Fixture to mock importlib.import_module"""
        return mocker.patch('importlib.import_module')

    def test_create_poll_schedulers_single_group(self, mock_importlib, mock_data_model,
                                                 mock_group_config):
        """Test create_poll_schedulers with a single specific group"""
        mock_scheduler_class = MagicMock()
        mock_importlib.return_value = MagicMock(StaticCyclicPollScheduler=mock_scheduler_class)

        group_configs = {'group1': mock_group_config}

        poll_schedulers = PollScheduler.create_poll_schedulers(data_model=mock_data_model,
                                                               group_configs=group_configs,
                                                               specific_groups=['group1'])

        mock_importlib.assert_called_once_with('scheduler.poll_scheduler')
        mock_scheduler_class.assert_called_once_with(mock_data_model, 'group1', mock_group_config)
        assert 'group1' in poll_schedulers

    def test_create_poll_schedulers_default_group(self, mock_importlib, mock_data_model,
                                                  mock_group_config):
        """Test create_poll_schedulers with no specific groups, using default poll_sets"""
        mock_scheduler_class = MagicMock()
        mock_importlib.return_value = MagicMock(StaticCyclicPollScheduler=mock_scheduler_class)

        group_configs = {}
        PollScheduler.poll_sets = ['default_group']

        poll_schedulers = PollScheduler.create_poll_schedulers(data_model=mock_data_model,
                                                               group_configs=group_configs)

        # Update the module name to match the actual one used in your implementation
        mock_importlib.assert_called_once_with('platform_driver.poll_scheduler')

    def test_create_poll_schedulers_multiple_groups(self, mock_importlib, mock_data_model,
                                                    mock_group_config):
        """Test create_poll_schedulers with multiple specific groups"""
        mock_scheduler_class = MagicMock()
        mock_importlib.return_value = MagicMock(StaticCyclicPollScheduler=mock_scheduler_class)

        group_configs = {'group1': mock_group_config, 'group2': mock_group_config}

        poll_schedulers = PollScheduler.create_poll_schedulers(
            data_model=mock_data_model,
            group_configs=group_configs,
            specific_groups=['group1', 'group2'])

        mock_importlib.assert_called_with('scheduler.poll_scheduler')
        assert mock_scheduler_class.call_count == 2
        mock_scheduler_class.assert_any_call(mock_data_model, 'group1', mock_group_config)
        mock_scheduler_class.assert_any_call(mock_data_model, 'group2', mock_group_config)
        assert 'group1' in poll_schedulers
        assert 'group2' in poll_schedulers


class TestBuildPollSets:

    @pytest.fixture
    def mock_data_model(self):
        """Fixture to create a mock data_model with necessary methods and attributes."""
        data_model = MagicMock(spec=EquipmentTree)

        # Mock remotes and points
        remote1 = MagicMock()
        remote1.point_set = [MagicMock(identifier='point1'), MagicMock(identifier='point2')]

        remote2 = MagicMock()
        remote2.point_set = [MagicMock(identifier='point3')]

        data_model.remotes = {'remote1': remote1, 'remote2': remote2}

        # Mock active points and groups
        data_model.is_active.side_effect = lambda identifier: identifier in [
            'point1', 'point2', 'point3'
        ]
        data_model.get_group.side_effect = lambda identifier: 'group1' if 'point1' in identifier else 'group2'
        data_model.get_polling_interval.side_effect = lambda identifier: 60 if 'point1' in identifier else 120

        return data_model

    @pytest.fixture
    def mock_poll_set(self, mocker):
        """Fixture to patch the PollSet class."""
        return mocker.patch('platform_driver.poll_scheduler.PollSet')

    @patch.object(PollScheduler, 'poll_sets', defaultdict(lambda: defaultdict(dict)))
    def test_build_poll_sets_single_remote(self, mock_data_model, mock_poll_set):
        """Test _build_poll_sets with a single remote and verify PollSet behavior."""
        mock_poll_set.return_value = MagicMock()

        PollScheduler._build_poll_sets(mock_data_model)

        # Since there are 3 unique group/interval combinations, expect 3 PollSet instantiations.
        assert mock_poll_set.call_count == 3
        mock_poll_set.assert_any_call(mock_data_model, mock_data_model.remotes['remote1'])
        mock_poll_set.assert_any_call(mock_data_model, mock_data_model.remotes['remote2'])
        assert 'group1' in PollScheduler.poll_sets
        assert 'group2' in PollScheduler.poll_sets

    @patch.object(PollScheduler, 'poll_sets', defaultdict(lambda: defaultdict(dict)))
    def test_build_poll_sets_with_active_points(self, mock_data_model, mock_poll_set):
        """Test _build_poll_sets and verify only active points are added to the poll_sets."""
        mock_poll_set.return_value = MagicMock()

        PollScheduler._build_poll_sets(mock_data_model)

        # Expect 3 PollSet instances if there are 3 unique group/interval combinations
        assert mock_poll_set.call_count == 3
        assert 'group1' in PollScheduler.poll_sets
        assert 'group2' in PollScheduler.poll_sets

    @patch.object(PollScheduler, 'poll_sets', defaultdict(lambda: defaultdict(dict)))
    def test_build_poll_sets_multiple_remotes(self, mock_data_model, mock_poll_set):
        """Test _build_poll_sets with multiple remotes."""
        mock_poll_set.return_value = MagicMock()

        PollScheduler._build_poll_sets(mock_data_model)

        # Since there are 3 unique group/interval combinations, expect 3 PollSet instantiations.
        assert mock_poll_set.call_count == 3
        assert 'group1' in PollScheduler.poll_sets
        assert 'group2' in PollScheduler.poll_sets

        # Check if the remotes were assigned correctly to the poll sets
        assert mock_data_model.remotes['remote1'] in PollScheduler.poll_sets['group1']
        assert mock_data_model.remotes['remote2'] in PollScheduler.poll_sets['group2']


def test_find_starting_datetime():
    """
    Test the calculation of the starting datetime.
    """
    now = datetime(2024, 1, 1, 10, 0, 0)
    interval = timedelta(hours=1)
    group_delay = timedelta(minutes=15)

    expected_start = datetime(2024, 1, 1, 11, 15, 0)
    result = PollScheduler.find_starting_datetime(now, interval, group_delay)

    assert result == expected_start


class TestPollSchedulerAddToSchedule:

    @pytest.fixture
    def mock_data_model(self):
        """Fixture to create a mock data_model with necessary methods and attributes."""
        data_model = MagicMock(spec=EquipmentTree)
        # Mock necessary methods on the data_model
        data_model.get_group.return_value = 'group1'
        data_model.get_remote.return_value = 'remote1'
        data_model.get_polling_interval.return_value = 60
        return data_model

    @pytest.fixture
    def mock_point(self):
        """Fixture to create a mock PointNode."""
        point = MagicMock(spec=PointNode)
        point.identifier = 'point1'
        return point

    @patch.object(PollScheduler, 'poll_sets',
                  defaultdict(lambda: defaultdict(lambda: defaultdict(MagicMock))))
    def test_add_to_schedule_reschedule_required_new_group(self, mock_data_model, mock_point):
        """Test when adding to schedule requires a reschedule due to a new group."""
        PollScheduler.poll_sets.clear()    # Clear the poll sets

        # Act
        reschedule_required = PollScheduler.add_to_schedule(mock_point, mock_data_model)

        # Assert
        assert reschedule_required is True    # Expecting reschedule because group1 is not in poll_sets
        assert mock_data_model.get_group.called_with(mock_point.identifier)
        assert mock_data_model.get_remote.called_with(mock_point.identifier)
        assert mock_data_model.get_polling_interval.called_with(mock_point.identifier)

    @patch.object(PollScheduler, 'poll_sets',
                  defaultdict(lambda: defaultdict(lambda: defaultdict(MagicMock))))
    def test_add_to_schedule_reschedule_required_new_remote(self, mock_data_model, mock_point):
        """Test when adding to schedule requires a reschedule due to a new remote in the group."""
        PollScheduler.poll_sets['group1'].clear()    # Ensure the group exists but remote does not

        reschedule_required = PollScheduler.add_to_schedule(mock_point, mock_data_model)

        assert reschedule_required is True    # Expecting reschedule because remote1 is not in group1
        assert mock_data_model.get_group.called_with(mock_point.identifier)
        assert mock_data_model.get_remote.called_with(mock_point.identifier)
        assert mock_data_model.get_polling_interval.called_with(mock_point.identifier)

    @patch.object(PollScheduler, 'poll_sets',
                  defaultdict(lambda: defaultdict(lambda: defaultdict(MagicMock))))
    def test_add_to_schedule_reschedule_not_required(self, mock_data_model, mock_point):
        """Test when adding to schedule does not require a reschedule."""
        # Ensure the group, remote, and interval exist in poll_sets
        PollScheduler.poll_sets['group1']['remote1'][60] = MagicMock()

        reschedule_required = PollScheduler.add_to_schedule(mock_point, mock_data_model)

        assert reschedule_required is False    # No reschedule required, poll_set exists
        assert PollScheduler.poll_sets['group1']['remote1'][60].add.called_with(mock_point)
        assert mock_data_model.get_group.called_with(mock_point.identifier)
        assert mock_data_model.get_remote.called_with(mock_point.identifier)
        assert mock_data_model.get_polling_interval.called_with(mock_point.identifier)

    @patch.object(PollScheduler, 'poll_sets',
                  defaultdict(lambda: defaultdict(lambda: defaultdict(MagicMock))))
    def test_add_to_schedule_reschedule_required_new_interval(self, mock_data_model, mock_point):
        """Test when adding to schedule requires a reschedule due to a new interval."""
        # Group and remote exist, but the interval doesn't
        PollScheduler.poll_sets['group1']['remote1'].clear()

        reschedule_required = PollScheduler.add_to_schedule(mock_point, mock_data_model)

        assert reschedule_required is True    # Expecting reschedule because interval 60 is not in remote1/group1
        assert mock_data_model.get_group.called_with(mock_point.identifier)
        assert mock_data_model.get_remote.called_with(mock_point.identifier)
        assert mock_data_model.get_polling_interval.called_with(mock_point.identifier)


class TestRemoveFromSchedule:

    @pytest.fixture
    def mock_data_model(self):
        """Fixture to create a mock data_model with necessary methods and attributes."""
        data_model = MagicMock(spec=EquipmentTree)
        data_model.get_group.return_value = 'group1'
        data_model.get_remote.return_value = 'remote1'
        data_model.get_polling_interval.return_value = 60
        return data_model

    @pytest.fixture
    def mock_point(self):
        """Fixture to create a mock PointNode."""
        point = MagicMock(spec=PointNode)
        point.identifier = 'point1'
        return point

    @patch.object(PollScheduler, 'poll_sets',
                  defaultdict(lambda: defaultdict(lambda: defaultdict(MagicMock))))
    @patch.object(PollScheduler, '_prune_poll_sets')
    def test_remove_from_schedule_success(self, mock_prune, mock_data_model, mock_point):
        """Test successful removal of a point from the schedule."""
        # Mock an existing poll set
        poll_set_mock = MagicMock()
        PollScheduler.poll_sets['group1']['remote1'][60] = poll_set_mock
        poll_set_mock.remove.return_value = True    # Simulate successful removal

        success = PollScheduler.remove_from_schedule(mock_point, mock_data_model)

        # Verify the result is True (successful removal)
        assert success is True
        poll_set_mock.remove.assert_called_once_with(mock_point)
        mock_prune.assert_called_once_with('group1', 60, 'remote1')    # Ensure pruning was called

    @patch.object(PollScheduler, 'poll_sets',
                  defaultdict(lambda: defaultdict(lambda: defaultdict(MagicMock))))
    @patch.object(PollScheduler, '_prune_poll_sets')
    def test_remove_from_schedule_failure(self, mock_prune, mock_data_model, mock_point):
        """Test failure to remove a point from the schedule (when the point doesn't exist in the poll set)."""
        # Mock an existing poll set
        poll_set_mock = MagicMock()
        PollScheduler.poll_sets['group1']['remote1'][60] = poll_set_mock
        poll_set_mock.remove.return_value = False    # Simulate failed removal

        success = PollScheduler.remove_from_schedule(mock_point, mock_data_model)

        # Verify the result is False (failed removal)
        assert success is False
        poll_set_mock.remove.assert_called_once_with(mock_point)
        mock_prune.assert_called_once_with('group1', 60,
                                           'remote1')    # Ensure pruning was still called

    @patch.object(PollScheduler, 'poll_sets',
                  defaultdict(lambda: defaultdict(lambda: defaultdict(MagicMock))))
    @patch.object(PollScheduler, '_prune_poll_sets')
    def test_remove_from_schedule_empty_poll_set(self, mock_prune, mock_data_model, mock_point):
        """Test removal from a poll set when it's empty, but still verify that pruning is done."""
        # Mock an existing poll set
        poll_set_mock = MagicMock()
        PollScheduler.poll_sets['group1']['remote1'][60] = poll_set_mock
        poll_set_mock.remove.return_value = True    # Simulate successful removal

        success = PollScheduler.remove_from_schedule(mock_point, mock_data_model)

        # Verify the result is True (successful removal)
        assert success is True
        poll_set_mock.remove.assert_called_once_with(mock_point)
        mock_prune.assert_called_once_with('group1', 60, 'remote1')    # Ensure pruning was called


class TestStaticCyclicPollSchedulerGetSchedule:

    def test_get_schedule_basic(self):
        """Test get_schedule with a single hyperperiod and single slot containing two points."""
        # Create an instance of StaticCyclicPollScheduler with mocked parameters
        scheduler = StaticCyclicPollScheduler(data_model=MagicMock(),
                                              group='group_1',
                                              group_config=MagicMock())

        # Mock the slot_plans attribute directly
        scheduler.slot_plans = [{
            timedelta(minutes=1): {    # Hyperperiod
                timedelta(seconds=0): [    # Slot
                    MagicMock(points={
                        'path/to/point1': None,
                        'path/to/point2': None
                    },
                              remote=MagicMock(unique_id='remote_1'))
                ]
            }
        }]
        schedule = scheduler.get_schedule()

        # Expected output
        expected_schedule = {'0:01:00': {'0:00:00': {'remote_1': ['point1', 'point2']}}}

        assert schedule == expected_schedule

    def test_get_schedule_multiple_slots(self):
        """Test get_schedule with multiple slots and remotes in a single hyperperiod."""
        # Create an instance with mocked parameters
        scheduler = StaticCyclicPollScheduler(data_model=MagicMock(),
                                              group='group_2',
                                              group_config=MagicMock())

        # Mock slot_plans with multiple slots and hyperperiods
        scheduler.slot_plans = [{
            timedelta(minutes=2): {    # Hyperperiod
                timedelta(seconds=0): [    # Slot 1
                    MagicMock(points={'path/to/point1': None},
                              remote=MagicMock(unique_id='remote_1'))
                ],
                timedelta(seconds=30): [    # Slot 2
                    MagicMock(points={'path/to/point2': None},
                              remote=MagicMock(unique_id='remote_2'))
                ]
            }
        }]
        schedule = scheduler.get_schedule()

        # Expected output
        expected_schedule = {
            '0:02:00': {
                '0:00:00': {
                    'remote_1': ['point1']
                },
                '0:00:30': {
                    'remote_2': ['point2']
                }
            }
        }
        assert schedule == expected_schedule

    def test_get_schedule_empty_slot_plans(self):
        """Test get_schedule when slot_plans is empty, expecting an empty schedule."""
        # Create an instance with mocked parameters
        scheduler = StaticCyclicPollScheduler(data_model=MagicMock(),
                                              group='group_3',
                                              group_config=MagicMock())

        # Set slot_plans to an empty list
        scheduler.slot_plans = []

        schedule = scheduler.get_schedule()

        # Expected output is an empty dictionary
        expected_schedule = {}

        assert schedule == expected_schedule

    def test_get_schedule_no_points(self):
        """Test get_schedule with a slot that has no points (empty poll set)."""
        # Create an instance with mocked parameters
        scheduler = StaticCyclicPollScheduler(data_model=MagicMock(),
                                              group='group_4',
                                              group_config=MagicMock())

        # Mock a slot plan with a slot that has an empty poll set
        scheduler.slot_plans = [{
            timedelta(minutes=1): {    # Hyperperiod
                timedelta(seconds=0): [    # Slot
                    MagicMock(
                        points={},    # Empty points
                        remote=MagicMock(unique_id='remote_1'))
                ]
            }
        }]
        schedule = scheduler.get_schedule()

        # Expected output reflects empty points for the remote
        expected_schedule = {'0:01:00': {'0:00:00': {'remote_1': []}}}
        assert schedule == expected_schedule

    def test_get_schedule_different_hyperperiods(self):
        """Test get_schedule with multiple hyperperiods."""
        # Create an instance with mocked parameters
        scheduler = StaticCyclicPollScheduler(data_model=MagicMock(),
                                              group='group_6',
                                              group_config=MagicMock())

        # Mock slot_plans with different hyperperiods
        scheduler.slot_plans = [{
            timedelta(minutes=1): {
                timedelta(seconds=0): [
                    MagicMock(points={'path/to/point1': None},
                              remote=MagicMock(unique_id='remote_1'))
                ]
            }
        }, {
            timedelta(minutes=2): {
                timedelta(seconds=30): [
                    MagicMock(points={'path/to/point2': None},
                              remote=MagicMock(unique_id='remote_2'))
                ]
            }
        }]
        schedule = scheduler.get_schedule()

        # Expected output with multiple hyperperiods
        expected_schedule = {
            '0:01:00': {
                '0:00:00': {
                    'remote_1': ['point1']
                }
            },
            '0:02:00': {
                '0:00:30': {
                    'remote_2': ['point2']
                }
            }
        }
        assert schedule == expected_schedule

    def test_get_schedule_duplicate_points(self):
        """Test get_schedule with the same point in multiple slots."""
        # Create an instance with mocked parameters
        scheduler = StaticCyclicPollScheduler(data_model=MagicMock(),
                                              group='group_7',
                                              group_config=MagicMock())

        # Mock a slot plan with duplicate points in different slots
        scheduler.slot_plans = [{
            timedelta(minutes=1): {
                timedelta(seconds=0): [
                    MagicMock(points={'path/to/point1': None},
                              remote=MagicMock(unique_id='remote_1'))
                ],
                timedelta(seconds=30): [
                    MagicMock(points={'path/to/point1': None},
                              remote=MagicMock(unique_id='remote_1'))
                ]
            }
        }]
        schedule = scheduler.get_schedule()

        # Expected output shows point1 scheduled at both slots
        expected_schedule = {
            '0:01:00': {
                '0:00:00': {
                    'remote_1': ['point1']
                },
                '0:00:30': {
                    'remote_1': ['point1']
                }
            }
        }

        # Assert that the schedule matches the expected output
        assert schedule == expected_schedule


def test_static_cyclic_poll_scheduler_init():
    """
    Test that StaticCyclicPollScheduler initializes properly.
    """
    data_model = MagicMock()
    group = 'test_group'
    group_config = MagicMock()

    scheduler = StaticCyclicPollScheduler(data_model, group, group_config)

    assert scheduler.slot_plans == []
    assert isinstance(scheduler, PollScheduler)


def test_calculate_hyperperiod():
    """
    Test hyperperiod calculation based on intervals.
    """
    intervals = [10, 15, 20]
    minimum_polling_interval = 5

    result = StaticCyclicPollScheduler.calculate_hyperperiod(intervals, minimum_polling_interval)
    expected_hyperperiod = 60    # LCM of 2, 3, 4 times 5

    assert result == expected_hyperperiod


def test_separate_coprimes():
    """
    Test separation of intervals into coprime sets.
    """
    intervals = [4, 6, 9, 25]
    expected_output = [[25], [9, 6], [4]]

    result = StaticCyclicPollScheduler._separate_coprimes(intervals)

    assert result == expected_output


def test_serial_poll_scheduler_init():
    """
    Test that SerialPollScheduler initializes with sleep_duration.
    """
    data_model = MagicMock(spec=EquipmentTree)
    group = 'test_group'
    group_config = MagicMock(spec=GroupConfig)
    sleep_duration = 1.0

    # Pass group and group_config as keyword arguments to avoid positional conflicts
    scheduler = SerialPollScheduler(agent=data_model,
                                    sleep_duration=sleep_duration,
                                    group=group,
                                    group_config=group_config)

    assert scheduler.data_model == data_model
    assert scheduler.group == group
    assert scheduler.group_config == group_config
    assert scheduler.sleep_duration == sleep_duration
    assert scheduler.status == {}


class TestSetupPublish:

    @pytest.fixture
    def mock_data_model(self):
        """Fixture to create a mock data_model with necessary methods"""
        data_model = MagicMock(spec=EquipmentTree)
        return data_model

    @pytest.fixture
    def scheduler(self, mock_data_model):
        """Fixture to create an instance of StaticCyclicPollScheduler with a mocked data_model"""
        group_config = GroupConfig(poll_scheduler_module='scheduler.poll_scheduler',
                                   poll_scheduler_class_name='StaticCyclicPollScheduler',
                                   minimum_polling_interval=60,
                                   start_offset=timedelta(seconds=0))
        scheduler_instance = StaticCyclicPollScheduler(data_model=mock_data_model,
                                                       group='group1',
                                                       group_config=group_config)
        return scheduler_instance

    def test_setup_publish_no_publish_setup_no_points(self, scheduler):
        """Test _setup_publish with no points and no existing publish_setup.
        Expect an empty publish_setup structure."""
        publish_setup = scheduler._setup_publish([])

        expected = {
            'single_depth': set(),
            'single_breadth': set(),
            'multi_depth': defaultdict(set),
            'multi_breadth': defaultdict(set)
        }

        assert publish_setup == expected

    def test_setup_publish_single_point_no_publication(self, scheduler, mock_data_model):
        """Test _setup_publish with a single point where no publication flags are True.
        Expect publish_setup to remain empty"""
        point = MagicMock(identifier='point1')

        mock_data_model.get_point_topics.return_value = ('depth1', 'breadth1')
        mock_data_model.get_device_topics.return_value = ('device_depth1', 'device_breadth1')
        mock_data_model.is_published_single_depth.return_value = False
        mock_data_model.is_published_single_breadth.return_value = False
        mock_data_model.is_published_multi_depth.return_value = False
        mock_data_model.is_published_multi_breadth.return_value = False

        publish_setup = scheduler._setup_publish([point])

        expected = {
            'single_depth': set(),
            'single_breadth': set(),
            'multi_depth': defaultdict(set),
            'multi_breadth': defaultdict(set)
        }

        assert publish_setup == expected

    def test_setup_publish_single_point_with_publication(self, scheduler, mock_data_model):
        """
        test _setup_publish with a single point where some publication flags are True
        expect publish_setup to be populated accordingly.
        """
        point = MagicMock(identifier='point1')

        mock_data_model.get_point_topics.return_value = ('depth1', 'breadth1')
        mock_data_model.get_device_topics.return_value = ('device_depth1', 'device_breadth1')
        mock_data_model.is_published_single_depth.return_value = True
        mock_data_model.is_published_single_breadth.return_value = False
        mock_data_model.is_published_multi_depth.return_value = True
        mock_data_model.is_published_multi_breadth.return_value = False

        publish_setup = scheduler._setup_publish([point])

        expected = {
            'single_depth': {'depth1'},
            'single_breadth': set(),
            'multi_depth': defaultdict(set, {'device_depth1': {'depth1'}}),
            'multi_breadth': defaultdict(set)
        }

        assert publish_setup['single_depth'] == expected['single_depth']
        assert publish_setup['single_breadth'] == expected['single_breadth']
        assert publish_setup['multi_depth'] == expected['multi_depth']
        assert publish_setup['multi_breadth'] == expected['multi_breadth']

    def test_setup_publish_multiple_points_mixed_publication(self, scheduler, mock_data_model):
        """
        test _setup_publish with multiple points having mixed publication flags
        Expect publish_setup to aggregate correctly
        """
        point1 = MagicMock(identifier='point1')
        point2 = MagicMock(identifier='point2')

        def get_point_topics_side_effect(identifier):
            if identifier == 'point1':
                return ('depth1', 'breadth1')
            elif identifier == 'point2':
                return ('depth2', 'breadth2')

        def get_device_topics_side_effect(identifier):
            if identifier == 'point1':
                return ('device_depth1', 'device_breadth1')
            elif identifier == 'point2':
                return ('device_depth2', 'device_breadth2')

        def is_published_single_depth_side_effect(identifier):
            return identifier == 'point1'

        def is_published_single_breadth_side_effect(identifier):
            return identifier == 'point2'

        def is_published_multi_depth_side_effect(identifier):
            return identifier == 'point1'

        def is_published_multi_breadth_side_effect(identifier):
            return identifier == 'point2'

        mock_data_model.get_point_topics.side_effect = get_point_topics_side_effect
        mock_data_model.get_device_topics.side_effect = get_device_topics_side_effect
        mock_data_model.is_published_single_depth.side_effect = is_published_single_depth_side_effect
        mock_data_model.is_published_single_breadth.side_effect = is_published_single_breadth_side_effect
        mock_data_model.is_published_multi_depth.side_effect = is_published_multi_depth_side_effect
        mock_data_model.is_published_multi_breadth.side_effect = is_published_multi_breadth_side_effect

        publish_setup = scheduler._setup_publish([point1, point2])

        expected = {
            'single_depth': {'depth1'},
            'single_breadth': {('depth2', 'breadth2')},
            'multi_depth': defaultdict(set, {'device_depth1': {'depth1'}}),
            'multi_breadth': defaultdict(set, {'device_breadth2': {'point2'}})
        }

        assert publish_setup['single_depth'] == expected['single_depth']
        assert publish_setup['single_breadth'] == expected['single_breadth']
        assert publish_setup['multi_depth'] == expected['multi_depth']
        assert publish_setup['multi_breadth'] == expected['multi_breadth']

    def test_setup_publish_existing_publish_setup(self, scheduler, mock_data_model):
        """
        Test setup_publish with an existing publish_setup and additional points.
        Expect publish_setup to be updated without overwriting existing data
        """
        point1 = MagicMock(identifier='point1')

        mock_data_model.get_point_topics.return_value = ('depth1', 'breadth1')
        mock_data_model.get_device_topics.return_value = ('device_depth1', 'device_breadth1')
        mock_data_model.is_published_single_depth.return_value = True
        mock_data_model.is_published_single_breadth.return_value = False
        mock_data_model.is_published_multi_depth.return_value = True
        mock_data_model.is_published_multi_breadth.return_value = False

        existing_publish_setup = {
            'single_depth': {'existing_depth'},
            'single_breadth': {('existing_depth', 'existing_breadth')},
            'multi_depth': defaultdict(set, {'existing_device_depth': {'existing_depth'}}),
            'multi_breadth': defaultdict(set, {'existing_device_breadth': {'existing_point'}})
        }

        publish_setup = scheduler._setup_publish([point1], publish_setup=existing_publish_setup)

        expected = {
            'single_depth': {'existing_depth', 'depth1'},
            'single_breadth': {('existing_depth', 'existing_breadth')},
            'multi_depth':
            defaultdict(set, {
                'existing_device_depth': {'existing_depth'},
                'device_depth1': {'depth1'}
            }),
            'multi_breadth':
            defaultdict(set, {'existing_device_breadth': {'existing_point'}})
        }

        assert publish_setup['single_depth'] == expected['single_depth']
        assert publish_setup['single_breadth'] == expected['single_breadth']
        assert publish_setup['multi_depth'] == expected['multi_depth']
        assert publish_setup['multi_breadth'] == expected['multi_breadth']


class TestPollSchedulerSchedulePolling:

    @pytest.fixture
    def mock_data_model(self):
        """
        Fixture to create a mock data_model with necessary attributes and methods.
        """
        data_model = MagicMock(spec=EquipmentTree)

        mock_remote1 = MagicMock()
        mock_remote1.unique_id = 'remote1_unique_id'
        mock_poller1 = MagicMock()
        mock_remote1.core.schedule.return_value = mock_poller1

        mock_remote2 = MagicMock()
        mock_remote2.unique_id = 'remote2_unique_id'
        mock_poller2 = MagicMock()
        mock_remote2.core.schedule.return_value = mock_poller2

        data_model.remotes = {'remote1': mock_remote1, 'remote2': mock_remote2}

        data_model.get_point_topics.return_value = ('depth_topic', 'breadth_topic')
        data_model.get_device_topics.return_value = ('device_depth', 'device_breadth')
        data_model.is_published_single_depth.return_value = True
        data_model.is_published_single_breadth.return_value = True
        data_model.is_published_multi_depth.return_value = False
        data_model.is_published_multi_breadth.return_value = False

        return data_model

    @pytest.fixture
    def group_configs(self):
        """
        Fixture to create a mock group_configs dictionary.
        """
        return {}

    @pytest.fixture
    def scheduler(self, mock_data_model, group_configs):
        """
        Fixture to create an instance of StaticCyclicPollScheduler with mocked data_model and group_configs
        """
        group_config = GroupConfig(
            poll_scheduler_module='platform_driver.poll_scheduler',
            poll_scheduler_class_name='StaticCyclicPollScheduler',
            minimum_polling_interval=60,    # seconds
            start_offset=timedelta(seconds=0))
        group_configs.update({'group1': group_config})

        scheduler_instance = StaticCyclicPollScheduler(data_model=mock_data_model,
                                                       group='group1',
                                                       group_config=group_config)

        scheduler_instance.slot_plans = []
        scheduler_instance.pollers = {}

        scheduler_instance.start_all_datetime = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        return scheduler_instance

    @patch('platform_driver.poll_scheduler.get_aware_utc_now')
    @patch('platform_driver.poll_scheduler._log.info')
    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler.get_poll_generator')
    def test_schedule_polling_calls(self, mock_get_poll_generator, mock_log_info,
                                    mock_get_aware_utc_now, scheduler):
        mock_now = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_get_aware_utc_now.return_value = mock_now

        mock_poll_generator = MagicMock()
        mock_poll_generator.__next__.return_value = (datetime(2024,
                                                              1,
                                                              1,
                                                              1,
                                                              0,
                                                              0,
                                                              tzinfo=timezone.utc), 'points',
                                                     'publish_setup',
                                                     scheduler.data_model.remotes['remote1'])
        mock_get_poll_generator.return_value = mock_poll_generator

        hyperperiod = timedelta(seconds=3600)    # 1 hour
        slot = timedelta(seconds=1800)    # 30 minutes
        plan = {
            slot: {
                'points': {
                    'point1': MagicMock()
                },
                'publish_setup': 'publish_setup_data',
                'remote': scheduler.data_model.remotes['remote1']
            }
        }
        scheduler.slot_plans = [{hyperperiod: plan}]

        scheduler._schedule_polling()

        # Expected initial_start
        expected_initial_start = mock_now + hyperperiod    # 1:00 AM

        mock_get_aware_utc_now.assert_called_once()
        mock_log_info.assert_called_once_with(
            f'Scheduled polling for {scheduler.group}--{hyperperiod} starts at {mock_poll_generator.__next__.return_value[0].time()}'
        )
        mock_get_poll_generator.assert_called_once_with(expected_initial_start, hyperperiod, plan)
        scheduler.data_model.remotes['remote1'].core.schedule.assert_called_once_with(
            expected_initial_start, scheduler._operate_polling, hyperperiod, mock_poll_generator,
            'points', 'publish_setup', scheduler.data_model.remotes['remote1'])


class TestFindSlots:

    @pytest.fixture
    def mock_scheduler(self):
        """Fixture to create a mock instance of StaticCyclicPollScheduler with group_config mocked"""
        scheduler = StaticCyclicPollScheduler(data_model=MagicMock(),
                                              group='test_group',
                                              group_config=MagicMock())

        scheduler.group_config.minimum_polling_interval = 60
        scheduler.group_config.parallel_subgroups = False

        scheduler.data_model.get_point_topics = MagicMock(return_value=('point_depth',
                                                                        'point_breadth'))
        scheduler.data_model.get_device_topics = MagicMock(return_value=('device_depth',
                                                                         'device_breadth'))

        return scheduler

    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler._separate_coprimes')
    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler.calculate_hyperperiod')
    @patch('platform_driver.poll_scheduler.WeakValueDictionary', lambda: defaultdict(dict))
    def test_find_slots_basic(self, mock_calculate_hyperperiod, mock_separate_coprimes,
                              mock_scheduler):
        """Test the _find_slots method with a basic input_dict and parallel_remote_index"""
        mock_separate_coprimes.return_value = [[60, 120], [180]]

        mock_calculate_hyperperiod.side_effect = lambda intervals, min_interval: max(intervals)

        mock_point_1 = MagicMock(identifier="point1")
        mock_point_2 = MagicMock(identifier="point2")
        input_dict = {
            60: {
                'remote1': [mock_point_1]
            },
            120: {
                'remote1': [mock_point_2]
            },
            180: {
                'remote2': [mock_point_1, mock_point_2]
            }
        }

        result = mock_scheduler._find_slots(input_dict, parallel_remote_index=1)

        assert len(result) == 2    # Two hyperperiods: 120 and 180
        assert timedelta(seconds=120) in result
        assert timedelta(seconds=180) in result

        # Verify the content of the result for hyperperiod 120
        plan_120 = result[timedelta(seconds=120)]
        assert len(plan_120) > 0    # Ensure there are slots in hyperperiod 120

    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler._separate_coprimes')
    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler.calculate_hyperperiod')
    def test_find_slots_single_interval(self, mock_calculate_hyperperiod, mock_separate_coprimes,
                                        mock_scheduler):
        """Test _find_slots with a single interval to ensure basic functionality"""
        mock_separate_coprimes.return_value = [[60]]

        mock_calculate_hyperperiod.return_value = 60

        mock_point = MagicMock(identifier="point1")
        input_dict = {60: {'remote1': [mock_point]}}

        result = mock_scheduler._find_slots(input_dict)

        assert len(result) == 1
        assert timedelta(seconds=60) in result

        plan = result[timedelta(seconds=60)]
        assert len(plan) > 0

    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler._separate_coprimes')
    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler.calculate_hyperperiod')
    def test_find_slots_parallel_subgroups(self, mock_calculate_hyperperiod,
                                           mock_separate_coprimes, mock_scheduler):
        """Test _find_slots when parallel_subgroups is True"""
        mock_scheduler.group_config.parallel_subgroups = True
        mock_separate_coprimes.return_value = [[60, 90]]

        mock_calculate_hyperperiod.side_effect = lambda intervals, min_interval: max(intervals)

        mock_point_1 = MagicMock(identifier="point1")
        mock_point_2 = MagicMock(identifier="point2")
        input_dict = {60: {'remote1': [mock_point_1]}, 90: {'remote2': [mock_point_2]}}

        result = mock_scheduler._find_slots(input_dict)

        assert len(result) == 1
        assert timedelta(seconds=90) in result

        plan = result[timedelta(seconds=90)]
        assert len(plan) > 0

    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler._separate_coprimes')
    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler.calculate_hyperperiod')
    def test_find_slots_large_number_of_remotes(self, mock_calculate_hyperperiod,
                                                mock_separate_coprimes, mock_scheduler):
        """Test _find_slots with a large number of remotes to check performance and correctness"""
        intervals = [60, 120, 180]
        mock_separate_coprimes.return_value = [intervals]

        mock_calculate_hyperperiod.side_effect = lambda intervals, min_interval: max(intervals)

        input_dict = {}
        for interval in intervals:
            remotes = {f'remote{i}': [MagicMock(identifier=f"point{i}")] for i in range(10)}
            input_dict[interval] = remotes

        result = mock_scheduler._find_slots(input_dict)

        assert len(result) == 1
        hyperperiod = timedelta(seconds=180)
        assert hyperperiod in result

        plan = result[hyperperiod]
        assert len(plan) > 0

    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler._separate_coprimes')
    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler.calculate_hyperperiod')
    def test_find_slots_with_parallel_offset(self, mock_calculate_hyperperiod,
                                             mock_separate_coprimes, mock_scheduler):
        """Test _find_slots with a non-zero parallel_remote_index to ensure offsets are calculated correctly"""
        mock_separate_coprimes.return_value = [[60]]

        mock_calculate_hyperperiod.return_value = 60

        mock_point = MagicMock(identifier="point1")
        input_dict = {60: {'remote1': [mock_point]}}

        # Set parallel_remote_index to a non-zero value
        parallel_remote_index = 2

        result = mock_scheduler._find_slots(input_dict,
                                            parallel_remote_index=parallel_remote_index)

        assert len(result) == 1
        assert timedelta(seconds=60) in result

        plan = result[timedelta(seconds=60)]
        # The slot times should be offset by parallel_remote_index * minimum_polling_interval
        expected_slot_time = timedelta(seconds=0) + timedelta(
            seconds=parallel_remote_index * mock_scheduler.group_config.minimum_polling_interval)
        assert expected_slot_time in plan

    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler._separate_coprimes')
    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler.calculate_hyperperiod')
    def test_find_slots_non_coprime_intervals(self, mock_calculate_hyperperiod,
                                              mock_separate_coprimes, mock_scheduler):
        """Test _find_slots with intervals that are not coprime to ensure correct grouping"""
        mock_separate_coprimes.return_value = [[60, 90], [120]]

        mock_calculate_hyperperiod.side_effect = lambda intervals, min_interval: max(intervals)

        mock_point_1 = MagicMock(identifier="point1")
        mock_point_2 = MagicMock(identifier="point2")
        mock_point_3 = MagicMock(identifier="point3")
        input_dict = {
            60: {
                'remote1': [mock_point_1]
            },
            90: {
                'remote2': [mock_point_2]
            },
            120: {
                'remote3': [mock_point_3]
            }
        }

        result = mock_scheduler._find_slots(input_dict)

        assert len(result) == 2    # Two hyperperiods
        assert timedelta(seconds=90) in result
        assert timedelta(seconds=120) in result

    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler._separate_coprimes')
    @patch('platform_driver.poll_scheduler.StaticCyclicPollScheduler.calculate_hyperperiod')
    def test_find_slots_interval_not_divisible(self, mock_calculate_hyperperiod,
                                               mock_separate_coprimes, mock_scheduler):
        """Test _find_slots when interval is not perfectly divisible into hyperperiod"""
        mock_separate_coprimes.return_value = [[45, 100]]

        mock_calculate_hyperperiod.side_effect = lambda intervals, min_interval: 900    # LCM of 45 and 100

        mock_point_1 = MagicMock(identifier="point1")
        mock_point_2 = MagicMock(identifier="point2")
        input_dict = {45: {'remote1': [mock_point_1]}, 100: {'remote2': [mock_point_2]}}

        result = mock_scheduler._find_slots(input_dict)

        assert len(result) == 1
        hyperperiod = timedelta(seconds=900)
        assert hyperperiod in result

        plan = result[hyperperiod]
        assert len(plan) > 0


class TestPollGenerator:

    @pytest.fixture
    def sample_slot_plan(self):
        return {
            0: {
                'points': 10,
                'publish_setup': True,
                'remote': False
            },
            5: {
                'points': 20,
                'publish_setup': False,
                'remote': True
            },
            10: {
                'points': 30,
                'publish_setup': True,
                'remote': True
            },
        }

    def test_basic_functionality(self, sample_slot_plan):
        hyperperiod_start = 100
        hyperperiod = 50
        generator = StaticCyclicPollScheduler.get_poll_generator(hyperperiod_start, hyperperiod,
                                                                 sample_slot_plan)

        expected_polls = [
            (100 + 0, 10, True, False),
            (100 + 5, 20, False, True),
            (100 + 10, 30, True, True),
        ]

        for expected in expected_polls:
            poll = next(generator)
            assert poll == expected, f"Expected {expected}, got {poll}"

    def test_hyperperiod_wrap_around(self, sample_slot_plan):
        hyperperiod_start = 100
        hyperperiod = 50
        generator = StaticCyclicPollScheduler.get_poll_generator(hyperperiod_start, hyperperiod,
                                                                 sample_slot_plan)

        # Exhaust the first hyperperiod
        first_hyperperiod_polls = [
            (100 + 0, 10, True, False),
            (100 + 5, 20, False, True),
            (100 + 10, 30, True, True),
        ]

        for expected in first_hyperperiod_polls:
            poll = next(generator)
            assert poll == expected, f"Expected {expected}, got {poll}"

        # Next hyperperiod_start should be 150
        second_hyperperiod_polls = [
            (150 + 0, 10, True, False),
            (150 + 5, 20, False, True),
            (150 + 10, 30, True, True),
        ]

        for expected in second_hyperperiod_polls:
            poll = next(generator)
            assert poll == expected, f"Expected {expected}, got {poll}"

    def test_multiple_hyperperiods(self, sample_slot_plan):
        hyperperiod_start = 0
        hyperperiod = 100
        generator = StaticCyclicPollScheduler.get_poll_generator(hyperperiod_start, hyperperiod,
                                                                 sample_slot_plan)

        # Let's test 3 hyperperiods
        for i in range(3):
            current_start = i * hyperperiod
            expected_polls = [
                (current_start + 0, 10, True, False),
                (current_start + 5, 20, False, True),
                (current_start + 10, 30, True, True),
            ]
            for expected in expected_polls:
                poll = next(generator)
                assert poll == expected, f"Expected {expected}, got {poll}"

    def test_slot_plan_with_negative_keys(self):
        # Modify the slot_plan to include negative keys
        slot_plan = {
            -10: {
                'points': 15,
                'publish_setup': False,
                'remote': True
            },
            0: {
                'points': 25,
                'publish_setup': True,
                'remote': False
            },
        }
        hyperperiod_start = 200
        hyperperiod = 100
        generator = StaticCyclicPollScheduler.get_poll_generator(hyperperiod_start, hyperperiod,
                                                                 slot_plan)

        expected_polls = [
            (200 - 10, 15, False, True),
            (200 + 0, 25, True, False),
        ]

        for expected in expected_polls:
            poll = next(generator)
            assert poll == expected, f"Expected {expected}, got {poll}"


class TestStaticCyclicPollSchedulerPrepareToSchedule:

    @pytest.fixture
    def scheduler(self, monkeypatch):
        """Fixture to set up the scheduler with mocked _find_slots."""
        mock_find_slots = MagicMock()
        monkeypatch.setattr('platform_driver.poll_scheduler.StaticCyclicPollScheduler._find_slots',
                            mock_find_slots)

        scheduler = StaticCyclicPollScheduler(data_model=MagicMock(),
                                              group='test_group',
                                              group_config=MagicMock())
        scheduler.group_config.minimum_polling_interval = 60
        scheduler.group_config.parallel_subgroups = False
        scheduler._find_slots = mock_find_slots
        return scheduler

    def test_prepare_to_schedule_sequential(self, scheduler):
        """Test _prepare_to_schedule when parallel_subgroups is False."""
        point1 = MagicMock()
        point2 = MagicMock()
        scheduler.poll_sets = {
            'test_group': {
                'remote1': {
                    'interval1': WeakSet([point1]),
                    'interval2': WeakSet([point2]),
                }
            }
        }
        scheduler.group_config.parallel_subgroups = False

        scheduler._prepare_to_schedule()

        # ensure _find_slots is called once with input_dict
        scheduler._find_slots.assert_called_once()
        input_dict_passed = scheduler._find_slots.call_args[0][0]
        assert len(input_dict_passed['interval1']['remote1']) > 0

    def test_prepare_to_schedule_parallel(self, scheduler):
        """Test _prepare_to_schedule when parallel_subgroups is True."""
        scheduler.poll_sets = {
            'test_group': {
                'remote1': {
                    'interval1': WeakSet([MagicMock()]),
                },
                'remote2': {
                    'interval1': WeakSet([MagicMock()]),
                }
            }
        }
        scheduler.group_config.parallel_subgroups = True

        scheduler._prepare_to_schedule()

        # ensure _find_slots is called twice, once per remote
        assert scheduler._find_slots.call_count == 2
        assert len(scheduler.slot_plans) == 2


class TestStaticCyclicPollSchedulerSchedulePolling:

    @pytest.fixture
    def scheduler(self):
        """Fixture to create a StaticCyclicPollScheduler instance with mocked dependencies."""
        data_model_mock = MagicMock()
        data_model_mock.agent.core.schedule = MagicMock()
        group_config_mock = MagicMock()
        group_config_mock.start_offset = timedelta(seconds=0)

        scheduler = StaticCyclicPollScheduler(data_model=data_model_mock,
                                              group='test_group',
                                              group_config=group_config_mock)

        # Mock slot_plans
        scheduler.slot_plans = [{
            timedelta(minutes=5): {    # Hyperperiod
                timedelta(seconds=0): 'plan'    # Placeholder for the plan
            }
        }]

        return scheduler

    def test_schedule_polling_calls_correct_methods(self, scheduler):
        """Test that _schedule_polling calls the correct methods with expected arguments."""
        # Use get_aware_utc_now to get a timezone-aware datetime
        aware_datetime = get_aware_utc_now()

        with patch.object(scheduler, 'find_starting_datetime', return_value=aware_datetime) as mock_find_starting_datetime, \
             patch.object(scheduler, 'get_poll_generator') as mock_get_poll_generator, \
             patch('volttron.utils.get_aware_utc_now', return_value=aware_datetime):

            # Mock get_poll_generator to return an iterator
            poll_generator_mock = MagicMock()
            poll_generator_mock.__next__.return_value = (aware_datetime, 'poll_set')
            mock_get_poll_generator.return_value = poll_generator_mock

            # Call the method under test
            scheduler._schedule_polling()

            # Assert that find_starting_datetime was called correctly
            mock_find_starting_datetime.assert_called()
            hyperperiod = timedelta(minutes=5)
            start_offset = scheduler.group_config.start_offset

            # Assert that get_poll_generator was called with the correct arguments
            mock_get_poll_generator.assert_called_with(
                aware_datetime,    # initial_start
                hyperperiod,
                {timedelta(seconds=0): 'plan'}    # plan
            )

            # Assert that data_model.agent.core.schedule was called with the correct arguments
            scheduler.data_model.agent.core.schedule.assert_called_with(
                aware_datetime,    # start time
                scheduler._operate_polling,
                hyperperiod,
                poll_generator_mock,
                'poll_set')


class TestStaticCyclicPollSchedulerOperatePolling:

    @pytest.fixture
    def scheduler(self):
        """Fixture to create a StaticCyclicPollScheduler instance with mocked dependencies."""
        data_model_mock = MagicMock()
        data_model_mock.agent.core.schedule = MagicMock()
        group_config_mock = MagicMock()
        group_config_mock.start_offset = timedelta(seconds=0)

        scheduler = StaticCyclicPollScheduler(data_model=data_model_mock,
                                              group='test_group',
                                              group_config=group_config_mock)

        scheduler.pollers = {}

        return scheduler

    def test_operate_polling_calls_correct_methods(self, scheduler):
        """Test that _operate_polling calls the correct methods with expected arguments."""
        now = get_aware_utc_now()

        # Mock current_poll_set
        current_poll_set = MagicMock()
        current_poll_set.remote.poll_data = MagicMock()

        # Mock poll_generator
        poll_generator = MagicMock()
        next_start = now + timedelta(seconds=10)    # Next start time in the future
        next_poll_set = MagicMock()
        next_poll_set.points = {'point1': None}    # Assume it has points
        poll_generator.__next__.return_value = (next_start, next_poll_set)

        # Patch get_aware_utc_now to return 'now'
        with patch('volttron.utils.get_aware_utc_now', return_value=now):
            # Call the method under test
            scheduler._operate_polling('poller_id', poll_generator, current_poll_set)

        # Verify that current_poll_set.remote.poll_data was called
        current_poll_set.remote.poll_data.assert_called_with(current_poll_set)

        # Verify that data_model.agent.core.schedule was called with correct arguments
        scheduler.data_model.agent.core.schedule.assert_called_with(next_start,
                                                                    scheduler._operate_polling,
                                                                    'poller_id', poll_generator,
                                                                    next_poll_set)

    def test_operate_polling_no_next_poll_set_points(self, scheduler):
        """Test that _operate_polling does not schedule next poll if next_poll_set has no points."""
        now = get_aware_utc_now()

        # Mock current_poll_set
        current_poll_set = MagicMock()
        current_poll_set.remote.poll_data = MagicMock()

        # Mock poll_generator
        poll_generator = MagicMock()
        next_start = now + timedelta(seconds=10)    # Next start time in the future
        next_poll_set = MagicMock()
        next_poll_set.points = {}    # Empty points

        poll_generator.__next__.return_value = (next_start, next_poll_set)

        # Patch get_aware_utc_now to return 'now'
        with patch('volttron.utils.get_aware_utc_now', return_value=now):
            # Call the method under test
            scheduler._operate_polling('poller_id', poll_generator, current_poll_set)

        # Verify that current_poll_set.remote.poll_data was called
        current_poll_set.remote.poll_data.assert_called_with(current_poll_set)

        # Verify that data_model.agent.core.schedule was not called since points are empty
        scheduler.data_model.agent.core.schedule.assert_not_called()


class TestPollSetInit:

    @pytest.fixture
    def poll_set(self):
        # Mocking EquipmentTree
        data_model = MagicMock(spec=EquipmentTree)

        # Mocking DriverAgent
        remote = MagicMock(spec=DriverAgent)
        remote.unique_id = "remote_id"

        # Initialize PollSet with mocked dependencies
        poll_set = PollSet(data_model=data_model,
                           remote=remote,
                           points=WeakValueDictionary(),
                           single_depth={"depth1"},
                           single_breadth={("depth1", "breadth1")},
                           multi_depth={"device1": {"depth1", "depth2"}},
                           multi_breadth={"device_breadth1": {"breadth1", "breadth2"}})
        return poll_set

    def test_initialization(self, poll_set):
        assert poll_set.data_model is not None
        assert poll_set.remote is not None
        assert isinstance(poll_set.points, WeakValueDictionary)
        assert poll_set.single_depth == {"depth1"}
        assert poll_set.single_breadth == {("depth1", "breadth1")}
        assert poll_set.multi_depth == {"device1": {"depth1", "depth2"}}
        assert poll_set.multi_breadth == {"device_breadth1": {"breadth1", "breadth2"}}


class TestPollSetAddToPublishSetup:

    @pytest.fixture
    def poll_set(self):
        # Mocking EquipmentTree
        data_model = MagicMock(spec=EquipmentTree)
        # Set default return values for publish flags
        data_model.is_published_single_depth.return_value = False
        data_model.is_published_single_breadth.return_value = False
        data_model.is_published_multi_depth.return_value = False
        data_model.is_published_multi_breadth.return_value = False

        # Mocking DriverAgent
        remote = MagicMock(spec=DriverAgent)
        remote.unique_id = "remote_id"

        # Initialize PollSet with mocked dependencies
        poll_set = PollSet(data_model=data_model, remote=remote)
        return poll_set

    @pytest.fixture
    def point_node(self):

        def _create_point(name):
            point = PointNode(name)
            point.identifier = name    # Ensure identifier matches the name
            return point

        return _create_point

    def test_add_to_publish_setup_single_depth(self, poll_set, point_node):
        point = point_node("point1")
        poll_set.data_model.is_published_single_depth.return_value = True
        poll_set.data_model.get_point_topics.return_value = ("depth_topic", "breadth_topic")
        poll_set.data_model.get_device_topics.return_value = ("device_depth", "device_breadth"
                                                              )    # Add this line

        poll_set._add_to_publish_setup(point)

        assert "depth_topic" in poll_set.single_depth

    def test_add_to_publish_setup_single_breadth(self, poll_set, point_node):
        point = point_node("point1")
        poll_set.data_model.is_published_single_breadth.return_value = True
        poll_set.data_model.get_point_topics.return_value = ("depth_topic", "breadth_topic")
        poll_set.data_model.get_device_topics.return_value = ("device_depth", "device_breadth"
                                                              )    # Add this line

        poll_set._add_to_publish_setup(point)

        assert ("depth_topic", "breadth_topic") in poll_set.single_breadth

    def test_add_to_publish_setup_multi_depth(self, poll_set, point_node):
        point = point_node("point1")
        poll_set.data_model.is_published_multi_depth.return_value = True
        poll_set.data_model.get_point_topics.return_value = ("depth_topic", "breadth_topic")
        poll_set.data_model.get_device_topics.return_value = ("device_depth", "device_breadth")

        poll_set._add_to_publish_setup(point)

        assert "depth_topic" in poll_set.multi_depth["device_depth"]

    def test_add_to_publish_setup_multi_breadth(self, poll_set, point_node):
        point = point_node("point1")
        poll_set.data_model.is_published_multi_breadth.return_value = True
        poll_set.data_model.get_point_topics.return_value = ("depth_topic", "breadth_topic"
                                                             )    # Add this line
        poll_set.data_model.get_device_topics.return_value = ("device_depth", "device_breadth")

        poll_set._add_to_publish_setup(point)

        assert "point1" in poll_set.multi_breadth["device_breadth"]


class TestPollSetRemoveFromPublishSetup:

    @pytest.fixture
    def poll_set(self):
        data_model = MagicMock(spec=EquipmentTree)
        # Mock methods to return values based on identifier
        data_model.get_point_topics.side_effect = lambda identifier: (f"depth_{identifier}",
                                                                      f"breadth_{identifier}")
        data_model.get_device_topics.side_effect = lambda identifier: (
            f"device_depth_{identifier}", f"device_breadth_{identifier}")
        data_model.is_published_single_depth.return_value = True
        data_model.is_published_single_breadth.return_value = True
        data_model.is_published_multi_depth.return_value = True
        data_model.is_published_multi_breadth.return_value = True

        remote = MagicMock(spec=DriverAgent)
        remote.unique_id = "remote_id"

        poll_set = PollSet(data_model=data_model, remote=remote)

        # Pre-populate PollSet with data corresponding to the mocked topics
        point_identifier = "point1"
        point_depth, point_breadth = data_model.get_point_topics(point_identifier)
        device_depth, device_breadth = data_model.get_device_topics(point_identifier)
        poll_set.single_depth = {point_identifier}
        poll_set.single_breadth = {(point_depth, point_breadth)}
        poll_set.multi_depth = {device_depth: {point_depth}}
        poll_set.multi_breadth = {device_breadth: {point_identifier}}
        return poll_set

    @pytest.fixture
    def point_node(self):

        def _create_point(name):
            point = PointNode(name)
            point.identifier = name    # Ensure identifier matches the name
            return point

        return _create_point

    def test_remove_from_publish_setup(self, poll_set, point_node):
        point = point_node("point1")

        poll_set._remove_from_publish_setup(point)

        point_identifier = point.identifier
        point_depth, point_breadth = poll_set.data_model.get_point_topics(point_identifier)
        device_depth, device_breadth = poll_set.data_model.get_device_topics(point_identifier)

        # Verify the point has been removed from all sets
        assert point_identifier not in poll_set.single_depth
        assert (point_depth, point_breadth) not in poll_set.single_breadth
        assert point_depth not in poll_set.multi_depth.get(device_depth, set())
        assert point_identifier not in poll_set.multi_breadth.get(device_breadth, set())


class TestPollSetOr:

    def test_or_same_data_model_and_remote(self):
        # Create mock data_model and remote
        data_model = MagicMock(name='data_model')
        remote = MagicMock(name='remote')
        remote.unique_id = 'remote_id'

        # Create two PollSet instances with minimal data
        poll_set1 = PollSet(data_model=data_model,
                            remote=remote,
                            points={'point1': 'value1'},
                            single_depth={'point1'},
                            single_breadth={('depth1', 'breadth1')},
                            multi_depth={'device1': {'depth1'}},
                            multi_breadth={'device_breadth1': {'point1'}})

        poll_set2 = PollSet(data_model=data_model,
                            remote=remote,
                            points={'point2': 'value2'},
                            single_depth={'point2'},
                            single_breadth={('depth2', 'breadth2')},
                            multi_depth={'device1': {'depth2'}},
                            multi_breadth={'device_breadth1': {'point2'}})

        # Combine poll_sets
        combined = poll_set1 | poll_set2

        # Assertions
        assert combined.data_model == data_model
        assert combined.remote == remote
        assert combined.points == {'point1': 'value1', 'point2': 'value2'}
        assert combined.single_depth == {'point1', 'point2'}
        assert combined.single_breadth == {('depth1', 'breadth1'), ('depth2', 'breadth2')}
        assert combined.multi_depth == {'device1': {'depth1', 'depth2'}}
        assert combined.multi_breadth == {'device_breadth1': {'point1', 'point2'}}

    def test_or_different_data_model(self):
        # Create different data_models
        data_model1 = MagicMock(name='data_model1')
        data_model2 = MagicMock(name='data_model2')
        remote = MagicMock(name='remote')
        remote.unique_id = 'remote_id'

        poll_set1 = PollSet(data_model=data_model1, remote=remote)
        poll_set2 = PollSet(data_model=data_model2, remote=remote)

        with pytest.raises(ValueError) as exc_info:
            _ = poll_set1 | poll_set2

        assert 'Cannot combine PollSets based on different data models' in str(exc_info.value)

    def test_or_different_remote(self):
        data_model = MagicMock(name='data_model')
        remote1 = MagicMock(name='remote1')
        remote1.unique_id = 'remote_id1'
        remote2 = MagicMock(name='remote2')
        remote2.unique_id = 'remote_id2'

        poll_set1 = PollSet(data_model=data_model, remote=remote1)
        poll_set2 = PollSet(data_model=data_model, remote=remote2)

        with pytest.raises(ValueError) as exc_info:
            _ = poll_set1 | poll_set2

        assert 'Cannot combine PollSets based on different remotes' in str(exc_info.value)


if __name__ == '__main__':
    pytest.main()
