import pickle
import pytest

from mock import Mock, MagicMock
from platform_driver.agent import PlatformDriverAgent
from platform_driver.reservations import ReservationManager
from pickle import dumps
from base64 import b64encode
from datetime import datetime, timedelta
from volttrontesting import TestServer
from volttron.client import Agent

class TestNewTask:
    sender = "test.agent"
    task_id = "test_task_id"
    requests = [['device1', '2022-01-01T00:00:00', '2022-01-01T01:00:00']]
    @pytest.fixture
    def setup(self):
        parent = Mock()
        parent.vip = Mock()
        parent.vip.config.get = MagicMock(return_value=pickle.dumps({}))
        parent.vip.config.set = MagicMock()
        parent.config = Mock()
        parent.config.reservation_publish_interval = 60  # Mock the interval for testing
        grace_time = 10

        reservation_manager = ReservationManager(parent, grace_time)
        reservation_manager._cleanup = MagicMock()
        reservation_manager.save_state = MagicMock()
        return reservation_manager
    @pytest.fixture
    def PDA(self):
        parent = Mock()
        parent.vip = Mock()
        parent.vip.config.get = MagicMock(return_value=pickle.dumps({}))
        parent.vip.config.set = MagicMock()
        parent.config = Mock()
        parent.config.reservation_publish_interval = 60  # Mock the interval for testing
        grace_time = 10

        PDA = PlatformDriverAgent()
        return PDA
    def test_new_task_valid_inputs(self, setup):
        result = setup.new_task(self.sender, self.task_id, priority='HIGH', requests=self.requests)
        assert result.success
    def test_new_task_with_invalid_sender(self, setup):
        result = setup.new_task(sender="", task_id=self.task_id, priority="HIGH", requests=self.requests)
        assert result.info_string == 'MALFORMED_REQUEST: TypeError: agent_id must be a nonempty string' and not result.success
    def test_missing_agent_id(self, setup):
        result = setup.new_task(sender=None, task_id=self.task_id, priority="HIGH", requests=self.requests)
        assert result.info_string == 'MISSING_AGENT_ID' and not result.success
    def test_invalid_task_id(self, setup):
        """ Tests task request with missing task id, empty task id, and int task id"""
        result = setup.new_task(self.sender, task_id=None, priority="HIGH", requests=self.requests)
        assert result.info_string == 'MISSING_TASK_ID' and not result.success

        result = setup.new_task(self.sender, task_id="", priority="HIGH", requests=self.requests)
        assert result.info_string == 'MALFORMED_REQUEST: TypeError: taskid must be a nonempty string' and not result.success

        result = setup.new_task(self.sender, task_id=1234, priority="HIGH", requests=self.requests)
        assert result.info_string == 'MALFORMED_REQUEST: TypeError: taskid must be a nonempty string' and not result.success
    def test_requests_malformed(self, setup):
        """ Tests malformed request by creating new task with empty dates"""
        result = setup.new_task(self.sender, self.task_id, priority="HIGH", requests=[])
        assert result.info_string == 'MALFORMED_REQUEST_EMPTY' and not result.success
    def test_new_task_missing_priority(self, setup):
        result = setup.new_task(self.sender, self.task_id, priority=None, requests=self.requests)
        assert result.info_string == 'MISSING_PRIORITY' and not result.success
    def test_lowercase_priority(self, setup):
        result = setup.new_task(self.sender, self.task_id, priority="low", requests=self.requests)
        assert result.success
    def test_invalid_priority(self, setup):
        """ Tests an invalid priority (Medium priority does not exist)"""
        result = setup.new_task(self.sender, self.task_id, priority="MEDIUM", requests=self.requests)
        assert result.info_string == 'INVALID_PRIORITY' and not result.success
    def test_task_exists(self, setup):
        task_id = "test_task_id"
        mock_task = Mock()
        mock_task.make_current = Mock()  # add the make_current method to the mock task
        setup.tasks[task_id] = mock_task

        result = setup.new_task(self.sender, task_id, priority="HIGH", requests=self.requests)
        assert result.info_string == 'TASK_ID_ALREADY_EXISTS' and result.success == False
    def test_request_new_task_should_succeed_on_preempt_self(self, setup):
        """
        Test schedule preemption by a higher priority task from the same sender.
        """
        result = setup.new_task(self.sender, self.task_id, priority='LOW_PREEMPT', requests=self.requests)
        assert result.success
        result = setup.new_task(self.sender, "high_priority_task_id", priority='HIGH', requests=self.requests)
        assert result.success
        assert result.info_string == 'TASKS_WERE_PREEMPTED'
    def test_schedule_preempt_other(self, setup):
        """
        Test schedule preemption by a higher priority task from a different sender.
        """
        result = setup.new_task("agent1", self.task_id, priority='LOW_PREEMPT', requests=self.requests)
        assert result.success
        result = setup.new_task("agent2", "high_priority_task_id", priority='HIGH', requests=self.requests)
        assert result.success
        assert result.info_string == 'TASKS_WERE_PREEMPTED'
    def test_reservation_conflict(self, setup):
        """
        Test task conflict from different agents.
        """
        result = setup.new_task("agent1", self.task_id, priority='LOW', requests=self.requests)
        assert result.success
        result = setup.new_task("agent2", "different_task_id", priority='LOW', requests=self.requests)
        assert result.info_string == 'CONFLICTS_WITH_EXISTING_RESERVATIONS'
    def test_reservation_conflict_self(self, setup):
        """
        Test task conflict from one request.
        """
        # two tasks with same time frame
        requests = [
            ['device1', '2022-01-01T00:00:00', '2022-01-01T01:00:00'],
            ['device1', '2022-01-01T00:00:00', '2022-01-01T01:00:00']
        ]
        result = setup.new_task("agent2", self.task_id, priority='LOW', requests=requests)
        assert result.info_string == 'REQUEST_CONFLICTS_WITH_SELF'
    def test_schedule_overlap(self, setup):
        """
        Test successful task when end time of one time slot is the same as
        start time of another slot.
        """
        time_1 = ['device1', '2022-01-01T00:00:00', '2022-01-01T01:00:00']
        time_2 = ['device2', '2022-01-01T01:00:00', '2022-01-02T01:00:00']
        result = setup.new_task("agent1", self.task_id, priority='LOW', requests=time_1)
        assert result.success
        result = setup.new_task("agent2", "different_task_id", priority='LOW', requests=time_2)
        assert result.success
    def test_cancel_error_invalid_task(self, setup):
        """
        Test invalid task id when trying to cancel a task.
        """
        # creating task with a task_id of "task_that_exists"
        result = setup.new_task(self.sender, task_id="task_that_exists", priority='LOW', requests=self.requests)
        assert result.success
        # trying to cancel a task with a task_id of "unexistent_task_id"
        result = setup.cancel_task(sender=self.sender, task_id="unexistent_task_id")
        assert result.info_string == 'TASK_ID_DOES_NOT_EXIST'

    def test_get_value_success(self, PDA):
        # TODO again, not sure if this is really even worth keeping. probably remove. does not really test anything.
        """
        Tests that get_point can retrieve points from a remote node.
        """
        # Create a dictionary to simulate device state
        device_state = {'devices/device1/point1': 20.5, 'devices/device1/point2': 30.5}
        mock_remote = Mock()

        # set up the get_point method to return values from the device_state dictionary
        def mock_get_point(point_name):
            if point_name in device_state:
                return device_state[point_name]
            else:
                raise ValueError(f'Point not found: {point_name}')

        # call our fake loop method instead of remote.get_point
        mock_remote.get_point.side_effect = mock_get_point

        # create a mock object to simulate a node in the equipment_tree
        mock_node = Mock()
        mock_node.get_remote.return_value = mock_remote

        # Replace the get_node method of the equipment_tree with a MagicMock
        PDA.equipment_tree.get_node = MagicMock(return_value=mock_node)

        result = PDA.get_point('device1', 'point1')
        assert result == 20.5
    def test_get_error_invalid_point_get_node_non_existent_point(self, PDA):
        """
        Tests that get_point can retrieve points from a remote node.
        """
        # Create a dictionary to simulate device state
        device_state = {'devices/device1/point1': 20.5, 'devices/device1/point2': 30.5}

        # Create a mock object to simulate the remote interface
        mock_remote = Mock()

        # create a mock object to simulate a node in the equipment_tree
        mock_node = Mock()
        mock_node.get_remote.return_value = mock_remote

        # Replace the get_node method of the equipment_tree with a MagicMock
        PDA.equipment_tree.get_node = MagicMock(return_value=None)

        with pytest.raises(ValueError) as excinfo:
            PDA.get_point('device1', 'non_existent_point')
        assert str(excinfo.value) == 'No equipment found for topic: devices/device1/non_existent_point'

    def test_handle_get(self, PDA):
        """Tests if handle_get correctly publishes the point it receives from get_point"""
        ts = TestServer()
        ts.connect_agent(PDA)

        # Mock the get_point method to return 20.5
        PDA.get_point = MagicMock(return_value=20.5)

        # Subscribe to the value response topic
        subscriber = ts.subscribe('devices/actuators/value/device1/point12')

        # Call the function you're testing with a valid point name
        PDA.handle_get(None, 'test_sender', None, 'devices/actuators/get/device1/point12', None, None)

        # Check that a message was published to the value response topic with the expected value
        messages = subscriber.received_messages()
        assert messages[0].message == 20.5

class TestSetPoint:
    sender = "test.agent"
    path = "devices/device1"
    point_name = "SampleWritableFloat1"
    value = 0.2

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        # Mock 'vip' components
        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        # Mock _equipment_id
        PDA._equipment_id = Mock(return_value="processed_point_name")

        # Mock 'equipment_tree.get_node'
        node_mock = MagicMock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.get_node = Mock(return_value=node_mock)

        # Mock other methods called in set_point
        node_mock.get_remote = Mock(return_value=Mock())
        PDA.equipment_tree.raise_on_locks = Mock()
        PDA._get_headers = Mock(return_value={})
        PDA._push_result_topic_pair = Mock()

        return PDA

    def test_set_point_calls_equipment_id_with_correct_parameters(self, PDA):
        """Test set_point calls equipment_id method with correct parameters."""
        PDA.set_point(path = 'device/topic', point_name = 'SampleWritableFloat', value = 42, kwargs = {})
        # Assert that self._equipment_id was called with the correct arguments
        PDA._equipment_id.assert_called_with("device/topic", "SampleWritableFloat")
    def test_set_point_with_topic_kwarg(self, PDA):
        """Test handling of 'topic' as keyword arg"""
        kwargs = {'topic': 'device/topic'}
        PDA.set_point(path = 'ignored_path', point_name = None, value = 42, **kwargs)
        PDA._equipment_id.assert_called_with('device/topic', None)
    def test_set_point_with_point_kwarg(self, PDA):
        """ Test handling of 'point' keyword arg """
        kwargs = {'point': 'SampleWritableFloat'}
        PDA.set_point(path = 'device/topic', point_name = None, value = 42, **kwargs)
        PDA._equipment_id.assert_called_with('device/topic', 'SampleWritableFloat')
    def test_set_point_with_combined_path_and_empty_point(self, PDA):
        """Test handling of path containing the point name and point_name is empty"""
        # TODO is this expected? to call it with None? it does not use the path with the point name when point name is none?
        kwargs = {}
        PDA.set_point(path = 'device/topic/SampleWritableFloat', point_name = None, value = 42, **kwargs)
        PDA._equipment_id.assert_called_with("device/topic/SampleWritableFloat", None)

    def test_set_point_raises_error_for_invalid_node(self, PDA):
        # Mock get_node to return None
        PDA.equipment_tree.get_node.return_value = None
        kwargs = {}

        # Call the set_point function and check for ValueError
        with pytest.raises(ValueError, match="No equipment found for topic: processed_point_name"):
            PDA.set_point(path = 'device/topic', point_name = 'SampleWritableFloat', value = 42, **kwargs)
    def test_set_point_deprecated(self, PDA):
        """Test old style actuator call"""
        PDA.set_point("ilc.agnet", 'device/topic', 42, 'SampleWritableFloat', {})
        # TODO shouldnt this work? its receiving old style params but adding none to the end...
        # TODO as of now it is mostly working it seems.
        # Assert that self._equipment_id was called with the correct arguments
        # TODO this should be the same as the test above it but for some reason its not.
        PDA._equipment_id.assert_called_with(("device/topic", "SampleWritableFloat"), None)

class TestHandleSet:
    sender = "test.agent"
    topic = "devices/actuators/set/device1/SampleWritableFloat1"
    message = 0.2

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        PDA._equipment_id = Mock(return_value="processed_point_name")

        # Mock 'equipment_tree.get_node'
        node_mock = MagicMock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.get_node = Mock(return_value=node_mock)

        PDA._get_headers = Mock(return_value={})
        PDA._push_result_topic_pair = Mock()

        PDA.set_point = Mock()

        PDA._push_result_topic_pair = Mock()
        PDA.get_point = Mock()
        return PDA
    def test_handle_set_calls_set_point_with_correct_parameters(self, PDA):
        """Test handle_set calls set_point with correct parameters"""
        PDA.handle_set(None, self.sender, None, self.topic, None, self.message)
        PDA.set_point.assert_called_with("device1/SampleWritableFloat1", None, self.message)

class TestHandleGet:
    sender = "test.agent"
    topic = "devices/actuators/get/device1/SampleWritableFloat1"

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        # Mock 'vip' components
        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        PDA._equipment_id = Mock(return_value="processed_point_name")

        # Mock 'equipment_tree.get_node'
        node_mock = MagicMock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.get_node = Mock(return_value=node_mock)

        PDA._get_headers = Mock(return_value={})
        PDA._push_result_topic_pair = Mock()

        PDA.get_point = Mock()

        return PDA
    def test_handle_get_calls_get_point_with_correct_parameters(self, PDA):
        """Test handle_get calls get_point with correct parameters."""
        PDA.handle_get(None, self.sender, None, self.topic, None, None)
        PDA.get_point.assert_called_with("device1/SampleWritableFloat1")

class TestGetPoint:
    sender = "test.agent"
    path = "devices/device1"
    point_name = "SampleWritableFloat1"
    value = 0.2

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        # Mock 'vip' components
        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        # Mock _equipment_id
        PDA._equipment_id = Mock(return_value="processed_point_name")

        # Mock 'equipment_tree.get_node'
        node_mock = MagicMock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.get_node = Mock(return_value=node_mock)

        # Mock other methods called in set_point
        node_mock.get_remote = Mock(return_value=Mock())
        PDA.equipment_tree.raise_on_locks = Mock()
        PDA._get_headers = Mock(return_value={})
        PDA._push_result_topic_pair = Mock()

        return PDA
    def test_get_point_calls_equipment_id_with_correct_parameters(self, PDA):
        """Test get_point calls equipment_id method with correct parameters."""
        PDA.get_point(path='device/topic', point_name='SampleWritableFloat', kwargs={})
        # Assert that self._equipment_id was called with the correct arguments
        PDA._equipment_id.assert_called_with("device/topic", "SampleWritableFloat")
    def test_get_point_with_topic_kwarg(self, PDA):
        """Test handling of 'topic' as keyword arg"""
        kwargs = {'topic': 'device/topic'}
        PDA.get_point(path=None, point_name=None, **kwargs)
        PDA._equipment_id.assert_called_with('device/topic', None)
    def test_get_point_with_point_kwarg(self, PDA):
        """ Test handling of 'point' keyword arg """
        kwargs = {'point': 'SampleWritableFloat'}
        PDA.get_point(path='device/topic', point_name=None, **kwargs)
        PDA._equipment_id.assert_called_with('device/topic', 'SampleWritableFloat')
    def test_get_point_with_combined_path_and_empty_point(self, PDA):
        """Test handling of path containing the point name and point_name is empty"""
        # TODO again, is it supposed to get rid of None? does it still work?
        kwargs = {}
        PDA.get_point(path='device/topic/SampleWritableFloat', point_name=None, **kwargs)
        PDA._equipment_id.assert_called_with("device/topic/SampleWritableFloat", None)
    def test_get_point_raises_error_for_invalid_node(self, PDA):
        """Test get_point raises error when node is invalid"""
        PDA.equipment_tree.get_node.return_value = None
        kwargs = {}
        with pytest.raises(ValueError, match="No equipment found for topic: processed_point_name"):
            PDA.get_point(path='device/topic', point_name='SampleWritableFloat', **kwargs)
    def test_get_point_raises_error_for_invalid_remote(self, PDA):
        """Test get_point raises error when remote is invalid"""
        # Ensure get_node returns a valid node mock
        node_mock = Mock()
        node_mock.get_remote = Mock(return_value=None)
        PDA.equipment_tree.get_node = Mock(return_value=node_mock)

        kwargs = {}

        with pytest.raises(ValueError, match="No remote found for topic: processed_point_name"):
            PDA.get_point(path = 'device/topic', point_name = 'SampleWritableFloat', **kwargs)
    def test_get_point_with_kwargs_as_topic_point(self, PDA):
        """Test handling of old actuator-style arguments"""

        kwargs = {'topic': 'device/topic', 'point': 'SampleWritableFloat'}

        result = PDA.get_point(path=None, point_name=None, **kwargs)

        PDA._equipment_id.assert_called_with('device/topic', 'SampleWritableFloat')
    def test_get_point_old_style_call(self, PDA):
        """Test get point with old actuator style call"""
        kwargs = {}
        PDA.get_point(topic='device/topic', point="SampleWritableFloat", **kwargs)
        PDA._equipment_id.assert_called_with("device/topic", "SampleWritableFloat")
    def test_get_point_old_style_call_with_kwargs(self, PDA):
        """Test get point with old actuator style call and with kwargs"""
        kwargs = {"random_thing": "test"}
        PDA.get_point(topic='device/topic', point="SampleWritableFloat", **kwargs)
        PDA._equipment_id.assert_called_with("device/topic", "SampleWritableFloat")

class TestCancelTask:
    sender = "test.agent"
    task_id = "test_task_id"

    @pytest.fixture
    def reservation_manager(self):
        parent = Mock(spec=PlatformDriverAgent)
        parent.vip = Mock()
        parent.vip.config = Mock()
        parent.vip.config.get = MagicMock(return_value=pickle.dumps({}))
        parent.config = Mock()
        parent.config.reservation_publish_interval = 10
        parent.core = Mock()
        grace_time = 10
        reservation_manager = ReservationManager(parent, grace_time)
        return reservation_manager
    def test_cancel_task_nonexistent_id(self, reservation_manager):
        result = reservation_manager.cancel_task(self.sender, self.task_id)
        assert result.success == False
        assert result.info_string == 'TASK_ID_DOES_NOT_EXIST'

    def test_cancel_task_agent_id_mismatch(self, reservation_manager):
        # Add a task with a different agent ID
        reservation_manager.tasks[self.task_id] = Mock(agent_id="different.agent")
        result = reservation_manager.cancel_task(self.sender, self.task_id)
        assert result.success == False
        assert result.info_string == 'AGENT_ID_TASK_ID_MISMATCH'

    def test_cancel_task_success(self, reservation_manager):
        # Add a task with the correct agent ID
        reservation_manager.tasks[self.task_id] = Mock(agent_id=self.sender)
        result = reservation_manager.cancel_task(self.sender, self.task_id)
        assert result.success == True
        assert self.task_id not in reservation_manager.tasks
class TestSaveState:
    sender = "test.agent"
    task_id = "test_task_id"
    now = datetime.now()

    @pytest.fixture
    def reservation_manager(self):
        parent = Mock()
        parent.vip = Mock()
        parent.vip.config = Mock()
        parent.vip.config.set = MagicMock()

        logger = Mock()
        logger.error = MagicMock()

        # Setting up the ReservationManager with mocked logger
        grace_time = 10
        manager = ReservationManager(parent, grace_time)
        manager._cleanup = MagicMock()
        manager._log = logger

        return manager
    def test_save_state_set_called_once(self, reservation_manager):
        expected_data = b64encode(dumps(reservation_manager.tasks)).decode("utf-8")

        reservation_manager.save_state(self.now)

        # Tests if our mocked object was called once, and with the correct args
        reservation_manager.parent.vip.config.set.assert_called_once_with(
            reservation_manager.reservation_state_file,
            expected_data,
            send_update=False
        )
    def test_save_state_correct_file_name(self, reservation_manager):
        # make sure it's correct before
        assert reservation_manager.reservation_state_file == "_reservation_state"  #
        reservation_manager.save_state(self.now)
        # and after calling save_state
        assert reservation_manager.reservation_state_file == "_reservation_state"

if __name__ == '__main__':
    pytest.main()