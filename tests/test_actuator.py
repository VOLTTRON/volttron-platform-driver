import pickle
import pytest

from unittest.mock import Mock, MagicMock
from platform_driver.agent import PlatformDriverAgent
from platform_driver.reservations import ReservationManager
from pickle import dumps
from base64 import b64encode
from datetime import datetime, timedelta

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
        result = setup.new_task(self.sender, self.task_id, priority="HIGH", requests=[])
        assert result.info_string == 'MALFORMED_REQUEST_EMPTY' and not result.success
    def test_new_task_missing_priority(self, setup):
        result = setup.new_task(self.sender, self.task_id, priority=None, requests=self.requests)
        assert result.info_string == 'MISSING_PRIORITY' and not result.success
    def test_lowercase_priority(self, setup):
        result = setup.new_task(self.sender, self.task_id, priority="low", requests=self.requests)
        assert result.success
    def test_invalid_priority(self, setup):
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
        Test schedule preemption by a higher priority task from the same agent.
        """
        result = setup.new_task(self.sender, self.task_id, priority='LOW_PREEMPT', requests=self.requests)
        assert result.success
        result = setup.new_task(self.sender, "high_priority_task_id", priority='HIGH', requests=self.requests)
        assert result.success
        assert result.info_string == 'TASKS_WERE_PREEMPTED'
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