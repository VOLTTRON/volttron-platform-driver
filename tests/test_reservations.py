import pickle
import pytest
from mock import MagicMock, Mock
from platform_driver.agent import PlatformDriverAgent, PlatformDriverConfigV1, PlatformDriverConfig
from platform_driver.reservations import ReservationManager, Task, TimeSlice
from pickle import dumps
from base64 import b64encode
from datetime import datetime, timedelta
from volttrontesting import TestServer
from volttron.client import Agent
from pydantic import ValidationError

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

class TestPopulateReservation:
    requests = [
        ["device1", datetime(2022, 1, 1, 12, 0), datetime(2022, 1, 1, 13, 0)],
        ["device2", datetime(2022, 1, 1, 14, 0), datetime(2022, 1, 1, 15, 0)]
    ]
    @pytest.fixture
    def task(self):
        # Create a Task instance. Assuming the constructor accepts `agent_id`, `priority`, and `requests`.
        return Task(agent_id="test_agent", priority="HIGH", requests=[])

    def test_populate_reservation_with_valid_inputs(self, task):
        task.populate_reservation(self.requests)
        assert "device1" in task.devices
        assert len(task.devices["device1"].time_slots) == 1
        assert task.time_slice.start == datetime(2022, 1, 1, 12, 0)
        assert task.time_slice.end == datetime(2022, 1, 1, 15, 0)

    def test_populate_reservation_device_not_string(self, task):
        """ Tests calling populate reservation with device as non string"""
        requests = [
            # device is a bool instead of a true
            [True, datetime(2022, 1, 1, 12, 0), datetime(2022, 1, 1, 13, 0)],
            ["device2", datetime(2022, 1, 1, 14, 0), datetime(2022, 1, 1, 15, 0)]
        ]
        with pytest.raises(ValueError, match="Device not string."):
            task.populate_reservation(requests)
class TestTaskMakeCurrent:
    @pytest.fixture
    def task(self):
        # Create a Task instance with mock reservations and a set time slice.
        t = Task(agent_id="test_agent", priority="HIGH", requests=[])
        # Mocking device reservations within the task
        t.devices = {
            'device1': MagicMock(),
            'device2': MagicMock()
        }
        # our task is active starting NOW for 1 hour
        start_time = datetime.now()
        end_time = datetime.now() + timedelta(hours=1)
        t.time_slice = TimeSlice(start_time, end_time)
        return t

    def test_task_already_finished(self, task):
        """set task state to finished, which clears devices, then we check that task.devices is empty"""
        task.state = Task.STATE_FINISHED
        task.make_current(datetime.now())
        assert not task.devices, "Devices should be cleared when the task is finished."
    def test_remove_finished_reservations(self, task):
        """tests automatic removal of a device when task is finished and keeping of a non finished device"""
        now = datetime.now()
        # Set one reservation to be finished
        task.devices['device1'].finished.return_value = True
        task.devices['device2'].finished.return_value = False
        task.make_current(now)
        assert 'device1' not in task.devices
        assert 'device2' in task.devices
    def test_state_transition_to_pre_run(self, task):
        """Tests calling make current with a time before the task is set to start"""
        past_time = datetime.now() - timedelta(hours=1) # one hour before task starts
        task.make_current(past_time)
        assert task.state == Task.STATE_PRE_RUN
    def test_state_transition_running(self, task):
        """Tests calling make current 30 minutes after it has started """
        within_time = datetime.now() + timedelta(minutes=30) # 30 minutes after task has started
        task.make_current(within_time)
        assert task.state == Task.STATE_RUNNING
    def test_state_transition_finished(self, task):
        """Tets calling make current with a time after the task is finished """
        past_time = datetime.now() + timedelta(hours=2) # 1 hr after task was set to end
        task.make_current(past_time)
        assert task.state == Task.STATE_FINISHED

if __name__ == '__main__':
    pytest.main()