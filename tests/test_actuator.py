import unittest
import pickle
import pytest
from unittest.mock import Mock, MagicMock

from platform_driver.agent import PlatformDriverAgent
from platform_driver.reservations import ReservationManager

class TestNewTask:
    @pytest.fixture
    def setup(self):
        parent = Mock(spec=PlatformDriverAgent)
        parent.vip = Mock()  # add the 'vip' attribute to the mock object
        parent.vip.config.get = MagicMock(
        return_value=pickle.dumps({}))  # set up a return value for vip.config.get, called in load state
        grace_time = 10
        reservation_manager = ReservationManager(parent, grace_time)
        return reservation_manager

    def test_new_task_valid_inputs(self, setup):
        sender = 'sender1'
        task_id = 'task1'
        priority = 'HIGH'
        requests = [['device1', '2022-01-01T00:00:00', '2022-01-01T01:00:00']]
        result = setup.new_task(sender, task_id, priority, requests)
        assert result.success

    def test_new_task_invalid_task_id(self, setup):
        sender = 'sender1'
        task_id = None  # providing a invalid task id
        priority = 'HIGH'
        requests = [['device1', '2022-01-01T00:00:00', '2022-01-01T01:00:00']]
        result = setup.new_task(sender, task_id, priority, requests)
        assert not result.success
        assert result.info_string == 'MISSING_TASK_ID'

    def test_new_task_missing_priority_lowercase_invalid_priority(self, setup):
        sender = 'sender1'
        task_id = 'task1'
        priority = None # providing a invalid task id
        requests = [['device1', '2022-01-01T00:00', '2022-01-01T01:00']]
        result = setup.new_task(sender, task_id, priority, requests)
        assert not result.success
        assert result.info_string == 'MISSING_PRIORITY'

        priority = 'low' # providing a lower case priority
        result = setup.new_task(sender, task_id, priority, requests)
        assert result.success

        priority = "MEDIUM" # providing a invalid task id
        result = setup.new_task(sender, task_id, priority, requests)
        assert not result.success
        assert result.info_string == 'INVALID_PRIORITY'

if __name__ == '__main__':
    unittest.main()
