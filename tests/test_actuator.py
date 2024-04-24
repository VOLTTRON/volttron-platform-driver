import unittest
import pickle
from unittest.mock import Mock, MagicMock

from platform_driver.agent import PlatformDriverAgent
from platform_driver.reservations import ReservationManager

class TestNewTask(unittest.TestCase):
    def setUp(self):
        self.parent = Mock(spec=PlatformDriverAgent)
        self.parent.vip = Mock()  # add the 'vip' attribute to the mock object
        self.parent.vip.config.get = MagicMock(return_value=pickle.dumps({}))  # set up a return value for vip.config.get, called in load state
        self.grace_time = 10
        self.reservation_manager = ReservationManager(self.parent, self.grace_time)

    def test_new_task_valid_inputs(self):
        sender = 'sender1'
        task_id = 'task1'
        priority = 'HIGH'
        requests = [['device1', '2022-01-01T00:00:00', '2022-01-01T01:00:00']]
        result = self.reservation_manager.new_task(sender, task_id, priority, requests)
        self.assertTrue(result.success)

    def test_new_task_invalid_task_id(self):
        sender = 'sender1'
        task_id = None  # providing a invalid task id
        priority = 'HIGH'
        requests = [['device1', '2022-01-01T00:00:00', '2022-01-01T01:00:00']]
        result = self.reservation_manager.new_task(sender, task_id, priority, requests)
        self.assertFalse(result.success)
        self.assertEqual(result.info_string, 'MISSING_TASK_ID')

    def test_new_task_missing_priority_lowercase_invalid_priority(self):
        sender = 'sender1'
        task_id = 'task1'
        priority = None # providing a invalid task id
        requests = [['device1', '2022-01-01T00:00', '2022-01-01T01:00']]
        result = self.reservation_manager.new_task(sender, task_id, priority, requests)
        self.assertFalse(result.success)
        self.assertEqual(result.info_string, 'MISSING_PRIORITY')

        priority = 'low' # providing a lower case priority
        result = self.reservation_manager.new_task(sender, task_id, priority, requests)
        self.assertTrue(result.success)

        priority = "MEDIUM" # providing a invalid task id
        result = self.reservation_manager.new_task(sender, task_id, priority, requests)
        self.assertFalse(result.success)
        self.assertEqual(result.info_string, 'INVALID_PRIORITY')

if __name__ == '__main__':
    unittest.main()
