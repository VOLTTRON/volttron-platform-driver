import pytest
from unittest.mock import MagicMock, Mock, patch
from datetime import datetime

from volttron.utils import format_timestamp, get_aware_utc_now
from platform_driver.agent import PlatformDriverAgent
from platform_driver.constants import VALUE_RESPONSE_PREFIX, RESERVATION_RESULT_TOPIC

class TestPlatformDriverAgentConfigureMain:

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.core = MagicMock()
        PDA.vip = MagicMock()

        PDA.vip.config.get = Mock(return_value='{}')

        return PDA

    def test_configure_main_calls_configure_publish_lock(self, PDA):
        """Tests the configure main calls setup_socket_lock and configure_publish_lock when action is new"""
        with patch('platform_driver.agent.setup_socket_lock') as mock_setup_socket_lock, \
             patch('platform_driver.agent.configure_publish_lock') as mock_configure_publish_lock:
            contents = {'config_version': 2, 'publish_depth_first_any': True}
            PDA.configure_main(_="", action="NEW", contents=contents)
            mock_setup_socket_lock.assert_called_once()
            mock_configure_publish_lock.assert_called_once()


class TestPlatformDriverAgentConfigureNewEquipment:
    """Tests for _configure_new_equipment."""
    # TODO wait for function to be fully finished
    pass


class TestPlatformDriverAgentGetOrCreateRemote:
    """Tests for _get_or_create_remote"""
    # TODO wait for function to be fully finished
    pass


class TestPlatformDriverAgentUpdateEquipment:
    """Tests for _update_equipment."""
    # TODO wait for function to be fully finished
    pass


class TestPlatformDriverAgentRemoveEquipment:
    """Tests for _remove_equipment."""
    # TODO wait for function to be fully finished
    pass


class TestPlatformDriverAgentSemanticQuery:
    """Tests for resolve_tags"""
    pass

    # @pytest.fixture
    # def PDA(self):
    #     agent = PlatformDriverAgent()
    #     agent.vip = MagicMock()
    #     return agent


class TestPlatformDriverAgentBuildQueryPlan:
    """Tests for build_query_plan"""

    @pytest.fixture
    def PDA(self):
        agent = PlatformDriverAgent()
        agent.vip = MagicMock()

        point_node_mock = MagicMock()
        point_node_mock.identifier = 'point1'
        driver_agent_mock = MagicMock()

        equipment_tree_mock = MagicMock()
        equipment_tree_mock.find_points = MagicMock(return_value=[point_node_mock])
        equipment_tree_mock.get_remote = MagicMock(return_value=driver_agent_mock)

        agent.equipment_tree = equipment_tree_mock

        agent.point_node_mock = point_node_mock
        agent.driver_agent_mock = driver_agent_mock

        return agent

    def test_find_points_called_correctly(self, PDA):
        """Tests find_points called with correct arguments"""
        PDA.build_query_plan(topic="topic")
        PDA.equipment_tree.find_points.assert_called_once()

    def test_get_remote_called_correctly(self, PDA):
        """Tests get_remote called with correct point identifier."""
        PDA.build_query_plan(topic="topic")
        PDA.equipment_tree.get_remote.assert_called_once_with('point1')

    def test_build_query_plan_result(self, PDA):
        """Tests build_query_plan returns correct result."""
        result = PDA.build_query_plan(topic="topic")

        expected_result = dict()
        expected_result[PDA.driver_agent_mock] = {PDA.point_node_mock}
        assert result == expected_result


class TestPlatformDriverAgentGet:
    """Tests for get."""

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.vip = MagicMock()
        PDA.equipment_tree = MagicMock()
        return PDA

    def test_get_no_points(self, PDA):
        """Test get method with no points in the query plan."""
        PDA.build_query_plan = MagicMock(return_value={})

        results, errors = PDA.get(topic=None, regex=None)

        assert results == {}
        assert errors == {}
        PDA.build_query_plan.assert_called_once_with(None, None)

    def test_get_with_node_not_found(self, PDA):
        """Test get method where a node is not found in the equipment tree"""
        remote_mock = MagicMock()
        point_mock = MagicMock(identifier="point")

        # Mock the build_query_plan to return a predefined query plan
        PDA.build_query_plan = MagicMock(return_value={remote_mock: {point_mock}})

        remote_mock.get_multiple_points.return_value = ({"point": "value"}, {"point_err": "error"})

        PDA.equipment_tree.get_node.return_value = None

        results, errors = PDA.get(topic="topic", regex="regex")

        assert results == {"point": "value"}
        assert errors == {"point_err": "error"}

        # Validate if methods were called with correct parameters
        PDA.build_query_plan.assert_called_once_with("topic", "regex")
        remote_mock.get_multiple_points.assert_called_once_with(["point"])


class TestPlatformDriverAgentSet:
    """Tests for set"""
    pass    # TODO wait for final additions


class TestPlatformDriverAgentRevert:
    """Tests for revert"""
    pass    # TODO wait for final additions


class TestPlatformDriverAgentLast:
    """Tests for Last"""

    @pytest.fixture
    def PDA(self):
        agent = PlatformDriverAgent()
        agent.vip = MagicMock()
        agent.equipment_tree = MagicMock()
        agent.poll_scheduler = MagicMock()
        return agent

    def test_last_default(self, PDA):
        """Test last method with default arguments."""
        point_mock = MagicMock(topic="point1",
                               last_value="value1",
                               last_updated="2023-01-01T00:00:00Z")
        PDA.equipment_tree.find_points.return_value = [point_mock]

        result = PDA.last(topic="topic")
        expected = {"point1": {"value": "value1", "updated": "2023-01-01T00:00:00Z"}}
        assert result == expected
        PDA.equipment_tree.find_points.assert_called_once_with("topic", None)


# class TestPlatformDriverAgentStart:
#     """Tests for Start"""
#
#     @pytest.fixture
#     def PDA(self):
#         agent = PlatformDriverAgent()
#         agent.vip = MagicMock()
#         agent.equipment_tree = MagicMock()
#         agent.poll_scheduler = MagicMock()
#         agent.config = MagicMock()
#         return agent
#
#     def test_start_no_points_found(self, PDA):
#         """Test start method with no matching points."""
#         PDA.equipment_tree.find_points.return_value = []
#
#         PDA.start(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         PDA.poll_scheduler.schedule.assert_not_called()
#         PDA.poll_scheduler.add_to_schedule.assert_not_called()
#
#     def test_start_points_already_active(self, PDA):
#         """Test start method where the points are already active."""
#         point_mock = MagicMock(topic="point1", active=True)
#         PDA.equipment_tree.find_points.return_value = [point_mock]
#
#         PDA.start(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert point_mock.active is True
#         PDA.poll_scheduler.schedule.assert_not_called()
#         PDA.poll_scheduler.add_to_schedule.assert_not_called()
#
#     def test_start_points_not_active_reschedule_allowed(self, PDA):
#         """Test start method where points are not active and rescheduling is allowed."""
#         PDA.config.allow_reschedule = True
#         point_mock = MagicMock(topic="point1", active=False)
#         PDA.equipment_tree.find_points.return_value = [point_mock]
#
#         PDA.start(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert point_mock.active is True
#         PDA.poll_scheduler.schedule.assert_called_once()
#         PDA.poll_scheduler.add_to_schedule.assert_not_called()
#
#     def test_start_points_not_active_reschedule_not_allowed(self, PDA):
#         """Test start method where points are not active and rescheduling is not allowed."""
#         PDA.config.allow_reschedule = False
#         point_mock = MagicMock(topic="point1", active=False)
#         PDA.equipment_tree.find_points.return_value = [point_mock]
#
#         PDA.start(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert point_mock.active is True
#         PDA.poll_scheduler.schedule.assert_not_called()
#         PDA.poll_scheduler.add_to_schedule.assert_called_once_with(point_mock)


# class TestPlatformDriverAgentStop:
#     """Tests for Stop"""
#
#     @pytest.fixture
#     def PDA(self):
#         agent = PlatformDriverAgent()
#         agent.vip = MagicMock()
#         agent.equipment_tree = MagicMock()
#         agent.poll_scheduler = MagicMock()
#         agent.config = MagicMock()
#         return agent
#
#     def test_stop_no_points_found(self, PDA):
#         """Test stop method with no matching points."""
#         PDA.equipment_tree.find_points.return_value = []
#
#         PDA.stop(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         PDA.poll_scheduler.schedule.assert_not_called()
#         PDA.poll_scheduler.remove_from_schedule.assert_not_called()
#
#     def test_stop_points_already_inactive(self, PDA):
#         """Test stop method where the points are already inactive."""
#         point_mock = MagicMock(topic="point1", active=False)
#         PDA.equipment_tree.find_points.return_value = [point_mock]
#
#         PDA.stop(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert point_mock.active is False
#         PDA.poll_scheduler.schedule.assert_not_called()
#         PDA.poll_scheduler.remove_from_schedule.assert_not_called()
#
#     def test_stop_points_active_reschedule_allowed(self, PDA):
#         """Test stop method where points are active and rescheduling is allowed."""
#         PDA.config.allow_reschedule = True
#         point_mock = MagicMock(topic="point1", active=True)
#         PDA.equipment_tree.find_points.return_value = [point_mock]
#
#         PDA.stop(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert point_mock.active is False
#         PDA.poll_scheduler.schedule.assert_called_once()
#         PDA.poll_scheduler.remove_from_schedule.assert_not_called()
#
#     def test_stop_points_active_reschedule_not_allowed(self, PDA):
#         """Test stop method where points are active and rescheduling is not allowed."""
#         PDA.config.allow_reschedule = False
#         point_mock = MagicMock(topic="point1", active=True)
#         PDA.equipment_tree.find_points.return_value = [point_mock]
#
#         PDA.stop(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert point_mock.active is False
#         PDA.poll_scheduler.schedule.assert_not_called()
#         PDA.poll_scheduler.remove_from_schedule.assert_called_once_with(point_mock)


# class TestPlatformDriverAgentEnable:
#
#     @pytest.fixture
#     def PDA(self):
#         agent = PlatformDriverAgent()
#         agent.vip = MagicMock()
#         agent.equipment_tree = MagicMock()
#         return agent
#
#     def test_enable_no_nodes_found(self, PDA):
#         """Test enable method with no matching nodes."""
#         PDA.equipment_tree.find_points.return_value = []
#
#         PDA.enable(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         PDA.vip.config.set.assert_not_called()
#         PDA.equipment_tree.get_device_node.assert_not_called()
#
#     def test_enable_non_point_nodes(self, PDA):
#         """Test enable method on non-point nodes without triggering callback."""
#         node_mock = MagicMock(is_point=False, topic="node1", config={})
#         PDA.equipment_tree.find_points.return_value = [node_mock]
#
#         PDA.enable(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert node_mock.config['active'] is True
#         PDA.vip.config.set.assert_called_once_with(node_mock.topic,
#                                                    node_mock.config,
#                                                    trigger_callback=False)
#         PDA.equipment_tree.get_device_node.assert_not_called()
#
#     def test_enable_point_nodes(self, PDA):
#         """Test enable method on point nodes and updating the registry."""
#         node_mock = MagicMock(is_point=True, topic="node1", config={}, identifier="node1_id")
#         device_node_mock = MagicMock()
#         PDA.equipment_tree.find_points.return_value = [node_mock]
#         PDA.equipment_tree.get_device_node.return_value = device_node_mock
#
#         PDA.enable(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert node_mock.config['active'] is True
#         PDA.equipment_tree.get_device_node.assert_called_once_with(node_mock.identifier)
#         device_node_mock.update_registry_row.assert_called_once_with(node_mock.config)
#         PDA.vip.config.set.assert_not_called()


# class TestPlatformDriverAgentDisable:
#     """ Tests for disable function"""
#
#     @pytest.fixture
#     def PDA(self):
#         agent = PlatformDriverAgent()
#         agent.vip = MagicMock()
#         agent.equipment_tree = MagicMock()
#         return agent
#
#     def test_disable_no_nodes_found(self, PDA):
#         """Test disable method with no matching nodes."""
#         PDA.equipment_tree.find_points.return_value = []
#
#         PDA.disable(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         PDA.vip.config.set.assert_not_called()
#         PDA.equipment_tree.get_device_node.assert_not_called()
#
#     def test_disable_non_point_nodes(self, PDA):
#         """Test disable method on non-point nodes without triggering callback."""
#         node_mock = MagicMock(is_point=False, topic="node1", config={})
#         PDA.equipment_tree.find_points.return_value = [node_mock]
#
#         PDA.disable(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert node_mock.config['active'] is False
#         PDA.vip.config.set.assert_called_once_with(node_mock.topic,
#                                                    node_mock.config,
#                                                    trigger_callback=False)
#         PDA.equipment_tree.get_device_node.assert_not_called()
#
#     def test_disable_point_nodes(self, PDA):
#         """Test disable method on point nodes and updating the registry."""
#         node_mock = MagicMock(is_point=True, topic="node1", config={}, identifier="node1_id")
#         device_node_mock = MagicMock()
#         PDA.equipment_tree.find_points.return_value = [node_mock]
#         PDA.equipment_tree.get_device_node.return_value = device_node_mock
#
#         PDA.disable(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert node_mock.config['active'] is False
#         PDA.equipment_tree.get_device_node.assert_called_once_with(node_mock.identifier)
#         device_node_mock.update_registry_row.assert_called_once_with(node_mock.config)
#         PDA.vip.config.set.assert_not_called()


# class TestPlatformDriverAgentNewReservation:
#     """ Tests for new reservation """
#
#     @pytest.fixture
#     def PDA(self):
#         agent = PlatformDriverAgent()
#         agent.vip = MagicMock()
#         agent.reservation_manager = MagicMock()
#         agent.vip.rpc.context.vip_message.peer = "test.agent"
#
#         return agent
#
#     def test_new_reservation(self, PDA):
#         PDA.new_reservation(task_id="task1", priority="LOW", requests=[])
#
#         PDA.reservation_manager.new_reservation.assert_called_once_with("test.agent",
#                                                                         "task1",
#                                                                         "LOW", [],
#                                                                         publish_result=False)


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
        kwargs = {}
        PDA.get_point(path='device/topic/SampleWritableFloat', point_name=None, **kwargs)
        PDA._equipment_id.assert_called_with("device/topic/SampleWritableFloat", None)

    def test_get_point_raises_error_for_invalid_node(self, PDA):
        """Test get_point raises error when node is invalid"""
        PDA.equipment_tree.get_node.return_value = None
        kwargs = {}
        with pytest.raises(ValueError, match="No equipment found for topic: processed_point_name"):
            PDA.get_point(path='device/topic', point_name='SampleWritableFloat', **kwargs)

    # def test_get_point_raises_error_for_invalid_remote(self, PDA):
    #     """Test get_point raises error when remote is invalid"""
    #     # Ensure get_node returns a valid node mock
    #     node_mock = Mock()
    #     node_mock.get_remote = Mock(return_value=None)
    #     PDA.equipment_tree.get_node = Mock(return_value=node_mock)
    #
    #     kwargs = {}
    #
    #     with pytest.raises(ValueError, match="No remote found for topic: processed_point_name"):
    #         PDA.get_point(path='device/topic', point_name='SampleWritableFloat', **kwargs)

    def test_get_point_with_kwargs_as_topic_point(self, PDA):
        """Test handling of old actuator-style arguments"""

        kwargs = {'topic': 'device/topic', 'point': 'SampleWritableFloat'}

        PDA.get_point(path=None, point_name=None, **kwargs)

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
        PDA.set_point(path='device/topic', point_name='SampleWritableFloat', value=42, kwargs={})
        # Assert that self._equipment_id was called with the correct arguments
        PDA._equipment_id.assert_called_with("device/topic", "SampleWritableFloat")

    # def test_set_point_with_topic_kwarg(self, PDA):
    #     """Test handling of 'topic' as keyword arg"""
    #     kwargs = {'device/topic'}
    #     PDA.set_point(path='ignored_path', point_name=None, value=42, **kwargs)
    #     PDA._equipment_id.assert_called_with('device/topic', None)

    def test_set_point_with_point_kwarg(self, PDA):
        """ Test handling of 'point' keyword arg """
        kwargs = {'point': 'SampleWritableFloat'}
        PDA.set_point(path='device/topic', point_name=None, value=42, **kwargs)
        PDA._equipment_id.assert_called_with('device/topic', 'SampleWritableFloat')

    def test_set_point_with_combined_path_and_empty_point(self, PDA):
        """Test handling of path containing the point name and point_name is empty"""
        kwargs = {}
        PDA.set_point(path='device/topic/SampleWritableFloat', point_name=None, value=42, **kwargs)
        PDA._equipment_id.assert_called_with("device/topic/SampleWritableFloat", None)

    def test_set_point_raises_error_for_invalid_node(self, PDA):
        """Tests that setpoint raises a ValueError exception"""
        # Mock get_node to return None
        PDA.equipment_tree.get_node.return_value = None
        kwargs = {}

        # Call the set_point function and check for ValueError
        with pytest.raises(ValueError, match="No equipment found for topic: processed_point_name"):
            PDA.set_point(path='device/topic',
                          point_name='SampleWritableFloat',
                          value=42,
                          **kwargs)

    def test_set_point_deprecated(self, PDA):
        """Test old style actuator call"""
        PDA.set_point("device/topic", 'SampleWritableFloat', 42)
        PDA._equipment_id.assert_called_with("device/topic", "SampleWritableFloat")


class TestGetMultiplePoints:
    sender = "test.agent"

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        PDA._equipment_id = Mock(side_effect={'device1/point2', 'device1/point1'}, )

        PDA.get = Mock(return_value=({}, {}))

        return PDA

    def test_get_multiple_points_with_single_path(self, PDA):
        """Test get_multiple_points with a single path"""
        PDA.get_multiple_points(path='device1')
        PDA.get.assert_called_once_with({'device1'})
        PDA._equipment_id.assert_not_called()

    def test_get_multiple_points_with_single_path_and_point_names(self, PDA):
        """Test get_multiple_points with a single path and point names."""
        PDA.get_multiple_points(path='device1', point_names=['point1', 'point2'])
        PDA._equipment_id.assert_any_call('device1', 'point1')
        PDA._equipment_id.assert_any_call('device1', 'point2')
        PDA.get.assert_called_once_with({'device1/point1', 'device1/point2'})

    def test_get_multiple_points_with_none_path(self, PDA):
        """Test get_multiple_points with None path."""
        with pytest.raises(TypeError, match='Argument "path" is required.'):
            PDA.get_multiple_points(path=None)

        PDA.get.assert_not_called()


class TestSetMultiplePoints:
    sender = "test.agent"

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        PDA._equipment_id = Mock(
            side_effect=['device1/point1', 'device1/point2', 'device2/point1'])

        PDA.set = Mock(return_value=(None, {}))

        return PDA

    def test_set_multiple_points_with_single_path(self, PDA):
        """Test set_multiple_points with a single path and point names/values"""
        point_names_values = [('point1', 100), ('point2', 200)]
        PDA.set_multiple_points(path='device1', point_names_values=point_names_values)
        PDA.set.assert_called_once_with({
            'device1/point1': 100,
            'device1/point2': 200
        },
                                        map_points=True)
        PDA._equipment_id.assert_any_call('device1', 'point1')
        PDA._equipment_id.assert_any_call('device1', 'point2')

    def test_set_multiple_points_with_missing_path(self, PDA):
        """Test set_multiple_points without providing the path"""
        point_names_values = [('point1', 100), ('point2', 200)]
        with pytest.raises(TypeError, match='missing 1 required positional argument'):
            PDA.set_multiple_points(point_names_values=point_names_values)
        PDA.set.assert_not_called()

    def test_set_multiple_points_with_additional_kwargs(self, PDA):
        """Test set_multiple_points with additional kwargs"""
        point_names_values = [('point1', 100), ('point2', 200)]
        additional_kwargs = {'some_key': 'some_value'}
        PDA.set_multiple_points(path='device1',
                                point_names_values=point_names_values,
                                **additional_kwargs)
        PDA.set.assert_called_once_with({
            'device1/point1': 100,
            'device1/point2': 200
        },
                                        map_points=True,
                                        some_key='some_value')
        PDA._equipment_id.assert_any_call('device1', 'point1')
        PDA._equipment_id.assert_any_call('device1', 'point2')

    def test_set_multiple_with_old_style_args(self, PDA):
        result = PDA.set_multiple_points(path="some/path",
                                         point_names_values=[('point1', 100), ('point2', 200)])
        assert result == {}    # returns no errors with old style args


class TestRevertPoint:
    sender = "test.agent"
    path = "devices/device1"
    point_name = "SampleWritableFloat1"

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        # Mock 'vip' components
        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        # Mock _equipment_id
        PDA._equipment_id = Mock(return_value="devices/device1/SampleWritableFloat1")

        # Mock 'equipment_tree.get_node'
        node_mock = MagicMock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.get_node = Mock(return_value=node_mock)

        # Mock other methods called in revert_point
        node_mock.get_remote = Mock(return_value=Mock())
        PDA.equipment_tree.raise_on_locks = Mock()
        PDA._get_headers = Mock(return_value={})
        PDA._push_result_topic_pair = Mock()

        return PDA

    def test_revert_point_normal_case(self, PDA):
        """Test normal case for reverting a point."""
        PDA.revert_point(self.path, self.point_name)

        PDA._equipment_id.assert_called_with(self.path, 'SampleWritableFloat1')
        PDA.equipment_tree.get_node.assert_called_once()


class TestRevertDevice:
    sender = "test.agent"
    path = "devices/device1"

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        PDA._equipment_id = Mock(return_value="devices/device1")

        node_mock = MagicMock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.get_node = Mock(return_value=node_mock)

        remote_mock = Mock()
        node_mock.get_remote = Mock(return_value=remote_mock)

        PDA.equipment_tree.raise_on_locks = Mock()
        PDA._get_headers = Mock(return_value={})
        PDA._push_result_topic_pair = Mock()

        return PDA

    def test_revert_device_normal_case(self, PDA):
        """Test normal case for reverting a device"""
        PDA.revert_device(self.path)

        PDA._equipment_id.assert_called_with(self.path, None)
        PDA._push_result_topic_pair.assert_called()

    def test_revert_device_actuator_style(self, PDA):
        """Test old actuator-style arguments """
        PDA.revert_device(self.sender, self.path)

        PDA._equipment_id.assert_called_with(self.path, None)
        PDA._push_result_topic_pair.assert_called()

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
        PDA.get_point.return_value = 42.0
        PDA._push_result_topic_pair = Mock()

        return PDA

    def test_handle_get_calls_get_point_with_correct_parameters(self, PDA):
        """Test handle_get calls get_point with correct parameters."""
        PDA.handle_get(None, self.sender, None, self.topic, None, None)
        PDA.get_point.assert_called_with("device1/SampleWritableFloat1")

    def test_handle_get_calls__push_result_topic_pair_with_correct_parameters(self, PDA):
        """Test handle_get calls push_result_topic_pair with correct values """
        PDA.handle_get(None, self.sender, None, self.topic, None, None)
        PDA._push_result_topic_pair.assert_called_with(VALUE_RESPONSE_PREFIX,
                                                       "device1/SampleWritableFloat1", {}, 42.0)


class TestHandleSet:
    sender = "test.sender"
    topic = "devices/actuators/set/device1/point1"
    message = 10

    @pytest.fixture
    def PDA(self):
        agent = PlatformDriverAgent()

        agent._get_headers = Mock(return_value={})
        agent._push_result_topic_pair = Mock()
        agent.set_point = Mock()
        agent._handle_error = Mock()

        return agent

    def test_handle_set_valid_message(self, PDA):
        """Test setting a point with a valid message"""
        pass
        # rewrite
        # PDA.handle_set(None, self.sender, None, self.topic, None, self.message)
        #
        # point = self.topic.replace("devices/actuators/set/", "", 1)
        #
        # # PDA.set_point.assert_called_once()
        # # PDA._push_result_topic_pair.assert_not_called()
        # # PDA._handle_error.assert_not_called()

    def test_handle_set_empty_message(self, PDA):
        """Test handling of an empty message """
        PDA.handle_set(None, self.sender, None, self.topic, None, None)

        point = self.topic.replace("devices/actuators/set/", "", 1)
        headers = PDA._get_headers(self.sender)
        error = {'type': 'ValueError', 'value': 'missing argument'}

        PDA._push_result_topic_pair.assert_called_with("devices/actuators/error", point, headers,
                                                       error)
        PDA.set_point.assert_not_called()
        PDA._handle_error.assert_not_called()


class TestHandleRevertPoint:
    sender = "test.sender"
    topic = "actuators/revert/point/device1/point1"

    @pytest.fixture
    def PDA(self):
        agent = PlatformDriverAgent()

        agent._get_headers = Mock(return_value={})
        agent._push_result_topic_pair = Mock()
        agent._handle_error = Mock()

        # Mock equipment tree
        mock_node = Mock()
        mock_remote = Mock()
        mock_node.get_remote.return_value = mock_remote
        equipment_tree_mock = Mock()
        equipment_tree_mock.get_node.return_value = mock_node
        equipment_tree_mock.root = 'devices'

        agent.equipment_tree = equipment_tree_mock

        return agent, mock_node, mock_remote

    def test_handle_revert_point_success(self, PDA):
        """Test reverting a point successfully."""
        agent_instance, mock_node, mock_remote = PDA
        agent_instance.handle_revert_point(None, self.sender, None, self.topic, None, None)

        expected_topic = "devices/actuators/revert/point/device1/point1"
        headers = agent_instance._get_headers(self.sender)

        agent_instance.equipment_tree.get_node.assert_called_with(expected_topic)
        agent_instance.equipment_tree.raise_on_locks.assert_called_with(mock_node, self.sender)
        agent_instance._push_result_topic_pair.assert_called_with(
            "devices/actuators/reverted/point", expected_topic, headers, None)

    def test_handle_revert_point_exception(self, PDA):
        """Test handling exception during revert process."""
        agent_instance, mock_node, mock_remote = PDA
        exception = Exception("test exception")
        agent_instance.equipment_tree.get_node.side_effect = exception
        agent_instance.handle_revert_point(None, self.sender, None, self.topic, None, None)

        expected_topic = "devices/actuators/revert/point/device1/point1"
        headers = agent_instance._get_headers(self.sender)

        agent_instance.equipment_tree.get_node.assert_called_with(expected_topic)
        agent_instance._handle_error.assert_called_with(exception, expected_topic, headers)


# class TestHandleRevertDevice:
#     sender = "test.sender"
#     topic = "devices/actuators/revert/device/device1"
#
#     @pytest.fixture
#     def PDA(self):
#         agent = PlatformDriverAgent()
#
#         agent._get_headers = Mock(return_value={})
#         agent._push_result_topic_pair = Mock()
#         agent._handle_error = Mock()
#
#         mock_node = Mock()
#         mock_remote = Mock()
#         mock_node.get_remote.return_value = mock_remote
#         equipment_tree_mock = Mock()
#         equipment_tree_mock.get_node.return_value = mock_node
#         equipment_tree_mock.root = 'devices'
#
#         agent.equipment_tree = equipment_tree_mock
#
#         return agent, mock_node, mock_remote
#
#     def test_handle_revert_device_success(self, PDA):
#         """Test reverting a device successfully."""
#         agent, mock_node, mock_remote = PDA
#         agent.handle_revert_device(None, self.sender, None, self.topic, None, None)
#
#         expected_topic = "devices/device1"
#         headers = agent._get_headers(self.sender)
#
#         agent.equipment_tree.get_node.assert_called_with(expected_topic)
#         agent.equipment_tree.raise_on_locks.assert_called_with(mock_node, self.sender)
#         mock_remote.revert_all.assert_called_once()
#         agent._push_result_topic_pair.assert_called_with("devices/actuators/reverted/device",
#                                                          expected_topic, headers, None)
#         agent._handle_error.assert_not_called()
#
#     def test_handle_revert_device_exception(self, PDA):
#         """Test handling exception during revert process """
#         agent_instance, mock_node, mock_remote = PDA
#         exception = Exception("test exception")
#         agent_instance.equipment_tree.get_node.side_effect = exception
#         agent_instance.handle_revert_device(None, self.sender, None, self.topic, None, None)
#
#         expected_topic = "devices/device1"
#         headers = agent_instance._get_headers(self.sender)
#
#         agent_instance.equipment_tree.get_node.assert_called_with(expected_topic)
#         agent_instance._handle_error.assert_called_with(exception, expected_topic, headers)


# class TestHandleReservationRequest:
#
#     @pytest.fixture
#     def PDA(self):
#         PDA = PlatformDriverAgent()
#
#         # Mock dependencies
#         PDA.vip = MagicMock()
#         PDA.vip.pubsub.publish = MagicMock()
#         PDA._get_headers = Mock()
#         PDA.reservation_manager = Mock()
#         PDA._handle_unknown_reservation_error = Mock()
#         PDA.reservation_manager.cancel_reservation = Mock()
#
#         return PDA
#
#     def test_handle_reservation_request_calls_publish_pubsub(self, PDA):
#         """Tests that it calls pubsub.publish when result type is new reservation"""
#         headers = {'type': 'NEW_RESERVATION', 'taskID': 'task1', 'priority': 1}
#         message = ['request1']
#
#         result = Mock()
#         result.success = True
#         result.data = {}
#         result.info_string = ''
#
#         PDA._get_headers.return_value = {}
#         PDA.reservation_manager.new_task.return_value = result
#
#         PDA.handle_reservation_request(None, 'sender', None, 'topic', headers, message)
#
#         PDA.vip.pubsub.publish.assert_called_with('pubsub',
#                                                   topic=RESERVATION_RESULT_TOPIC,
#                                                   headers={},
#                                                   message={
#                                                       'result': 'SUCCESS',
#                                                       'data': {},
#                                                       'info': ''
#                                                   })
#
#     def test_handle_reservation_reservation_action_cancel(self, PDA):
#         """Tests that it calls pubsub.publish when result type is cancel reservation"""
#         headers = {'type': 'CANCEL_RESERVATION', 'taskID': 'task1', 'priority': 1}
#         message = ['request1']
#
#         result = Mock()
#         result.success = True
#         result.data = {}
#         result.info_string = ''
#
#         PDA._get_headers.return_value = {}
#         PDA.reservation_manager.cancel_reservation.return_value = result
#
#         PDA.handle_reservation_request(None, 'sender', None, 'topic', headers, message)
#
#         PDA.vip.pubsub.publish.assert_called_with('pubsub',
#                                                   topic=RESERVATION_RESULT_TOPIC,
#                                                   headers={},
#                                                   message={
#                                                       'result': 'SUCCESS',
#                                                       'data': {},
#                                                       'info': ''
#                                                   })
#
#     def test_handle_reservation_request_calls_publish_pubsub(self, PDA):
#         """Tests that it calls pubsub.publish when new_task result responds with failed"""
#         headers = {'type': 'NEW_RESERVATION', 'taskID': 'task1', 'priority': 1}
#         message = ['request1']
#
#         result = Mock()
#         result.success = False
#         result.data = {}
#         result.info_string = ''
#
#         PDA._get_headers.return_value = {}
#         PDA.reservation_manager.new_task.return_value = result
#
#         PDA.handle_reservation_request(None, 'sender', None, 'topic', headers, message)
#
#         PDA.vip.pubsub.publish.assert_called_with('pubsub',
#                                                   topic=RESERVATION_RESULT_TOPIC,
#                                                   headers={},
#                                                   message={
#                                                       'result': 'FAILURE',
#                                                       'data': {},
#                                                       'info': ''
#                                                   })


class TestEquipmentId:
    """ Tests for _equipment_id in the PlatFromDriveragent class"""

    @pytest.fixture
    def PDA(self):
        """Fixture to set up a PlatformDriverAgent with a mocked equipment_tree."""
        agent = PlatformDriverAgent()
        agent.equipment_tree = Mock()
        agent.equipment_tree.root = "devices"
        return agent

    def test_equipment_id_basic(self, PDA):
        """Normal call"""
        result = PDA._equipment_id("some/path", "point")
        assert result == "devices/some/path/point"

    def test_equipment_id_no_point(self, PDA):
        """Tests calling equipment_id with no point."""
        result = PDA._equipment_id("some/path")
        assert result == "devices/some/path"

    def test_equipment_id_leading_trailing_slashes(self, PDA):
        """Tests calling equipment_id with leading and trailing slashes."""
        result = PDA._equipment_id("/some/path/", "point")
        assert result == "devices/some/path/point"

    def test_equipment_id_no_point_leading_trailing_slashes(self, PDA):
        """Tests calling equipment_id with leading and trailing slashes and no point"""
        result = PDA._equipment_id("/some/path/")
        assert result == "devices/some/path"

    def test_equipment_id_path_with_root(self, PDA):
        """Tests calling equipment_id with root in a path."""
        result = PDA._equipment_id("devices/some/path", "point")
        assert result == "devices/some/path/point"

    def test_equipment_id_path_with_root_no_point(self, PDA):
        """Tests calling equipment_id with root and no point"""
        result = PDA._equipment_id("devices/some/path")
        assert result == "devices/some/path"

    def test_equipment_id_only_path(self, PDA):
        """Tests calling equipment_id with only path, no point or root"""
        result = PDA._equipment_id("some/path")
        assert result == "devices/some/path"


class TestGetHeaders:
    """Tests for _get_headers in the PlatformDriverAgent class."""
    now = get_aware_utc_now()

    def test_get_headers_no_optional(self):
        """Tests _get_headers with time as now"""
        formatted_now = format_timestamp(self.now)
        result = PlatformDriverAgent()._get_headers(requester="test_requester", time=self.now)
        assert result == {'time': formatted_now, 'requesterID': "test_requester", 'type': None}

    def test_get_headers_with_time(self):
        custom_time = datetime(2024, 7, 25, 18, 52, 29, 37938)
        formatted_custom_time = format_timestamp(custom_time)
        result = PlatformDriverAgent()._get_headers("test_requester", time=custom_time)
        assert result == {
            'time': formatted_custom_time,
            'requesterID': "test_requester",
            'type': None
        }

    def test_get_headers_with_task_id(self):
        task_id = "task123"
        formatted_now = format_timestamp(self.now)
        result = PlatformDriverAgent()._get_headers(requester="test_requester",
                                                    time=self.now,
                                                    task_id=task_id)
        assert result == {
            'time': formatted_now,
            'requesterID': "test_requester",
            'taskID': task_id,
            'type': None
        }

    def test_get_headers_with_action_type(self):
        action_type = "NEW_SCHEDULE"
        formatted_now = format_timestamp(self.now)
        result = PlatformDriverAgent()._get_headers(requester="test_requester",
                                                    time=self.now,
                                                    action_type=action_type)
        assert result == {
            'time': formatted_now,
            'requesterID': "test_requester",
            'type': action_type
        }

    def test_get_headers_all_optional(self):
        custom_time = datetime(2024, 7, 25, 18, 52, 29, 37938)
        formatted_custom_time = format_timestamp(custom_time)
        task_id = "task123"
        action_type = "NEW_SCHEDULE"
        result = PlatformDriverAgent()._get_headers(requester="test_requester",
                                                    time=custom_time,
                                                    task_id=task_id,
                                                    action_type=action_type)
        assert result == {
            'time': formatted_custom_time,
            'requesterID': "test_requester",
            'taskID': task_id,
            'type': action_type
        }


if __name__ == '__main__':
    pytest.main()
