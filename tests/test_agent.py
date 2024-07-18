import pytest
from unittest.mock import MagicMock, Mock
from platform_driver.agent import PlatformDriverAgent


class TestPlatformDriverAgentLoadVersionedConfig:
    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.core = Mock()
        PDA.vip = Mock()
        return PDA
    def test_load_empty_config(self, PDA):
        """Test loading an empty config."""
        config = {}
        result = PDA._load_versioned_config(config)
        assert result.config_version == 1
    def test_loading_based_on_version(self, PDA):
        """Tests loading a config based on version."""
        # Load v1 this also tests using a version less than the current version
        config_v1 = {'config_version': 1, 'publish_depth_first_all': True}
        result = PDA._load_versioned_config(config_v1)
        assert result.publish_depth_first_all == True
        assert result.config_version == 1

        config_v2 = {'config_version': 2, 'publish_depth_first_any': True}
        result_v2 = PDA._load_versioned_config(config_v2)
        assert result_v2.config_version == 2
        assert result_v2.publish_depth_first_any == True
    def test_deprecation_warning_for_old_config_versions(self, PDA, caplog):
        config_old_version = {'config_version': 1}
        result = PDA._load_versioned_config(config_old_version)
        assert "Deprecation Warning" in caplog.text

class TestPlatformDriverAgentConfigureMain:
    @pytest.fixture
    def PDA(self):
        from platform_driver.agent import PlatformDriverAgent
        agent = PlatformDriverAgent()
        agent.vip = Mock()
        agent.config = Mock()
        agent.remote_heartbeat_interval = Mock()
        agent.heartbeat_greenlet = Mock()
        agent.poll_scheduler = Mock()
        agent.reservation_manager = Mock()
        agent.override_manager = Mock()
        agent.scalability_test = Mock()
        return agent

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
    """Tests for remove_equipment."""
    # TODO wait for function to be fully finished
    pass

class TestPlatformDriverAgentResolveTags:
    """Tests for resolve_tags"""
    @pytest.fixture
    def PDA(self):
        agent = PlatformDriverAgent()
        agent.vip = MagicMock()
        return agent

    def test_resolve_tags_success(self, PDA):
        """Test resolve_tags method with successful return from tagging service."""
        tags = ["tag1", "tag2"]
        expected_tag_list = ["topic1", "topic2"]

        # Mock the RPC call to return a successful result
        PDA.vip.rpc.call.return_value.get.return_value = expected_tag_list

        result = PDA.resolve_tags(tags)

        # Validate the result and that the RPC call was made with correct parameters
        assert result == expected_tag_list
        PDA.vip.rpc.call.assert_called_once_with('platform.tagging', 'get_topics_by_tags', tags)

    def test_resolve_tags_no_tags(self, PDA):
        """Test resolve_tags method returning no tags."""
        tags = ["tag1", "tag2"]
        PDA.vip.rpc.call.return_value.get.return_value = []

        result = PDA.resolve_tags(tags)

        assert result == []
        PDA.vip.rpc.call.assert_called_once_with('platform.tagging', 'get_topics_by_tags', tags)

class TestPlatformDriverAgentBuildQueryPlan:
    """Tests for build_query_plan."""
    # TODO come back to this, not really understanding it
    @pytest.fixture
    def PDA(self):
        agent = PlatformDriverAgent()
        agent.vip = MagicMock()
        return agent

class TestPlatformDriverAgentGet:
    """Tests for get."""
    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.vip = MagicMock()  # Mock vip if needed
        PDA.equipment_tree = MagicMock()
        return PDA

    def test_get_no_points(self, PDA):
        """Test get method with no points in the query plan."""
        PDA.build_query_plan = MagicMock(return_value={})

        results, errors = PDA.get(topic=None, tag=None, regex=None)

        assert results == {}
        assert errors == {}
        PDA.build_query_plan.assert_called_once_with(None, None, None)

    def test_get_with_node_not_found(self, PDA):
        """Test get method where a node is not found in the equipment tree"""
        remote_mock = MagicMock()
        point_mock = MagicMock(identifier="point")

        # Mock the build_query_plan to return a predefined query plan
        PDA.build_query_plan = MagicMock(return_value={
            remote_mock: {point_mock}
        })

        # mock
        remote_mock.get_multiple_points.return_value = (
            {"point": "value"},
            {"point_err": "error"}
        )

        PDA.equipment_tree.get_node.return_value = None

        results, errors = PDA.get(topic="topic", tag="tag", regex="regex")

        assert results == {"point": "value"}
        assert errors == {"point_err": "error"}

        # Validate if methods were called with correct parameters
        PDA.build_query_plan.assert_called_once_with("topic", "regex", "tag")
        remote_mock.get_multiple_points.assert_called_once_with(["point"])

class TestPlatformDriverAgentSet:
    """Tests for set"""
    pass # TODO wait for final additions

class TestPlatformDriverAgentRevert:
    """Tests for revert"""
    pass # TODO wait for final additions

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
        point_mock = MagicMock(topic="point1", last_value="value1", last_updated="2023-01-01T00:00:00Z")
        PDA.equipment_tree.find_points.return_value = [point_mock]

        result = PDA.last(topic="topic")
        expected = {
            "point1": {"value": "value1", "updated": "2023-01-01T00:00:00Z"}
        }
        assert result == expected
        PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)

    def test_start_points_not_active_reschedule_allowed(self, PDA):
        """Test start method where points are not active and rescheduling is allowed."""
        PDA.config.allow_reschedule = True
        point_mock = MagicMock(topic="point1", active=False)
        PDA.equipment_tree.find_points.return_value = [point_mock]

        PDA.start(topic="topic")

        PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
        assert point_mock.active is True
        PDA.poll_scheduler.schedule.assert_called_once()
        PDA.poll_scheduler.add_to_schedule.assert_not_called()

class TestPlatformDriverAgentStart:
    """Tests for Start"""

    @pytest.fixture
    def PDA(self):
        agent = PlatformDriverAgent()
        agent.vip = MagicMock()
        agent.equipment_tree = MagicMock()
        agent.poll_scheduler = MagicMock()
        agent.config = MagicMock()
        return agent

    def test_start_no_points_found(self, PDA):
        """Test start method with no matching points."""
        PDA.equipment_tree.find_points.return_value = []

        PDA.start(topic="topic")

        PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
        PDA.poll_scheduler.schedule.assert_not_called()
        PDA.poll_scheduler.add_to_schedule.assert_not_called()

    def test_start_points_already_active(self, PDA):
        """Test start method where the points are already active."""
        point_mock = MagicMock(topic="point1", active=True)
        PDA.equipment_tree.find_points.return_value = [point_mock]

        PDA.start(topic="topic")

        PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
        assert point_mock.active is True
        PDA.poll_scheduler.schedule.assert_not_called()
        PDA.poll_scheduler.add_to_schedule.assert_not_called()

    def test_start_points_not_active_reschedule_allowed(self, PDA):
        """Test start method where points are not active and rescheduling is allowed."""
        PDA.config.allow_reschedule = True
        point_mock = MagicMock(topic="point1", active=False)
        PDA.equipment_tree.find_points.return_value = [point_mock]

        PDA.start(topic="topic")

        PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
        assert point_mock.active is True
        PDA.poll_scheduler.schedule.assert_called_once()
        PDA.poll_scheduler.add_to_schedule.assert_not_called()

    def test_start_points_not_active_reschedule_not_allowed(self, PDA):
        """Test start method where points are not active and rescheduling is not allowed."""
        PDA.config.allow_reschedule = False
        point_mock = MagicMock(topic="point1", active=False)
        PDA.equipment_tree.find_points.return_value = [point_mock]

        PDA.start(topic="topic")

        PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
        assert point_mock.active is True
        PDA.poll_scheduler.schedule.assert_not_called()
        PDA.poll_scheduler.add_to_schedule.assert_called_once_with(point_mock)
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


if __name__ == '__main__':
    pytest.main()