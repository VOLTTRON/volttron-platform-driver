import pickle
import pytest
from unittest.mock import MagicMock, Mock
# from mock import Mock, MagicMock
from platform_driver.agent import PlatformDriverAgent
from platform_driver.reservations import ReservationManager
from pickle import dumps
from base64 import b64encode
from datetime import datetime, timedelta
from volttrontesting import TestServer
from volttron.client import Agent
from pydantic import ValidationError
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
class TestLoadVersionedConfig:
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

        # Load v2
        config_v2 = {'config_version': 2, 'publish_depth_first_any': True}
        result_v2 = PDA._load_versioned_config(config_v2)
        assert result_v2.config_version == 2
        assert result_v2.publish_depth_first_any == True
    def test_deprecation_warning_for_old_config_versions(self, PDA, caplog):
        config_old_version = {'config_version': 1}
        result = PDA._load_versioned_config(config_old_version)
        assert "Deprecation Warning" in caplog.text

    # def test_load_invalid_config(self, PDA, caplog):
        # """Test that an invalid config logs a warning and raises a ValidationError."""
        # # TODO catch type error and validation error
        # config = {'config_version': "two", 'invalid_field': "someting"}
        #
        # # Expecting a ValidationError to be raised
        # with pytest.raises(ValueError):
        #     PDA._load_versioned_config(config)
        #
        # # Expecting a specific warning message in the logs
        # expected_warning = "Validation of platform driver configuration file failed."
        # assert expected_warning in caplog.text, "Expected warning message not found in log"

# class TestConfigureMain:
#     @pytest.fixture()
#     def PDA(self):
#     # TODO come back to

if __name__ == '__main__':
    pytest.main()