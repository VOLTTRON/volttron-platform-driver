import pytest

from platform_driver.equipment import EquipmentTree, EquipmentNode
from mock import MagicMock, Mock


class TestEquipmentNodeGetRemote():

    def test_get_remote_is_device_true(self):
        """Tests that get_remote returns interface when is_device is true"""
        EN = EquipmentNode()

        # Set the segment_type to 'DEVICE' to make is_device return True
        EN.data['segment_type'] = 'DEVICE'
        EN.data['interface'] = 'my_custom_interface'

        result = EN.get_remote(tree=None)
        assert result == 'my_custom_interface'

    def test_get_remote_is_device_false(self):
        """Tests that get_remote returns None when ins_device is none"""
        EN = EquipmentNode()

        # Set the segment_type to 'NOTDEVICE' to make is_device return False
        EN.data['segment_type'] = 'NOTDEVICE'

        result = EN.get_remote(tree=None)
        assert result == None


# class TestEquipmentTreeAddDevice():
#     # TODO get back to
#
#     def test_add_device(self):
#         ET = EquipmentTree()
#
#         result = ET.add_device(device_topic="some/device/topic",
#                                config={"registry_config": ["some/device/config", "some/device/config"],
#                                        "equipment_specific_fields": "some/device/topic"},
#                                driver_agent="idk"
#                                )
#         assert result == "something"

class TestEquipmentTreeAddSegment():
    @pytest.fixture
    def equipment_tree(self):
        return EquipmentTree()

    def test_add_segment_successful(self, equipment_tree):
        """Test adding a new segment successfully."""
        topic = "devices/building/floor"

        nid = equipment_tree.add_segment(topic)

        assert nid == "devices/building/floor"
        assert "devices/building" in equipment_tree.nodes
        assert "devices/building/floor" in equipment_tree.nodes

    def test_add_segment_with_config(self, equipment_tree):
        """Test adding a new segment with configuration."""
        topic = "devices/building/floor"
        config = {"temp_setting": "22C"}

        nid = equipment_tree.add_segment(topic, config)

        assert nid == "devices/building/floor"
        assert "devices/building/floor" in equipment_tree.nodes
        assert equipment_tree.nodes[nid].config == config

    def test_add_segment_with_existing_node(self, equipment_tree):
        """Test adding a segment where a node already exists."""
        topic = "devices/building/floor"

        # Add an initial segment
        equipment_tree.add_segment(topic)

        # Attempt to add the same segment again
        nid = equipment_tree.add_segment(topic)

        assert nid == "devices/building/floor"
        assert "devices/building/floor" in equipment_tree.nodes
        # Verify that no exceptions were raised, and the segment exists

    def test_add_segment_partial_existing_nodes(self, equipment_tree):
        """Test adding a segment where some ancestor nodes already exist."""
        # Add an initial segment up to the building
        topic1 = "devices/building"
        equipment_tree.add_segment(topic1)

        # Add a new segment extending to the floor
        topic2 = "devices/building/floor"
        nid = equipment_tree.add_segment(topic2)

        assert nid == "devices/building/floor"
        assert "devices/building" in equipment_tree.nodes
        assert "devices/building/floor" in equipment_tree.nodes

if __name__ == '__main__':
    pytest.main()