import json
import pytest
from unittest.mock import MagicMock, ANY, patch
from pathlib import Path

from volttron.utils import get_aware_utc_now
from volttrontesting.server_mock import TestServer

from platform_driver.agent import PlatformDriverAgent
from platform_driver.overrides import OverrideManager
from platform_driver.reservations import ReservationManager
from platform_driver.topic_tree import DeviceTree
from platform_driver.constants import SET_TOPIC, VALUE_RESPONSE_PREFIX


@pytest.fixture(scope="module")
def driver_agent_fixture():
    """
    Create and configure a PlatformDriverAgent with a 'fake' device,
    returning the agent so multiple tests can use it.
    """
    ts = TestServer()
    pda = ts.instantiate_agent(PlatformDriverAgent)

    driver_config = {
        "driver_type":
        "fake",
        "driver_config": {},
        "active": True,
        "registry_name": "test_registry",
        "registry_config": [{
            "Point Name": "EKG",
            "Volttron Point Name": "EKG",
            "Units": "waveform",
            "Units Details": "waveform",
            "Writable": True,
            "Starting Value": "sin",
            "Type": "float",
            "Notes": "Sine wave for baseline output"
        }, {
            "Point Name": "Heartbeat",
            "Volttron Point Name": "Heartbeat",
            "Units": "On/Off",
            "Units Details": "On/Off",
            "Writable": True,
            "Starting Value": "0",
            "Type": "boolean",
            "Notes": "Heartbeat toggle"
        }, {
            "Point Name": "OutsideAirTemperature1",
            "Volttron Point Name": "OutsideAirTemperature1",
            "Units": "F",
            "Units Details": "-100 to 300",
            "Writable": False,
            "Starting Value": "50",
            "Type": "float",
            "Notes": "Example sensor"
        }, {
            "Point Name": "SampleWritableFloat1",
            "Volttron Point Name": "SampleWritableFloat1",
            "Units": "PPM",
            "Units Details": "1000.00 (default)",
            "Writable": True,
            "Starting Value": "10",
            "Type": "float",
            "Notes": "Setpoint #1"
        }, {
            "Point Name": "SampleWritableFloat2",
            "Volttron Point Name": "SampleWritableFloat2",
            "Units": "PPM",
            "Units Details": "1000.00 (default)",
            "Writable": True,
            "Starting Value": "10",
            "Type": "float",
            "Notes": "Setpoint #2"
        }],
        "interval":
        5,
        "timezone":
        "US/Pacific",
        "heart_beat_point":
        "Heartbeat",
        "publish_breadth_first_all":
        False,
        "publish_depth_first":
        False,
        "publish_breadth_first":
        False
    }

    driver_config_path = Path("/tmp/driver_config.config")
    with driver_config_path.open("w") as file:
        json.dump(driver_config, file)

    with open('/tmp/driver_config.config') as f:
        FakeConfig = json.load(f)

    def return_config(pattern):
        if pattern == '_override_patterns':
            return b''

    pda.vip.config.get = return_config
    pda.override_manager = OverrideManager(pda)
    now = get_aware_utc_now()
    pda.reservation_manager = ReservationManager(pda, pda.config.reservation_preempt_grace_time,
                                                 now)

    pda._configure_new_equipment('devices/campus/building/fake',
                                 'NEW',
                                 FakeConfig,
                                 schedule_now=True)


    pda.vip = MagicMock()
    pda.vip.rpc.context = MagicMock()
    pda.vip.rpc.context.vip_message.peer = 'some.caller'


    yield pda


def test_basic_get_set(driver_agent_fixture):
    """Basic test to verify get_point / set_point on a single point."""
    pda = driver_agent_fixture

    # get initial
    val = pda.get_point('devices/campus/building/fake/SampleWritableFloat1')
    assert val == 10

    # set
    ret = pda.set_point('devices/campus/building/fake', 'SampleWritableFloat1', 15)
    assert ret == 15

    # read again
    val2 = pda.get_point('devices/campus/building/fake/SampleWritableFloat1')
    assert val2 == 15


def test_get_multiple_points(driver_agent_fixture):
    """Test reading multiple points at once."""
    pda = driver_agent_fixture

    # set multiple
    pda.set_multiple_points(
        'devices/campus/building/fake',
        [
            ("SampleWritableFloat1", 1),
            ("SampleWritableFloat2", 1),
            ("OutsideAirTemperature1", 1)    # not writable, so shouldn't change
        ])
    pda.set_multiple_points('devices/campus/building/fake', [("SampleWritableFloat1", 15),
                                                             ("SampleWritableFloat2", 15),
                                                             ("OutsideAirTemperature1", 100)])

    points, errors = pda.get_multiple_points([
        'devices/campus/building/fake/SampleWritableFloat1',
        'devices/campus/building/fake/SampleWritableFloat2',
        'devices/campus/building/fake/OutsideAirTemperature1'
    ])
    assert errors == {}
    assert points == {
        'devices/campus/building/fake/OutsideAirTemperature1': 50.0,
        'devices/campus/building/fake/SampleWritableFloat1': 15.0,
        'devices/campus/building/fake/SampleWritableFloat2': 15
    }


def test_override_on_off(driver_agent_fixture):
    """Check set_override_on / set_override_off logic."""
    pda = driver_agent_fixture
    pda.core.spawn = MagicMock()

    pda.set_override_on("devices/campus/building/fake")
    assert "devices/campus/building/fake" in pda.get_override_devices()

    pda.set_override_off("devices/campus/building/fake")
    assert "devices/campus/building/fake" not in pda.get_override_devices()

# TODO Create test for clear override / get override patterns /


def test_poll_schedule(driver_agent_fixture):
    """Check get_poll_schedule is not empty, has 'default' group."""
    pda = driver_agent_fixture
    schedule = pda.get_poll_schedule()
    assert schedule, "Poll schedule should not be empty."
    assert "default" in schedule


def test_scrape_all(driver_agent_fixture):
    """Scrape all from the device (legacy method) and check known values."""
    pda = driver_agent_fixture
    result = pda.scrape_all('devices/campus/building/fake')
    expected_result = ({
        'devices/campus/building/fake/EKG': ANY,
        'devices/campus/building/fake/Heartbeat': True,
        'devices/campus/building/fake/OutsideAirTemperature1': 50.0,
        'devices/campus/building/fake/SampleWritableFloat1': 15.0,
        'devices/campus/building/fake/SampleWritableFloat2': 15.0
    }, {})
    assert result == expected_result


def test_semantic_get_set(driver_agent_fixture):
    """Test semantic_get and semantic_set with a mocked semantic_query."""
    pda = driver_agent_fixture
    # set some initial values
    pda.set_point('devices/campus/building/fake', 'SampleWritableFloat1', 10)
    pda.set_point('devices/campus/building/fake', 'SampleWritableFloat2', 20)

    # Now mock semantic_query
    pda.semantic_query = MagicMock(return_value=[
        'devices/campus/building/fake/SampleWritableFloat1',
        'devices/campus/building/fake/SampleWritableFloat2'
    ])

    # semantic_get
    results, errors = pda.semantic_get("SOME SEMANTIC QUERY")
    assert not errors
    assert results['devices/campus/building/fake/SampleWritableFloat1'] == 10
    assert results['devices/campus/building/fake/SampleWritableFloat2'] == 20

    # semantic_set
    _, errors2 = pda.semantic_set(33, "SOME SEMANTIC QUERY 2")
    assert not errors2

    # verify
    val1 = pda.get_point('devices/campus/building/fake/SampleWritableFloat1')
    val2 = pda.get_point('devices/campus/building/fake/SampleWritableFloat2')
    assert val1 == 33
    assert val2 == 33


def test_start_stop(driver_agent_fixture):
    # TODO actually figure out how to test if its polled
    # TODO make the semantic version
    """Test stopping and starting a point from being polled (minimal)."""
    pda = driver_agent_fixture
    pda.stop('devices/campus/building/fake/SampleWritableFloat1')
    # Possibly check internal flags or logs
    pda.start('devices/campus/building/fake/SampleWritableFloat1')
    # No exception => success


def test_list_topics(driver_agent_fixture):
    """Check that 'devices/campus/building/fake' is in listed topics."""
    pda = driver_agent_fixture
    topics_list = pda.list_topics("devices/campus/building")
    assert "devices/campus/building/fake" in topics_list


def test_device_tree_to_json_mock():
    # Create a DeviceTree instance if needed (with or without real topic data)
    device_tree = DeviceTree()

    # Patch the to_json method on the DeviceTree class:
    with patch.object(DeviceTree, "to_json", return_value={"mock": "tree"}) as mock_to_json:
        result = device_tree.to_json(with_data=True)
        assert result == {"mock": "tree"}
        mock_to_json.assert_called_once()


def test_last_and_semantic_last(driver_agent_fixture):
    """ """
    pda = driver_agent_fixture

    pda.set_point("devices/campus/building/fake", "SampleWritableFloat1", 123)
    pda.get_point("devices/campus/building/fake/SampleWritableFloat1")

    result = pda.last(topic="devices/campus/building/fake/SampleWritableFloat1")

    assert "campus/building/fake/SampleWritableFloat1" in result

    last_data = result["campus/building/fake/SampleWritableFloat1"]
    assert "value" in last_data
    assert "updated" in last_data

    assert last_data["value"] == 10.0


def test_add_node_device(driver_agent_fixture):
    """
    Test that adding a new device node via the add_node RPC adds
    a new device to the PlatformDriverAgent's equipment_tree.
    """
    pda = driver_agent_fixture

    new_device_config = {
        "driver_type": "fake",
        "driver_config": {},
        "active": True,
        "registry_name": "test_registry_for_add_node",
        "registry_config": [
            {
                "Point Name": "SomeWritablePoint",
                "Volttron Point Name": "SomeWritablePoint",
                "Units": "unitless",
                "Writable": True,
                "Starting Value": "10",
                "Type": "float",
                "Notes": "Example point"
            }
        ],
        "interval": 5,
        "timezone": "US/Pacific",
        "heart_beat_point": "Heartbeat",
        "publish_breadth_first_all": False,
        "publish_depth_first": False,
        "publish_breadth_first": False
    }

    node_topic = "devices/campus/building/new_test_device"

    # Call the RPC to add a new device node
    result = pda.add_node(node_topic=node_topic,
                          config=new_device_config,
                          update_schedule=True)

    # 1. Verify add_node returned True
    assert result is True, "add_node should return True for a successful add."

    # 2. Check that the new device node is registered in the equipment_tree
    node = pda.equipment_tree.get_node(node_topic)
    assert node is not None, "Equipment tree should now contain the newly added device."

    # 3. Confirm the node is active (if that is part of your logic):
    assert node.config.active is True, "Newly added device should be active per config."


def test_request_new_schedule(driver_agent_fixture):
    """
    Test that calling the request_new_schedule RPC method results in
    a call to reservation_manager.new_task with correctly processed parameters
    and returns the expected structure.
    """
    pda = driver_agent_fixture

    # Mock the reservation_manager's new_task method so we can verify inputs & outputs.
    mock_return_value = {
        "success": True,
        "data": {},
        "info_string": "Successfully reserved."
    }
    pda.reservation_manager.new_task = MagicMock(return_value=mock_return_value)

    # Prepare a schedule request (e.g. single block: [device_path, start_time, end_time]).
    schedule_requests = [
        [
            "devices/campus/building/fake",
            "2025-12-01T09:00:00",
            "2025-12-01T10:00:00"
        ]
    ]

    # Call request_new_schedule.
    task_id = "test_task"
    priority = "HIGH"
    result = pda.request_new_schedule(None, task_id, priority, schedule_requests)

    # Check reservation_manager.new_task was called with correct arguments.
    pda.reservation_manager.new_task.assert_called_once()
    call_args = pda.reservation_manager.new_task.call_args[0]

    # call_args = (sender, task_id, priority, requests, now=None)
    # Confirm the method received the correct parameters:
    assert call_args[1] == "test_task"
    assert call_args[2] == "HIGH"
    assert call_args[3] == schedule_requests  # might be further transformed inside new_task

    # Check final returned dictionary structure:
    assert result["success"] is True
    assert result["data"] == {}
    assert result["info_string"] == "Successfully reserved."

def test_request_cancel_schedule(driver_agent_fixture):
    """
    Test that calling the request_cancel_schedule RPC method invokes
    reservation_manager.cancel_task with the correct arguments and
    returns the expected structure.
    """
    pda = driver_agent_fixture

    # Mock the reservation_manager's cancel_task return value
    mock_return_value = {
        "success": True,
        "data": {},
        "info_string": "Task canceled."
    }
    pda.reservation_manager.cancel_task = MagicMock(return_value=mock_return_value)

    # The first argument (`_`) is ignored in the agent method signature;
    # we can just pass None.
    task_id = "test_task"
    result = pda.request_cancel_schedule(None, task_id)

    # Verify cancel_task was called exactly once, with the internal "sender" and task_id
    from unittest.mock import ANY
    pda.reservation_manager.cancel_task.assert_called_once_with(ANY, task_id)

    # Check the structure/values of the returned dictionary
    assert result["success"] is True
    assert result["data"] == {}
    assert result["info_string"] == "Task canceled."

def test_revert_device(driver_agent_fixture):
    """
    Test that the revert_device RPC method calls the agent's revert method
    and publishes the revert-device response topic as expected.
    """
    pda = driver_agent_fixture

    # We'll mock out the internal 'revert' method and
    # the '_push_result_topic_pair' method so we can verify calls.
    pda.revert = MagicMock()
    pda._push_result_topic_pair = MagicMock()

    # This is our test input path
    device_path = "devices/campus/building/fake"

    # Call the public RPC method
    pda.revert_device(device_path)

    # 1. Confirm that the agent calls `revert` with the correct "equipment ID".
    #    The revert_device method calls `self.revert(self._equipment_id(path, None))`.
    expected_equip_id = pda._equipment_id(device_path, None)
    pda.revert.assert_called_once_with(expected_equip_id)

    # 2. Confirm a "reverted device" message was published via _push_result_topic_pair.
    #    By default it publishes to e.g. 'devices/actuators/reverted/device/<path>'.
    #    The prefix for the device revert is REVERT_DEVICE_RESPONSE_PREFIX
    #    The last argument is always `None`.
    from platform_driver.constants import REVERT_DEVICE_RESPONSE_PREFIX
    pda._push_result_topic_pair.assert_called_once()
    call_args = pda._push_result_topic_pair.call_args[0]

    # call_args is a tuple: (prefix, path, headers, message)
    assert call_args[0] == REVERT_DEVICE_RESPONSE_PREFIX
    assert call_args[1] == device_path
    # call_args[2] is the headers dictionary (we won't check every detail).
    # call_args[3] should be None.
    assert call_args[3] is None


# def test_enable_disable_semantic(driver_agent_fixture):
#     """
#     Test that enable/disable RPC methods (and their semantic variants)
#     correctly change a node’s active status.
#     """
#     pda = driver_agent_fixture
#     # (Optional) Patch set_registry_name so that if it is later called, it returns a valid registry name.
#     pda.equipment_tree.set_registry_name = MagicMock(return_value="test_registry")
#
#     # Use the device node created in the fixture.
#     node_topic = "devices/campus/building/fake"
#     node = pda.equipment_tree.get_node(node_topic)
#     assert node is not None, "Device node must exist for testing."
#
#     # Set the node's registry_name to bypass the exception in _add_fields_to_device_configuration_for_save.
#     node.registry_name = "test_registry"
#
#     # Disable the node using the standard RPC.
#     pda.disable(node_topic)
#     assert node.config.active is False, "Node should be disabled."
#
#     # Enable it back.
#     pda.enable(node_topic)
#     assert node.config.active is True, "Node should be enabled."
#
#     # Now test the semantic variants.
#     # Patch semantic_query to return our node topic.
#     pda.semantic_query = MagicMock(return_value=node_topic)
#     # Disable via semantic RPC.
#     pda.semantic_disable("dummy query")
#     assert node.config.active is False, "Node should be disabled via semantic_disable."
#
#     # Enable via semantic RPC.
#     pda.semantic_enable("dummy query")
#     assert node.config.active is True, "Node should be enabled via semantic_enable."



def test_remove_node(driver_agent_fixture):
    """
    Test that remove_node RPC correctly removes a node from the equipment tree.
    """
    pda = driver_agent_fixture
    # Prepare a new device configuration for a removable node.
    new_device_config = {
        "driver_type": "fake",
        "driver_config": {},
        "active": True,
        "registry_name": "test_registry_for_remove_node",
        "registry_config": [
            {
                "Point Name": "RemovablePoint",
                "Volttron Point Name": "RemovablePoint",
                "Units": "unitless",
                "Writable": True,
                "Starting Value": "20",
                "Type": "float",
                "Notes": "A removable point"
            }
        ],
        "interval": 5,
        "timezone": "US/Pacific",
        "heart_beat_point": "Heartbeat",
        "publish_breadth_first_all": False,
        "publish_depth_first": False,
        "publish_breadth_first": False
    }
    node_topic = "devices/campus/building/new_test_device_to_remove"

    # Add the node
    added = pda.add_node(node_topic=node_topic,
                         config=new_device_config,
                         update_schedule=True)
    assert added is True, "add_node should return True."
    node = pda.equipment_tree.get_node(node_topic)
    assert node is not None, "Equipment tree should contain the new node."

    # Now remove the node
    removed = pda.remove_node(node_topic, leave_disconnected=False)
    assert removed is True, "remove_node should return True on successful removal."
    node_after = pda.equipment_tree.get_node(node_topic)
    assert node_after is None, "Node should be removed from the equipment tree."


def test_status_not_implemented(driver_agent_fixture):
    """
    Test that calling status (and semantic_status) raises NotImplementedError.
    """
    pda = driver_agent_fixture
    with pytest.raises(NotImplementedError):
        pda.status("devices/campus/building/fake")
    # Patch semantic_query to return a valid topic string
    pda.semantic_query = MagicMock(return_value="devices/campus/building/fake/SampleWritableFloat1")
    with pytest.raises(NotImplementedError):
        pda.semantic_status("dummy query")



def test_list_interfaces(driver_agent_fixture):
    """
    Test list_interfaces RPC. We patch iter_modules to simulate installed interfaces.
    """
    pda = driver_agent_fixture
    # Patch iter_modules so that it returns a fake module with an attribute 'name'
    fake_module = MagicMock()
    fake_module.name = "fake_driver"
    with patch("platform_driver.agent.iter_modules", return_value=[fake_module]) as mock_iter:
        interfaces_list = pda.list_interfaces()
        assert isinstance(interfaces_list, list)
        assert "fake_driver" in interfaces_list


def test_remove_interface(driver_agent_fixture):
    """
    Test remove_interface RPC by simulating a successful uninstall via subprocess.
    """
    pda = driver_agent_fixture
    # Patch subprocess.run to simulate a zero return code.
    with patch("subprocess.run", return_value=MagicMock(returncode=0)) as mock_run:
        result = pda.remove_interface("fake")
        assert result is True


def test_legacy_revert_point(driver_agent_fixture):
    """
    Test the legacy revert_point RPC method.
    """
    pda = driver_agent_fixture
    equip_id = pda._equipment_id("devices/campus/building/fake", "SomePoint")
    dummy_node = MagicMock()
    dummy_node.identifier = equip_id
    # Patch get_node to return our dummy node.
    pda.equipment_tree.get_node = MagicMock(return_value=dummy_node)
    # Patch get_remote to return a fake remote with a revert_point method.
    fake_remote = MagicMock()
    pda.equipment_tree.get_remote = MagicMock(return_value=fake_remote)
    # Patch raise_on_locks to do nothing.
    pda.equipment_tree.raise_on_locks = MagicMock()
    # Patch _push_result_topic_pair so we can verify it is called.
    pda._push_result_topic_pair = MagicMock()

    # Call revert_point (legacy RPC).
    pda.revert_point("devices/campus/building/fake", "SomePoint")

    # Verify that the remote’s revert_point method was called with the full topic.
    fake_remote.revert_point.assert_called_once_with(equip_id)
    # Verify that the result was published with the correct response prefix.
    from platform_driver.constants import REVERT_POINT_RESPONSE_PREFIX
    pda._push_result_topic_pair.assert_called_once()
    call_args = pda._push_result_topic_pair.call_args[0]
    assert call_args[0] == REVERT_POINT_RESPONSE_PREFIX
    assert call_args[1] == equip_id
    assert call_args[3] is None  # the message is None



def test_forward_bacnet_cov_value(driver_agent_fixture):
    """
    Test that forward_bacnet_cov_value correctly forwards BACnet COV values.
    """
    pda = driver_agent_fixture
    fake_remote = MagicMock()
    # Simulate a remote identified by "remote_test".
    pda.equipment_tree.remotes["remote_test"] = fake_remote

    topic = "SomePoint"
    point_values = {"val": 42}
    pda.forward_bacnet_cov_value("remote_test", topic, point_values)
    fake_remote.publish_cov_value.assert_called_once_with(topic, point_values)


def test_handle_get(driver_agent_fixture):
    """
    Test the pubsub handler for get requests.
    """
    pda = driver_agent_fixture
    # Simulate that get_point returns 123.
    pda.get_point = MagicMock(return_value=123)
    pda._push_result_topic_pair = MagicMock()
    # Import constants.
    from platform_driver.constants import GET_TOPIC, VALUE_RESPONSE_PREFIX
    # Construct a topic as published by a client.
    test_point = "devices/campus/building/fake/SampleWritableFloat1"
    topic = f"{GET_TOPIC}/{test_point}"
    # Call the handler.
    pda.handle_get(None, 'caller', None, topic, None, None)
    pda.get_point.assert_called_once_with(test_point)
    pda._push_result_topic_pair.assert_called_once_with(VALUE_RESPONSE_PREFIX, test_point, ANY, 123)


def test_handle_set(driver_agent_fixture):
    """
    Test the pubsub handler for set requests.
    """
    pda = driver_agent_fixture
    # Simulate _set_point to return a new value.
    pda._set_point = MagicMock(return_value=999)
    pda._push_result_topic_pair = MagicMock()
    test_point = "devices/campus/building/fake/SampleWritableFloat1"
    topic = f"{SET_TOPIC}/{test_point}"
    # Call the handler with a value of 999.
    # Pass 'some.caller' as the sender (to match what _get_headers would use in a real call).
    pda.handle_set(None, 'some.caller', None, topic, None, 999)
    # _set_point should have been called with the equipment id and value.
    expected_equip_id = pda._equipment_id("devices/campus/building/fake", "SampleWritableFloat1")
    pda._set_point.assert_called_once_with(expected_equip_id, 999, 'some.caller', **{})



def test_equipment_id_helper(driver_agent_fixture):
    """
    Test the helper method _equipment_id to ensure it produces the expected topic.
    """
    pda = driver_agent_fixture
    # Given a path and point, the equipment id should start with the equipment_tree root.
    result = pda._equipment_id("campus/building/fake", "Point")
    assert result.startswith(pda.equipment_tree.root), "Equipment id should be prefixed with the root."
    # If the path already starts with the root, it should not be prefixed twice.
    result2 = pda._equipment_id("devices/campus/building/fake", "Point")
    assert result2.startswith("devices/"), "Equipment id should still start with 'devices/'."
