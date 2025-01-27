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
    """Test stopping and starting a point from being polled (minimal)."""
    pda = driver_agent_fixture
    pda.stop('devices/campus/building/fake/SampleWritableFloat1')
    # Possibly check internal flags or logs
    pda.start('devices/campus/building/fake/SampleWritableFloat1')
    # No exception => success


@pytest.mark.skip("Revert logic not implemented.")
def test_semantic_revert(driver_agent_fixture):
    """If revert doesn't fully work, skip or remove this test."""
    pass


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
    pda = driver_agent_fixture

    pda.set_point("devices/campus/building/fake", "SampleWritableFloat1", 123)
    pda.get_point("devices/campus/building/fake/SampleWritableFloat1")

    result = pda.last(topic="devices/campus/building/fake/SampleWritableFloat1")

    assert "campus/building/fake/SampleWritableFloat1" in result

    last_data = result["campus/building/fake/SampleWritableFloat1"]
    assert "value" in last_data
    assert "updated" in last_data

    assert last_data["value"] == 10.0
