# -*- coding: utf-8 -*-
# ===----------------------------------------------------------------------===
#                 Integration Tests using VOLTTRON Instance Fixture
# ===----------------------------------------------------------------------===

import csv
import json
import time
import tempfile
from pathlib import Path

import pytest
from volttrontesting.platformwrapper import InstallAgentOptions


@pytest.fixture(scope="module")
def driver_setup(volttron_instance):
    """Set up a volttron instance with a fake driver."""
    vi = volttron_instance

    # Install the fake driver library
    library_path = Path("/home/riley/DRIVERWORK/11rc1/volttron-lib-fake-driver").resolve()
    vi.install_library(library_path)

    # Create and store the main platform driver config
    main_config = {
        "allow_duplicate_remotes": False,
        "max_open_sockets": 5,
        "max_concurrent_publishes": 5,
        "scalability_test": False,
        "groups": {
            "default": {
                "frequency": 5.0,    # Polling frequency in seconds
                "points": ["*"]    # Poll all points
            }
        }
    }
    main_config_path = Path(tempfile.mktemp(suffix="_main_driver_config.json"))
    with main_config_path.open("w") as file:
        json.dump(main_config, file)

    # Store the main driver config in the config store
    vi.run_command(
        ["vctl", "config", "store", "platform.driver", "config",
         str(main_config_path), "--json"])

    # Create and store a fake driver device config
    device_config = {
        "driver_config": {},
        "registry_config": "config://singletestfake.csv",
        "interval": 5,
        "timezone": "US/Pacific",
        "heart_beat_point": "Heartbeat",
        "driver_type": "fake",
        "active": True
    }
    device_config_path = Path(tempfile.mktemp(suffix="_driver_config.json"))
    with device_config_path.open("w") as file:
        json.dump(device_config, file)

    vi.run_command([
        "vctl", "config", "store", "platform.driver", "devices/singletestfake",
        str(device_config_path), "--json"
    ])

    # Create and store a CSV registry config
    with tempfile.NamedTemporaryFile(mode='w', suffix=".csv", delete=False) as temp_csv:
        temp_csv.write(
            "Point Name,Volttron Point Name,Units,Units Details,Writable,Starting Value,Type,Notes\n"
            "TestPoint1,TestPoint1,PPM,1000.00 (default),TRUE,10,float,Test point 1\n"
            "TestPoint2,TestPoint2,PPM,1000.00 (default),TRUE,20,float,Test point 2\n"
            "Heartbeat,Heartbeat,On/Off,On/Off,TRUE,0,boolean,Heartbeat point\n")
        csv_path = temp_csv.name

    vi.run_command(
        ["vctl", "config", "store", "platform.driver", "singletestfake.csv", csv_path, "--csv"])

    # Install and start the platform driver agent
    agent_dir = str(Path(__file__).parent.parent.resolve())

    agent_uuid = vi.install_agent(agent_dir=agent_dir,
                                  install_options=InstallAgentOptions(
                                      start=True, vip_identity="platform.driver"))

    assert agent_uuid is not None

    # Wait for the agent to start and load configs
    time.sleep(5)

    # Create a test agent to interact with the driver
    ba = vi.build_agent(identity="test_agent")

    return vi, ba, "singletestfake"


def test_get_point(driver_setup):
    vi, ba, device_name = driver_setup
    value = ba.vip.rpc.call("platform.driver", "get_point", f"devices/{device_name}",
                            "TestPoint1").get(timeout=10)
    assert value == 10.0, "Initial value of TestPoint1 should be 10.0"


def test_set_point(driver_setup):
    vi, ba, device_name = driver_setup
    ba.vip.rpc.call("platform.driver", "set_point", f"devices/{device_name}", "TestPoint1",
                    33.3).get(timeout=10)
    new_val = ba.vip.rpc.call("platform.driver", "get_point", f"devices/{device_name}",
                              "TestPoint1").get(timeout=10)
    assert new_val == 33.3, "TestPoint1 should be updated to 33.3"


def test_multiple_points(driver_setup):
    vi, ba, device_name = driver_setup

    # Retrieve multiple points
    result, errors = ba.vip.rpc.call("platform.driver",
                                     "get_multiple_points",
                                     path=f"devices/{device_name}",
                                     point_names=["TestPoint1", "TestPoint2"]).get(timeout=10)

    assert not errors, f"Errors found: {errors}"
    assert result[f"devices/{device_name}/TestPoint1"] == 33.3
    assert result[f"devices/{device_name}/TestPoint2"] == 20.0


def test_revert_point(driver_setup):
    vi, ba, device_name = driver_setup

    # Set TestPoint1 to 50.0
    ba.vip.rpc.call(
        "platform.driver",
        "set_point",
        f"devices/{device_name}",    # "devices/singletestfake"
        "TestPoint1",
        50.0).get(timeout=10)

    # Revert the point using short path for the device
    ba.vip.rpc.call(
        "platform.driver",
        "revert_point",
        f"devices/{device_name}",    # "devices/singletestfake"
        "TestPoint1").get(timeout=10)

    # Now read it back
    val = ba.vip.rpc.call(
        "platform.driver",
        "get_point",
        f"devices/{device_name}",    # "devices/singletestfake"
        "TestPoint1").get(timeout=10)

    # Should be back to 10.0
    assert val == 10.0


def test_revert_device(driver_setup):
    vi, ba, device_name = driver_setup
    ba.vip.rpc.call("platform.driver", "set_point", f"devices/{device_name}", "TestPoint2",
                    999.9).get(timeout=10)
    val = ba.vip.rpc.call("platform.driver", "get_point", f"devices/{device_name}",
                          "TestPoint2").get(timeout=10)
    assert val == 999.9

    ba.vip.rpc.call("platform.driver", "revert_device", f"devices/{device_name}").get(timeout=10)
    val = ba.vip.rpc.call("platform.driver", "get_point", f"devices/{device_name}",
                          "TestPoint2").get(timeout=10)
    assert val == 20.0, "After revert_device, TestPoint2 should return to its default value."


def test_override_on_off(driver_setup):
    vi, ba, device_name = driver_setup

    # Enable override
    ba.vip.rpc.call("platform.driver", "set_override_on", f"devices/{device_name}").get(timeout=10)
    overridden_devices = ba.vip.rpc.call("platform.driver", "get_override_devices").get(timeout=10)
    assert f"devices/{device_name}" in overridden_devices, "Device should be in override mode."

    # Disable override
    ba.vip.rpc.call("platform.driver", "set_override_off",
                    f"devices/{device_name}").get(timeout=10)
    overridden_devices = ba.vip.rpc.call("platform.driver", "get_override_devices").get(timeout=10)
    assert f"devices/{device_name}" not in overridden_devices, "Device should not be in override mode."


def test_poll_schedule(driver_setup):
    vi, ba, _ = driver_setup

    schedule = ba.vip.rpc.call("platform.driver", "get_poll_schedule").get(timeout=10)
    assert schedule, "Poll schedule should not be empty."
    assert "default" in schedule, "Default polling group should exist."


def test_scrape_all(driver_setup):
    vi, ba, device_name = driver_setup
    result = ba.vip.rpc.call("platform.driver", "scrape_all",
                             f"devices/{device_name}").get(timeout=10)
    expected_result = [{
        'devices/singletestfake/Heartbeat': True,
        'devices/singletestfake/TestPoint1': 10.0,
        'devices/singletestfake/TestPoint2': 20.0
    }, {}]
    assert result == expected_result


def test_set_multiple_points(driver_setup):
    vi, ba, device_name = driver_setup

    # Prepare point-name-value tuples
    points_values = [("TestPoint1", 45.0), ("TestPoint2", 55.0)]

    errors = ba.vip.rpc.call("platform.driver", "set_multiple_points", f"devices/{device_name}",
                             points_values).get(timeout=10)

    assert not errors, f"Failed to set multiple points: {errors}"

    # Now read them back
    val1 = ba.vip.rpc.call("platform.driver", "get_point", f"devices/{device_name}",
                           "TestPoint1").get(timeout=10)
    val2 = ba.vip.rpc.call("platform.driver", "get_point", f"devices/{device_name}",
                           "TestPoint2").get(timeout=10)
    assert val1 == 45.0, "TestPoint1 should be set to 45.0"
    assert val2 == 55.0, "TestPoint2 should be set to 55.0"


import gevent


def test_pubsub_get_point(driver_setup):
    """
    Verify that publishing a 'get' message to the platform driver triggers
    a 'value' response on the Pub/Sub bus.
    """
    vi, ba, device_name = driver_setup

    # 1. Create a test agent that will SUBSCRIBE to the 'devices/actuators/value/#' topic
    subscriber = vi.build_agent(identity="subscriber_agent")
    received_events = []

    def on_value_topic(peer, sender, bus, topic, headers, message):
        # Callback captures any "devices/actuators/value/..." messages
        received_events.append((topic, headers, message))

    sub_id = subscriber.vip.pubsub.subscribe(peer='pubsub',
                                             prefix='devices/actuators/value',
                                             callback=on_value_topic)

    # 2. Publish a GET request to "devices/actuators/get/devices/<device_name>/TestPoint1"
    get_topic = f"devices/actuators/get/devices/{device_name}/TestPoint1"

    subscriber.vip.pubsub.publish(peer='pubsub',
                                  topic=get_topic,
                                  headers={"requesterID": "test_pubsub_agent"},
                                  message=None)

    # 3. Wait briefly for the agent to respond on the bus
    gevent.sleep(2)

    # 4. Check that we received the expected pubsub message
    assert received_events, "No 'devices/actuators/value' messages were received!"

    topic, headers, msg = received_events[0]

    # 5. Verify the topic is correct
    # Typically: "devices/actuators/value/devices/<device_name>/TestPoint1"
    assert topic.startswith("devices/actuators/value/devices/"), f"Unexpected topic: {topic}"
    assert topic.endswith("/TestPoint1"), f"Unexpected topic: {topic}"

    # 6. Verify the message is the point's value (float or None, depending on the driver)
    # For a fake driver with default 'TestPoint1' = 10.0, you might see 10.0
    assert isinstance(msg, (int, float)), f"Expected numeric point value, got {msg}"

    print(f"Pub/Sub GET test passed! Received topic={topic}, value={msg}")


# test_platform_driver_pubsub.py
# -*- coding: utf-8 -*-
import pytest
import gevent
from unittest.mock import MagicMock

# For demonstration, assume you have an existing fixture "driver_setup"
# that returns (volttron_instance, test_agent, device_name), with a device
# already configured: "devices/singletestfake" or similar.


def test_pubsub_get_point(driver_setup):
    """
    Publish a GET request to "devices/actuators/get/devices/<device_name>/TestPoint1"
    and verify the driver publishes a VALUE message.
    """
    vi, ba, device_name = driver_setup

    # Create a subscriber agent for pubsub
    subscriber = vi.build_agent(identity="sub_agent_get")
    received_events = []

    def on_value_topic(peer, sender, bus, topic, headers, message):
        # Callback for "devices/actuators/value/..."
        received_events.append((topic, headers, message))

    # 1. Subscribe to "devices/actuators/value/#"
    subscriber.vip.pubsub.subscribe(peer='pubsub',
                                    prefix='devices/actuators/value',
                                    callback=on_value_topic)

    # 2. Publish GET request
    get_topic = f"devices/actuators/get/devices/{device_name}/TestPoint1"
    subscriber.vip.pubsub.publish(peer='pubsub',
                                  topic=get_topic,
                                  headers={"requesterID": "test_pubsub_agent"},
                                  message=None)

    # 3. Wait for response
    gevent.sleep(2)

    # 4. Assert a "value" message was received
    assert received_events, "No 'devices/actuators/value' messages were received for GET."

    # Typically the topic is "devices/actuators/value/devices/singletestfake/TestPoint1"
    topic, headers, msg = received_events[0]
    assert topic.startswith("devices/actuators/value/devices/"), f"Unexpected topic: {topic}"
    assert topic.endswith("/TestPoint1"), f"Unexpected topic: {topic}"
    # msg is usually the float or int value of the point
    assert isinstance(msg, (int, float)), f"Expected numeric point value, got {msg}"


def test_pubsub_set_point(driver_setup):
    """
    Publish a SET request to "devices/actuators/set/devices/<device_name>/TestPoint1"
    with a numeric message, then verify a VALUE response or error is published.
    """
    vi, ba, device_name = driver_setup

    subscriber = vi.build_agent(identity="sub_agent_set")
    received_events = []

    def on_value_or_error_topic(peer, sender, bus, topic, headers, message):
        # Capture both "value" and "error" messages for the point
        if topic.startswith("devices/actuators/value/") or topic.startswith(
                "devices/actuators/error/"):
            received_events.append((topic, headers, message))

    # 1. Subscribe to both "value" and "error" prefixes
    subscriber.vip.pubsub.subscribe(peer='pubsub',
                                    prefix='devices/actuators/value',
                                    callback=on_value_or_error_topic)
    subscriber.vip.pubsub.subscribe(peer='pubsub',
                                    prefix='devices/actuators/error',
                                    callback=on_value_or_error_topic)

    # 2. Publish SET request
    set_topic = f"devices/actuators/set/devices/{device_name}/TestPoint1"
    new_value = 99.9
    subscriber.vip.pubsub.publish(peer='pubsub',
                                  topic=set_topic,
                                  headers={"requesterID": "test_pubsub_agent"},
                                  message=new_value)

    # 3. Wait for response
    gevent.sleep(2)

    # 4. Check for a value or error response
    assert received_events, "No response message on set request."

    topic, headers, msg = received_events[0]
    if topic.startswith("devices/actuators/error/"):
        pytest.fail(f"Set request resulted in an error: {msg}")
    else:
        # Should be "devices/actuators/value/devices/<device_name>/TestPoint1"
        assert topic.startswith("devices/actuators/value/devices/")
        assert topic.endswith("/TestPoint1")
        # The message is typically the point's new value
        assert msg == new_value, f"Expected {new_value}, got {msg}"


# def test_pubsub_revert_point(driver_setup):
#     """
#     Publish a revert point command to "actuators/revert/point/devices/<device_name>/TestPoint1"
#     and confirm a "reverted" message is published.
#     """
#     vi, ba, device_name = driver_setup
#
#     subscriber = vi.build_agent(identity="sub_agent_revert_pt")
#     received_events = []
#
#     def on_revert_or_error_topic(peer, sender, bus, topic, headers, message):
#         if "reverted/point" in topic or "error" in topic:
#             received_events.append((topic, headers, message))
#
#     # 1. Subscribe to "devices/actuators/reverted/point/#" and "devices/actuators/error/#"
#     subscriber.vip.pubsub.subscribe(peer='pubsub', prefix='devices/actuators/reverted/point', callback=on_revert_or_error_topic)
#     subscriber.vip.pubsub.subscribe(peer='pubsub', prefix='devices/actuators/error', callback=on_revert_or_error_topic)
#
#     # 2. Publish revert command
#     revert_topic = f"actuators/revert/point/devices/{device_name}/TestPoint1"
#     subscriber.vip.pubsub.publish(
#         peer='pubsub',
#         topic=revert_topic,
#         headers={"requesterID": "test_pubsub_agent"},
#         message=None
#     )
#
#     # 3. Wait for response
#     gevent.sleep(2)
#
#     # 4. Check for revert or error message
#     assert received_events, "No revert or error message after revert_point."
#
#     topic, headers, msg = received_events[0]
#     if "error" in topic:
#         pytest.fail(f"Revert point request caused error: {msg}")
#     else:
#         # Usually: "devices/actuators/reverted/point/devices/<device_name>/TestPoint1"
#         assert "reverted/point/devices/" in topic, f"Unexpected revert topic: {topic}"


def test_pubsub_revert_device(driver_setup):
    """
    Publish a revert device command to "devices/actuators/revert/device/devices/<device_name>"
    and confirm a "reverted" message is published.
    """
    vi, ba, device_name = driver_setup

    subscriber = vi.build_agent(identity="sub_agent_revert_dev")
    received_events = []

    def on_revert_or_error_topic(peer, sender, bus, topic, headers, message):
        if "reverted/device" in topic or "error" in topic:
            received_events.append((topic, headers, message))

    # 1. Subscribe to "devices/actuators/reverted/device/#" and "devices/actuators/error/#"
    subscriber.vip.pubsub.subscribe(peer='pubsub',
                                    prefix='devices/actuators/reverted/device',
                                    callback=on_revert_or_error_topic)
    subscriber.vip.pubsub.subscribe(peer='pubsub',
                                    prefix='devices/actuators/error',
                                    callback=on_revert_or_error_topic)

    # 2. Publish revert device command
    revert_dev_topic = f"devices/actuators/revert/device/devices/{device_name}"
    subscriber.vip.pubsub.publish(peer='pubsub',
                                  topic=revert_dev_topic,
                                  headers={"requesterID": "test_pubsub_agent"},
                                  message=None)

    gevent.sleep(2)

    # 3. Check for revert or error
    assert received_events, "No revert or error message after revert_device."

    topic, headers, msg = received_events[0]
    if "error" in topic:
        pytest.fail(f"Revert device request caused error: {msg}")
    else:
        # Typically: "devices/actuators/reverted/device/devices/<device_name>"
        assert "reverted/device/devices/" in topic, f"Unexpected revert device topic: {topic}"


def test_pubsub_schedule_request(driver_setup):
    """
    Publish a schedule request to 'devices/actuators/schedule/request'
    with headers 'type': 'NEW_SCHEDULE', 'taskID', 'priority', etc.
    Then confirm the result is published on 'devices/actuators/schedule/result'.
    """
    vi, ba, device_name = driver_setup

    subscriber = vi.build_agent(identity="sub_agent_schedule")
    received_events = []

    def on_schedule_result(peer, sender, bus, topic, headers, message):
        if topic == "devices/actuators/schedule/result":
            received_events.append((topic, headers, message))

    subscriber.vip.pubsub.subscribe(peer='pubsub',
                                    prefix='devices/actuators/schedule/result',
                                    callback=on_schedule_result)

    # 1. Publish schedule request
    # Example request: "NEW_SCHEDULE", "taskID":"testTask123", "priority":"HIGH"
    # The message is a list of requested time slots, e.g. [["devices/singletestfake","2025-01-01 00:00:00","2025-01-01 01:00:00"]]
    req_headers = {
        "type": "NEW_SCHEDULE",
        "requesterID": "test_pubsub_agent",
        "taskID": "testTask123",
        "priority": "HIGH"
    }
    schedule_topic = "devices/actuators/schedule/request"
    schedule_message = [["devices/singletestfake", "2025-01-01 00:00:00", "2025-01-01 01:00:00"]]

    subscriber.vip.pubsub.publish(peer='pubsub',
                                  topic=schedule_topic,
                                  headers=req_headers,
                                  message=schedule_message)

    # 2. Wait for response
    gevent.sleep(2)

    # 3. Check we got a "devices/actuators/schedule/result" message
    assert received_events, "No schedule result message received."

    topic, headers, msg = received_events[0]
    # 'msg' might be a dict like {'result': 'SUCCESS', 'data': {...}, 'info': '...' }
    assert topic == "devices/actuators/schedule/result"
    assert "result" in msg, f"Missing 'result' in schedule result message: {msg}"
    # Usually "SUCCESS" or "FAILURE"
    print(f"Received schedule result: {msg['result']}, info={msg.get('info')}")


def test_pubsub_get_nonexistent_point(driver_setup):
    vi, ba, device_name = driver_setup
    subscriber = vi.build_agent(identity="sub_agent_nonexistent")
    received_errors = []

    def on_error_topic(peer, sender, bus, topic, headers, message):
        if "error" in topic:
            received_errors.append((topic, headers, message))

    subscriber.vip.pubsub.subscribe(peer='pubsub',
                                    prefix='devices/actuators/error',
                                    callback=on_error_topic)

    # Publish GET request for a non-existent point
    get_topic = f"devices/actuators/get/devices/{device_name}/NonExistentPoint"
    subscriber.vip.pubsub.publish(peer='pubsub',
                                  topic=get_topic,
                                  headers={"requesterID": "test_pubsub_agent"},
                                  message=None)

    # Wait for response with retries
    start_time = time.time()
    while not received_errors and time.time() - start_time < 5:
        gevent.sleep(0.1)

    assert received_errors, "Expected error message for non-existent point."
    topic, headers, msg = received_errors[0]
    assert "NonExistentPoint" in topic, f"Error topic mismatch: {topic}"
