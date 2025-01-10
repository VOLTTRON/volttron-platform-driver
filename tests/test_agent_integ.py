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
                "frequency": 5.0,  # Polling frequency in seconds
                "points": ["*"]     # Poll all points
            }
        }
    }
    main_config_path = Path(tempfile.mktemp(suffix="_main_driver_config.json"))
    with main_config_path.open("w") as file:
        json.dump(main_config, file)

    # Store the main driver config in the config store
    vi.run_command(["vctl", "config", "store", "platform.driver", "config", str(main_config_path), "--json"])

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

    vi.run_command(["vctl", "config", "store", "platform.driver", "devices/singletestfake", str(device_config_path), "--json"])

    # Create and store a CSV registry config
    with tempfile.NamedTemporaryFile(mode='w', suffix=".csv", delete=False) as temp_csv:
        temp_csv.write(
            "Point Name,Volttron Point Name,Units,Units Details,Writable,Starting Value,Type,Notes\n"
            "TestPoint1,TestPoint1,PPM,1000.00 (default),TRUE,10,float,Test point 1\n"
            "TestPoint2,TestPoint2,PPM,1000.00 (default),TRUE,20,float,Test point 2\n"
            "Heartbeat,Heartbeat,On/Off,On/Off,TRUE,0,boolean,Heartbeat point\n"
        )
        csv_path = temp_csv.name

    vi.run_command(["vctl", "config", "store", "platform.driver", "singletestfake.csv", csv_path, "--csv"])

    # Install and start the platform driver agent
    agent_dir = str(Path(__file__).parent.parent.resolve())

    agent_uuid = vi.install_agent(
        agent_dir=agent_dir,
        install_options=InstallAgentOptions(
            start=True,
            vip_identity="platform.driver"
        )
    )

    assert agent_uuid is not None

    # Wait for the agent to start and load configs
    time.sleep(5)

    # Create a test agent to interact with the driver
    ba = vi.build_agent(identity="test_agent")

    return vi, ba, "singletestfake"



def test_get_point(driver_setup):
    vi, ba, device_name = driver_setup
    value = ba.vip.rpc.call("platform.driver", "get_point", f"devices/{device_name}", "TestPoint1").get(timeout=10)
    assert value == 10.0, "Initial value of TestPoint1 should be 10.0"


def test_set_point(driver_setup):
    vi, ba, device_name = driver_setup
    ba.vip.rpc.call("platform.driver", "set_point", f"devices/{device_name}", "TestPoint1", 33.3).get(timeout=10)
    new_val = ba.vip.rpc.call("platform.driver", "get_point", f"devices/{device_name}", "TestPoint1").get(timeout=10)
    assert new_val == 33.3, "TestPoint1 should be updated to 33.3"

def test_multiple_points(driver_setup):
    vi, ba, device_name = driver_setup

    # Retrieve multiple points
    result, errors = ba.vip.rpc.call(
        "platform.driver",
        "get_multiple_points",
        path=f"devices/{device_name}",
        point_names=["TestPoint1", "TestPoint2"]
    ).get(timeout=10)

    assert not errors, f"Errors found: {errors}"
    assert result[f"devices/{device_name}/TestPoint1"] == 33.3
    assert result[f"devices/{device_name}/TestPoint2"] == 20.0


def test_revert_point(driver_setup):
    vi, ba, device_name = driver_setup

    # Set TestPoint1 to 50.0
    ba.vip.rpc.call(
        "platform.driver", "set_point",
        f"devices/{device_name}",  # "devices/singletestfake"
        "TestPoint1", 50.0
    ).get(timeout=10)

    # Revert the point using short path for the device
    ba.vip.rpc.call(
        "platform.driver", "revert_point",
        f"devices/{device_name}",  # "devices/singletestfake"
        "TestPoint1"
    ).get(timeout=10)

    # Now read it back
    val = ba.vip.rpc.call(
        "platform.driver", "get_point",
        f"devices/{device_name}",  # "devices/singletestfake"
        "TestPoint1"
    ).get(timeout=10)

    # Should be back to 10.0
    assert val == 10.0



def test_revert_device(driver_setup):
    vi, ba, device_name = driver_setup
    ba.vip.rpc.call("platform.driver", "set_point", f"devices/{device_name}", "TestPoint2", 999.9).get(timeout=10)
    val = ba.vip.rpc.call("platform.driver", "get_point", f"devices/{device_name}", "TestPoint2").get(timeout=10)
    assert val == 999.9

    ba.vip.rpc.call("platform.driver", "revert_device", f"devices/{device_name}").get(timeout=10)
    val = ba.vip.rpc.call("platform.driver", "get_point", f"devices/{device_name}", "TestPoint2").get(timeout=10)
    assert val == 20.0, "After revert_device, TestPoint2 should return to its default value."


def test_override_on_off(driver_setup):
    vi, ba, device_name = driver_setup

    # Enable override
    ba.vip.rpc.call("platform.driver", "set_override_on", f"devices/{device_name}").get(timeout=10)
    overridden_devices = ba.vip.rpc.call("platform.driver", "get_override_devices").get(timeout=10)
    assert f"devices/{device_name}" in overridden_devices, "Device should be in override mode."

    # Disable override
    ba.vip.rpc.call("platform.driver", "set_override_off", f"devices/{device_name}").get(timeout=10)
    overridden_devices = ba.vip.rpc.call("platform.driver", "get_override_devices").get(timeout=10)
    assert f"devices/{device_name}" not in overridden_devices, "Device should not be in override mode."



def test_poll_schedule(driver_setup):
    vi, ba, _ = driver_setup

    schedule = ba.vip.rpc.call("platform.driver", "get_poll_schedule").get(timeout=10)
    assert schedule, "Poll schedule should not be empty."
    assert "default" in schedule, "Default polling group should exist."
