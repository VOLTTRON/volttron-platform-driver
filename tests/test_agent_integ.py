# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Installable Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2024 Battelle Memorial Institute
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# ===----------------------------------------------------------------------===
# }}}
import csv
import json
import time
from pathlib import Path

import gevent

from volttrontesting import PlatformWrapper
from volttrontesting.platformwrapper import InstallAgentOptions
from volttron.client.known_identities import CONTROL

"""
Integration tests for volttron-platform-driver. These tests
utilize the platform wrapper, meaning a real and full volttron instance
is installed and ran.
"""

import json
import time
from pathlib import Path
import gevent
import tempfile
import csv
def test_startup_instance(volttron_instance: PlatformWrapper):
    # Simple test to install the platform.driver and make sure it gets a UUID
    assert volttron_instance.is_running()
    # Install and start the platform driver agent
    vi = volttron_instance
    agent_pth = str(Path(__file__).parent.parent.resolve())
    auuid = vi.install_agent(agent_dir=agent_pth,
                             install_options=InstallAgentOptions(start=True, vip_identity="platform.driver1"))
    assert auuid is not None
def test_startup_instance_with_fake_driver(volttron_instance: PlatformWrapper):
    assert volttron_instance.is_running()

    vi = volttron_instance

    # Paths to agent and library
    agent_pth = str(Path(__file__).parent.parent.resolve())
    library_path = Path("/home/riley/DRIVERWORK/11rc1/volttron-lib-fake-driver").resolve() # TODO replace with pypi

    # Install the library first
    try:
        vi.install_library(library_path)
        print("Installed volttron-lib-fake-driver successfully")
    except Exception as e:
        print("Failed to install fake driver library", e)

    time.sleep(1)

    # Create the driver configuration JSON
    driver_config = {
        "driver_config": {},
        "registry_config": "config://fake.csv",
        "interval": 5,
        "timezone": "US/Pacific",
        "heart_beat_point": "Heartbeat",
        "driver_type": "fake",
        "publish_breadth_first_all": False,
        "publish_depth_first": False,
        "publish_breadth_first": False
    }
    driver_config_path = Path("/tmp/driver_config.config")
    with driver_config_path.open("w") as file:
        json.dump(driver_config, file)

    # Store the driver device config in the config store before installing the agent
    vi.run_command(["vctl", "config", "store", "platform.driver", "devices/fake", str(driver_config_path), "--json"])

    # Create and store the CSV registry configuration in a temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix=".csv", delete=False) as temp_csv:
        csv_writer = csv.writer(temp_csv)
        # Write header
        csv_writer.writerow(
            ["Point Name", "Volttron Point Name", "Units", "Units Details", "Writable", "Starting Value", "Type",
             "Notes"])
        # Add points
        csv_writer.writerow(
            ["SampleWritableFloat1", "SampleWritableFloat1", "PPM", "1000.00 (default)", "TRUE", "10", "float",
             "Setpoint to enable demand control ventilation"])

        csv_writer.writerow(
            ["SampleWritableFloat2", "SampleWritableFloat2", "PPM", "1000.00(default)", "TRUE", "10", "float",
             "Setpoint to enable demand control ventilation"])

        csv_writer.writerow(
            ["Heartbeat", "Heartbeat", "On/Off", "On/Off", "TRUE", "0", "boolean", "Point for heartbeat toggle"])

        fake_csv_path = temp_csv.name

    # Store the CSV registry config
    vi.run_command(["vctl", "config", "store", "platform.driver", "fake.csv", fake_csv_path, "--csv"])

    # Install and start the platform driver agent
    auuid = vi.install_agent(agent_dir=agent_pth,
                             install_options=InstallAgentOptions(start=True, vip_identity="platform.driver"))
    assert auuid is not None

    time.sleep(5)  # Give the agent some time to start and load configs

    # Verify that configurations are stored
    list_configs = vi.run_command(["vctl", "config", "list", "platform.driver"])
    print("Final platform.driver config store contents:")
    print(list_configs)
    list_configs = vi.run_command(["vctl", "config", "get", "platform.driver", "devices/fake"])
    print("Final platform.driver driver config contents:", list_configs)

    # Create an agent to make RPC calls
    ba = vi.build_agent(identity="world")
    agent_identity = ba.vip.rpc.call(CONTROL, 'agent_vip_identity', auuid).get(timeout=10)
    print(f"Agent identity obtained: {agent_identity}")

    # Test setting and getting the point
    ba.vip.rpc.call("platform.driver", "set_point", "devices/fake", "SampleWritableFloat1", 1).get(timeout=20)
    result = ba.vip.rpc.call("platform.driver", "get_point", "devices/fake", "SampleWritableFloat1").get(timeout=20)
    assert result == 1.0

    # result, errors = ba.vip.rpc.call(
    #     "platform.driver",
    #     "get_multiple_points",
    #     path="devices/fake",
    #     point_names=["SampleWritableFloat1", "SampleWritableFloat2"]
    # ).get(timeout=20)
    #
    # assert result == {"devices/fake/SampleWritableFloat1": 1.0, "devices/fake/SampleWritableFloat2": 10.0}
    # assert not errors


