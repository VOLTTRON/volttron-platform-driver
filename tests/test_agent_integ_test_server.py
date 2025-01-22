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

import json
from unittest.mock import MagicMock
from platform_driver.agent import PlatformDriverAgent
from platform_driver.overrides import OverrideManager
from platform_driver.reservations import ReservationManager
from pathlib import Path
from volttron.utils import get_aware_utc_now
from volttrontesting.server_mock import TestServer

"""
This tests the agent using the test_server. This means that
we are not creating a real instance of the volttron platform
like we do with the platform wrapper. Instead, we are mocking
components of the platform driver, passing in configs directly
bypassing the config store, and testing our driver. 

To run these tests, create a helper toggle named volttrontest in your Home Assistant instance.
This can be done by going to Settings > Devices & services > Helpers > Create Helper > Toggle
"""

def return_config(pattern):
    if pattern == '_override_patterns':
        return b''

def test_instantiate():
    ts = TestServer()
    pda = ts.instantiate_agent(PlatformDriverAgent)

    driver_config = {
        "driver_type": "fake",
        "driver_config": {},
        "registry_config": [
    {
        "Point Name": "EKG",
        "Volttron Point Name": "EKG",
        "Units": "waveform",
        "Units Details": "waveform",
        "Writable": True,
        "Starting Value": "sin",
        "Type": "float",
        "Notes": "Sine wave for baseline output"
    },
    {
        "Point Name": "Heartbeat",
        "Volttron Point Name": "Heartbeat",
        "Units": "On/Off",
        "Units Details": "On/Off",
        "Writable": True,
        "Starting Value": "0",
        "Type": "boolean",
        "Notes": "Point for heartbeat toggle"
    },
    {
        "Point Name": "OutsideAirTemperature1",
        "Volttron Point Name": "OutsideAirTemperature1",
        "Units": "F",
        "Units Details": "-100 to 300",
        "Writable": False,
        "Starting Value": "50",
        "Type": "float",
        "Notes": "CO2 Reading 0.00-2000.0 ppm"
    },
    {
        "Point Name": "SampleWritableFloat1",
        "Volttron Point Name": "SampleWritableFloat1",
        "Units": "PPM",
        "Units Details": "1000.00 (default)",
        "Writable": True,
        "Starting Value": "10",
        "Type": "float",
        "Notes": "Setpoint to enable demand control ventilation"
    }
],
        "interval": 5,
        "timezone": "US/Pacific",
        "heart_beat_point": "Heartbeat",
        "publish_breadth_first_all": False,
        "publish_depth_first": False,
        "publish_breadth_first": False
    }
    driver_config_path = Path("/tmp/driver_config.config")
    with driver_config_path.open("w") as file:
        json.dump(driver_config, file)

    with open('/tmp/driver_config.config') as f:
        FakeConfig = json.load(f)

    pda.vip.config.get = return_config
    pda.override_manager = OverrideManager(pda)
    now = get_aware_utc_now()
    pda.reservation_manager = ReservationManager(pda, pda.config.reservation_preempt_grace_time, now)
    pda._configure_new_equipment('devices/campus/building/fake', 'NEW', FakeConfig, schedule_now=False)
    pda.vip = MagicMock()
    pda.vip.rpc.context = MagicMock()
    pda.vip.rpc.context.vip_message.peer = 'some.caller'

    result = pda.get_point('devices/campus/building/fake/SampleWritableFloat1')
    print(f"result: {result}")
    assert result == 10
    result = pda.set_point('devices/campus/building/fake', 'SampleWritableFloat1', 15)
    assert result == 15


    result = pda.get_multiple_points(['devices/campus/building/fake/SampleWritableFloat1',
                                      'devices/campus/building/fake/OutsideAirTemperature1'])
    assert result == ({'devices/campus/building/fake/OutsideAirTemperature1': 50.0,
                       'devices/campus/building/fake/SampleWritableFloat1': 15.0}, {})

