import json
import pathlib
import pytest

from volttron.services.driver.data_structures import RemoteTree, DeviceNode, EquipmentTree, EquipmentNode, PointNode
from volttron.services.driver.platform_driver_service import DriverAgent

SAMPLE_REGISTRY = [{'Point Name': 'EKG', 'Volttron Point Name': 'EKG', 'Units': 'waveform', 'Units Details': 'waveform', 'Writable': 'TRUE', 'Starting Value': 'sin', 'Type': 'float', 'Notes': 'Sine wavefor baseline output'},
{'Point Name': 'Heartbeat', 'Volttron Point Name': 'Heartbeat', 'Units': 'On/Off', 'Units Details': 'On/Off', 'Writable': 'TRUE', 'Starting Value': '0', 'Type': 'boolean', 'Notes': 'Point for heartbeat toggle'},
{'Point Name': 'OutsideAirTemperature1', 'Volttron Point Name': 'OutsideAirTemperature1', 'Units': 'F', 'Units Details': '-100 to 300', 'Writable': 'FALSE', 'Starting Value': '50', 'Type': 'float', 'Notes': 'CO2 Reading 0.00-2000.0 ppm'},
{'Point Name': 'SampleWritableFloat1', 'Volttron Point Name': 'SampleWritableFloat1', 'Units': 'PPM', 'Units Details': '1000.00 (default)', 'Writable': 'TRUE', 'Starting Value': '10','Type': 'float', 'Notes': 'Setpoint to enable demand control ventilation'},
{'Point Name': 'SampleLong1', 'Volttron Point Name': 'SampleLong1', 'Units': 'Enumeration', 'Units Details': '1 through 13', 'Writable': 'FALSE', 'Starting Value': '50', 'Type': 'int', 'Notes': 'Status indicator of service switch'},
{'Point Name': 'SampleWritableShort1', 'Volttron Point Name': 'SampleWritableShort1', 'Units': '%', 'Units Details': '0.00 to 100.00 (20 default)', 'Writable': 'TRUE', 'Starting Value': '20', 'Type': 'int', 'Notes': 'Minimum damper position during the standard mode'},
{'Point Name': 'SampleBool1', 'Volttron Point Name': 'SampleBool1', 'Units': 'On / Off', 'Units Details': 'on/off', 'Writable': 'FALSE', 'Starting Value': 'TRUE', 'Type': 'boolean', 'Notes': 'Status indidcator of cooling stage 1'},
{'Point Name': 'SampleWritableBool1', 'Volttron Point Name': 'SampleWritableBool1', 'Units': 'On / Off', 'Units Details': 'on/off', 'Writable': 'TRUE', 'Starting Value': 'TRUE', 'Type': 'boolean', 'Notes': 'Status indicator'},
{'Point Name': 'OutsideAirTemperature2', 'Volttron Point Name': 'OutsideAirTemperature2', 'Units': 'F', 'Units Details': '-100 to 300', 'Writable': 'FALSE', 'Starting Value': '50', 'Type': 'float', 'Notes': 'CO2 Reading 0.00-2000.0 ppm'},
{'Point Name': 'SampleWritableFloat2', 'Volttron Point Name': 'SampleWritableFloat2', 'Units': 'PPM', 'Units Details': '1000.00 (default)', 'Writable': 'TRUE', 'Starting Value': '10','Type': 'float', 'Notes': 'Setpoint to enable demand control ventilation'},
{'Point Name': 'SampleLong2', 'Volttron Point Name': 'SampleLong2', 'Units': 'Enumeration', 'Units Details': '1 through 13', 'Writable': 'FALSE', 'Starting Value': '50', 'Type': 'int', 'Notes': 'Status indicator of service switch'},
{'Point Name': 'SampleWritableShort2', 'Volttron Point Name': 'SampleWritableShort2', 'Units': '%', 'Units Details': '0.00 to 100.00 (20 default)', 'Writable': 'TRUE', 'Starting Value': '20', 'Type': 'int', 'Notes': 'Minimum damper position during the standard mode'},
{'Point Name': 'SampleBool2', 'Volttron Point Name': 'SampleBool2', 'Units': 'On / Off', 'Units Details': 'on/off', 'Writable': 'FALSE', 'Starting Value': 'TRUE', 'Type': 'boolean', 'Notes': 'Status indidcator of cooling stage 1'},
{'Point Name': 'SampleWritableBool2', 'Volttron Point Name': 'SampleWritableBool2', 'Units': 'On / Off', 'Units Details': 'on/off', 'Writable': 'TRUE', 'Starting Value': 'TRUE', 'Type': 'boolean', 'Notes': 'Status indicator'},
{'Point Name': 'OutsideAirTemperature3', 'Volttron Point Name': 'OutsideAirTemperature3', 'Units': 'F', 'Units Details': '-100 to 300', 'Writable': 'FALSE', 'Starting Value': '50', 'Type': 'float', 'Notes': 'CO2 Reading 0.00-2000.0 ppm'},
{'Point Name': 'SampleWritableFloat3', 'Volttron Point Name': 'SampleWritableFloat3', 'Units': 'PPM', 'Units Details': '1000.00 (default)', 'Writable': 'TRUE', 'Starting Value': '10','Type': 'float', 'Notes': 'Setpoint to enable demand control ventilation'},
{'Point Name': 'SampleLong3', 'Volttron Point Name': 'SampleLong3', 'Units': 'Enumeration', 'Units Details': '1 through 13', 'Writable': 'FALSE', 'Starting Value': '50', 'Type': 'int', 'Notes': 'Status indicator of service switch'},
{'Point Name': 'SampleWritableShort3', 'Volttron Point Name': 'SampleWritableShort3', 'Units': '%', 'Units Details': '0.00 to 100.00 (20 default)', 'Writable': 'TRUE', 'Starting Value': '20', 'Type': 'int', 'Notes': 'Minimum damper position during the standard mode'},
{'Point Name': 'SampleBool3', 'Volttron Point Name': 'SampleBool3', 'Units': 'On / Off', 'Units Details': 'on/off', 'Writable': 'FALSE', 'Starting Value': 'TRUE', 'Type': 'boolean', 'Notes': 'Status indidcator of cooling stage 1'},
{'Point Name': 'SampleWritableBool3', 'Volttron Point Name': 'SampleWritableBool3', 'Units': 'On / Off', 'Units Details': 'on/off', 'Writable': 'TRUE', 'Starting Value': 'TRUE', 'Type': 'boolean', 'Notes': 'Status indicator'},
{'Point Name': 'HPWH_Phy0_PowerState', 'Volttron Point Name': 'PowerState', 'Units': '1/0', 'Units Details': '1/0', 'Writable': 'TRUE', 'Starting Value': '0', 'Type': 'int', 'Notes': 'Power on off status'},
{'Point Name': 'ERWH_Phy0_ValveState', 'Volttron Point Name': 'ValveState', 'Units': '1/0', 'Units Details': '1/0', 'Writable': 'TRUE', 'Starting Value': '0', 'Type': 'int', 'Notes': 'power on off status'}
]

def test_build_remote_tree(remote_tree):
    assert remote_tree.to_dict() == {'remotes': {'children': ['0', {'Panel2': {'children': ['Bus1', 'Bus2']}}]}}

def test_add_and_remove_remote(remote_tree, driver_agent):
    group_id = 'remotes/Panel2/Bus1'
    remote_tree.add_remote(driver_agent, group_id)
    assert driver_agent in remote_tree.get_node(group_id).remotes
    remote_tree.remove_remote(driver_agent, group_id)
    assert driver_agent not in remote_tree.get_node(group_id).remotes

def test_build_equipment_tree(driver_agent):
    et = EquipmentTree()
    assert et.root == 'devices'
    assert len(et.all_nodes()) == 1
    config = {}

    # Test adding of a device:
    et.add_device('devices/Foo/Bar/Baz', config, driver_agent)
    assert len(et.all_nodes()) == 4
    foo = et.get_node('devices/Foo')
    bar = et.get_node('devices/Foo/Bar')
    baz = et.get_node('devices/Foo/Bar/Baz')
    assert not foo.is_point and not foo.is_device
    assert isinstance(foo, EquipmentNode) and not isinstance(foo, DeviceNode)
    assert not bar.is_point and not bar.is_device
    assert isinstance(bar, EquipmentNode) and not isinstance(bar, DeviceNode)
    assert not baz.is_point and baz.is_device
    assert isinstance(baz, EquipmentNode) and isinstance(baz, DeviceNode)
    et.add_device('devices/Foo/Car/Baz', {'registry_config': SAMPLE_REGISTRY}, driver_agent)
    print(et.show())
    car = et.get_node('devices/Foo/Car')
    carbaz = et.get_node('devices/Foo/Car/Baz')
    assert len(et.all_nodes()) == 28
    assert not car.is_point and not car.is_device
    assert isinstance(car, EquipmentNode) and not isinstance(car, DeviceNode)
    assert not carbaz.is_point and carbaz.is_device
    assert isinstance(carbaz, EquipmentNode) and isinstance(carbaz, DeviceNode)
    assert all([et.get_node(n).is_point and not et.get_node(n).is_device for n in carbaz.successors(et.identifier)])
    assert all([isinstance(et.get_node(n), PointNode) for n in carbaz.successors(et.identifier)])
