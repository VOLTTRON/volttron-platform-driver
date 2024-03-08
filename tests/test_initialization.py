import pytest


@pytest.mark.parametrize(
    ['config_version', 'config_contents', 'allow_duplicates'],
    [
        (2, {'controller_config': {'driver_type': 'TestInterface', 'group': '0'}}, True),
        (2, {'controller_config': {'driver_type': 'TestInterface', 'group': '0'}}, False),
        (1, {'driver_type': 'TestInterface', 'group': '0', 'driver_config': {}}, True),
        (1, {'driver_type': 'TestInterface', 'group': '0', 'driver_config': {}}, False),
        (2, {'controller_config': {'driver_type': 'UnknownInterface', 'module': 'tests.test_files.unknown_interface', 'group': '0'}}, False),
        (2, {'controller_config': {'driver_type': 'NonExistentInterface', 'group': '0'}}, False),
        (2, {'controller_config': {'group': '0'}}, False)
    ]
)
def test_get_or_create_controller(driver_service, config_version, config_contents, allow_duplicates):
    pds = driver_service
    pds.config_version = config_version
    pds.allow_duplicate_controllers = allow_duplicates

    config_name = 'Foo/Bar/Baz'
    if config_version == 2 and config_contents['controller_config'].get('driver_type') == 'UnknownInterface':
        controller = pds._get_or_create_controller(config_name, config_contents)
        assert controller.config == {'driver_type': 'UnknownInterface', 'group': 'controllers/0', 'module': 'tests.test_files.unknown_interface'}
    elif config_version == 2 and config_contents['controller_config'].get('driver_type') == 'NonExistentInterface':
        with pytest.raises(ValueError) as e:
            controller = None
            pds._get_or_create_controller(config_name, config_contents)
            assert "This interface type is currently unknown or not installed." in e.value
    elif config_version == 2 and config_contents['controller_config'].get('driver_type') is None:
        with pytest.raises(ValueError) as e:
            controller = None
            pds._get_or_create_controller(config_name, config_contents)
            assert "it does not have a specified interface." in e.value
    else:
        controller = pds._get_or_create_controller(config_name, config_contents)
        assert controller.config == {'driver_type': 'TestInterface', 'group': 'controllers/0'}
    if not controller:
        return

    # Check that "unique_id" is unique to equipment if duplicates are allowed and unique to controller otherwise.
    if allow_duplicates:
        assert controller.core.unique_id == (config_name,)
    else:
        assert controller.core.unique_id == ('some', 'unique', 'id')

    # Check controller is correctly in self.controllers and referenced by grouping tree.
    assert pds.controllers.get(controller.core.unique_id) == controller
    assert controller in pds.controller_grouping_tree.get_node(controller.config.get('group')).controllers

@pytest.mark.parametrize(
    ('driver_type', 'action'),
    [
        ('TestInterface', 'NEW'),
        (None, 'NEW')
    ]
)
def test_configure_new_equipment(driver_service, driver_agent, driver_type, action):
    pds = driver_service
    pds.config_version = 2
    pds._get_or_create_controller = lambda x, y: driver_agent
    topic = 'devices/Foo/Bar/Baz'
    contents = {'driver_type': driver_type}
    pds._configure_new_equipment(topic, action, contents)
    if action == 'NEW' and driver_type:
        assert pds.equipment_tree.contains(topic)
        assert pds.equipment_tree.get_node(topic).is_device()
    elif action == 'NEW' and not driver_type:
        assert pds.equipment_tree.contains(topic)
        assert not pds.equipment_tree.get_node(topic).is_device()


