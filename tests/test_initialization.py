import pytest


@pytest.mark.parametrize(
    ['config_version', 'config_contents', 'allow_duplicates'],
    [
        (2, {'remote_config': {'driver_type': 'TestInterface', 'group': '0'}}, True),
        (2, {'remote_config': {'driver_type': 'TestInterface', 'group': '0'}}, False),
        (1, {'driver_type': 'TestInterface', 'group': '0', 'driver_config': {}}, True),
        (1, {'driver_type': 'TestInterface', 'group': '0', 'driver_config': {}}, False),
        (2, {'remote_config': {'driver_type': 'UnknownInterface', 'module': 'tests.test_files.unknown_interface', 'group': '0'}}, False),
        (2, {'remote_config': {'driver_type': 'NonExistentInterface', 'group': '0'}}, False),
        (2, {'remote_config': {'group': '0'}}, False)
    ]
)
def test_get_or_create_remote(driver_service, config_version, config_contents, allow_duplicates):
    pds = driver_service
    pds.config_version = config_version
    pds.allow_duplicate_remotes = allow_duplicates

    config_name = 'Foo/Bar/Baz'
    if config_version == 2 and config_contents['remote_config'].get('driver_type') == 'UnknownInterface':
        remote = pds._get_or_create_remote(config_name, config_contents)
        assert remote.config == {'driver_type': 'UnknownInterface', 'group': 'remotes/0', 'module': 'tests.test_files.unknown_interface'}
    elif config_version == 2 and config_contents['remote_config'].get('driver_type') == 'NonExistentInterface':
        with pytest.raises(ValueError) as e:
            remote = None
            pds._get_or_create_remote(config_name, config_contents)
            assert "This interface type is currently unknown or not installed." in e.value
    elif config_version == 2 and config_contents['remote_config'].get('driver_type') is None:
        with pytest.raises(ValueError) as e:
            remote = None
            pds._get_or_create_remote(config_name, config_contents)
            assert "it does not have a specified interface." in e.value
    else:
        remote = pds._get_or_create_remote(config_name, config_contents)
        assert remote.config == {'driver_type': 'TestInterface', 'group': 'remotes/0'}
    if not remote:
        return

    # Check that "unique_id" is unique to equipment if duplicates are allowed and unique to remote otherwise.
    if allow_duplicates:
        assert remote.core.unique_id == (config_name,)
    else:
        assert remote.core.unique_id == ('some', 'unique', 'id')

    # Check remote is correctly in self.remotes and referenced by grouping tree.
    assert pds.remotes.get(remote.core.unique_id) == remote
    assert remote in pds.remote_grouping_tree.get_node(remote.config.get('group')).remotes

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
    pds._get_or_create_remote = lambda x, y: driver_agent
    topic = 'devices/Foo/Bar/Baz'
    contents = {'driver_type': driver_type}
    pds._configure_new_equipment(topic, action, contents)
    if action == 'NEW' and driver_type:
        assert pds.equipment_tree.contains(topic)
        assert pds.equipment_tree.get_node(topic).is_device()
    elif action == 'NEW' and not driver_type:
        assert pds.equipment_tree.contains(topic)
        assert not pds.equipment_tree.get_node(topic).is_device()


