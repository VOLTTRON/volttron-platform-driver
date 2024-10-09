import pytest
from unittest.mock import MagicMock, Mock, patch
from datetime import datetime

from volttron.utils import format_timestamp, get_aware_utc_now
from platform_driver.agent import PlatformDriverAgent, PlatformDriverConfig, STATUS_BAD, RemoteConfig, DeviceConfig, \
    PointConfig
from platform_driver.constants import VALUE_RESPONSE_PREFIX, RESERVATION_RESULT_TOPIC

class TestPDALoadAgentConfig:
    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.core = MagicMock()
        PDA.vip = MagicMock()
        PDA.vip.health.set_status = MagicMock()
        PDA.core.connected = True
        return PDA

    def test_load_agent_config_with_valid_config(self, PDA):
        """Tests that the result is the config we provided, and that its an instance of PlatformDriverAgentConfig."""
        PDA.core.connected = True
        valid_config = {
            'max_open_sockets': 5,
            'max_concurrent_publishes': 10,
            'scalability_test': False,
            'remote_heartbeat_interval': 30,
            'reservation_preempt_grace_time': 60
        }

        result = PDA._load_agent_config(valid_config)

        assert isinstance(result, PlatformDriverConfig)
        assert result.max_open_sockets == 5
        assert result.max_concurrent_publishes == 10
        assert result.scalability_test == False
        assert result.remote_heartbeat_interval == 30
        assert result.reservation_preempt_grace_time == 60

        PDA.vip.health.set_status.assert_not_called()

    def test_load_agent_config_with_invalid_config(self, PDA, caplog):
        """tests that a default config is returned when invalid type is provided"""
        PDA.core.connected = True
        # Prepare an invalid configuration dictionary
        invalid_config = {
            'max_open_sockets': 'invalid',  # should be an int
            'max_concurrent_publishes': 10,
            'scalability_test': False,
            'remote_heartbeat_interval': 30,
            'reservation_preempt_grace_time': 60
        }

        result = PDA._load_agent_config(invalid_config)

        assert isinstance(result, PlatformDriverConfig)
        # Check that 'invalid' is not kept in the config
        assert result.max_open_sockets != 'invalid'
        assert any(
            'Validation of platform driver configuration file failed. Using default values.' in message for message in
            caplog.text.splitlines())
        # Ensure health status was set to bad
        PDA.vip.health.set_status.assert_called_once()
        status_args = PDA.vip.health.set_status.call_args[0]
        assert status_args[0] == STATUS_BAD
        assert 'Error processing configuration' in status_args[1]

    def test_load_agent_config_with_invalid_config_agent_not_connected(self, PDA, caplog):
        PDA.core.connected = False
        # invalid configuration dictionary
        invalid_config = {
            'max_open_sockets': 'invalid',  # Should be an int
            'max_concurrent_publishes': 10,
            'scalability_test': False,
            'remote_heartbeat_interval': 30,
            'reservation_preempt_grace_time': 60
        }
        result = PDA._load_agent_config(invalid_config)
        assert isinstance(result, PlatformDriverConfig)
        assert any(
            'Validation of platform driver configuration file failed. Using default values.' in message for message in
            caplog.text.splitlines())
        # make sure health status was not set since agent is not connected
        PDA.vip.health.set_status.assert_not_called()

class TestPDAConfigureMain:

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.core = MagicMock()
        PDA.vip = MagicMock()

        PDA.vip.config.get = Mock(return_value='{}')

        return PDA

    def test_configure_main_calls_configure_publish_lock(self, PDA):
        """Tests the configure main calls setup_socket_lock and configure_publish_lock when action is new"""
        with patch('platform_driver.agent.setup_socket_lock') as mock_setup_socket_lock, \
             patch('platform_driver.agent.configure_publish_lock') as mock_configure_publish_lock:
            contents = {'config_version': 2, 'publish_depth_first_any': True}
            PDA.configure_main(_="", action="NEW", contents=contents)
            mock_setup_socket_lock.assert_called_once()
            mock_configure_publish_lock.assert_called_once()

class TestPDASeparateEquipmentConfigs:
    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.core = MagicMock()
        PDA.vip = MagicMock()
        PDA.vip.config.get = Mock(return_value='{}')
        return PDA

    def test_separate_equipment_configs(self, PDA):
        """Tests that the separate equipment configs work as expected, input config, output remote, dev, point"""
        # Mock the _get_configured_interface method to return a mock interface
        mock_interface = MagicMock()
        mock_interface.INTERFACE_CONFIG_CLASS = MagicMock()
        mock_interface.REGISTER_CONFIG_CLASS = MagicMock()
        PDA._get_configured_interface = MagicMock(return_value=mock_interface)

        # Define a sample configuration dictionary
        config_dict = {
            'remote_config': {
                'driver_type': 'mock_driver',
                'some_remote_setting': 'value'
            },
            'registry_config': [
                {'point_name': 'temperature', 'unit': 'C'},
                {'point_name': 'humidity', 'unit': '%'}
            ],
            'some_device_setting': 'device_value'
        }

        # Mock the instantiation of INTERFACE_CONFIG_CLASS
        remote_config_instance = MagicMock()
        remote_config_instance.driver_type = 'mock_driver'
        remote_config_instance.some_remote_setting = 'value'
        mock_interface.INTERFACE_CONFIG_CLASS.return_value = remote_config_instance

        # Mock the instantiation of DeviceConfig
        dev_config_instance = MagicMock()
        dev_config_instance.some_device_setting = 'device_value'
        dev_config_instance.equipment_specific_fields = {}

        # Mock the instantiation of REGISTER_CONFIG_CLASS
        point_config_instances = []
        for reg in config_dict['registry_config']:
            point_config_instance = MagicMock()
            point_config_instance.point_name = reg['point_name']
            point_config_instance.unit = reg['unit']
            point_config_instances.append(point_config_instance)

        # Set side effect so that each call to REGISTER_CONFIG_CLASS returns the next point config instance
        mock_interface.REGISTER_CONFIG_CLASS.side_effect = point_config_instances

        # Patch DeviceConfig and RemoteConfig where they are imported in the module
        with patch('platform_driver.agent.DeviceConfig', return_value=dev_config_instance) as mock_device_config_class, \
             patch('platform_driver.agent.RemoteConfig', return_value=remote_config_instance) as mock_remote_config_class:

            remote_config, dev_config, point_configs = PDA._separate_equipment_configs(config_dict)

        # Check remote_config
        assert remote_config == remote_config_instance
        assert remote_config.driver_type == 'mock_driver'
        assert remote_config.some_remote_setting == 'value'

        # Check dev_config
        assert dev_config == dev_config_instance
        assert dev_config.some_device_setting == 'device_value'

        # Check point_configs
        assert len(point_configs) == 2
        point_names = {pc.point_name for pc in point_configs}
        units = {pc.unit for pc in point_configs}
        assert point_names == {'temperature', 'humidity'}
        assert units == {'C', '%'}

class TestPDAConfigureNewEquipment:
    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.core = MagicMock()
        PDA.vip = MagicMock()
        PDA.vip.config.get = Mock(return_value='{}')

        # Mock dependencies
        PDA.equipment_tree = MagicMock()
        PDA._update_equipment = MagicMock()
        PDA._separate_equipment_configs = MagicMock()
        PDA._get_or_create_remote = MagicMock()
        PDA.poll_schedulers = {}
        return PDA

    def test_configure_new_equipment_existing_node_config_not_finished(self, PDA):
        equipment_name = 'existing_equipment'
        contents = {'some': 'contents'}

        existing_node = MagicMock()
        existing_node.config_finished = False
        PDA.equipment_tree.get_node.return_value = existing_node

        result = PDA._configure_new_equipment(equipment_name, None, contents)

        assert existing_node.config_finished == True
        assert result == False
        PDA._update_equipment.assert_not_called()

    def test_configure_new_equipment_existing_node_config_finished(self, PDA):
        equipment_name = 'existing_equipment'
        contents = {'some': 'contents'}

        existing_node = MagicMock()
        existing_node.config_finished = True
        PDA.equipment_tree.get_node.return_value = existing_node

        # Set up _update_equipment to return True
        PDA._update_equipment.return_value = True

        result = PDA._configure_new_equipment(equipment_name, None, contents)

        PDA._update_equipment.assert_called_once_with(equipment_name, 'UPDATE', contents, True)
        assert result == True

    def test_configure_new_equipment_new_device_node(self, PDA):
        equipment_name = 'new_device'
        contents = {'some': 'contents'}

        PDA.equipment_tree.get_node.return_value = None

        # mock _separate_equipment_configs
        remote_config = MagicMock()
        dev_config = MagicMock()
        dev_config.allow_duplicate_remotes = False
        registry_config = MagicMock()
        PDA._separate_equipment_configs.return_value = (remote_config, dev_config, registry_config)

        # Mock _get_or_create_remote
        driver = MagicMock()
        PDA._get_or_create_remote.return_value = driver

        # Mock equipment_tree.add_device
        device_node = MagicMock()
        PDA.equipment_tree.add_device.return_value = device_node

        # Mock driver.add_equipment
        driver.add_equipment = MagicMock()

        # Mock get_group
        PDA.equipment_tree.get_group.return_value = 'group1'
        # Mock poll_schedulers
        poll_scheduler = MagicMock()
        PDA.poll_schedulers = {'group1': poll_scheduler}

        result = PDA._configure_new_equipment(equipment_name, None, contents)


        PDA._separate_equipment_configs.assert_called_once_with(contents)
        PDA._get_or_create_remote.assert_called_once_with(equipment_name, remote_config,
                                                          dev_config.allow_duplicate_remotes)
        PDA.equipment_tree.add_device.assert_called_once_with(
            device_topic=equipment_name, dev_config=dev_config,
            driver_agent=driver, registry_config=registry_config
        )
        driver.add_equipment.assert_called_once_with(device_node)
        PDA.equipment_tree.get_group.assert_called_once_with(equipment_name)
        poll_scheduler.schedule.assert_called_once()
        assert result == True

    def test_configure_new_equipment_new_segment_node(self, PDA):
        equipment_name = 'new_segment'
        contents = {'some': 'contents'}

        PDA.equipment_tree.get_node.return_value = None

        remote_config = MagicMock()
        dev_config = None
        registry_config = MagicMock()
        PDA._separate_equipment_configs.return_value = (remote_config, dev_config, registry_config)

        # Mock EquipmentConfig
        with patch('platform_driver.agent.EquipmentConfig') as MockEquipmentConfig:
            equipment_config_instance = MockEquipmentConfig.return_value

            PDA.equipment_tree.add_segment = MagicMock()

            PDA.equipment_tree.get_group.return_value = 'group1'
            poll_scheduler = MagicMock()
            PDA.poll_schedulers = {'group1': poll_scheduler}

            result = PDA._configure_new_equipment(equipment_name, None, contents)

            PDA._separate_equipment_configs.assert_called_once_with(contents)
            MockEquipmentConfig.assert_called_once_with(**contents)
            PDA.equipment_tree.add_segment.assert_called_once_with(equipment_name, equipment_config_instance)
            PDA.equipment_tree.get_group.assert_called_once_with(equipment_name)
            poll_scheduler.schedule.assert_called_once()
            assert result == True

    def test_configure_new_equipment_separate_equipment_configs_raises_value_error(self, PDA):
        equipment_name = 'new_equipment'
        contents = {'some': 'contents'}

        PDA.equipment_tree.get_node.return_value = None

        # Mock _separate_equipment_configs to raise ValueError
        PDA._separate_equipment_configs.side_effect = ValueError('Invalid configuration')

        # Mock logger
        with patch('platform_driver.agent._log') as mock_log:
            result = PDA._configure_new_equipment(equipment_name, None, contents)

            # Assertions
            PDA._separate_equipment_configs.assert_called_once_with(contents)
            # Check that the warning was logged
            mock_log.warning.assert_called_once_with(
                f'Skipping configuration of equipment: {equipment_name} after encountering error --- Invalid configuration'
            )
            # Check that result is False
            assert result == False

class TestPDAGetOrCreateRemote:
    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.core = MagicMock()
        PDA.vip = MagicMock()
        PDA.vip.config.get = Mock(return_value='{}')

        # Mock dependencies
        PDA._get_configured_interface = MagicMock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.remotes = {}
        PDA.config = MagicMock()
        PDA.config.allow_duplicate_remotes = False
        PDA.scalability_test = MagicMock()
        PDA.config.timezone = 'UTC'
        PDA.vip = MagicMock()
        return PDA

    def test_get_or_create_remote_driver_exists(self, PDA):
        equipment_name = 'equipment1'
        remote_config = MagicMock()
        allow_duplicate_remotes = False

        # Mock interface and unique_remote_id
        interface = MagicMock()
        interface.unique_remote_id.return_value = 'unique_id_1'
        PDA._get_configured_interface.return_value = interface

        # Existing DriverAgent
        existing_driver_agent = MagicMock()
        PDA.equipment_tree.remotes['unique_id_1'] = existing_driver_agent

        result = PDA._get_or_create_remote(equipment_name, remote_config, allow_duplicate_remotes)

        PDA._get_configured_interface.assert_called_once_with(remote_config)
        interface.unique_remote_id.assert_called_once_with(equipment_name, remote_config)
        assert result == existing_driver_agent

    def test_get_or_create_remote_driver_not_exists(self, PDA):
        equipment_name = 'equipment2'
        remote_config = MagicMock()
        allow_duplicate_remotes = False

        # Mock interface and unique_remote_id
        interface = MagicMock()
        interface.unique_remote_id.return_value = 'unique_id_2'
        PDA._get_configured_interface.return_value = interface

        # No existing DriverAgent
        PDA.equipment_tree.remotes = {}

        with patch('platform_driver.agent.DriverAgent') as MockDriverAgent:
            driver_agent_instance = MockDriverAgent.return_value

            # Call the method
            result = PDA._get_or_create_remote(equipment_name, remote_config, allow_duplicate_remotes)

            # Assertions
            PDA._get_configured_interface.assert_called_once_with(remote_config)
            interface.unique_remote_id.assert_called_once_with(equipment_name, remote_config)
            MockDriverAgent.assert_called_once_with(
                remote_config,
                PDA.core,
                PDA.equipment_tree,
                PDA.scalability_test,
                PDA.config.timezone,
                'unique_id_2',
                PDA.vip
            )
            # Check that the new driver agent is stored
            assert PDA.equipment_tree.remotes['unique_id_2'] == driver_agent_instance
            assert result == driver_agent_instance

    def test_get_or_create_remote_allow_duplicate_remotes_true(self, PDA):
        equipment_name = 'equipment3'
        remote_config = MagicMock()
        remote_config.driver_type = 'fake_driver'  # Set driver_type to a valid string
        allow_duplicate_remotes = True

        # Mock interface
        interface = MagicMock()
        PDA._get_configured_interface.return_value = interface

        # Mock BaseInterface.unique_remote_id and get_interface_subclass
        with patch('volttron.driver.base.interfaces.BaseInterface.unique_remote_id', return_value='unique_id_base'), \
                patch('platform_driver.agent.DriverAgent') as MockDriverAgent, \
                patch('volttron.driver.base.interfaces.BaseInterface.get_interface_subclass', return_value=MagicMock()):
            # Call the method
            result = PDA._get_or_create_remote(equipment_name, remote_config, allow_duplicate_remotes)

            # Assertions
            PDA._get_configured_interface.assert_called_once_with(remote_config)
            interface.unique_remote_id.assert_not_called()  # Should not be called when duplicates are allowed
            MockDriverAgent.assert_called_once_with(
                remote_config,
                PDA.core,
                PDA.equipment_tree,
                PDA.scalability_test,
                PDA.config.timezone,
                'unique_id_base',
                PDA.vip
            )
            # Check that the new driver agent is stored
            assert PDA.equipment_tree.remotes['unique_id_base'] == MockDriverAgent.return_value
            assert result == MockDriverAgent.return_value

    def test_get_or_create_remote_allow_duplicate_remotes_false_config_true(self, PDA):
        equipment_name = 'equipment4'
        remote_config = MagicMock()
        remote_config.driver_type = 'fake_driver'  # Set driver_type to a valid string
        allow_duplicate_remotes = False

        # PDA.config.allow_duplicate_remotes is True
        PDA.config.allow_duplicate_remotes = True

        # Mock interface
        interface = MagicMock()
        PDA._get_configured_interface.return_value = interface

        # Mock BaseInterface.unique_remote_id and get_interface_subclass
        with patch('volttron.driver.base.interfaces.BaseInterface.unique_remote_id',
                   return_value='unique_id_base_config_true'), \
                patch('platform_driver.agent.DriverAgent') as MockDriverAgent, \
                patch('volttron.driver.base.interfaces.BaseInterface.get_interface_subclass', return_value=MagicMock()):

            result = PDA._get_or_create_remote(equipment_name, remote_config, allow_duplicate_remotes)

            PDA._get_configured_interface.assert_called_once_with(remote_config)
            interface.unique_remote_id.assert_not_called()  # Should not be called when duplicates are allowed
            MockDriverAgent.assert_called_once_with(
                remote_config,
                PDA.core,
                PDA.equipment_tree,
                PDA.scalability_test,
                PDA.config.timezone,
                'unique_id_base_config_true',
                PDA.vip
            )
            # Check that the new driver agent is stored
            assert PDA.equipment_tree.remotes['unique_id_base_config_true'] == MockDriverAgent.return_value
            assert result == MockDriverAgent.return_value

class TestPDAGetConfiguredInterface:
    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.interface_classes = {}
        return PDA

    def test_get_configured_interface_cached(self, PDA):
        remote_config = MagicMock()
        remote_config.driver_type = 'driver_type_1'

        # Mock cached interface
        cached_interface = MagicMock()
        PDA.interface_classes['driver_type_1'] = cached_interface

        result = PDA._get_configured_interface(remote_config)

        assert result == cached_interface

    def test_get_configured_interface_not_cached_loads_successfully(self, PDA):
        remote_config = MagicMock()
        remote_config.driver_type = 'driver_type_2'
        remote_config.module = 'module_2'

        # No cached interface
        PDA.interface_classes = {}

        # Mock BaseInterface.get_interface_subclass
        with patch('platform_driver.agent.BaseInterface.get_interface_subclass') as mock_get_interface_subclass:
            loaded_interface = MagicMock()
            mock_get_interface_subclass.return_value = loaded_interface

            result = PDA._get_configured_interface(remote_config)

            mock_get_interface_subclass.assert_called_once_with('driver_type_2', 'module_2')
            # Check that the interface is cached
            assert PDA.interface_classes['driver_type_2'] == loaded_interface
            assert result == loaded_interface

class TestPDAUpdateEquipment:
    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.equipment_tree = MagicMock()
        PDA._separate_equipment_configs = MagicMock()
        PDA._get_or_create_remote = MagicMock()
        PDA.poll_schedulers = {}
        return PDA

    def test_update_equipment_device_config_present_update_successful(self, PDA):
        config_name = 'equipment1'
        contents = {'some': 'contents'}

        # Mock _separate_equipment_configs
        remote_config = MagicMock()
        dev_config = MagicMock()
        dev_config.allow_duplicate_remotes = False
        registry_config = MagicMock()
        PDA._separate_equipment_configs.return_value = (remote_config, dev_config, registry_config)

        # Mock _get_or_create_remote
        remote = MagicMock()
        PDA._get_or_create_remote.return_value = remote

        # Mock update_equipment
        PDA.equipment_tree.update_equipment.return_value = True

        # Mock points and poll_schedulers
        point1 = MagicMock()
        point1.identifier = 'point1'
        point2 = MagicMock()
        point2.identifier = 'point2'
        PDA.equipment_tree.points.return_value = [point1, point2]
        PDA.equipment_tree.get_group.side_effect = ['group1', 'group2']
        poll_scheduler1 = MagicMock()
        poll_scheduler2 = MagicMock()
        PDA.poll_schedulers = {'group1': poll_scheduler1, 'group2': poll_scheduler2}

        result = PDA._update_equipment(config_name, None, contents)

        PDA._separate_equipment_configs.assert_called_once_with(contents)
        PDA._get_or_create_remote.assert_called_once_with(config_name, remote_config, dev_config.allow_duplicate_remotes)
        PDA.equipment_tree.update_equipment.assert_called_once_with(config_name, dev_config, remote, registry_config)
        PDA.equipment_tree.points.assert_called_once_with(config_name)
        PDA.equipment_tree.get_group.assert_any_call('point1')
        PDA.equipment_tree.get_group.assert_any_call('point2')
        poll_scheduler1.check_for_reschedule.assert_called_once()
        poll_scheduler2.check_for_reschedule.assert_called_once()
        assert result == True

    def test_update_equipment_device_config_present_update_not_needed(self, PDA):
        config_name = 'equipment2'
        contents = {'some': 'contents'}

        # Mock _separate_equipment_configs
        remote_config = MagicMock()
        dev_config = MagicMock()
        dev_config.allow_duplicate_remotes = False
        registry_config = MagicMock()
        PDA._separate_equipment_configs.return_value = (remote_config, dev_config, registry_config)

        # Mock _get_or_create_remote
        remote = MagicMock()
        PDA._get_or_create_remote.return_value = remote

        # Mock update_equipment
        PDA.equipment_tree.update_equipment.return_value = False

        result = PDA._update_equipment(config_name, None, contents)

        PDA._separate_equipment_configs.assert_called_once_with(contents)
        PDA._get_or_create_remote.assert_called_once_with(config_name, remote_config, dev_config.allow_duplicate_remotes)
        PDA.equipment_tree.update_equipment.assert_called_once_with(config_name, dev_config, remote, registry_config)
        # Polling should not be rescheduled
        PDA.equipment_tree.points.assert_not_called()
        assert result == False

    def test_update_equipment_device_config_absent(self, PDA):
        config_name = 'equipment3'
        contents = {'some': 'contents'}

        # Mock _separate_equipment_configs
        remote_config = MagicMock()
        dev_config = None  # Device config absent
        registry_config = MagicMock()
        PDA._separate_equipment_configs.return_value = (remote_config, dev_config, registry_config)

        # Mock update_equipment
        PDA.equipment_tree.update_equipment.return_value = True

        # Mock points and poll_schedulers
        point1 = MagicMock()
        point1.identifier = 'point1'
        PDA.equipment_tree.points.return_value = [point1]
        PDA.equipment_tree.get_group.return_value = 'group1'
        poll_scheduler1 = MagicMock()
        PDA.poll_schedulers = {'group1': poll_scheduler1}

        result = PDA._update_equipment(config_name, None, contents)

        PDA._separate_equipment_configs.assert_called_once_with(contents)
        PDA._get_or_create_remote.assert_not_called()
        PDA.equipment_tree.update_equipment.assert_called_once_with(config_name, dev_config, None, registry_config)
        PDA.equipment_tree.points.assert_called_once_with(config_name)
        PDA.equipment_tree.get_group.assert_called_once_with('point1')
        poll_scheduler1.check_for_reschedule.assert_called_once()
        assert result == True

    def test_update_equipment_exception_during_remote_creation(self, PDA):
        config_name = 'equipment4'
        contents = {'some': 'contents'}

        # Mock _separate_equipment_configs
        remote_config = MagicMock()
        dev_config = MagicMock()
        dev_config.allow_duplicate_remotes = False
        registry_config = MagicMock()
        PDA._separate_equipment_configs.return_value = (remote_config, dev_config, registry_config)

        PDA._get_or_create_remote.side_effect = ValueError('Error message')

        with patch('platform_driver.agent._log') as mock_log:

            result = PDA._update_equipment(config_name, None, contents)

            PDA._separate_equipment_configs.assert_called_once_with(contents)
            PDA._get_or_create_remote.assert_called_once_with(config_name, remote_config, dev_config.allow_duplicate_remotes)
            PDA.equipment_tree.update_equipment.assert_not_called()
            mock_log.warning.assert_called_once_with(
                f'Skipping configuration of equipment: {config_name} after encountering error --- Error message'
            )
            assert result == False

    def test_update_equipment_allow_reschedule_false(self, PDA):
        config_name = 'equipment5'
        contents = {'some': 'contents'}

        # Mock _separate_equipment_configs
        remote_config = MagicMock()
        dev_config = MagicMock()
        dev_config.allow_duplicate_remotes = False
        registry_config = MagicMock()
        PDA._separate_equipment_configs.return_value = (remote_config, dev_config, registry_config)

        # Mock _get_or_create_remote
        remote = MagicMock()
        PDA._get_or_create_remote.return_value = remote

        # Mock update_equipment
        PDA.equipment_tree.update_equipment.return_value = True

        # Call the method with allow_reschedule=False
        result = PDA._update_equipment(config_name, None, contents, allow_reschedule=False)

        PDA._separate_equipment_configs.assert_called_once_with(contents)
        PDA._get_or_create_remote.assert_called_once_with(config_name, remote_config, dev_config.allow_duplicate_remotes)
        PDA.equipment_tree.update_equipment.assert_called_once_with(config_name, dev_config, remote, registry_config)
        # Polling should not be rescheduled
        PDA.equipment_tree.points.assert_not_called()
        assert result == True


class TestPlatformDriverAgentRemoveEquipment:
    """Tests for _remove_equipment."""
    # TODO wait for function to be fully finished
    pass


class TestPlatformDriverAgentSemanticQuery:
    """Tests for resolve_tags"""
    pass

    # @pytest.fixture
    # def PDA(self):
    #     agent = PlatformDriverAgent()
    #     agent.vip = MagicMock()
    #     return agent


class TestPlatformDriverAgentBuildQueryPlan:
    """Tests for build_query_plan"""

    @pytest.fixture
    def PDA(self):
        agent = PlatformDriverAgent()
        agent.vip = MagicMock()

        point_node_mock = MagicMock()
        point_node_mock.identifier = 'point1'
        driver_agent_mock = MagicMock()

        equipment_tree_mock = MagicMock()
        equipment_tree_mock.find_points = MagicMock(return_value=[point_node_mock])
        equipment_tree_mock.get_remote = MagicMock(return_value=driver_agent_mock)

        agent.equipment_tree = equipment_tree_mock

        agent.point_node_mock = point_node_mock
        agent.driver_agent_mock = driver_agent_mock

        return agent

    def test_find_points_called_correctly(self, PDA):
        """Tests find_points called with correct arguments"""
        PDA.build_query_plan(topic="topic")
        PDA.equipment_tree.find_points.assert_called_once()

    def test_get_remote_called_correctly(self, PDA):
        """Tests get_remote called with correct point identifier."""
        PDA.build_query_plan(topic="topic")
        PDA.equipment_tree.get_remote.assert_called_once_with('point1')

    def test_build_query_plan_result(self, PDA):
        """Tests build_query_plan returns correct result."""
        result = PDA.build_query_plan(topic="topic")

        expected_result = dict()
        expected_result[PDA.driver_agent_mock] = {PDA.point_node_mock}
        assert result == expected_result


class TestPlatformDriverAgentGet:
    """Tests for get."""

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.vip = MagicMock()
        PDA.equipment_tree = MagicMock()
        return PDA

    def test_get_no_points(self, PDA):
        """Test get method with no points in the query plan."""
        PDA.build_query_plan = MagicMock(return_value={})

        results, errors = PDA.get(topic=None, regex=None)

        assert results == {}
        assert errors == {}
        PDA.build_query_plan.assert_called_once_with(None, None)

    def test_get_with_node_not_found(self, PDA):
        """Test get method where a node is not found in the equipment tree"""
        remote_mock = MagicMock()
        point_mock = MagicMock(identifier="point")

        # Mock the build_query_plan to return a predefined query plan
        PDA.build_query_plan = MagicMock(return_value={remote_mock: {point_mock}})

        remote_mock.get_multiple_points.return_value = ({"point": "value"}, {"point_err": "error"})

        PDA.equipment_tree.get_node.return_value = None

        results, errors = PDA.get(topic="topic", regex="regex")

        assert results == {"point": "value"}
        assert errors == {"point_err": "error"}

        # Validate if methods were called with correct parameters
        PDA.build_query_plan.assert_called_once_with("topic", "regex")
        remote_mock.get_multiple_points.assert_called_once_with(["point"])


class TestPlatformDriverAgentSet:
    """Tests for set"""
    pass    # TODO wait for final additions


class TestPlatformDriverAgentRevert:
    """Tests for revert"""
    pass    # TODO wait for final additions


class TestPlatformDriverAgentLast:
    """Tests for Last"""

    @pytest.fixture
    def PDA(self):
        agent = PlatformDriverAgent()
        agent.vip = MagicMock()
        agent.equipment_tree = MagicMock()
        agent.poll_scheduler = MagicMock()
        return agent

    def test_last_default(self, PDA):
        """Test last method with default arguments."""
        point_mock = MagicMock(topic="point1",
                               last_value="value1",
                               last_updated="2023-01-01T00:00:00Z")
        PDA.equipment_tree.find_points.return_value = [point_mock]

        result = PDA.last(topic="topic")
        expected = {"point1": {"value": "value1", "updated": "2023-01-01T00:00:00Z"}}
        assert result == expected
        PDA.equipment_tree.find_points.assert_called_once_with("topic", None)


# class TestPlatformDriverAgentStart:
#     """Tests for Start"""
#
#     @pytest.fixture
#     def PDA(self):
#         agent = PlatformDriverAgent()
#         agent.vip = MagicMock()
#         agent.equipment_tree = MagicMock()
#         agent.poll_scheduler = MagicMock()
#         agent.config = MagicMock()
#         return agent
#
#     def test_start_no_points_found(self, PDA):
#         """Test start method with no matching points."""
#         PDA.equipment_tree.find_points.return_value = []
#
#         PDA.start(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         PDA.poll_scheduler.schedule.assert_not_called()
#         PDA.poll_scheduler.add_to_schedule.assert_not_called()
#
#     def test_start_points_already_active(self, PDA):
#         """Test start method where the points are already active."""
#         point_mock = MagicMock(topic="point1", active=True)
#         PDA.equipment_tree.find_points.return_value = [point_mock]
#
#         PDA.start(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert point_mock.active is True
#         PDA.poll_scheduler.schedule.assert_not_called()
#         PDA.poll_scheduler.add_to_schedule.assert_not_called()
#
#     def test_start_points_not_active_reschedule_allowed(self, PDA):
#         """Test start method where points are not active and rescheduling is allowed."""
#         PDA.config.allow_reschedule = True
#         point_mock = MagicMock(topic="point1", active=False)
#         PDA.equipment_tree.find_points.return_value = [point_mock]
#
#         PDA.start(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert point_mock.active is True
#         PDA.poll_scheduler.schedule.assert_called_once()
#         PDA.poll_scheduler.add_to_schedule.assert_not_called()
#
#     def test_start_points_not_active_reschedule_not_allowed(self, PDA):
#         """Test start method where points are not active and rescheduling is not allowed."""
#         PDA.config.allow_reschedule = False
#         point_mock = MagicMock(topic="point1", active=False)
#         PDA.equipment_tree.find_points.return_value = [point_mock]
#
#         PDA.start(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert point_mock.active is True
#         PDA.poll_scheduler.schedule.assert_not_called()
#         PDA.poll_scheduler.add_to_schedule.assert_called_once_with(point_mock)


# class TestPlatformDriverAgentStop:
#     """Tests for Stop"""
#
#     @pytest.fixture
#     def PDA(self):
#         agent = PlatformDriverAgent()
#         agent.vip = MagicMock()
#         agent.equipment_tree = MagicMock()
#         agent.poll_scheduler = MagicMock()
#         agent.config = MagicMock()
#         return agent
#
#     def test_stop_no_points_found(self, PDA):
#         """Test stop method with no matching points."""
#         PDA.equipment_tree.find_points.return_value = []
#
#         PDA.stop(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         PDA.poll_scheduler.schedule.assert_not_called()
#         PDA.poll_scheduler.remove_from_schedule.assert_not_called()
#
#     def test_stop_points_already_inactive(self, PDA):
#         """Test stop method where the points are already inactive."""
#         point_mock = MagicMock(topic="point1", active=False)
#         PDA.equipment_tree.find_points.return_value = [point_mock]
#
#         PDA.stop(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert point_mock.active is False
#         PDA.poll_scheduler.schedule.assert_not_called()
#         PDA.poll_scheduler.remove_from_schedule.assert_not_called()
#
#     def test_stop_points_active_reschedule_allowed(self, PDA):
#         """Test stop method where points are active and rescheduling is allowed."""
#         PDA.config.allow_reschedule = True
#         point_mock = MagicMock(topic="point1", active=True)
#         PDA.equipment_tree.find_points.return_value = [point_mock]
#
#         PDA.stop(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert point_mock.active is False
#         PDA.poll_scheduler.schedule.assert_called_once()
#         PDA.poll_scheduler.remove_from_schedule.assert_not_called()
#
#     def test_stop_points_active_reschedule_not_allowed(self, PDA):
#         """Test stop method where points are active and rescheduling is not allowed."""
#         PDA.config.allow_reschedule = False
#         point_mock = MagicMock(topic="point1", active=True)
#         PDA.equipment_tree.find_points.return_value = [point_mock]
#
#         PDA.stop(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert point_mock.active is False
#         PDA.poll_scheduler.schedule.assert_not_called()
#         PDA.poll_scheduler.remove_from_schedule.assert_called_once_with(point_mock)


# class TestPlatformDriverAgentEnable:
#
#     @pytest.fixture
#     def PDA(self):
#         agent = PlatformDriverAgent()
#         agent.vip = MagicMock()
#         agent.equipment_tree = MagicMock()
#         return agent
#
#     def test_enable_no_nodes_found(self, PDA):
#         """Test enable method with no matching nodes."""
#         PDA.equipment_tree.find_points.return_value = []
#
#         PDA.enable(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         PDA.vip.config.set.assert_not_called()
#         PDA.equipment_tree.get_device_node.assert_not_called()
#
#     def test_enable_non_point_nodes(self, PDA):
#         """Test enable method on non-point nodes without triggering callback."""
#         node_mock = MagicMock(is_point=False, topic="node1", config={})
#         PDA.equipment_tree.find_points.return_value = [node_mock]
#
#         PDA.enable(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert node_mock.config['active'] is True
#         PDA.vip.config.set.assert_called_once_with(node_mock.topic,
#                                                    node_mock.config,
#                                                    trigger_callback=False)
#         PDA.equipment_tree.get_device_node.assert_not_called()
#
#     def test_enable_point_nodes(self, PDA):
#         """Test enable method on point nodes and updating the registry."""
#         node_mock = MagicMock(is_point=True, topic="node1", config={}, identifier="node1_id")
#         device_node_mock = MagicMock()
#         PDA.equipment_tree.find_points.return_value = [node_mock]
#         PDA.equipment_tree.get_device_node.return_value = device_node_mock
#
#         PDA.enable(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert node_mock.config['active'] is True
#         PDA.equipment_tree.get_device_node.assert_called_once_with(node_mock.identifier)
#         device_node_mock.update_registry_row.assert_called_once_with(node_mock.config)
#         PDA.vip.config.set.assert_not_called()


# class TestPlatformDriverAgentDisable:
#     """ Tests for disable function"""
#
#     @pytest.fixture
#     def PDA(self):
#         agent = PlatformDriverAgent()
#         agent.vip = MagicMock()
#         agent.equipment_tree = MagicMock()
#         return agent
#
#     def test_disable_no_nodes_found(self, PDA):
#         """Test disable method with no matching nodes."""
#         PDA.equipment_tree.find_points.return_value = []
#
#         PDA.disable(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         PDA.vip.config.set.assert_not_called()
#         PDA.equipment_tree.get_device_node.assert_not_called()
#
#     def test_disable_non_point_nodes(self, PDA):
#         """Test disable method on non-point nodes without triggering callback."""
#         node_mock = MagicMock(is_point=False, topic="node1", config={})
#         PDA.equipment_tree.find_points.return_value = [node_mock]
#
#         PDA.disable(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert node_mock.config['active'] is False
#         PDA.vip.config.set.assert_called_once_with(node_mock.topic,
#                                                    node_mock.config,
#                                                    trigger_callback=False)
#         PDA.equipment_tree.get_device_node.assert_not_called()
#
#     def test_disable_point_nodes(self, PDA):
#         """Test disable method on point nodes and updating the registry."""
#         node_mock = MagicMock(is_point=True, topic="node1", config={}, identifier="node1_id")
#         device_node_mock = MagicMock()
#         PDA.equipment_tree.find_points.return_value = [node_mock]
#         PDA.equipment_tree.get_device_node.return_value = device_node_mock
#
#         PDA.disable(topic="topic")
#
#         PDA.equipment_tree.find_points.assert_called_once_with("topic", None, None)
#         assert node_mock.config['active'] is False
#         PDA.equipment_tree.get_device_node.assert_called_once_with(node_mock.identifier)
#         device_node_mock.update_registry_row.assert_called_once_with(node_mock.config)
#         PDA.vip.config.set.assert_not_called()


# class TestPlatformDriverAgentNewReservation:
#     """ Tests for new reservation """
#
#     @pytest.fixture
#     def PDA(self):
#         agent = PlatformDriverAgent()
#         agent.vip = MagicMock()
#         agent.reservation_manager = MagicMock()
#         agent.vip.rpc.context.vip_message.peer = "test.agent"
#
#         return agent
#
#     def test_new_reservation(self, PDA):
#         PDA.new_reservation(task_id="task1", priority="LOW", requests=[])
#
#         PDA.reservation_manager.new_reservation.assert_called_once_with("test.agent",
#                                                                         "task1",
#                                                                         "LOW", [],
#                                                                         publish_result=False)


class TestGetPoint:
    sender = "test.agent"
    path = "devices/device1"
    point_name = "SampleWritableFloat1"
    value = 0.2

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        # Mock 'vip' components
        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        # Mock _equipment_id
        PDA._equipment_id = Mock(return_value="processed_point_name")

        # Mock 'equipment_tree.get_node'
        node_mock = MagicMock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.get_node = Mock(return_value=node_mock)

        # Mock other methods called in set_point
        node_mock.get_remote = Mock(return_value=Mock())
        PDA.equipment_tree.raise_on_locks = Mock()
        PDA._get_headers = Mock(return_value={})
        PDA._push_result_topic_pair = Mock()

        return PDA

    def test_get_point_calls_equipment_id_with_correct_parameters(self, PDA):
        """Test get_point calls equipment_id method with correct parameters."""
        PDA.get_point(path='device/topic', point_name='SampleWritableFloat', kwargs={})
        # Assert that self._equipment_id was called with the correct arguments
        PDA._equipment_id.assert_called_with("device/topic", "SampleWritableFloat")

    def test_get_point_with_topic_kwarg(self, PDA):
        """Test handling of 'topic' as keyword arg"""
        kwargs = {'topic': 'device/topic'}
        PDA.get_point(path=None, point_name=None, **kwargs)
        PDA._equipment_id.assert_called_with('device/topic', None)

    def test_get_point_with_point_kwarg(self, PDA):
        """ Test handling of 'point' keyword arg """
        kwargs = {'point': 'SampleWritableFloat'}
        PDA.get_point(path='device/topic', point_name=None, **kwargs)
        PDA._equipment_id.assert_called_with('device/topic', 'SampleWritableFloat')

    def test_get_point_with_combined_path_and_empty_point(self, PDA):
        """Test handling of path containing the point name and point_name is empty"""
        kwargs = {}
        PDA.get_point(path='device/topic/SampleWritableFloat', point_name=None, **kwargs)
        PDA._equipment_id.assert_called_with("device/topic/SampleWritableFloat", None)

    def test_get_point_raises_error_for_invalid_node(self, PDA):
        """Test get_point raises error when node is invalid"""
        PDA.equipment_tree.get_node.return_value = None
        kwargs = {}
        with pytest.raises(ValueError, match="No equipment found for topic: processed_point_name"):
            PDA.get_point(path='device/topic', point_name='SampleWritableFloat', **kwargs)

    # def test_get_point_raises_error_for_invalid_remote(self, PDA):
    #     """Test get_point raises error when remote is invalid"""
    #     # Ensure get_node returns a valid node mock
    #     node_mock = Mock()
    #     node_mock.get_remote = Mock(return_value=None)
    #     PDA.equipment_tree.get_node = Mock(return_value=node_mock)
    #
    #     kwargs = {}
    #
    #     with pytest.raises(ValueError, match="No remote found for topic: processed_point_name"):
    #         PDA.get_point(path='device/topic', point_name='SampleWritableFloat', **kwargs)

    def test_get_point_with_kwargs_as_topic_point(self, PDA):
        """Test handling of old actuator-style arguments"""

        kwargs = {'topic': 'device/topic', 'point': 'SampleWritableFloat'}

        PDA.get_point(path=None, point_name=None, **kwargs)

        PDA._equipment_id.assert_called_with('device/topic', 'SampleWritableFloat')

    def test_get_point_old_style_call(self, PDA):
        """Test get point with old actuator style call"""
        kwargs = {}
        PDA.get_point(topic='device/topic', point="SampleWritableFloat", **kwargs)
        PDA._equipment_id.assert_called_with("device/topic", "SampleWritableFloat")

    def test_get_point_old_style_call_with_kwargs(self, PDA):
        """Test get point with old actuator style call and with kwargs"""
        kwargs = {"random_thing": "test"}
        PDA.get_point(topic='device/topic', point="SampleWritableFloat", **kwargs)
        PDA._equipment_id.assert_called_with("device/topic", "SampleWritableFloat")


class TestSetPoint:
    sender = "test.agent"
    path = "devices/device1"
    point_name = "SampleWritableFloat1"
    value = 0.2

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        # Mock 'vip' components
        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        # Mock _equipment_id
        PDA._equipment_id = Mock(return_value="processed_point_name")

        # Mock 'equipment_tree.get_node'
        node_mock = MagicMock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.get_node = Mock(return_value=node_mock)

        # Mock other methods called in set_point
        node_mock.get_remote = Mock(return_value=Mock())
        PDA.equipment_tree.raise_on_locks = Mock()
        PDA._get_headers = Mock(return_value={})
        PDA._push_result_topic_pair = Mock()

        return PDA

    def test_set_point_calls_equipment_id_with_correct_parameters(self, PDA):
        """Test set_point calls equipment_id method with correct parameters."""
        PDA.set_point(path='device/topic', point_name='SampleWritableFloat', value=42, kwargs={})
        # Assert that self._equipment_id was called with the correct arguments
        PDA._equipment_id.assert_called_with("device/topic", "SampleWritableFloat")

    # def test_set_point_with_topic_kwarg(self, PDA):
    #     """Test handling of 'topic' as keyword arg"""
    #     kwargs = {'device/topic'}
    #     PDA.set_point(path='ignored_path', point_name=None, value=42, **kwargs)
    #     PDA._equipment_id.assert_called_with('device/topic', None)

    def test_set_point_with_point_kwarg(self, PDA):
        """ Test handling of 'point' keyword arg """
        kwargs = {'point': 'SampleWritableFloat'}
        PDA.set_point(path='device/topic', point_name=None, value=42, **kwargs)
        PDA._equipment_id.assert_called_with('device/topic', 'SampleWritableFloat')

    def test_set_point_with_combined_path_and_empty_point(self, PDA):
        """Test handling of path containing the point name and point_name is empty"""
        kwargs = {}
        PDA.set_point(path='device/topic/SampleWritableFloat', point_name=None, value=42, **kwargs)
        PDA._equipment_id.assert_called_with("device/topic/SampleWritableFloat", None)

    def test_set_point_raises_error_for_invalid_node(self, PDA):
        """Tests that setpoint raises a ValueError exception"""
        # Mock get_node to return None
        PDA.equipment_tree.get_node.return_value = None
        kwargs = {}

        # Call the set_point function and check for ValueError
        with pytest.raises(ValueError, match="No equipment found for topic: processed_point_name"):
            PDA.set_point(path='device/topic',
                          point_name='SampleWritableFloat',
                          value=42,
                          **kwargs)

    def test_set_point_deprecated(self, PDA):
        """Test old style actuator call"""
        PDA.set_point("device/topic", 'SampleWritableFloat', 42)
        PDA._equipment_id.assert_called_with("device/topic", "SampleWritableFloat")


class TestGetMultiplePoints:
    sender = "test.agent"

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        PDA._equipment_id = Mock(side_effect={'device1/point2', 'device1/point1'}, )

        PDA.get = Mock(return_value=({}, {}))

        return PDA

    def test_get_multiple_points_with_single_path(self, PDA):
        """Test get_multiple_points with a single path"""
        PDA.get_multiple_points(path='device1')
        PDA.get.assert_called_once_with({'device1'})
        PDA._equipment_id.assert_not_called()

    def test_get_multiple_points_with_single_path_and_point_names(self, PDA):
        """Test get_multiple_points with a single path and point names."""
        PDA.get_multiple_points(path='device1', point_names=['point1', 'point2'])
        PDA._equipment_id.assert_any_call('device1', 'point1')
        PDA._equipment_id.assert_any_call('device1', 'point2')
        PDA.get.assert_called_once_with({'device1/point1', 'device1/point2'})

    def test_get_multiple_points_with_none_path(self, PDA):
        """Test get_multiple_points with None path."""
        with pytest.raises(TypeError, match='Argument "path" is required.'):
            PDA.get_multiple_points(path=None)

        PDA.get.assert_not_called()


class TestSetMultiplePoints:
    sender = "test.agent"

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        PDA._equipment_id = Mock(
            side_effect=['device1/point1', 'device1/point2', 'device2/point1'])

        PDA.set = Mock(return_value=(None, {}))

        return PDA

    def test_set_multiple_points_with_single_path(self, PDA):
        """Test set_multiple_points with a single path and point names/values"""
        point_names_values = [('point1', 100), ('point2', 200)]
        PDA.set_multiple_points(path='device1', point_names_values=point_names_values)
        PDA.set.assert_called_once_with({
            'device1/point1': 100,
            'device1/point2': 200
        },
                                        map_points=True)
        PDA._equipment_id.assert_any_call('device1', 'point1')
        PDA._equipment_id.assert_any_call('device1', 'point2')

    def test_set_multiple_points_with_missing_path(self, PDA):
        """Test set_multiple_points without providing the path"""
        point_names_values = [('point1', 100), ('point2', 200)]
        with pytest.raises(TypeError, match='missing 1 required positional argument'):
            PDA.set_multiple_points(point_names_values=point_names_values)
        PDA.set.assert_not_called()

    def test_set_multiple_points_with_additional_kwargs(self, PDA):
        """Test set_multiple_points with additional kwargs"""
        point_names_values = [('point1', 100), ('point2', 200)]
        additional_kwargs = {'some_key': 'some_value'}
        PDA.set_multiple_points(path='device1',
                                point_names_values=point_names_values,
                                **additional_kwargs)
        PDA.set.assert_called_once_with({
            'device1/point1': 100,
            'device1/point2': 200
        },
                                        map_points=True,
                                        some_key='some_value')
        PDA._equipment_id.assert_any_call('device1', 'point1')
        PDA._equipment_id.assert_any_call('device1', 'point2')

    def test_set_multiple_with_old_style_args(self, PDA):
        result = PDA.set_multiple_points(path="some/path",
                                         point_names_values=[('point1', 100), ('point2', 200)])
        assert result == {}    # returns no errors with old style args


class TestRevertPoint:
    sender = "test.agent"
    path = "devices/device1"
    point_name = "SampleWritableFloat1"

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        # Mock 'vip' components
        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        # Mock _equipment_id
        PDA._equipment_id = Mock(return_value="devices/device1/SampleWritableFloat1")

        # Mock 'equipment_tree.get_node'
        node_mock = MagicMock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.get_node = Mock(return_value=node_mock)

        # Mock other methods called in revert_point
        node_mock.get_remote = Mock(return_value=Mock())
        PDA.equipment_tree.raise_on_locks = Mock()
        PDA._get_headers = Mock(return_value={})
        PDA._push_result_topic_pair = Mock()

        return PDA

    def test_revert_point_normal_case(self, PDA):
        """Test normal case for reverting a point."""
        PDA.revert_point(self.path, self.point_name)

        PDA._equipment_id.assert_called_with(self.path, 'SampleWritableFloat1')
        PDA.equipment_tree.get_node.assert_called_once()


class TestRevertDevice:
    sender = "test.agent"
    path = "devices/device1"

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        PDA._equipment_id = Mock(return_value="devices/device1")

        node_mock = MagicMock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.get_node = Mock(return_value=node_mock)

        remote_mock = Mock()
        node_mock.get_remote = Mock(return_value=remote_mock)

        PDA.equipment_tree.raise_on_locks = Mock()
        PDA._get_headers = Mock(return_value={})
        PDA._push_result_topic_pair = Mock()

        return PDA

    def test_revert_device_normal_case(self, PDA):
        """Test normal case for reverting a device"""
        PDA.revert_device(self.path)

        PDA._equipment_id.assert_called_with(self.path, None)
        PDA._push_result_topic_pair.assert_called()

    def test_revert_device_actuator_style(self, PDA):
        """Test old actuator-style arguments """
        PDA.revert_device(self.sender, self.path)

        PDA._equipment_id.assert_called_with(self.path, None)
        PDA._push_result_topic_pair.assert_called()

class TestHandleGet:
    sender = "test.agent"
    topic = "devices/actuators/get/device1/SampleWritableFloat1"

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()

        # Mock 'vip' components
        PDA.vip = MagicMock()
        PDA.vip.rpc.context = MagicMock()
        PDA.vip.rpc.context.vip_message.peer = self.sender

        PDA._equipment_id = Mock(return_value="processed_point_name")

        # Mock 'equipment_tree.get_node'
        node_mock = MagicMock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.get_node = Mock(return_value=node_mock)

        PDA._get_headers = Mock(return_value={})
        PDA._push_result_topic_pair = Mock()

        PDA.get_point = Mock()
        PDA.get_point.return_value = 42.0
        PDA._push_result_topic_pair = Mock()

        return PDA

    def test_handle_get_calls_get_point_with_correct_parameters(self, PDA):
        """Test handle_get calls get_point with correct parameters."""
        PDA.handle_get(None, self.sender, None, self.topic, None, None)
        PDA.get_point.assert_called_with("device1/SampleWritableFloat1")

    def test_handle_get_calls__push_result_topic_pair_with_correct_parameters(self, PDA):
        """Test handle_get calls push_result_topic_pair with correct values """
        PDA.handle_get(None, self.sender, None, self.topic, None, None)
        PDA._push_result_topic_pair.assert_called_with(VALUE_RESPONSE_PREFIX,
                                                       "device1/SampleWritableFloat1", {}, 42.0)


class TestHandleSet:
    sender = "test.sender"
    topic = "devices/actuators/set/device1/point1"
    message = 10

    @pytest.fixture
    def PDA(self):
        agent = PlatformDriverAgent()

        agent._get_headers = Mock(return_value={})
        agent._push_result_topic_pair = Mock()
        agent.set_point = Mock()
        agent._handle_error = Mock()

        return agent

    def test_handle_set_valid_message(self, PDA):
        """Test setting a point with a valid message"""
        pass
        # rewrite
        # PDA.handle_set(None, self.sender, None, self.topic, None, self.message)
        #
        # point = self.topic.replace("devices/actuators/set/", "", 1)
        #
        # # PDA.set_point.assert_called_once()
        # # PDA._push_result_topic_pair.assert_not_called()
        # # PDA._handle_error.assert_not_called()

    def test_handle_set_empty_message(self, PDA):
        """Test handling of an empty message """
        PDA.handle_set(None, self.sender, None, self.topic, None, None)

        point = self.topic.replace("devices/actuators/set/", "", 1)
        headers = PDA._get_headers(self.sender)
        error = {'type': 'ValueError', 'value': 'missing argument'}

        PDA._push_result_topic_pair.assert_called_with("devices/actuators/error", point, headers,
                                                       error)
        PDA.set_point.assert_not_called()
        PDA._handle_error.assert_not_called()


class TestHandleRevertPoint:
    sender = "test.sender"
    topic = "actuators/revert/point/device1/point1"

    @pytest.fixture
    def PDA(self):
        agent = PlatformDriverAgent()

        agent._get_headers = Mock(return_value={})
        agent._push_result_topic_pair = Mock()
        agent._handle_error = Mock()

        # Mock equipment tree
        mock_node = Mock()
        mock_remote = Mock()
        mock_node.get_remote.return_value = mock_remote
        equipment_tree_mock = Mock()
        equipment_tree_mock.get_node.return_value = mock_node
        equipment_tree_mock.root = 'devices'

        agent.equipment_tree = equipment_tree_mock

        return agent, mock_node, mock_remote

    def test_handle_revert_point_success(self, PDA):
        """Test reverting a point successfully."""
        agent_instance, mock_node, mock_remote = PDA
        agent_instance.handle_revert_point(None, self.sender, None, self.topic, None, None)

        expected_topic = "devices/actuators/revert/point/device1/point1"
        headers = agent_instance._get_headers(self.sender)

        agent_instance.equipment_tree.get_node.assert_called_with(expected_topic)
        agent_instance.equipment_tree.raise_on_locks.assert_called_with(mock_node, self.sender)
        agent_instance._push_result_topic_pair.assert_called_with(
            "devices/actuators/reverted/point", expected_topic, headers, None)

    def test_handle_revert_point_exception(self, PDA):
        """Test handling exception during revert process."""
        agent_instance, mock_node, mock_remote = PDA
        exception = Exception("test exception")
        agent_instance.equipment_tree.get_node.side_effect = exception
        agent_instance.handle_revert_point(None, self.sender, None, self.topic, None, None)

        expected_topic = "devices/actuators/revert/point/device1/point1"
        headers = agent_instance._get_headers(self.sender)

        agent_instance.equipment_tree.get_node.assert_called_with(expected_topic)
        agent_instance._handle_error.assert_called_with(exception, expected_topic, headers)


# class TestHandleRevertDevice:
#     sender = "test.sender"
#     topic = "devices/actuators/revert/device/device1"
#
#     @pytest.fixture
#     def PDA(self):
#         agent = PlatformDriverAgent()
#
#         agent._get_headers = Mock(return_value={})
#         agent._push_result_topic_pair = Mock()
#         agent._handle_error = Mock()
#
#         mock_node = Mock()
#         mock_remote = Mock()
#         mock_node.get_remote.return_value = mock_remote
#         equipment_tree_mock = Mock()
#         equipment_tree_mock.get_node.return_value = mock_node
#         equipment_tree_mock.root = 'devices'
#
#         agent.equipment_tree = equipment_tree_mock
#
#         return agent, mock_node, mock_remote
#
#     def test_handle_revert_device_success(self, PDA):
#         """Test reverting a device successfully."""
#         agent, mock_node, mock_remote = PDA
#         agent.handle_revert_device(None, self.sender, None, self.topic, None, None)
#
#         expected_topic = "devices/device1"
#         headers = agent._get_headers(self.sender)
#
#         agent.equipment_tree.get_node.assert_called_with(expected_topic)
#         agent.equipment_tree.raise_on_locks.assert_called_with(mock_node, self.sender)
#         mock_remote.revert_all.assert_called_once()
#         agent._push_result_topic_pair.assert_called_with("devices/actuators/reverted/device",
#                                                          expected_topic, headers, None)
#         agent._handle_error.assert_not_called()
#
#     def test_handle_revert_device_exception(self, PDA):
#         """Test handling exception during revert process """
#         agent_instance, mock_node, mock_remote = PDA
#         exception = Exception("test exception")
#         agent_instance.equipment_tree.get_node.side_effect = exception
#         agent_instance.handle_revert_device(None, self.sender, None, self.topic, None, None)
#
#         expected_topic = "devices/device1"
#         headers = agent_instance._get_headers(self.sender)
#
#         agent_instance.equipment_tree.get_node.assert_called_with(expected_topic)
#         agent_instance._handle_error.assert_called_with(exception, expected_topic, headers)


# class TestHandleReservationRequest:
#
#     @pytest.fixture
#     def PDA(self):
#         PDA = PlatformDriverAgent()
#
#         # Mock dependencies
#         PDA.vip = MagicMock()
#         PDA.vip.pubsub.publish = MagicMock()
#         PDA._get_headers = Mock()
#         PDA.reservation_manager = Mock()
#         PDA._handle_unknown_reservation_error = Mock()
#         PDA.reservation_manager.cancel_reservation = Mock()
#
#         return PDA
#
#     def test_handle_reservation_request_calls_publish_pubsub(self, PDA):
#         """Tests that it calls pubsub.publish when result type is new reservation"""
#         headers = {'type': 'NEW_RESERVATION', 'taskID': 'task1', 'priority': 1}
#         message = ['request1']
#
#         result = Mock()
#         result.success = True
#         result.data = {}
#         result.info_string = ''
#
#         PDA._get_headers.return_value = {}
#         PDA.reservation_manager.new_task.return_value = result
#
#         PDA.handle_reservation_request(None, 'sender', None, 'topic', headers, message)
#
#         PDA.vip.pubsub.publish.assert_called_with('pubsub',
#                                                   topic=RESERVATION_RESULT_TOPIC,
#                                                   headers={},
#                                                   message={
#                                                       'result': 'SUCCESS',
#                                                       'data': {},
#                                                       'info': ''
#                                                   })
#
#     def test_handle_reservation_reservation_action_cancel(self, PDA):
#         """Tests that it calls pubsub.publish when result type is cancel reservation"""
#         headers = {'type': 'CANCEL_RESERVATION', 'taskID': 'task1', 'priority': 1}
#         message = ['request1']
#
#         result = Mock()
#         result.success = True
#         result.data = {}
#         result.info_string = ''
#
#         PDA._get_headers.return_value = {}
#         PDA.reservation_manager.cancel_reservation.return_value = result
#
#         PDA.handle_reservation_request(None, 'sender', None, 'topic', headers, message)
#
#         PDA.vip.pubsub.publish.assert_called_with('pubsub',
#                                                   topic=RESERVATION_RESULT_TOPIC,
#                                                   headers={},
#                                                   message={
#                                                       'result': 'SUCCESS',
#                                                       'data': {},
#                                                       'info': ''
#                                                   })
#
#     def test_handle_reservation_request_calls_publish_pubsub(self, PDA):
#         """Tests that it calls pubsub.publish when new_task result responds with failed"""
#         headers = {'type': 'NEW_RESERVATION', 'taskID': 'task1', 'priority': 1}
#         message = ['request1']
#
#         result = Mock()
#         result.success = False
#         result.data = {}
#         result.info_string = ''
#
#         PDA._get_headers.return_value = {}
#         PDA.reservation_manager.new_task.return_value = result
#
#         PDA.handle_reservation_request(None, 'sender', None, 'topic', headers, message)
#
#         PDA.vip.pubsub.publish.assert_called_with('pubsub',
#                                                   topic=RESERVATION_RESULT_TOPIC,
#                                                   headers={},
#                                                   message={
#                                                       'result': 'FAILURE',
#                                                       'data': {},
#                                                       'info': ''
#                                                   })


class TestEquipmentId:
    """ Tests for _equipment_id in the PlatFromDriveragent class"""

    @pytest.fixture
    def PDA(self):
        """Fixture to set up a PlatformDriverAgent with a mocked equipment_tree."""
        agent = PlatformDriverAgent()
        agent.equipment_tree = Mock()
        agent.equipment_tree.root = "devices"
        return agent

    def test_equipment_id_basic(self, PDA):
        """Normal call"""
        result = PDA._equipment_id("some/path", "point")
        assert result == "devices/some/path/point"

    def test_equipment_id_no_point(self, PDA):
        """Tests calling equipment_id with no point."""
        result = PDA._equipment_id("some/path")
        assert result == "devices/some/path"

    def test_equipment_id_leading_trailing_slashes(self, PDA):
        """Tests calling equipment_id with leading and trailing slashes."""
        result = PDA._equipment_id("/some/path/", "point")
        assert result == "devices/some/path/point"

    def test_equipment_id_no_point_leading_trailing_slashes(self, PDA):
        """Tests calling equipment_id with leading and trailing slashes and no point"""
        result = PDA._equipment_id("/some/path/")
        assert result == "devices/some/path"

    def test_equipment_id_path_with_root(self, PDA):
        """Tests calling equipment_id with root in a path."""
        result = PDA._equipment_id("devices/some/path", "point")
        assert result == "devices/some/path/point"

    def test_equipment_id_path_with_root_no_point(self, PDA):
        """Tests calling equipment_id with root and no point"""
        result = PDA._equipment_id("devices/some/path")
        assert result == "devices/some/path"

    def test_equipment_id_only_path(self, PDA):
        """Tests calling equipment_id with only path, no point or root"""
        result = PDA._equipment_id("some/path")
        assert result == "devices/some/path"


class TestGetHeaders:
    """Tests for _get_headers in the PlatformDriverAgent class."""
    now = get_aware_utc_now()

    def test_get_headers_no_optional(self):
        """Tests _get_headers with time as now"""
        formatted_now = format_timestamp(self.now)
        result = PlatformDriverAgent()._get_headers(requester="test_requester", time=self.now)
        assert result == {'time': formatted_now, 'requesterID': "test_requester", 'type': None}

    def test_get_headers_with_time(self):
        custom_time = datetime(2024, 7, 25, 18, 52, 29, 37938)
        formatted_custom_time = format_timestamp(custom_time)
        result = PlatformDriverAgent()._get_headers("test_requester", time=custom_time)
        assert result == {
            'time': formatted_custom_time,
            'requesterID': "test_requester",
            'type': None
        }

    def test_get_headers_with_task_id(self):
        task_id = "task123"
        formatted_now = format_timestamp(self.now)
        result = PlatformDriverAgent()._get_headers(requester="test_requester",
                                                    time=self.now,
                                                    task_id=task_id)
        assert result == {
            'time': formatted_now,
            'requesterID': "test_requester",
            'taskID': task_id,
            'type': None
        }

    def test_get_headers_with_action_type(self):
        action_type = "NEW_SCHEDULE"
        formatted_now = format_timestamp(self.now)
        result = PlatformDriverAgent()._get_headers(requester="test_requester",
                                                    time=self.now,
                                                    action_type=action_type)
        assert result == {
            'time': formatted_now,
            'requesterID': "test_requester",
            'type': action_type
        }

    def test_get_headers_all_optional(self):
        custom_time = datetime(2024, 7, 25, 18, 52, 29, 37938)
        formatted_custom_time = format_timestamp(custom_time)
        task_id = "task123"
        action_type = "NEW_SCHEDULE"
        result = PlatformDriverAgent()._get_headers(requester="test_requester",
                                                    time=custom_time,
                                                    task_id=task_id,
                                                    action_type=action_type)
        assert result == {
            'time': formatted_custom_time,
            'requesterID': "test_requester",
            'taskID': task_id,
            'type': action_type
        }


if __name__ == '__main__':
    pytest.main()
