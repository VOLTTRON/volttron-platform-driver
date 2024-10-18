import pytest
from unittest.mock import MagicMock, Mock, patch, call
from datetime import datetime
import gevent
from typing import Set, Dict

from volttron.utils import format_timestamp, get_aware_utc_now
from platform_driver.agent import PlatformDriverAgent, PlatformDriverConfig, STATUS_BAD, RemoteConfig, DeviceConfig, \
    PointConfig, DriverAgent, PointNode
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
            'max_open_sockets': 'invalid',    # should be an int
            'max_concurrent_publishes': 10,
            'scalability_test': False,
            'remote_heartbeat_interval': 30,
            'reservation_preempt_grace_time': 60
        }

        result = PDA._load_agent_config(invalid_config)

        assert isinstance(result, PlatformDriverConfig)
        # Check that 'invalid' is not kept in the config
        assert result.max_open_sockets != 'invalid'
        assert any('Validation of platform driver configuration file failed. Using default values.'
                   in message for message in caplog.text.splitlines())
        # Ensure health status was set to bad
        PDA.vip.health.set_status.assert_called_once()
        status_args = PDA.vip.health.set_status.call_args[0]
        assert status_args[0] == STATUS_BAD
        assert 'Error processing configuration' in status_args[1]

    def test_load_agent_config_with_invalid_config_agent_not_connected(self, PDA, caplog):
        PDA.core.connected = False
        # invalid configuration dictionary
        invalid_config = {
            'max_open_sockets': 'invalid',    # Should be an int
            'max_concurrent_publishes': 10,
            'scalability_test': False,
            'remote_heartbeat_interval': 30,
            'reservation_preempt_grace_time': 60
        }
        result = PDA._load_agent_config(invalid_config)
        assert isinstance(result, PlatformDriverConfig)
        assert any('Validation of platform driver configuration file failed. Using default values.'
                   in message for message in caplog.text.splitlines())
        # make sure health status was not set since agent is not connected
        PDA.vip.health.set_status.assert_not_called()


class TestPDAConfigureMain:

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.core = MagicMock()
        PDA.vip = MagicMock()
        PDA.vip.config.get = Mock(return_value='{}')

        # Set initial config using MagicMock for attribute access
        PDA.config = MagicMock()
        PDA.config.max_open_sockets = 100
        PDA.config.max_concurrent_publishes = 5
        PDA.config.scalability_test = False
        PDA.config.scalability_test_iterations = 10
        PDA.config.reservation_preempt_grace_time = 5
        PDA.config.reservation_publish_interval = 30
        PDA.config.remote_heartbeat_interval = 60

        # Set PDA.config.copy() to return a copy of the config
        def copy_config():
            copied_config = MagicMock()
            copied_config.max_open_sockets = PDA.config.max_open_sockets
            copied_config.max_concurrent_publishes = PDA.config.max_concurrent_publishes
            copied_config.scalability_test = PDA.config.scalability_test
            copied_config.scalability_test_iterations = PDA.config.scalability_test_iterations
            copied_config.reservation_preempt_grace_time = PDA.config.reservation_preempt_grace_time
            copied_config.reservation_publish_interval = PDA.config.reservation_publish_interval
            copied_config.remote_heartbeat_interval = PDA.config.remote_heartbeat_interval
            return copied_config

        PDA.config.copy = copy_config

        return PDA

    def test_configure_main_new_action(self, PDA):
        """Tests configure main when action is "NEW"""
        # Arrange
        new_config = MagicMock()
        new_config.max_open_sockets = 100
        new_config.max_concurrent_publishes = 10
        new_config.scalability_test = False
        new_config.scalability_test_iterations = 10
        new_config.reservation_preempt_grace_time = 5
        new_config.reservation_publish_interval = 30
        new_config.remote_heartbeat_interval = 60

        PDA._load_agent_config = Mock(return_value=new_config)

        # Mock dependencies
        with patch('platform_driver.agent.EquipmentTree') as mock_equipment_tree_class, \
             patch('platform_driver.agent.setup_socket_lock') as mock_setup_socket_lock, \
             patch('platform_driver.agent.configure_publish_lock') as mock_configure_publish_lock, \
             patch('platform_driver.agent.OverrideManager') as mock_override_manager_class, \
             patch('platform_driver.agent.ReservationManager') as mock_reservation_manager_class, \
             patch('platform_driver.agent.get_aware_utc_now', return_value='now'):

            # Act
            PDA.configure_main(_="", action="NEW", contents={})

            # Assert
            PDA._load_agent_config.assert_called_once_with({})
            assert PDA.config == new_config
            mock_equipment_tree_class.assert_called_once_with(PDA)
            mock_setup_socket_lock.assert_called_once_with(new_config.max_open_sockets)
            mock_configure_publish_lock.assert_called_once_with(
                new_config.max_concurrent_publishes)
            mock_override_manager_class.assert_called_once_with(PDA)
            mock_reservation_manager_class.assert_called_once_with(
                PDA, new_config.reservation_preempt_grace_time, 'now')
            PDA.reservation_manager.update.assert_called_once_with('now')
            # Check that heartbeat_greenlet is set up
            PDA.core.periodic.assert_called_once_with(new_config.remote_heartbeat_interval,
                                                      PDA.heart_beat)

    def test_configure_main_update_action(self, PDA):
        """tests when action is update"""
        # Arrange
        new_config = MagicMock()
        new_config.max_open_sockets = 200    # Different to trigger log message
        new_config.max_concurrent_publishes = 15    # Different to trigger log message
        new_config.scalability_test = False
        new_config.scalability_test_iterations = 20
        new_config.reservation_preempt_grace_time = 10
        new_config.reservation_publish_interval = 30
        new_config.remote_heartbeat_interval = 60

        PDA._load_agent_config = Mock(return_value=new_config)

        PDA.override_manager = Mock()
        PDA.reservation_manager = Mock()

        with patch('platform_driver.agent._log') as mock_log:
            # Act
            PDA.configure_main(_="", action="UPDATE", contents={})

            # Assert
            mock_log.info.assert_any_call('Updated configuration received for Platform Driver.')
            mock_log.info.assert_any_call(
                'Restart Platform Driver for changes to the max_open_sockets setting to take effect'
            )
            mock_log.info.assert_any_call(
                'Restart Platform Driver for changes to the max_concurrent_publishes setting to take effect'
            )

            # Check that new_config attributes are reverted to old values
            assert new_config.max_open_sockets == PDA.config.max_open_sockets
            assert new_config.max_concurrent_publishes == PDA.config.max_concurrent_publishes

            # Since scalability_test is False, config should be updated
            assert PDA.config == new_config

            # Verify that reservation_manager's grace period is updated
            PDA.reservation_manager.set_grace_period.assert_called_once_with(
                new_config.reservation_preempt_grace_time)

    def test_configure_main_creates_override_manager(self, PDA):
        """Tests configure main created override manager"""
        # Arrange
        new_config = MagicMock()
        new_config.max_open_sockets = 100
        new_config.max_concurrent_publishes = 10
        new_config.scalability_test = False
        new_config.scalability_test_iterations = 10
        new_config.reservation_preempt_grace_time = 5
        new_config.reservation_publish_interval = 30
        new_config.remote_heartbeat_interval = 60

        PDA._load_agent_config = Mock(return_value=new_config)
        PDA.override_manager = None    # Ensure it's None

        with patch('platform_driver.agent.OverrideManager') as mock_override_manager_class:
            # Act
            PDA.configure_main(_="", action="UPDATE", contents={})

            # Assert
            mock_override_manager_class.assert_called_once_with(PDA)
            assert PDA.override_manager is not None

    def test_configure_main_creates_reservation_manager(self, PDA):
        """configure main creates reservation manager"""
        # Arrange
        new_config = MagicMock()
        new_config.max_open_sockets = 100
        new_config.max_concurrent_publishes = 10
        new_config.scalability_test = False
        new_config.scalability_test_iterations = 10
        new_config.reservation_preempt_grace_time = 5
        new_config.reservation_publish_interval = 30
        new_config.remote_heartbeat_interval = 60

        PDA._load_agent_config = Mock(return_value=new_config)
        PDA.reservation_manager = None    # Ensure it's None

        with patch('platform_driver.agent.ReservationManager') as mock_reservation_manager_class, \
                patch('platform_driver.agent.get_aware_utc_now', return_value='now'):
            # Act
            PDA.configure_main(_="", action="UPDATE", contents={})

            # Assert
            mock_reservation_manager_class.assert_called_once_with(
                PDA, new_config.reservation_preempt_grace_time, 'now')
            assert PDA.reservation_manager is not None
            PDA.reservation_manager.update.assert_called_once_with('now')


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
            'registry_config': [{
                'point_name': 'temperature',
                'unit': 'C'
            }, {
                'point_name': 'humidity',
                'unit': '%'
            }],
            'some_device_setting':
            'device_value'
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

        PDA._update_equipment.assert_called_once_with(equipment_name, 'UPDATE', contents)

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
        PDA.equipment_tree.add_device.assert_called_once_with(device_topic=equipment_name,
                                                              dev_config=dev_config,
                                                              driver_agent=driver,
                                                              registry_config=registry_config)
        driver.add_equipment.assert_called_once_with(device_node)
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
            PDA.equipment_tree.add_segment.assert_called_once_with(equipment_name,
                                                                   equipment_config_instance)
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
            result = PDA._get_or_create_remote(equipment_name, remote_config,
                                               allow_duplicate_remotes)

            # Assertions
            PDA._get_configured_interface.assert_called_once_with(remote_config)
            interface.unique_remote_id.assert_called_once_with(equipment_name, remote_config)
            MockDriverAgent.assert_called_once_with(remote_config, PDA.core, PDA.equipment_tree,
                                                    PDA.scalability_test, PDA.config.timezone,
                                                    'unique_id_2', PDA.vip)
            # Check that the new driver agent is stored
            assert PDA.equipment_tree.remotes['unique_id_2'] == driver_agent_instance
            assert result == driver_agent_instance

    def test_get_or_create_remote_allow_duplicate_remotes_true(self, PDA):
        equipment_name = 'equipment3'
        remote_config = MagicMock()
        remote_config.driver_type = 'fake_driver'    # Set driver_type to a valid string
        allow_duplicate_remotes = True

        # Mock interface
        interface = MagicMock()
        PDA._get_configured_interface.return_value = interface

        # Mock BaseInterface.unique_remote_id and get_interface_subclass
        with patch('volttron.driver.base.interfaces.BaseInterface.unique_remote_id', return_value='unique_id_base'), \
                patch('platform_driver.agent.DriverAgent') as MockDriverAgent, \
                patch('volttron.driver.base.interfaces.BaseInterface.get_interface_subclass', return_value=MagicMock()):
            # Call the method
            result = PDA._get_or_create_remote(equipment_name, remote_config,
                                               allow_duplicate_remotes)

            # Assertions
            PDA._get_configured_interface.assert_called_once_with(remote_config)
            interface.unique_remote_id.assert_not_called(
            )    # Should not be called when duplicates are allowed
            MockDriverAgent.assert_called_once_with(remote_config, PDA.core, PDA.equipment_tree,
                                                    PDA.scalability_test, PDA.config.timezone,
                                                    'unique_id_base', PDA.vip)
            # Check that the new driver agent is stored
            assert PDA.equipment_tree.remotes['unique_id_base'] == MockDriverAgent.return_value
            assert result == MockDriverAgent.return_value

    def test_get_or_create_remote_allow_duplicate_remotes_false_config_true(self, PDA):
        equipment_name = 'equipment4'
        remote_config = MagicMock()
        remote_config.driver_type = 'fake_driver'    # Set driver_type to a valid string
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

            result = PDA._get_or_create_remote(equipment_name, remote_config,
                                               allow_duplicate_remotes)

            PDA._get_configured_interface.assert_called_once_with(remote_config)
            interface.unique_remote_id.assert_not_called(
            )    # Should not be called when duplicates are allowed
            MockDriverAgent.assert_called_once_with(remote_config, PDA.core, PDA.equipment_tree,
                                                    PDA.scalability_test, PDA.config.timezone,
                                                    'unique_id_base_config_true', PDA.vip)
            # Check that the new driver agent is stored
            assert PDA.equipment_tree.remotes[
                'unique_id_base_config_true'] == MockDriverAgent.return_value
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
        with patch('platform_driver.agent.BaseInterface.get_interface_subclass'
                   ) as mock_get_interface_subclass:
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

        # Mock _separate_equipment_configs to return expected configs
        remote_config = Mock()
        dev_config = Mock()
        dev_config.allow_duplicate_remotes = False
        registry_config = Mock()
        PDA._separate_equipment_configs.return_value = (remote_config, dev_config, registry_config)

        # Mock methods to prevent side effects
        PDA._get_or_create_remote.return_value = Mock()
        PDA.equipment_tree.update_equipment.return_value = True

        # Mock points to return an empty list to avoid processing that causes KeyError
        PDA.equipment_tree.points.return_value = []

        result = PDA._update_equipment(config_name, None, contents)

        assert result is True
        PDA._separate_equipment_configs.assert_called_once_with(contents)
        PDA._get_or_create_remote.assert_called_once_with(config_name, remote_config,
                                                          dev_config.allow_duplicate_remotes)
        PDA.equipment_tree.update_equipment.assert_called_once_with(
            config_name, dev_config, PDA._get_or_create_remote.return_value, registry_config)

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
        PDA._get_or_create_remote.assert_called_once_with(config_name, remote_config,
                                                          dev_config.allow_duplicate_remotes)
        PDA.equipment_tree.update_equipment.assert_called_once_with(config_name, dev_config,
                                                                    remote, registry_config)
        # Polling should not be rescheduled
        PDA.equipment_tree.points.assert_not_called()
        assert result == False

    def test_update_equipment_device_config_absent(self, PDA):
        config_name = 'equipment3'
        contents = {'some': 'contents'}

        # Mock _separate_equipment_configs to return expected configs
        remote_config = Mock()
        dev_config = None    # Device config absent
        registry_config = Mock()
        PDA._separate_equipment_configs.return_value = (remote_config, dev_config, registry_config)

        # Since dev_config is None, _get_or_create_remote should not be called
        PDA._get_or_create_remote = Mock()

        PDA.equipment_tree.update_equipment.return_value = True

        PDA.equipment_tree.points.return_value = []

        result = PDA._update_equipment(config_name, None, contents)

        assert result is True
        PDA._separate_equipment_configs.assert_called_once_with(contents)
        PDA._get_or_create_remote.assert_not_called()
        PDA.equipment_tree.update_equipment.assert_called_once_with(config_name, dev_config, None,
                                                                    registry_config)

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
            PDA._get_or_create_remote.assert_called_once_with(config_name, remote_config,
                                                              dev_config.allow_duplicate_remotes)
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

        result = PDA._update_equipment(config_name, None, contents)

        PDA._separate_equipment_configs.assert_called_once_with(contents)
        PDA._get_or_create_remote.assert_called_once_with(config_name, remote_config,
                                                          dev_config.allow_duplicate_remotes)
        PDA.equipment_tree.update_equipment.assert_called_once_with(config_name, dev_config,
                                                                    remote, registry_config)
        # Polling should not be rescheduled
        PDA.equipment_tree.points.assert_called()
        assert result == True


class TestPDARemoveEquipment:

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.equipment_tree = MagicMock()
        PDA.poll_schedulers = {}
        return PDA

    def test_remove_equipment_with_points(self, PDA):
        config_name = 'equipment_with_points'
        # Mock points associated with the equipment
        point1 = MagicMock()
        point1.identifier = 'point1'
        point2 = MagicMock()
        point2.identifier = 'point2'
        PDA.equipment_tree.points.return_value = [point1, point2]

        # Mock get_group to return group names
        PDA.equipment_tree.get_group.side_effect = ['group1', 'group2']

        # Mock poll schedulers
        poll_scheduler1 = MagicMock()
        poll_scheduler2 = MagicMock()
        PDA.poll_schedulers = {'group1': poll_scheduler1, 'group2': poll_scheduler2}

        PDA.equipment_tree.remove_segment.return_value = 1

        PDA._remove_equipment(config_name, None, None)

        PDA.equipment_tree.points.assert_called_once_with(config_name)
        PDA.equipment_tree.get_group.assert_any_call('point1')
        PDA.equipment_tree.get_group.assert_any_call('point2')
        PDA.equipment_tree.remove_segment.assert_called_once()

    def test_remove_equipment_no_points(self, PDA):
        config_name = 'equipment_no_points'
        # Mock no points associated with the equipment
        PDA.equipment_tree.points.return_value = []

        # Ensure get_group is not called
        PDA.equipment_tree.get_group = MagicMock()

        # Empty poll_schedulers dict
        PDA.poll_schedulers = {}

        PDA.equipment_tree.remove_segment.return_value = 0

        PDA._remove_equipment(config_name, None, None)

        PDA.equipment_tree.points.assert_called_once_with(config_name)
        PDA.equipment_tree.get_group.assert_not_called()
        PDA.equipment_tree.remove_segment.assert_called_once()


class TestPDAStartAllPublishes:

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.equipment_tree = MagicMock()
        PDA.poll_schedulers = {}
        PDA.publishers = {}
        PDA.core = MagicMock()
        return PDA

    def test_start_all_publishes_no_devices(self, PDA):
        # Mock devices method to return an empty list
        PDA.equipment_tree.devices.return_value = []

        PDA._start_all_publishes()

        PDA.core.schedule.assert_not_called()

    def test_start_all_publishes_device_no_interval(self, PDA):
        # Mock a device without all_publish_interval
        device = MagicMock()
        device.identifier = 'device1'
        device.all_publish_interval = None
        PDA.equipment_tree.devices.return_value = [device]
        # Mock publishing methods to return False
        PDA.equipment_tree.is_published_all_depth.return_value = False
        PDA.equipment_tree.is_published_all_breadth.return_value = False
        # Mock poll_schedulers
        poller = MagicMock()
        poller.start_all_datetime = 100
        PDA.poll_schedulers = {'poller1': poller}

        PDA._start_all_publishes()

        PDA.core.schedule.assert_not_called()    # no scheduling was done

    def test_start_all_publishes_device_not_published(self, PDA):
        # Mock a device with all_publish_interval
        device = MagicMock()
        device.identifier = 'device1'
        device.all_publish_interval = 60
        PDA.equipment_tree.devices.return_value = [device]
        # Mock publishing methods to return False
        PDA.equipment_tree.is_published_all_depth.return_value = False
        PDA.equipment_tree.is_published_all_breadth.return_value = False

        PDA._start_all_publishes()

        PDA.core.schedule.assert_not_called()    # no scheduling was done

    def test_start_all_publishes_device_published_depth(self, PDA):
        # Mock a device with all_publish_interval
        device = MagicMock()
        device.identifier = 'device1'
        device.all_publish_interval = 60
        PDA.equipment_tree.devices.return_value = [device]
        # Mock publishing methods
        PDA.equipment_tree.is_published_all_depth.return_value = True
        PDA.equipment_tree.is_published_all_breadth.return_value = False
        # Mock poll_schedulers
        poller = MagicMock()
        poller.start_all_datetime = 100
        PDA.poll_schedulers = {'poller1': poller}

        PDA._start_all_publishes()

        PDA.core.schedule.assert_called_once()
        assert device in PDA.publishers

    def test_start_all_publishes_device_published_breadth(self, PDA):
        # Mock a device with all_publish_interval
        device = MagicMock()
        device.identifier = 'device2'
        device.all_publish_interval = 120
        PDA.equipment_tree.devices.return_value = [device]
        # Mock publishing methods
        PDA.equipment_tree.is_published_all_depth.return_value = False
        PDA.equipment_tree.is_published_all_breadth.return_value = True
        # Mock poll_schedulers
        poller = MagicMock()
        poller.start_all_datetime = 200
        PDA.poll_schedulers = {'poller2': poller}

        PDA._start_all_publishes()

        PDA.core.schedule.assert_called_once()
        assert device in PDA.publishers


class TestPDAAllPublish:

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.equipment_tree = MagicMock()
        PDA.vip = MagicMock()
        return PDA

    def test_all_publish_device_not_ready(self, PDA):
        node = MagicMock(identifier='device1')
        PDA.equipment_tree.get_node.return_value = node
        PDA.equipment_tree.is_ready.return_value = False

        PDA._all_publish(node)

        PDA.equipment_tree.is_ready.assert_called_once_with('device1')
        PDA.vip.pubsub.publish.assert_not_called()

    def test_all_publish_device_is_stale(self, PDA):
        node = MagicMock(identifier='device2')
        PDA.equipment_tree.get_node.return_value = node
        PDA.equipment_tree.is_ready.return_value = True
        PDA.equipment_tree.is_stale.return_value = True

        PDA._all_publish(node)

        PDA.equipment_tree.is_stale.assert_called_once_with('device2')
        PDA.vip.pubsub.publish.assert_not_called()

    @patch('platform_driver.agent.publish_wrapper')
    @patch('platform_driver.agent.publication_headers')
    def test_all_publish_published_all_depth(self, mock_publication_headers, mock_publish_wrapper,
                                             PDA):
        node = MagicMock(identifier='device3')
        PDA.equipment_tree.get_node.return_value = node
        PDA.equipment_tree.is_ready.return_value = True
        PDA.equipment_tree.is_stale.return_value = False
        PDA.equipment_tree.is_published_all_depth.return_value = True
        PDA.equipment_tree.get_device_topics.return_value = ('depth_topic', 'breadth_topic')
        point = MagicMock(identifier='device3/point1', last_value=42, meta_data={'units': 'degC'})
        PDA.equipment_tree.points.return_value = [point]
        mock_publication_headers.return_value = {}

        PDA._all_publish(node)

        mock_publish_wrapper.assert_called_once_with(PDA.vip,
                                                     f'depth_topic/all',
                                                     headers={},
                                                     message=[{
                                                         'point1': 42
                                                     }, {
                                                         'point1': {
                                                             'units': 'degC'
                                                         }
                                                     }])

    @patch('platform_driver.agent.publish_wrapper')
    @patch('platform_driver.agent.publication_headers')
    def test_all_publish_published_all_breadth(self, mock_publication_headers,
                                               mock_publish_wrapper, PDA):
        node = MagicMock(identifier='device4')
        PDA.equipment_tree.get_node.return_value = node
        PDA.equipment_tree.is_ready.return_value = True
        PDA.equipment_tree.is_stale.return_value = False
        PDA.equipment_tree.is_published_all_depth.return_value = False
        PDA.equipment_tree.is_published_all_breadth.return_value = True
        PDA.equipment_tree.get_device_topics.return_value = ('depth_topic', 'breadth_topic')
        point = MagicMock(identifier='device4/point1', last_value=100, meta_data={'units': 'kW'})
        PDA.equipment_tree.points.return_value = [point]
        mock_publication_headers.return_value = {}

        PDA._all_publish(node)

        mock_publish_wrapper.assert_called_once_with(PDA.vip,
                                                     f'breadth_topic/all',
                                                     headers={},
                                                     message=[{
                                                         'point1': 100
                                                     }, {
                                                         'point1': {
                                                             'units': 'kW'
                                                         }
                                                     }])

    @patch('platform_driver.agent.publish_wrapper')
    @patch('platform_driver.agent.publication_headers')
    def test_all_publish_no_publishing(self, mock_publication_headers, mock_publish_wrapper, PDA):
        node = MagicMock(identifier='device5')
        PDA.equipment_tree.get_node.return_value = node
        PDA.equipment_tree.is_ready.return_value = True
        PDA.equipment_tree.is_stale.return_value = False
        PDA.equipment_tree.is_published_all_depth.return_value = False
        PDA.equipment_tree.is_published_all_breadth.return_value = False
        PDA.equipment_tree.get_device_topics.return_value = ('depth_topic', 'breadth_topic')
        mock_publication_headers.return_value = {}

        PDA._all_publish(node)

        mock_publish_wrapper.assert_not_called()


class TestPDASemanticQuery:

    @pytest.fixture
    def PDA(self):
        PDA = PlatformDriverAgent()
        PDA.vip = MagicMock()
        return PDA

    def test_semantic_query_success(self, PDA):
        query = {'some': 'query'}
        expected_result = {'result': 'data'}
        # Mock the RPC call to return expected_result
        PDA.vip.rpc.call.return_value.get.return_value = expected_result

        result = PDA.semantic_query(query)

        PDA.vip.rpc.call.assert_called_once_with('platform.semantic', 'semantic_query', query)
        PDA.vip.rpc.call.return_value.get.assert_called_once_with(timeout=5)
        assert result == expected_result

    def test_semantic_query_timeout(self, PDA, caplog):
        query = {'some': 'query'}
        # Mock the RPC call to raise gevent.Timeout
        PDA.vip.rpc.call.return_value.get.side_effect = gevent.Timeout

        result = PDA.semantic_query(query)

        PDA.vip.rpc.call.assert_called_once_with('platform.semantic', 'semantic_query', query)
        PDA.vip.rpc.call.return_value.get.assert_called_once_with(timeout=5)
        assert result == {}
        assert any("Semantic Interoperability Service timed out" in record.message
                   for record in caplog.records)


class TestPDABuildQueryPlan:
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


class TestPDAGet:
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


class TestPDASemanticGet:

    @pytest.fixture
    def PDA(self):
        """Fixture to create a PlatformDriverAgent instance with mocked dependencies."""
        PDA = PlatformDriverAgent()
        PDA.semantic_query = MagicMock()
        PDA.build_query_plan = MagicMock()
        PDA._get = MagicMock()
        return PDA

    def test_semantic_get_success(self, PDA):
        """Test semantic_get returns expected result when dependencies work correctly."""
        query = "temperature sensor"
        exact_matches = {"sensor": ["temp_sensor_1", "temp_sensor_2"]}
        query_plan = {"steps": ["fetch_data", "aggregate"]}
        expected_result = ({"data": "aggregated_data"}, {"metadata": "info"})

        # Set up mocks
        PDA.semantic_query.return_value = exact_matches
        PDA.build_query_plan.return_value = query_plan
        PDA._get.return_value = expected_result

        result = PDA.semantic_get(query)

        PDA.semantic_query.assert_called_once_with(query)
        PDA.build_query_plan.assert_called_once_with(exact_matches)
        PDA._get.assert_called_once_with(query_plan)
        assert result == expected_result

    def test_semantic_get_no_matches(self, PDA):
        """Test semantic_get returns expected result when semantic_query finds no matches."""
        query = "unknown device"
        exact_matches = {}
        query_plan = {}
        expected_result = ({}, {})

        # Set up mocks
        PDA.semantic_query.return_value = exact_matches
        PDA.build_query_plan.return_value = query_plan
        PDA._get.return_value = expected_result

        result = PDA.semantic_get(query)

        PDA.semantic_query.assert_called_once_with(query)
        PDA.build_query_plan.assert_called_once_with(exact_matches)
        PDA._get.assert_called_once_with(query_plan)
        assert result == expected_result

    def test_semantic_get_exception_in_semantic_query(self, PDA, caplog):
        """Test semantic_get handles exceptions raised by semantic_query."""
        query = "faulty query"
        PDA.semantic_query.side_effect = Exception("Semantic service error")

        with pytest.raises(Exception) as exc_info:
            PDA.semantic_get(query)

        PDA.semantic_query.assert_called_once_with(query)
        PDA.build_query_plan.assert_not_called()
        PDA._get.assert_not_called()
        assert "Semantic service error" in str(exc_info.value)

    def test_semantic_get_exception_in_build_query_plan(self, PDA, caplog):
        """Test semantic_get handles exceptions raised by build_query_plan."""
        query = "temperature sensor"
        exact_matches = {"sensor": ["temp_sensor_1", "temp_sensor_2"]}
        PDA.semantic_query.return_value = exact_matches
        PDA.build_query_plan.side_effect = Exception("Query plan error")

        with pytest.raises(Exception) as exc_info:
            PDA.semantic_get(query)

        PDA.semantic_query.assert_called_once_with(query)
        PDA.build_query_plan.assert_called_once_with(exact_matches)
        PDA._get.assert_not_called()
        assert "Query plan error" in str(exc_info.value)

    def test_semantic_get_exception_in_get(self, PDA, caplog):
        """Test semantic_get handles exceptions raised by _get."""
        query = "temperature sensor"
        exact_matches = {"sensor": ["temp_sensor_1", "temp_sensor_2"]}
        query_plan = {"steps": ["fetch_data", "aggregate"]}
        PDA.semantic_query.return_value = exact_matches
        PDA.build_query_plan.return_value = query_plan
        PDA._get.side_effect = Exception("Get data error")

        with pytest.raises(Exception) as exc_info:
            PDA.semantic_get(query)

        PDA.semantic_query.assert_called_once_with(query)
        PDA.build_query_plan.assert_called_once_with(exact_matches)
        PDA._get.assert_called_once_with(query_plan)
        assert "Get data error" in str(exc_info.value)


class TestPDAUnderscoreGet:
    """Tests for _get"""

    @pytest.fixture
    def PDA(self):
        """Fixture to create a PlatformDriverAgent instance with mocked dependencies."""
        PDA = PlatformDriverAgent()
        PDA.equipment_tree = MagicMock()
        return PDA

    def test_get_success(self, PDA):
        """Test that _get successfully retrieves and processes data from remotes."""

        remote = MagicMock(spec=DriverAgent)
        point1 = MagicMock(spec=PointNode)
        point1.identifier = 'topic1'
        point2 = MagicMock(spec=PointNode)
        point2.identifier = 'topic2'
        point_set: Set[PointNode] = {point1, point2}
        query_plan: Dict[DriverAgent, Set[PointNode]] = {remote: point_set}

        q_return_values = {'topic1': 100, 'topic2': 200}
        remote.get_multiple_points.return_value = (q_return_values, {})

        # Mock equipment_tree.get_node to return nodes
        node1 = MagicMock(spec=PointNode)
        node2 = MagicMock(spec=PointNode)
        PDA.equipment_tree.get_node.side_effect = lambda topic: {
            'topic1': node1,
            'topic2': node2
        }.get(topic)

        results, errors = PDA._get(query_plan)

        remote.get_multiple_points.assert_called_once()
        args, _ = remote.get_multiple_points.call_args
        assert set(args[0]) == {'topic1', 'topic2'}

        PDA.equipment_tree.get_node.assert_any_call('topic1')
        PDA.equipment_tree.get_node.assert_any_call('topic2')

        node1.last_value = 100
        node2.last_value = 200
        assert node1.last_value == 100
        assert node2.last_value == 200

        assert results == q_return_values
        assert errors == {}

    def test_get_with_errors(self, PDA):
        """Test that _get correctly captures errors returned by remotes."""

        remote = MagicMock(spec=DriverAgent)
        point1 = MagicMock(spec=PointNode)
        point1.identifier = 'topic1'
        point_set: Set[PointNode] = {point1}
        query_plan: Dict[DriverAgent, Set[PointNode]] = {remote: point_set}

        remote.get_multiple_points.return_value = ({}, {'topic1': 'Error fetching data'})

        results, errors = PDA._get(query_plan)

        remote.get_multiple_points.assert_called_once()
        args, _ = remote.get_multiple_points.call_args
        assert set(args[0]) == {'topic1'}

        PDA.equipment_tree.get_node.assert_not_called()

        assert results == {}
        assert errors == {'topic1': 'Error fetching data'}

    def test_get_empty_query_plan(self, PDA):
        """Test that _get handles an empty query_plan gracefully."""
        query_plan: Dict[DriverAgent, Set[PointNode]] = {}

        results, errors = PDA._get(query_plan)

        # No remotes should be called
        remote_calls = []
        for mock in PDA.equipment_tree.mock_calls:
            if 'get_node' in mock[0]:
                remote_calls.append(mock)
        assert not remote_calls

        assert results == {}
        assert errors == {}


class TestPDASet:

    @pytest.fixture
    def pda(self):
        """Fixture to create a PlatformDriverAgent instance with mocked dependencies."""
        pda = PlatformDriverAgent()
        pda.build_query_plan = MagicMock()
        pda._set = MagicMock()
        return pda

    def test_set_with_single_topic_and_single_value(self, pda):
        """
        Test the 'set' method with a single topic and a single value,
        without confirmation and without mapping points.
        """
        value = 100    # Single value for all points
        topics = ['topic1']
        regex = None
        confirm_values = False
        map_points = False

        expected_query_plan = {'remote1': {'point1'}}

        pda.build_query_plan.return_value = expected_query_plan

        expected_results = {'topic1': 'success'}
        expected_errors = {}
        pda._set.return_value = (expected_results, expected_errors)

        results, errors = pda.set(value=value,
                                  topic=topics,
                                  regex=regex,
                                  confirm_values=confirm_values,
                                  map_points=map_points)
        pda.build_query_plan.assert_called_once_with(topics, regex)
        pda._set.assert_called_once_with(value, expected_query_plan, confirm_values, map_points)
        assert results == expected_results
        assert errors == expected_errors

    def test_set_with_multiple_topics_and_multiple_values(self, pda):
        """
        Test the 'set' method with multiple topics and multiple values,
        with confirmation and with mapping points.
        """
        value = {'topic1': 100, 'topic2': 200}    # Different values for each topic
        topics = ['topic1', 'topic2']
        regex = None
        confirm_values = True
        map_points = True

        expected_query_plan = {'remote1': {'point1'}, 'remote2': {'point2'}}

        pda.build_query_plan.return_value = expected_query_plan

        expected_results = {'topic1': 'confirmed', 'topic2': 'confirmed'}
        expected_errors = {}
        pda._set.return_value = (expected_results, expected_errors)

        results, errors = pda.set(value=value,
                                  topic=topics,
                                  regex=regex,
                                  confirm_values=confirm_values,
                                  map_points=map_points)
        pda.build_query_plan.assert_called_once_with(topics, regex)
        pda._set.assert_called_once_with(value, expected_query_plan, confirm_values, map_points)
        assert results == expected_results
        assert errors == expected_errors

    def test_set_with_regex(self, pda):
        """
        Test the 'set' method with a regex pattern for topic selection.
        """
        value = 150    # Single value for all matched points
        topics = None
        regex = r'^sensor_.*$'    # Regex to match topics starting with 'sensor_'
        confirm_values = False
        map_points = False

        expected_query_plan = {'remote1': {'point_sensor1', 'point_sensor2'}}

        pda.build_query_plan.return_value = expected_query_plan

        expected_results = {'sensor1': 'success', 'sensor2': 'success'}
        expected_errors = {}
        pda._set.return_value = (expected_results, expected_errors)

        results, errors = pda.set(value=value,
                                  topic=topics,
                                  regex=regex,
                                  confirm_values=confirm_values,
                                  map_points=map_points)
        pda.build_query_plan.assert_called_once_with(topics, regex)
        pda._set.assert_called_once_with(value, expected_query_plan, confirm_values, map_points)
        assert results == expected_results
        assert errors == expected_errors

    def test_set_returns_errors_when_set_fails(self, pda):
        """
        Test the 'set' method returns errors when the '_set' method indicates failures.
        """
        value = 50    # Single value for all points
        topics = ['topic5']
        regex = None
        confirm_values = False
        map_points = False

        expected_query_plan = {'remote3': {'point5'}}

        pda.build_query_plan.return_value = expected_query_plan

        expected_results = {}
        expected_errors = {'topic5': 'Failed to set value'}
        pda._set.return_value = (expected_results, expected_errors)

        results, errors = pda.set(value=value,
                                  topic=topics,
                                  regex=regex,
                                  confirm_values=confirm_values,
                                  map_points=map_points)

        pda.build_query_plan.assert_called_once_with(topics, regex)
        pda._set.assert_called_once_with(value, expected_query_plan, confirm_values, map_points)
        assert results == expected_results
        assert errors == expected_errors


class TestPDASemanticSet:

    @pytest.fixture
    def pda(self):
        pda = PlatformDriverAgent()
        pda.semantic_query = MagicMock()
        pda.build_query_plan = MagicMock()
        pda._set = MagicMock()
        return pda

    def test_semantic_set_with_valid_query_no_confirm(self, pda):
        """Test 'semantic_set' with a valid query and no confirmation"""
        value = 100
        query = "temperature sensors"
        confirm_values = False

        exact_matches = {'remote1': {'point1', 'point2'}}
        expected_query_plan = {'remote1': {'point1', 'point2'}}

        pda.semantic_query.return_value = exact_matches
        pda.build_query_plan.return_value = expected_query_plan

        # Mock _set to return specific results and errors
        expected_results = {'point1': 100, 'point2': 100}
        expected_errors = {}
        pda._set.return_value = (expected_results, expected_errors)

        results, errors = pda.semantic_set(value=value, query=query, confirm_values=confirm_values)

        pda.semantic_query.assert_called_once_with(query)
        pda.build_query_plan.assert_called_once_with(exact_matches)
        pda._set.assert_called_once_with(value, expected_query_plan, confirm_values)
        assert results == expected_results
        assert errors == expected_errors

    def test_semantic_set_with_valid_query_with_confirm(self, pda):
        """Test 'semantic_set' with a valid query and confirmation"""
        value = {'point1': 100, 'point2': 200}
        query = "humidity sensors"
        confirm_values = True

        exact_matches = {'remote2': {'point3', 'point4'}}
        expected_query_plan = {'remote2': {'point3', 'point4'}}

        # Mock semantic_query and build_query_plan
        pda.semantic_query.return_value = exact_matches
        pda.build_query_plan.return_value = expected_query_plan

        # Mock _set to return specific results and errors
        expected_results = {'point3': 100, 'point4': 200}
        expected_errors = {}
        pda._set.return_value = (expected_results, expected_errors)

        results, errors = pda.semantic_set(value=value, query=query, confirm_values=confirm_values)

        pda.semantic_query.assert_called_once_with(query)
        pda.build_query_plan.assert_called_once_with(exact_matches)
        pda._set.assert_called_once_with(value, expected_query_plan, confirm_values)
        assert results == expected_results
        assert errors == expected_errors

    def test_semantic_set_with_no_matches(self, pda):
        """Test 'semantic_set' when 'semantic_query' returns no matches"""
        value = 50
        query = "unknown devices"
        confirm_values = False

        exact_matches = {}
        expected_query_plan = {}

        # Mock semantic_query and build_query_plan
        pda.semantic_query.return_value = exact_matches
        pda.build_query_plan.return_value = expected_query_plan

        # Mock _set to return specific results and errors
        expected_results = {}
        expected_errors = {}
        pda._set.return_value = (expected_results, expected_errors)

        results, errors = pda.semantic_set(value=value, query=query, confirm_values=confirm_values)

        pda.semantic_query.assert_called_once_with(query)
        pda.build_query_plan.assert_called_once_with(exact_matches)
        pda._set.assert_called_once_with(value, expected_query_plan, confirm_values)
        assert results == expected_results
        assert errors == expected_errors

    def test_semantic_set_returns_errors_when_set_fails(self, pda):
        """Test 'semantic_set' when '_set' returns errors"""
        value = 75
        query = "pressure sensors"
        confirm_values = False

        exact_matches = {'remote3': {'point5'}}
        expected_query_plan = {'remote3': {'point5'}}

        pda.semantic_query.return_value = exact_matches
        pda.build_query_plan.return_value = expected_query_plan

        # Mock _set to return empty results and specific errors
        expected_results = {}
        expected_errors = {'point5': 'Failed to set value'}
        pda._set.return_value = (expected_results, expected_errors)

        results, errors = pda.semantic_set(value=value, query=query, confirm_values=confirm_values)

        pda.semantic_query.assert_called_once_with(query)
        pda.build_query_plan.assert_called_once_with(exact_matches)
        pda._set.assert_called_once_with(value, expected_query_plan, confirm_values)
        assert results == expected_results
        assert errors == expected_errors


class TestUnderscoreSet:
    """Tests for the _set function in platform_driver.agent."""

    def test_set_single_value_no_confirm(self):
        """Verify _set sets multiple points with a single value without confirmation."""
        value = "new_value"
        remote = Mock()
        point1 = Mock()
        point1.identifier = "point1"
        point2 = Mock()
        point2.identifier = "point2"
        query_plan = {remote: {point1, point2}}
        confirm_values = False
        map_points = False

        remote.set_multiple_points.return_value = {}

        results, errors = PlatformDriverAgent._set(value, query_plan, confirm_values, map_points)

        expected_tuples = [("point1", "new_value"), ("point2", "new_value")]
        remote.set_multiple_points.assert_called_once()
        actual_args, actual_kwargs = remote.set_multiple_points.call_args
        assert set(actual_args[0]) == set(
            expected_tuples), "set_multiple_points called with incorrect arguments."
        assert not errors, "Errors should be empty when no errors are returned."
        assert not results, "Results should be empty when confirm_values is False."

    def test_set_single_value_with_confirm(self):
        """Ensure _set sets multiple points with a single value and confirms the changes."""
        value = "new_value"
        remote = Mock()
        point1 = Mock()
        point1.identifier = "point1"
        point2 = Mock()
        point2.identifier = "point2"
        query_plan = {remote: {point1, point2}}
        confirm_values = True
        map_points = False

        remote.set_multiple_points.return_value = {}
        remote.get_multiple_points.return_value = {"point1": "new_value", "point2": "new_value"}

        results, errors = PlatformDriverAgent._set(value, query_plan, confirm_values, map_points)
        expected_tuples = [("point1", "new_value"), ("point2", "new_value")]
        remote.set_multiple_points.assert_called_once()
        actual_set_args, _ = remote.set_multiple_points.call_args
        assert set(actual_set_args[0]) == set(
            expected_tuples), "set_multiple_points called with incorrect arguments."

        remote.get_multiple_points.assert_called_once()
        actual_get_args, _ = remote.get_multiple_points.call_args
        assert set(actual_get_args[0]) == {
            "point1", "point2"
        }, "get_multiple_points called with incorrect arguments."

        assert "point1" in results, "Result should contain 'point1'."
        assert "point2" in results, "Result should contain 'point2'."
        assert results["point1"] == "new_value", "'point1' should have the updated value."
        assert results["point2"] == "new_value", "'point2' should have the updated value."
        assert not errors, "Errors should be empty when no errors are returned."

    def test_set_mapped_values_no_confirm(self):
        """Check _set sets multiple points with different values without confirmation."""
        value = {"point1": "value1", "point2": "value2"}
        remote = Mock()
        point1 = Mock()
        point1.identifier = "point1"
        point2 = Mock()
        point2.identifier = "point2"
        query_plan = {remote: {point1, point2}}
        confirm_values = False
        map_points = True

        remote.set_multiple_points.return_value = {}

        results, errors = PlatformDriverAgent._set(value, query_plan, confirm_values, map_points)

        expected_tuples = [("point1", "value1"), ("point2", "value2")]
        remote.set_multiple_points.assert_called_once()
        actual_args, actual_kwargs = remote.set_multiple_points.call_args
        assert set(actual_args[0]) == set(
            expected_tuples), "set_multiple_points called with incorrect arguments."
        assert not errors, "Errors should be empty when no errors are returned."
        assert not results, "Results should be empty when confirm_values is False."

    def test_set_mapped_values_with_confirm(self):
        """Validate _set sets multiple points with different values and confirms the changes."""
        value = {"point1": "value1", "point2": "value2"}
        remote = Mock()
        point1 = Mock()
        point1.identifier = "point1"
        point2 = Mock()
        point2.identifier = "point2"
        query_plan = {remote: {point1, point2}}
        confirm_values = True
        map_points = True

        remote.set_multiple_points.return_value = {"point2": "Set error"}
        remote.get_multiple_points.return_value = {"point1": "value1", "point2": "old_value"}

        results, errors = PlatformDriverAgent._set(value, query_plan, confirm_values, map_points)

        expected_tuples = [("point1", "value1"), ("point2", "value2")]
        remote.set_multiple_points.assert_called_once()
        actual_set_args, _ = remote.set_multiple_points.call_args
        assert set(actual_set_args[0]) == set(
            expected_tuples), "set_multiple_points called with incorrect arguments."

        remote.get_multiple_points.assert_called_once()
        actual_get_args, _ = remote.get_multiple_points.call_args
        assert set(actual_get_args[0]) == {
            "point1", "point2"
        }, "get_multiple_points called with incorrect arguments."

        assert "point1" in results, "Result should contain 'point1'."
        assert "point2" in results, "Result should contain 'point2'."
        assert results["point1"] == "value1", "'point1' should have the updated value."
        assert results[
            "point2"] == "old_value", "'point2' should reflect the old value due to the error."

        # Check that the specific error is captured
        assert "point2" in errors, "Errors should contain 'point2'."
        assert errors["point2"] == "Set error", "'point2' should have the correct error message."


class TestPlatformDriverAgentRevertMethods:

    @pytest.fixture
    def pda(self):
        """
        Fixture to create a PlatformDriverAgent instance with mocked dependencies.
        """
        pda = PlatformDriverAgent()
        pda.build_query_plan = MagicMock()
        pda.semantic_query = MagicMock()
        return pda

    # -------------------------
    # Tests for the 'revert' method
    # -------------------------

    def test_revert_with_single_topic_no_errors(self, pda, mocker):
        """Test the 'revert' method with a single topic and no errors during revert"""
        topic = 'topic1'
        regex = None

        remote1 = MagicMock(spec=DriverAgent)
        point1 = MagicMock(spec=PointNode, identifier='point1')

        expected_query_plan = {remote1: {point1}}

        # Mock build_query_plan to return the expected_query_plan
        pda.build_query_plan.return_value = expected_query_plan

        # Mock the _revert static method to return no errors
        mock_revert = mocker.patch.object(PlatformDriverAgent, '_revert', return_value={})

        errors = pda.revert(topic=topic, regex=regex)

        pda.build_query_plan.assert_called_once_with(topic, regex)
        mock_revert.assert_called_once_with(expected_query_plan)
        assert errors == {}
        mock_revert.stop()    # stop the patch to avoid side effects

    def test_revert_with_multiple_topics_with_errors(self, pda, mocker):
        """Test the 'revert' method with multiple topics and some errors during revert."""
        topics = ['topic1', 'topic2']
        regex = None

        # Create mock DriverAgents and PointNodes
        remote1 = MagicMock(spec=DriverAgent)
        remote2 = MagicMock(spec=DriverAgent)
        point1 = MagicMock(spec=PointNode, identifier='point1')
        point2 = MagicMock(spec=PointNode, identifier='point2')

        expected_query_plan = {remote1: {point1}, remote2: {point2}}

        # Mock build_query_plan to return the expected_query_plan
        pda.build_query_plan.return_value = expected_query_plan

        # Mock the _revert static method to simulate an error on remote2
        mock_revert = mocker.patch.object(PlatformDriverAgent,
                                          '_revert',
                                          return_value={'point2': 'Revert failed'})

        errors = pda.revert(topic=topics, regex=regex)

        pda.build_query_plan.assert_called_once_with(topics, regex)
        mock_revert.assert_called_once_with(expected_query_plan)
        assert errors == {'point2': 'Revert failed'}
        mock_revert.stop()

    def test_revert_with_no_matches(self, pda, mocker):
        """Test the 'revert' method when no matches are found in build_query_plan"""

        topic = 'nonexistent_topic'
        regex = None
        expected_query_plan = {}

        # Mock build_query_plan to return empty query_plan
        pda.build_query_plan.return_value = expected_query_plan

        # Mock the _revert static method to return no errors
        mock_revert = mocker.patch.object(PlatformDriverAgent, '_revert', return_value={})
        errors = pda.revert(topic=topic, regex=regex)

        pda.build_query_plan.assert_called_once_with(topic, regex)
        mock_revert.assert_called_once_with(expected_query_plan)
        assert errors == {}
        mock_revert.stop()

    # -------------------------
    # Tests for the 'semantic_revert' method
    # -------------------------

    def test_semantic_revert_with_valid_query_no_errors(self, pda, mocker):
        """Test the 'semantic_revert' method with a valid semantic query and no errors during revert."""

        query = "temperature sensors"

        # Create mock DriverAgent and PointNode
        remote1 = MagicMock(spec=DriverAgent)
        point1 = MagicMock(spec=PointNode, identifier='point1')

        expected_exact_matches = {remote1: {point1}}
        expected_query_plan = expected_exact_matches

        # Mock semantic_query to return exact_matches
        pda.semantic_query.return_value = expected_exact_matches

        # Mock build_query_plan to return the expected_query_plan
        pda.build_query_plan.return_value = expected_query_plan

        # Mock the _revert static method to return no errors
        mock_revert = mocker.patch.object(PlatformDriverAgent, '_revert', return_value={})

        errors = pda.semantic_revert(query=query)

        pda.semantic_query.assert_called_once_with(query)
        pda.build_query_plan.assert_called_once_with(expected_exact_matches)
        mock_revert.assert_called_once_with(expected_query_plan)
        assert errors == {}
        mock_revert.stop()

    def test_semantic_revert_with_no_matches(self, pda, mocker):
        """Test the 'semantic_revert' method when semantic_query returns no matches."""

        query = "unknown devices"
        expected_exact_matches = {}
        expected_query_plan = {}

        # Mock semantic_query to return no matches
        pda.semantic_query.return_value = expected_exact_matches

        # Mock build_query_plan to return empty query_plan
        pda.build_query_plan.return_value = expected_query_plan

        # Mock the _revert static method to return no errors
        mock_revert = mocker.patch.object(PlatformDriverAgent, '_revert', return_value={})

        errors = pda.semantic_revert(query=query)

        pda.semantic_query.assert_called_once_with(query)
        pda.build_query_plan.assert_called_once_with(expected_exact_matches)
        mock_revert.assert_called_once_with(expected_query_plan)
        assert errors == {}
        mock_revert.stop()

    def test_semantic_revert_with_revert_errors(self, pda, mocker):
        """Test the 'semantic_revert' method when some revert operations fail."""

        query = "humidity sensors"

        # Create mock DriverAgent and PointNodes
        remote1 = MagicMock(spec=DriverAgent)
        point1 = MagicMock(spec=PointNode, identifier='point1')
        point2 = MagicMock(spec=PointNode, identifier='point2')

        expected_exact_matches = {remote1: {point1, point2}}
        expected_query_plan = expected_exact_matches

        # Mock semantic_query to return exact_matches
        pda.semantic_query.return_value = expected_exact_matches

        # Mock build_query_plan to return the expected_query_plan
        pda.build_query_plan.return_value = expected_query_plan

        # Mock the _revert static method to simulate an error on point2
        mock_revert = mocker.patch.object(PlatformDriverAgent,
                                          '_revert',
                                          return_value={'point2': 'Revert failed'})

        errors = pda.semantic_revert(query=query)

        pda.semantic_query.assert_called_once_with(query)
        pda.build_query_plan.assert_called_once_with(expected_exact_matches)
        mock_revert.assert_called_once_with(expected_query_plan)
        assert errors == {'point2': 'Revert failed'}
        mock_revert.stop()

    # -------------------------
    # Tests for the '_revert' static method
    # -------------------------

    def test__revert_all_success(self):
        """Test the '_revert' method when all revert operations succeed."""

        remote1 = MagicMock(spec=DriverAgent)
        remote2 = MagicMock(spec=DriverAgent)
        point1 = MagicMock(spec=PointNode, identifier='point1')
        point2 = MagicMock(spec=PointNode, identifier='point2')
        query_plan = {remote1: {point1}, remote2: {point2}}

        # Mock revert_point to not raise any exceptions
        remote1.revert_point.return_value = None
        remote2.revert_point.return_value = None

        errors = PlatformDriverAgent._revert(query_plan)

        remote1.revert_point.assert_called_once_with('point1')
        remote2.revert_point.assert_called_once_with('point2')
        assert errors == {}

    def test__revert_with_some_failures(self):
        """Test the '_revert' method when some revert operations fail"""

        remote1 = MagicMock(spec=DriverAgent)
        remote2 = MagicMock(spec=DriverAgent)
        point1 = MagicMock(spec=PointNode, identifier='point1')
        point2 = MagicMock(spec=PointNode, identifier='point2')
        point3 = MagicMock(spec=PointNode, identifier='point3')
        query_plan = {remote1: {point1, point2}, remote2: {point3}}

        # Define side effect functions based on identifier
        def remote1_revert_point_side_effect(identifier):
            if identifier == 'point1':
                return None
            elif identifier == 'point2':
                raise Exception("Revert failed for point2")

        def remote2_revert_point_side_effect(identifier):
            if identifier == 'point3':
                raise Exception("Revert failed for point3")

        remote1.revert_point.side_effect = remote1_revert_point_side_effect
        remote2.revert_point.side_effect = remote2_revert_point_side_effect

        # Act
        errors = PlatformDriverAgent._revert(query_plan)

        # Assert
        expected_errors = {
            'point2': 'Revert failed for point2',
            'point3': 'Revert failed for point3'
        }
        assert errors == expected_errors

    def test__revert_with_empty_query_plan(self):
        """Test the '_revert' method with an empty query_plan"""
        query_plan = {}
        errors = PlatformDriverAgent._revert(query_plan)
        assert errors == {}

    def test__revert_with_multiple_remotes_and_points(self):
        """Test the '_revert' method with multiple remotes and multiple points."""

        remote1 = MagicMock(spec=DriverAgent)
        remote2 = MagicMock(spec=DriverAgent)
        point1 = MagicMock(spec=PointNode, identifier='point1')
        point2 = MagicMock(spec=PointNode, identifier='point2')
        point3 = MagicMock(spec=PointNode, identifier='point3')
        point4 = MagicMock(spec=PointNode, identifier='point4')
        query_plan = {remote1: {point1, point2}, remote2: {point3, point4}}

        # Define side effect functions based on identifier
        def remote1_revert_point_side_effect(identifier):
            if identifier in ['point1', 'point2']:
                return None

        def remote2_revert_point_side_effect(identifier):
            if identifier == 'point3':
                raise Exception("Failed to revert point3")
            elif identifier == 'point4':
                return None

        remote1.revert_point.side_effect = remote1_revert_point_side_effect
        remote2.revert_point.side_effect = remote2_revert_point_side_effect

        errors = PlatformDriverAgent._revert(query_plan)

        expected_errors = {'point3': 'Failed to revert point3'}
        assert errors == expected_errors


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

    now = get_aware_utc_now()

    def test_get_headers_no_optional(self):
        """Test _get_headers with only requester and current time provided."""
        formatted_now = format_timestamp(self.now)
        result = PlatformDriverAgent()._get_headers(requester="test_requester", time=self.now)
        expected = {'time': formatted_now, 'requesterID': "test_requester", 'type': None}
        assert result == expected

    def test_get_headers_with_time(self):
        """Test _get_headers with a custom time provided."""
        custom_time = datetime(2024, 7, 25, 18, 52, 29, 37938)
        formatted_custom_time = format_timestamp(custom_time)
        result = PlatformDriverAgent()._get_headers("test_requester", time=custom_time)
        expected = {'time': formatted_custom_time, 'requesterID': "test_requester", 'type': None}
        assert result == expected

    def test_get_headers_with_task_id(self):
        """Test _get_headers with a task ID provided."""
        task_id = "task123"
        formatted_now = format_timestamp(self.now)
        result = PlatformDriverAgent()._get_headers(requester="test_requester",
                                                    time=self.now,
                                                    task_id=task_id)
        expected = {
            'time': formatted_now,
            'requesterID': "test_requester",
            'taskID': task_id,
            'type': None
        }
        assert result == expected

    def test_get_headers_with_action_type(self):
        """Test _get_headers with an action type provided."""
        action_type = "NEW_SCHEDULE"
        formatted_now = format_timestamp(self.now)
        result = PlatformDriverAgent()._get_headers(requester="test_requester",
                                                    time=self.now,
                                                    action_type=action_type)
        expected = {'time': formatted_now, 'requesterID': "test_requester", 'type': action_type}
        assert result == expected

    def test_get_headers_all_optional(self):
        """Test _get_headers with all optional parameters provided."""
        custom_time = datetime(2024, 7, 25, 18, 52, 29, 37938)
        formatted_custom_time = format_timestamp(custom_time)
        task_id = "task123"
        action_type = "NEW_SCHEDULE"
        result = PlatformDriverAgent()._get_headers(requester="test_requester",
                                                    time=custom_time,
                                                    task_id=task_id,
                                                    action_type=action_type)
        expected = {
            'time': formatted_custom_time,
            'requesterID': "test_requester",
            'taskID': task_id,
            'type': action_type
        }
        assert result == expected


if __name__ == '__main__':
    pytest.main()
