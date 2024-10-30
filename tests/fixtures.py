import argparse
import json
import pathlib
import pytest
import os

from volttron.driver.base.interfaces import BaseInterface
from volttron.driver.base.driver import DriverAgent  # TODO: This should import real DriverAgent from base driver and/or a better mock?
from platform_driver.agent import PlatformDriverAgent
from volttron.types.server_config import ServerConfig
from unittest.mock import MagicMock, Mock, patch

@pytest.fixture
def driver_agent():
    return DriverAgent(None, {}, ('some', 'unique', 'id'))

@pytest.fixture
def equipped_driver_service(driver_service):
    pds = driver_service
    pds.config_version = 2
    pds._get_or_create_remote = lambda x, y: driver_agent

    topic = 'devices/Foo/Bar/Baz'
    contents = {'driver_type': DummyInterface}
    pds._configure_new_equipment(topic, 'NEW', contents)

@pytest.fixture
def driver_service():
    # Set up mock ServerConfig:
    parser = argparse.ArgumentParser()
    parser.set_defaults(volttron_publickey='DEADBEEF')
    opts = parser.parse_args([])
    server_config = ServerConfig()
    server_config.opts = opts

    # Instantiate PlatformDriverAgent:
    pds = PlatformDriverAgent(server_config)
    assert isinstance(pds, PlatformDriverAgent)
    pds.interface_classes = {'TestInterface': DummyInterface()}
    return pds


class DummyInterface(BaseInterface):
    def configure(self, config_dict, registry_config_str):
        pass

    def get_point(self, point_name, **kwargs):
        pass

    def set_point(self, point_name, value, **kwargs):
        pass

    def scrape_all(self):
        pass

    def revert_all(self, **kwargs):
        pass

    def revert_point(self, point_name, **kwargs):
        pass

    @classmethod
    def unique_remote_id(cls, equipment_name, config, **kwargs):
        return 'some', 'unique', 'id'

@pytest.fixture(autouse=True)
def set_agent_identity():
    os.environ["AGENT_VIP_IDENTITY"] = "test_identity"


@pytest.fixture
def base_PDA():
    # Set the required environment variable for AGENT_VIP_IDENTITY
    os.environ["AGENT_VIP_IDENTITY"] = "test_identity"

    with patch('volttron.client.decorators.get_core_builder') as mock_get_core_builder, \
            patch(
                'volttron.types.auth.auth_credentials.CredentialsFactory.load_credentials_from_file') as mock_load_credentials:
        # Mock core with necessary attributes
        mock_core = MagicMock()
        mock_core._annotations = {"__rpc__.exports": set()}
        mock_get_core_builder.return_value.build.return_value = mock_core

        # Mock credentials to bypass file loading
        mock_credentials = MagicMock()
        mock_credentials.identity = "test_identity"
        mock_credentials.publickey = "test_public_key"
        mock_load_credentials.return_value = mock_credentials

        # Initialize the agent and add frequently used mocks
        PDA = PlatformDriverAgent()
        PDA.core = mock_core
        PDA.vip = MagicMock()
        PDA.vip.pubsub.publish = MagicMock()
        PDA._push_result_topic_pair = Mock()
        PDA.equipment_tree = MagicMock()
        PDA.equipment_tree.root = "root"

        return PDA