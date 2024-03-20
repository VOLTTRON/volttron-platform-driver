import argparse
import json
import pathlib
import pytest

from volttron.driver.base.interfaces import BaseInterface
from volttron.services.driver.data_structures import RemoteTree
from volttron.services.driver.platform_driver_service import DriverAgent  # TODO: This should import real DriverAgent from base driver and/or a better mock?
from volttron.services.driver.platform_driver_service import PlatformDriverService
from volttron.types.server_config import ServerConfig


@pytest.fixture
def remote_tree():
    config_file = pathlib.Path(__file__).parent.parent.absolute() / 'sample_configs/remote_grouping.json'
    with open(config_file) as f:
        config = json.load(f)
        return RemoteTree(config)

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

    # Instantiate PlatformDriverService:
    pds = PlatformDriverService(server_config)
    assert isinstance(pds, PlatformDriverService)
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
