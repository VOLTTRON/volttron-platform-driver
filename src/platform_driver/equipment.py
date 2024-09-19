import gevent
import json
import logging

from datetime import datetime
from treelib.exceptions import DuplicatedNodeIdError
from typing import Iterable, Optional, Union
from weakref import WeakValueDictionary

from volttron.client.known_identities import CONFIGURATION_STORE
from volttron.driver.base.driver import DriverAgent
from volttron.driver.base.config import DataSource, DeviceConfig, EquipmentConfig
from volttron.lib.topic_tree import TopicNode, TopicTree
from volttron.utils import get_aware_utc_now, setup_logging

from .overrides import OverrideError
from .reservations import ReservationLockError

setup_logging()
_log = logging.getLogger(__name__)


class EquipmentNode(TopicNode):
    def __init__(self, config=None, *args, **kwargs):
        super(EquipmentNode, self).__init__(*args, **kwargs)
        self.data['config'] = config if config is not None else EquipmentConfig()
        self.data['remote'] = None
        # TODO: should ephemeral values like overridden and reserved_by be properties stored in data?
        self.data['overridden']: bool = False
        self.data['reserved_by'] = None
        self.data['segment_type'] = 'TOPIC_SEGMENT'

    @property
    def active(self) -> bool:
        # TODO: Make this inherit from parents or use et.rsearch when accessing it.
        return self.data['config'].active

    @active.setter
    def active(self, value: bool):
        self.data['config'].active = value

    @property
    def config(self) -> EquipmentConfig:
        return self.data['config']

    @config.setter
    def config(self, value: dict):
        self.data['config'] = value

    @property
    def group(self) -> int:
        return self.data['config'].group

    @property
    def meta_data(self) -> dict:
        return self.data['config'].meta_data
    # TODO: How does this fit with the metadata in the interface used for registers (points)?
    @meta_data.setter
    def meta_data(self, value: dict):
        self.data['config'].meta_data = value

    @property
    def polling_interval(self) -> float:
        # TODO: Should this be a property that inherits from parents?
        return self.data['config'].polling_interval

    @polling_interval.setter
    def polling_interval(self, value: float):
        self.data['config'].polling_interval = value

    @property
    def is_point(self) -> bool:
        return True if self.segment_type == 'POINT' else False

    @property
    def is_device(self) -> bool:
        return True if self.segment_type == 'DEVICE' else False

    @property
    def overridden(self) -> bool:
        return self.data['overridden']

    @overridden.setter
    def overridden(self, value: bool):
        self.data['overridden'] = value
        
    @property
    def publish_single_depth(self) -> bool:
        return self.data['config'].publish_single_depth

    @property
    def publish_single_breadth(self) -> bool:
        return self.data['config'].publish_single_breadth

    @property
    def publish_multi_depth(self) -> bool:
        return self.data['config'].publish_multi_depth

    @property
    def publish_multi_breadth(self) -> bool:
        return self.data['config'].publish_multi_breadth
    
    @property
    def publish_all_depth(self) -> bool:
        return self.data['config'].publish_all_depth

    @property
    def publish_all_breadth(self) -> bool:
        return self.data['config'].publish_all_breadth

    @property
    def reservation_required_for_write(self) -> bool:
        return self.data['config'].reservation_required_for_write

    @reservation_required_for_write.setter
    def reservation_required_for_write(self, value: bool):
        self.data['config'].reservation_required_for_write = value

    @property
    def reserved(self) -> str:
        return self.data['reserved_by']

    @reserved.setter
    def reserved(self, holder: str):
        self.data['reserved_by'] = holder

class DeviceNode(EquipmentNode):
    def __init__(self, config, driver, *args, **kwargs):
        config = config.copy()
        super(DeviceNode, self).__init__(config, *args, **kwargs)
        self.data['remote']: DriverAgent = driver
        self.data['registry_name'] = None
        self.data['segment_type'] = 'DEVICE'

    @property
    def all_publish_interval(self) -> float:
        return self.data['config'].all_publish_interval

    @property
    def remote(self) -> DriverAgent:
        return self.data['remote']

    @property
    def registry_name(self) -> str:
        return self.data['registry_name']

    def stop_device(self):
        _log.info(f"Stopping driver: {self.identifier}")
        try:
            self.remote.core.stop(timeout=5.0)
        except Exception as e:
            _log.error(f"Failure during {self.identifier} driver shutdown: {e}")


class PointNode(EquipmentNode):
    def __init__(self, config, *args, **kwargs):
        super(PointNode, self).__init__(config, *args, **kwargs)
        self.data['last_value']: any = None
        self.data['last_updated']: Optional[datetime] = None
        self.data['segment_type'] = 'POINT'
        # self._stale = True

    @property
    def data_source(self) -> DataSource:
        return self.data['config'].data_source

    @data_source.setter
    def data_source(self, value: Union[str, int, DataSource]):
        if isinstance(value, DataSource | str):
            self.data['config'].data_source = value
        else:
            raise ValueError(f'Data source must be a DataSource or a string in: {list(DataSource.__members__.keys())}.')

    @property
    def last_value(self) -> any:
        return self.data['last_value']

    @last_value.setter
    def last_value(self, value: any):
        self.data['last_value'] = value
        self.data['last_updated'] = get_aware_utc_now()

    @property
    def last_updated(self) -> datetime:
        return self.data['last_updated']

    @property
    def stale(self) -> bool:
        if self.active is False:
            return False
        elif self.data['config'].stale_timeout is None:
            return False
        else:
            return True if get_aware_utc_now() - self.last_updated > self.data['config'].stale_timeout else False


class EquipmentTree(TopicTree):
    def __init__(self, agent, *args, **kwargs):
        super(EquipmentTree, self).__init__(root_name=agent.config.depth_first_base, node_class=EquipmentNode,
                                            *args, **kwargs)
        self.agent = agent
        self.remotes = WeakValueDictionary()

        root_config = self[self.root].data['config']
        root_config.group = 0
        root_config.polling_interval = agent.config.default_polling_interval
        root_config.publish_single_depth = agent.config.publish_single_depth
        root_config.publish_single_breadth = agent.config.publish_single_breadth
        root_config.publish_multi_depth = agent.config.publish_multi_depth
        root_config.publish_multi_breadth = agent.config.publish_multi_breadth
        root_config.publish_all_depth = agent.config.publish_all_depth
        root_config.publish_all_breadth = agent.config.publish_all_breadth

    def _set_registry_name(self, nid):
        # TODO: This method should be unnecessary, if we can just get the registry_name in the config_store push.
        #  The registry name itself was not available at configuration time
        #   and is not returned by the self.config.get() method ( it is dereferenced, already).
        remote_conf = {}
        try:
            remote_conf_json = self.agent.vip.rpc.call(CONFIGURATION_STORE, 'manage_get',self.agent.core.identity,
                                                        nid).get(timeout=5)
            remote_conf = json.loads(remote_conf_json)
        except (Exception, gevent.Timeout) as e:
            _log.warning(f'Unable to get registry_name for device: {nid} -- {e}')
        finally:
            return remote_conf.get('registry_config')

    def add_device(self, device_topic: str, config: DeviceConfig, driver_agent: DriverAgent,
                   registry_config: list[dict]):
        """
        Add Device
        Adds a device node to the equipment tree. Also adds any necessary ancestor topic nodes and child point nodes.
        Returns a reference to the device node.
        """
        # Set up ancestor nodes.
        ancestral_topic = device_topic.split('/')
        device_name = ancestral_topic.pop()
        parent = self.add_segment('/'.join(ancestral_topic))

        # Set up the device node itself.
        try:
            device_node = DeviceNode(config=config, driver=driver_agent, tag=device_name, identifier=device_topic)
            device_node.data['registry_name'] = self._set_registry_name(device_node.identifier)
            self.add_node(device_node, parent=parent)
        except DuplicatedNodeIdError:
            # TODO: If the node already exists, update it as necessary?
            device_node = self.get_node(device_topic)

        # Set up any point nodes which are children of this device.
        for reg in registry_config:
            # If there are fields in device config for all registries, add them where they are not overridden:
            for k, v in config.equipment_specific_fields.items():
                if not reg.get(k):
                    reg[k] = v
            point_config = self.agent.interface_classes[driver_agent.config.driver_type].config_class(**reg)
            try:
                node = PointNode(config=point_config, tag=point_config.volttron_point_name,
                                 identifier=('/'.join([device_topic, point_config.volttron_point_name])))
                self.add_node(node, parent=device_topic)
            except DuplicatedNodeIdError:
                _log.warning(f'Duplicate Volttron Point Name "{point_config.volttron_point_name}" on {device_topic}.'
                             f'Duplicate register will not be created. Please ensure ')
        return device_node

    def add_segment(self, topic: str, config: EquipmentConfig = None):
        topic = topic.split('/')
        if topic[0] == self.root:
            topic.pop(0)
        parent = self.root
        nid, node = self.root, None
        # Set up node after setting up any missing ancestors.
        for segment in topic:
            nid = '/'.join([parent, segment])
            try:
                node = EquipmentNode(tag=segment, identifier=nid, config={})
                self.add_node(node, parent)  # TODO: This does raise the DuplicatedNodeIdError, not just replace, right?
            except DuplicatedNodeIdError:
                # TODO: How to handle updates if this node is the intended target?
                pass  # We are not creating nor updating this node, which already exists.
            parent = nid
        if node and config:
            node.config = config
        return nid

    def remove_segment(self, identifier: str):
        node = self.get_node(identifier)
        if node.is_device:
            node.stop_device()
        if node.has_concrete_successors(self): # TODO: Implement EquipmentNode.has_concrete_successors().
            node.wipe_configuration()  # TODO: Implement EquipmentNode.wipe_configuration().
        else:
            self.remove_node(node.identifier) # Removes node and the subtree below.

    def points(self, nid: str = None) -> Iterable[PointNode]:
        if nid is None:
            points = [n for n in self._nodes.values() if n.is_point]
        else:
            points = [self[n] for n in self.expand_tree(nid) if self[n].is_point]
        return points

    def devices(self, nid: str = None) -> Iterable[DeviceNode]:
        if nid is None:
            devices = [n for n in self._nodes.values() if n.is_device]
        else:
            devices = [self[n] for n in self.expand_tree(nid) if self[n].is_device]
        return devices

    def find_points(self, topic_pattern: str = '', regex: str = None, exact_matches: Iterable = None
                    ) -> Iterable[PointNode]:
        return (p for p in self.find_leaves(topic_pattern, regex, exact_matches) if p.is_point)

    def raise_on_locks(self, node: EquipmentNode, requester: str):
        reserved = next(self.rsearch(node.identifier, lambda n: n.reserved))
        if reserved and not node.identifier == reserved:
            raise ReservationLockError(f"Equipment {node.identifier} is reserved by another party."
                                       f" ({requester}) does not have permission to write at this time.")
        elif not reserved and any(self.rsearch(node.identifier, lambda n: n.reservation_required_for_write)):
            raise ReservationLockError(f'Caller ({requester}) does not have a reservation '
                                       f'for equipment {node.identifier}. A reservation is required to write.')
        elif any(self.rsearch(node.identifier, lambda n: n.overridden)):
            raise OverrideError(f"Cannot set point on {node.identifier} since global override is set")
        
    def get_device_node(self, nid: str) -> DeviceNode:
        return self.get_node(next(self.rsearch(nid, lambda n: n.is_device)))

    def get_remote(self, nid: str) -> DriverAgent:
        return self.get_device_node(nid).remote

    def get_group(self, nid: str) -> int:
        return self[next(self.rsearch(nid, lambda n: n.group is not None))].group

    def get_point_topics(self, nid: str) -> tuple[str, str]:
        return nid, '/'.join([self.agent.config.breadth_first_base] + list(reversed(nid.split('/')[1:])))

    def get_device_topics(self, nid: str) -> tuple[str, str]:
        return self.get_point_topics(self.get_device_node(nid).identifier)

    def get_polling_interval(self, nid: str) -> float:
        return self[next(self.rsearch(nid, lambda n: n.polling_interval is not None))].polling_interval

    def is_published_single_depth(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.publish_single_depth is not None))].publish_single_depth
    
    def is_published_single_breadth(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.publish_single_breadth is not None))].publish_single_breadth

    def is_published_multi_depth(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.publish_multi_depth is not None))].publish_multi_depth

    def is_published_multi_breadth(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.publish_multi_breadth is not None))].publish_multi_breadth

    def is_published_all_depth(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.publish_all_depth is not None))].publish_all_depth

    def is_published_all_breadth(self, nid: str) -> bool:
        return self[next(self.rsearch(nid, lambda n: n.publish_all_breadth is not None))].publish_all_breadth

    def is_stale(self, nid: str) -> bool:
        return any(p.stale for p in self.points(nid))

    def update_stored_registry_config(self, nid: str):
        device_node = self.equipment_tree.get_device_node(nid)
        registry = [p.config for p in self.points(device_node.nid)]
        if device_node.registry_name:
            self.agent.vip.config.set(device_node.registry_name, registry)