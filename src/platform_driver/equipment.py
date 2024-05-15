import gevent
import json
import logging

from datetime import datetime
from enum import Enum
from treelib.exceptions import DuplicatedNodeIdError
from typing import Iterable, Optional, Union

from volttron.client.known_identities import CONFIGURATION_STORE
from volttron.driver.base.driver import DriverAgent
from volttron.lib.topic_tree import TopicNode, TopicTree
from volttron.utils import get_aware_utc_now, setup_logging

from .overrides import OverrideError
from .reservations import ReservationLockError

setup_logging()
_log = logging.getLogger(__name__)


class EquipmentNode(TopicNode):
    def __init__(self, config=None, *args, **kwargs):
        super(EquipmentNode, self).__init__(*args, **kwargs)
        config = config if config is not None else {}
        self.data['active']: bool = config.get('active', config.get('Active', True))
        self.data['config'] = config
        self.data['interface'] = None
        self.data['meta_data']: dict = config.get('meta_data', {})
        # TODO: should ephemeral values like overridden and reserved_by be properties stored in data?
        self.data['overridden']: bool = False
        self.data['polling_interval']: float = config.get('polling_interval', 0)
        self.data['reservation_required_for_write']: bool = config.get('reservation_required_for_write', False)
        self.data['reserved_by'] = None
        self.data['segment_type'] = 'TOPIC_SEGMENT'

    @property
    def active(self) -> bool:
        # TODO: Make this inherit from parents or use et.rsearch when accessing it.
        return self.data['active']

    @active.setter
    def active(self, value: bool):
        self.data['active'] = value

    @property
    def config(self) -> dict:
        return self.data['config']

    @config.setter
    def config(self, value: dict):
        self.data['config'] = value

    # TODO: Consider replacing uses of this with et.get_remote(nid).
    def get_remote(self, tree):
        if self.is_device:
            return self.data['interface']
        elif not self.is_root():
            return tree.get_node(self.predecessor(tree.identifier)).get_remote(tree.identifier)
        else:
            return None

    @property
    def meta_data(self) -> dict:
        return self.data['meta_data']

    @meta_data.setter
    def meta_data(self, value: dict):
        self.data['meta_data'] = value

    @property
    def polling_interval(self) -> float:
        # TODO: Should this be a property that inherits from parents?
        return self.data['polling_interval']

    @polling_interval.setter
    def polling_interval(self, value: float):
        self.data['polling_interval'] = value

    @property
    def is_point(self):
        return True if self.segment_type == 'POINT' else False

    @property
    def is_device(self):
        return True if self.segment_type == 'DEVICE' else False

    @property
    def overridden(self) -> bool:
        return self.data['overridden']

    @overridden.setter
    def overridden(self, value: bool):
        self.data['overridden'] = value

    @property
    def reservation_required_for_write(self) -> bool:
        return self.data['reservation_required_for_write']

    @reservation_required_for_write.setter
    def reservation_required_for_write(self, value: bool):
        self.data['reservation_required_for_write'] = value

    @property
    def reserved(self):
        return self.data['reserved_by']

    @reserved.setter
    def reserved(self, holder: str):
        self.data['reserved_by'] = holder

class DeviceNode(EquipmentNode):
    def __init__(self, config, driver, *args, **kwargs):
        config = config.copy()
        super(DeviceNode, self).__init__(config, *args, **kwargs)
        self.data['interface']: DriverAgent = driver  # TODO: Should this just be the interface?
        self.data['registry']: dict = config.pop('registry_config', [])
        self.data['registry_name'] = None
        self.data['segment_type'] = 'DEVICE'

    @property
    def interface(self) -> DriverAgent:
        return self.data['interface']

    @property
    def registry(self) -> dict:
        return self.data['registry']

    def set_registry_name(self):
        # TODO: This method should be unnecessary, if we can just get the registry_name in the config_store push.
        #  The registry name itself was not available at configuration time
        #   and is not returned by the self.config.get() method ( it is dereferenced, already).
        try:
            remote_conf_json = self.interface.parent.vip.rpc.call(CONFIGURATION_STORE, 'manage_get',
                                                             self.interface.parent.core.identity, self.identifier
                                                             ).get(timeout=5)
            remote_conf = json.loads(remote_conf_json)
            self.data['registry_name'] = remote_conf.get('registry_config')
        except (Exception, gevent.Timeout) as e:
            _log.warning(f'Unable to set registry_name for device: {self.identifier} -- {e}')

    def update_registry_row(self, row: dict):
        self.data['registry'] = [row if r['Volttron Point Name'] == row['Volttron Point Name'] else r
                                 for r in self.data['registry']]
        if self.data['registry_name']:
            self.interface.parent.vip.config.set(self.data['registry_name'], self.registry)

    def stop_device(self):
        _log.info(f"Stopping driver: {self.identifier}")
        try:
            self.interface.core.stop(timeout=5.0)
        except Exception as e:
            _log.error(f"Failure during {self.identifier} driver shutdown: {e}")


# TODO: Add data source types.
DataSource = Enum('data_source', ['SHORT_POLL'])


class PointNode(EquipmentNode):
    def __init__(self, config, *args, **kwargs):
        super(PointNode, self).__init__(config, *args, **kwargs)
        self.data['data_source']: DataSource = DataSource[
            "short_poll".upper()]  # TODO: Not a generally useful setting like this.
        self.data['last_value']: any = None
        self.data['last_updated']: Optional[datetime] = None
        self.data['segment_type'] = 'POINT'

    @property
    def data_source(self) -> DataSource:
        return self.data['data_source']

    @data_source.setter
    def data_source(self, value: Union[str, int, DataSource]):
        if isinstance(value, DataSource):
            self.data['data_source'] = value
        elif isinstance(value, str):
            self.data['data_source'] = DataSource['value']
        elif isinstance(value, int):
            self.data['data_source'] = DataSource(value)
        else:
            raise ValueError('Data source must be a DataSource, integer or string.')

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


class EquipmentTree(TopicTree):
    def __init__(self, root_name='devices', *args, **kwargs):
        super(EquipmentTree, self).__init__(root_name=root_name, node_class=EquipmentNode, *args, **kwargs)

    def add_device(self, device_topic, config, driver_agent):
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
        registry_config = config.get('registry_config', [])
        equipment_specific_fields = config.get('equipment_specific_fields', {})
        try:
            device_node = DeviceNode(config=config, driver=driver_agent, tag=device_name, identifier=device_topic)
            device_node.set_registry_name()
            self.add_node(device_node, parent=parent)
        except DuplicatedNodeIdError:
            # TODO: If the node already exists, update it as necessary?
            device_node = self.get_node(device_topic)

        # Set up any point nodes which are children of this device.
        for reg in registry_config:
            # If there are fields in device config for all registries, add them where they are not overridden:
            for k, v in equipment_specific_fields.items():
                if not reg.get(k):
                    reg[k] = v
            try:
                point_name = reg['Volttron Point Name']
                nid = '/'.join([device_topic, point_name])
                node = PointNode(config=reg, tag=point_name, identifier=nid)
                self.add_node(node, parent=device_topic)
            except DuplicatedNodeIdError:
                pass  # TODO: Should we warn if we somehow have an existing point on a new device?
        return device_node

    def stop_device(self):
        # TODO: Intended for
        pass

    def add_segment(self, topic, config=None):
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

    def remove_segment(self, identifier):
        node = self.get_node(identifier)
        if node.is_device:
            node.stop_device()
        if node.has_concrete_successors(self): # TODO: Implement EquipmentNode.has_concrete_successors().
            node.wipe_configuration()  # TODO: Implement EquipmentNode.wipe_configuration().
        else:
            self.remove_node(node.identifier) # Removes node and the subtree below.

    def points(self, nid=None):
        if nid is None:
            points = [n for n in self._nodes.values() if n.is_point]
        else:
            points = [self[n] for n in self.expand_tree(nid) if self[n].is_point]
        return points

    def devices(self, nid=None):
        if nid is None:
            devices = [n for n in self._nodes.values() if n.is_device]
        else:
            devices = [self[n] for n in self.expand_tree(nid) if self[n].is_device]
        return devices

    def find_points(self, topic_pattern: str = '', regex: str = None, exact_matches: Iterable = None) -> Iterable:
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
        return self.get_device_node(nid).interface


# TODO: Probably remove this block entirely.
# RemoteGroupingType = Enum('node_type', ['Parallel', 'Sequential', 'Serial'])
#
# class DuplicateRemoteError(Exception):
#     """Exception thrown if Remote is already attached to another group."""
#     pass
#
#
# class RemoteNode(Node):
#     def __init__(self,
#                  group_id: str = None,
#                  grouping_type: str = 'Sequential',
#                  minimum_offset: float = 0.0,
#                  *args, **kwargs):
#         identifier = kwargs.pop('identifier', group_id)
#         super(RemoteNode, self).__init__(tag=group_id, identifier=identifier, *args, **kwargs)
#         self.type = RemoteGroupingType[grouping_type.title()]
#         self.minimum_offset = minimum_offset
#         self.remotes = WeakSet()
#
#
# class RemoteTree(Tree):
#     def __init__(self,
#                  config: dict = None,
#                  legacy_group_offset_interval: float = 0.0,
#                  minimum_polling_interval: float = 0.02,
#                  *args, **kwargs):
#         super(RemoteTree, self).__init__(node_class=RemoteNode, *args, **kwargs)
#         config = config if config else {
#             'minimum_offset': legacy_group_offset_interval,
#             'children': [{'group_id': '0', 'minimum_offset': minimum_polling_interval}]
#         }
#         children = config.pop('children', [])
#         identifier = config.pop('identifier', config.pop('group_id', 'remotes'))
#         self.add_node(RemoteNode(identifier=identifier, **config))
#         if children:
#             self._add_children(children, self.get_node(self.root))
#
#     def _add_children(self, config: list, parent: RemoteNode):
#         for child in config:
#             child_group_id = child.get('group_id')
#             nid = '/'.join([parent.identifier, child_group_id]) if child_group_id else None
#             grandchildren = child.pop('children', [])
#             self.add_node(RemoteNode(identifier=nid, **child), parent)
#             if grandchildren:
#                 self._add_children(grandchildren, self.get_node(nid))
#
#     def add_remote(self, remote: DriverAgent, remote_group_id: str,
#                        allow_duplicate_remotes: bool = False):
#         if not allow_duplicate_remotes:
#             for n in self.all_nodes():
#                 if remote in n.remotes and n.group_id != remote_group_id:
#                     err = f'Remote already exists in group: {n.identifier} but duplication is not allowed.'
#                     raise DuplicatedNodeIdError(err)
#         self.get_node(remote_group_id).remotes.add(remote)
#
#     def remove_remote(self, remote: DriverAgent, nid: str):
#         self.get_node(nid).remotes.remove(remote)
#
#     def schedule_polling(self):
#         pass
