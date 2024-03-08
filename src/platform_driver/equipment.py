import logging

from datetime import datetime
from enum import Enum
from treelib.exceptions import DuplicatedNodeIdError
from typing import Optional, Union

from volttron.lib.topic_tree import TopicNode, TopicTree
from volttron.driver.base.driver import DriverAgent
from volttron.utils import get_aware_utc_now, setup_logging

from .overrides import OverrideError
from .reservations import ReservationLockError

setup_logging()
_log = logging.getLogger(__name__)


class EquipmentNode(TopicNode):
    def __init__(self, config=None, *args, **kwargs):
        super(EquipmentNode, self).__init__(*args, **kwargs)
        config = config if config is not None else {}
        self.data['active']: bool = config.get('active', True)
        self.data['config'] = config
        self.data['meta_data']: dict = config.get('meta_data', {})
        self.data['polling_interval']: float = config.get('polling_interval', 0)
        self.data['segment_type'] = 'TOPIC_SEGMENT'

    @property
    def active(self) -> bool:
        # TODO: Make this inherit from parents.
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

    def get_controller(self, tree):
        if self.is_device():
            return self.data['interface']
        elif not self.is_root():
            return tree.get_node(self.predecessor(tree.identifier)).get_controller(tree.identifier)
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

    def is_point(self):
        return True if self.segment_type == 'POINT' else False

    def is_device(self):
        return True if self.segment_type == 'DEVICE' else False

    @property
    def overridden(self):
        # TODO: Implement overridden property (Override handling)
        #  -- include parents.
        return False

    def reserved(self,  sender):
        # TODO: Implement reserved(sender)
        #  -- check that the topic is not reserved by someone other than sender.
        #  -- include parents.
        is_reserved = False
        holder = ''
        return is_reserved, holder


class DeviceNode(EquipmentNode):
    def __init__(self, config, driver, *args, **kwargs):
        super(DeviceNode, self).__init__(config, *args, **kwargs)
        self.data['interface']: DriverAgent = driver  # TODO: Should this just be the interface?
        self.data['segment_type'] = 'DEVICE'

    @property
    def interface(self) -> DriverAgent:
        return self.data['interface']

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
        if node.is_device():
            node.stop_device()
        if node.has_concrete_successors(self): # TODO: Implement EquipmentNode.has_concrete_successors().
            node.wipe_configuration()  # TODO: Implement EquipmentNode.wipe_configuration().
        else:
            self.remove_node(node.identifier) # Removes node and the subtree below.

    def points(self, nid=None):
        if nid is None:
            points = [n for n in self._nodes.values() if n.is_point()]
        else:
            points = [self[n] for n in self.expand_tree(nid) if self[n].is_point()]
        return points

    def devices(self, nid=None):
        if nid is None:
            points = [n for n in self._nodes.values() if n.is_device()]
        else:
            points = [self[n] for n in self.expand_tree(nid) if self[n].is_device()]
        return points

    def raise_on_locks(self, node: EquipmentNode, requester: str):
        reserved, holder = node.reserved(requester)
        if reserved and not node.identifier == holder:
            raise ReservationLockError(f"Equipment {node.identifier} is reserved by another party."
                                       f" ({requester}) does not have permission to write at this time.")
        elif not reserved and self.reservation_required_for_write:
            raise ReservationLockError(f'Caller ({requester}) does not have a reservation '
                                       f'for equipment {node.identifier}. A reservation is required to write.')
        elif node.overridden:
            raise OverrideError(f"Cannot set point on {node.identifier} since global override is set")


# TODO: Probably remove this block entirely.
# ControllerGroupingType = Enum('node_type', ['Parallel', 'Sequential', 'Serial'])
#
# class DuplicateControllerError(Exception):
#     """Exception thrown if Controller is already attached to another group."""
#     pass
#
#
# class ControllerNode(Node):
#     def __init__(self,
#                  group_id: str = None,
#                  grouping_type: str = 'Sequential',
#                  minimum_offset: float = 0.0,
#                  *args, **kwargs):
#         identifier = kwargs.pop('identifier', group_id)
#         super(ControllerNode, self).__init__(tag=group_id, identifier=identifier, *args, **kwargs)
#         self.type = ControllerGroupingType[grouping_type.title()]
#         self.minimum_offset = minimum_offset
#         self.controllers = WeakSet()
#
#
# class ControllerTree(Tree):
#     def __init__(self,
#                  config: dict = None,
#                  legacy_group_offset_interval: float = 0.0,
#                  minimum_polling_interval: float = 0.02,
#                  *args, **kwargs):
#         super(ControllerTree, self).__init__(node_class=ControllerNode, *args, **kwargs)
#         config = config if config else {
#             'minimum_offset': legacy_group_offset_interval,
#             'children': [{'group_id': '0', 'minimum_offset': minimum_polling_interval}]
#         }
#         children = config.pop('children', [])
#         identifier = config.pop('identifier', config.pop('group_id', 'controllers'))
#         self.add_node(ControllerNode(identifier=identifier, **config))
#         if children:
#             self._add_children(children, self.get_node(self.root))
#
#     def _add_children(self, config: list, parent: ControllerNode):
#         for child in config:
#             child_group_id = child.get('group_id')
#             nid = '/'.join([parent.identifier, child_group_id]) if child_group_id else None
#             grandchildren = child.pop('children', [])
#             self.add_node(ControllerNode(identifier=nid, **child), parent)
#             if grandchildren:
#                 self._add_children(grandchildren, self.get_node(nid))
#
#     def add_controller(self, controller: DriverAgent, controller_group_id: str,
#                        allow_duplicate_controllers: bool = False):
#         if not allow_duplicate_controllers:
#             for n in self.all_nodes():
#                 if controller in n.controllers and n.group_id != controller_group_id:
#                     err = f'Controller already exists in group: {n.identifier} but duplication is not allowed.'
#                     raise DuplicatedNodeIdError(err)
#         self.get_node(controller_group_id).controllers.add(controller)
#
#     def remove_controller(self, controller: DriverAgent, nid: str):
#         self.get_node(nid).controllers.remove(controller)
#
#     def schedule_polling(self):
#         pass
