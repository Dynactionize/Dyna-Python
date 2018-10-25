from ..dynagatewaytypes.instance_pb2 import (
    CreateInstanceReq, GetInstanceReq, UpdateInstanceReq, DeleteInstanceReq
)
from ..dynagatewaytypes.instance_pb2 import InstanceBatch, BatchCreateInstancesReq
from ..dynagatewaytypes.instance_pb2 import InstanceRes
from ..dynagatewaytypes.general_types_pb2 import Value, ValueList
from ..dynagatewaytypes.query_pb2 import InstanceFetchRes, InstanceGroupFetchRes
from ..dynagatewaytypes.query_pb2 import InstanceGroupedFetchRes

from .action import Action
from .topology import Topology
from .value import Value
from .auth import Token
from typing import Sequence


class Instance:
    def __init__(self, action: Action=None, topology: Topology=None, id=None,
                 values: Sequence[Value]=[]):
        self._action = action
        self._topology = topology
        self._id = id
        self._values = values

    def log(self):
        actionname = ""
        if not self._action is None and not self._action.get_name() is None:
            actionname = self._action.get_name()

        topologysequence = ""
        if not self._topology is None and len(self._topology.get_sequence()) > 2:
            topologysequence = "".join(self._topology.get_sequence())
        print("INSTANCE({0} - {1}): [{3}]".format(actionname, topologysequence,
                                                  ','.join(map(str, self._values))))

    def set_id(self, id: int):
        self._id = id

    def get_id(self):
        return self._id

    def add_value(self, value: Value):
        self._values.append(value)

    def extend_values(self, values: Sequence[Value]):
        self._values.extend(values)

    def set_value(self, index: int, value: Value):
        self._values[index] = value

    def get_values(self) -> Sequence[Value]:
        return self._values

    def set_action(self, action: Action):
        self._action = action

    def get_action(self) -> Action:
        return self._action

    def set_topology(self, topology: Topology):
        self._topology = topology

    def get_topology(self) -> Topology:
        return self._topology

    @staticmethod
    def from_instance_response(resp: InstanceRes):
        action = None
        topology = None

        if resp.action_id != 0:
            action = Action(id=resp.action_id)

        if resp.topology_id != 0:
            topology = Topology(id=resp.topology_id)

        return Instance(action, topology, resp.id, list(map(Value._from_pb, resp.values.values)))

    @staticmethod
    def from_query_fetch(resp: InstanceFetchRes):
        res = []
        for inst in resp.instances:
            res.append(Instance(inst.id, values=list(map(Value._from_pb, inst.values))))
        return res

    def create_request(self, token: Token) -> CreateInstanceReq:
        if self._action is None:
            raise RuntimeError("No action provided")
        if self._topology is None:
            raise RuntimeError("No topology provided")

        pb = CreateInstanceReq()
        if not token is None:
            pb.token = token._to_message()

        if not self._action.get_id() is None:
            pb.action_id = self._action.get_id()
        else:
            pb.name = self._action.get_name()

        if not self._topology.get_id() is None:
            pb.topology_id = self._topology.get_id()
        else:
            pb.sequence = self._topology.get_sequence()

        vl = ValueList()
        vl.values.extend(list(map(Value._value_to_pb, self._values)))
        pb.values.extend([vl])

        return pb

    def get_request(self, token: Token, return_action_id: bool=False, return_topology_id: bool=False) -> GetInstanceReq:
        if self._id is None:
            raise RuntimeError("No instance id provided")

        pb = GetInstanceReq()
        if not token is None:
            pb.token = token._to_message()

        pb.id = self._id
        pb.return_action_id = return_action_id
        pb.return_topology_id = return_topology_id
        return pb

    def update_request(self, token: Token) -> UpdateInstanceReq:
        if self._id is None:
            raise RuntimeError("No instance id provided")

        pb = UpdateInstanceReq()
        if not token is None:
            pb.token = token._to_message()

        pb.id = self._id
        vl = ValueList()
        vl.values.extend(list(map(Value._value_to_pb, self._values)))
        pb.values.extend([vl])

        return pb

    def delete_request(self, token: Token) -> DeleteInstanceReq:
        if self._id is None:
            raise RuntimeError("No instance id provided")

        pb = DeleteInstanceReq()
        if not token is None:
            pb.token = token._to_mesage()

        pb.id = self._id
        return pb


class InstanceBatches:
    def __init__(self, instances: Sequence[Instance]=[]):
        self._instances = instances

    def add_instance(self, instance: Instance):
        self._instances.append(instance)

    def extend_instances(self, instances: Sequence[Instance]):
        self._instances.extend(instances)

    def set_instance(self, index: int, instance: Instance):
        self._instances[index] = instance

    def get_instances(self) -> Sequence[Instance]:
        return self._instances

    def batches(self, token: Token) -> Sequence[BatchCreateInstancesReq]:
        # Group by action/topology
        dct = {}
        for inst in self._instances:
            if inst.get_action() is None or (
                    inst.get_action().get_id() is None and inst.get_action().get_name() is None):
                raise RuntimeError("No valid action set for instance")
            if inst.get_topology() is None or (
                    inst.get_topology().get_id() is None and len(
                        inst.get_topology().get_sequence()) < 2):
                raise RuntimeError("No valid topoloy set for")

            act_key = ""
            top_key = ""

            if inst.get_action().get_id() is None:
                act_key = inst.get_action().get_name()
            else:
                act_key = inst.get_action().get_id()

            if inst.get_topology().get_id() is None:
                top_key = ','.join(inst.get_topology().get_sequence())
            else:
                top_key = inst.get_topology().get_id()

            key = "{0}/{1}".format(act_key, top_key)
            batch = dct.get(key)
            if batch is None:
                batch = InstanceBatch()

                if not inst.get_action().get_id() is None:
                    batch.action_id = inst.get_action().get_id()
                else:
                    batch.name = inst.get_action().get_name()

                if not inst.get_topology().get_id() is None:
                    batch.topology_id = inst.get_topology().get_id()
                else:
                    batch.sequence = inst.get_topology().get_sequence()

            vl = ValueList()
            vl.values.extend(list(map(Value._value_to_pb, inst.get_values())))
            batch.values.extend([vl])

        batches = []
        for key, arr in dct.items():
            pb = BatchCreateInstancesReq()
            if not token is None:
                pb.token = token
            pb.batches.extend(arr)
            batches.append(pb)

        return batches

class InstanceGroup:
    def __init__(self, action_id: int, topology_id: int, labels: Sequence[str],
                 instances: Sequence[Instance]):
        self._action_id = action_id
        self._topology_id = topology_id
        self._labels = labels
        self._instances = instances

    def get_action_id(self) -> int:
        return self._action_id

    def get_topology_id(self) -> int:
        return self._topology_id

    def get_labels(self) -> Sequence[str]:
        return self._labels

    def get_instances(self) -> Sequence[Instance]:
        return self._instances

    @staticmethod
    def from_group_query_fetch(resp: InstanceGroupFetchRes):
        instances = []
        for inst in resp.group.instances:
            instances.append(Instance(inst.id, values=(list(map(Value._from_pb, inst.values)))))
        return InstanceGroup(resp.group.action_id,
                             resp.group.topology_id,
                             resp.group.labels,
                             instances)

    @staticmethod
    def from_grouped_query_fetch(resp: InstanceGroupedFetchRes):
        groups = []
        for group in resp.groups:
            instances = []
            for inst in group.instances:
                instances.append(Instance(inst.id, values=(list(map(Value._from_pb, inst.values)))))
            groups.append(InstanceGroup(group.action_id,
                                        group.topology_id,
                                        group.labels,
                                        instances))
        return groups
        



