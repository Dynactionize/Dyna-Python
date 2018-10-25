from ..dynagatewaytypes.label_pb2 import CreateLabelsReq, GetLabelsReq, RenameLabelReq, DeleteLabelReq, DeleteLabelsReq
from ..dynagatewaytypes.label_pb2 import LabelsRes
from ..dynagatewaytypes.general_types_pb2 import LabelPosition
from .auth import Token
from .action import Action
from .topology import Topology
from typing import Sequence

class Label:
    def __init__(self, position: int, value: str):
        self._position = position
        self._value = value

    @staticmethod
    def _from_pb(label_position: LabelPosition):
        return Label(label_position.position, label_position.label)

    @staticmethod
    def _label_to_pb(label) -> LabelPosition:
        return label._to_pb()

    def _to_pb(self) -> LabelPosition:
        pb = LabelPosition()
        pb.position = self._position
        pb.label = self._value
        return pb

    def set_position(self, position: int):
        self._position = position
    
    def get_position(self) -> int:
        return self._position

    def set_value(self, value: str):
        self._value = value

    def get_value(self) -> str:
        return self._value

class Labels:
    def __init__(self, action: Action, topology: Topology, labels: Sequence[Label]=[]):
        self._action = action
        self._topology = topology
        self._labels = labels


    def add_label(self, label: Label):
        newPos = label.get_position()

        for lbl in self._labels:
            if lbl.get_position() == newPos:
                lbl.set_value(label.get_value())
                return

        self._labels.append(label)


    def extend_labels(self, labels: Sequence[Label]):
        for lbl in labels:
            self.add_label(lbl)

    def get_labels(self) -> Sequence[Label]:
        return self._labels

    def set_action(self, action: Action):
        self._action = action
    
    def get_action(self) -> Action:
        return self._action

    def set_topology(self, topology: Topology):
        self._topology = topology
    
    def get_topology(self) -> Topology:
        return self._topology


    @staticmethod
    def from_labels_response(resp: LabelsRes, action: Action=None, topology: Topology=None):
        lst = []
        for lbl in resp.labels:
            lst.append(Label(lbl.position, lbl.label))
        return Labels(None, None, lst)

    def create_request(self, token: Token) -> CreateLabelsReq:
        if self._action is None:
            raise RuntimeError("No action provided")
        if self._topology is None:
            raise RuntimeError("No topology provided")

        pb = CreateLabelsReq(
            labels=self._label_position_list()
        )
        if not token is None:
            pb.token = token._to_message()
        
        if not self._action.get_id() is None:
            pb.action_id = self._action.get_id()
        else:
            pb.name = self._action.get_name()

        if not self._topology.get_id() is None:
            pb.topology_id = self._topology.get_id()
        else:
            pb.sequence.extend(self._topology.get_sequence())
            
        return pb


    def get_request(self, token: Token) -> GetLabelsReq:
        if self._action is None:
            raise RuntimeError("No action provided")
        if self._topology is None:
            raise RuntimeError("No topology provided")

        pb = GetLabelsReq()
        if not token is None:
            pb.token = token._to_message()

        if not self._action.get_id() is None:
            pb.action_id = self._action.get_id()
        else:
            pb.name = self._action.get_name()

        if not self._topology.get_id() is None:
            pb.topology_id = self._topology.get_id()
        else:
            pb.sequence.extend(self._topology.get_sequence())
            
        return pb


    def rename_request(self, token: Token, label: Label) -> RenameLabelReq:
        if self._action is None:
            raise RuntimeError("No action provided")
        if self._topology is None:
            raise RuntimeError("No topology provided")

        pb = RenameLabelReq()
        pb.label = label._to_pb()
        if not token is None:
            pb.token = token._to_message()

        if not self._action.get_id() is None:
            pb.action_id = self._action.get_id()
        else:
            pb.name = self._action.get_name()

        if not self._topology.get_id() is None:
            pb.topology_id = self._topology.get_id()
        else:
            pb.sequence.extend(self._topology.get_sequence())

        return pb


    def delete_request(self, token: Token, position: int) -> DeleteLabelReq:
        if self._action is None:
            raise RuntimeError("No action provided")
        if self._topology is None:
            raise RuntimeError("No topology provided")

        pb = DeleteLabelReq()
        pb.position = position
        if not token is None:
            pb.token = token._to_message()

        if not self._action.get_id() is None:
            pb.action_id = self._action.get_id()
        else:
            pb.name = self._action.get_name()

        if not self._topology.get_id() is None:
            pb.topology_id = self._topology.get_id()
        else:
            pb.sequence.extend(self._topology.get_sequence())

        return pb


    def delete_all_request(self, token: Token) -> DeleteLabelsReq:
        if self._action is None:
            raise RuntimeError("No action provided")
        if self._topology is None:
            raise RuntimeError("No topology provided")

        pb = DeleteLabelsReq()
        if not token is None:
            pb.token = token._to_message()

        if not self._action.get_id() is None:
            pb.action_id = self._action.get_id()
        else:
            pb.name = self._action.get_name()

        if not self._topology.get_id() is None:
            pb.topology_id = self._topology.get_id()
        else:
            pb.sequence.extend(self._topology.get_sequence())

        return pb



    def _label_position_list(self) -> Sequence[LabelPosition]:
        seq = []
        for lbl in self._labels:
            seq.append(lbl._to_pb())
        return seq

    
