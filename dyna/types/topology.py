from ..dynagatewaytypes.topology_pb2 import CreateTopologyReq, GetTopologyReq
from ..dynagatewaytypes.topology_pb2 import TopologyRes
from ..dynagatewaytypes.enums_pb2 import ComponentType
from ..dynagatewaytypes.query_pb2 import TopologyFetchRes
from ..dynagatewaytypes.general_types_pb2 import TopologySequence
from .auth import Token
from typing import Sequence

class Topology:
    def __init__(self, id=None, sequence=[]):
        self._id = id
        self._sequence = sequence

    def log(self):
        id = 0
        sequence = ''
        if not self._id is None:
            id = self._id
        if len(sequence) > 1:
            sequence = ''.join(sequence)
        print('TOPOLOGY({0}): {1}'.format(id, sequence))

    def set_id(self, id: int):
        self._id = id

    def get_id(self) -> int:
        return self._id

    def set_sequence(self, sequence: Sequence[int]):
        self._sequence = sequence

    def get_sequence(self) -> Sequence[int]:
        return self._sequence

    @staticmethod
    def from_component_array(components: Sequence[int]):
        return Topology(sequence = components)


    @staticmethod
    def from_topology_response(resp: TopologyRes):
        id = resp.id
        if resp.sequence is None:
            sequence = []
        else:
            sequence = resp.sequence.components
        return Topology(id, sequence)

    @staticmethod
    def from_query_fetch(resp: TopologyFetchRes):
        res = []
        for top in resp.topologies:
            res.append(Topology(top.id, top.sequence.components))
        return res

        
    def create_request(self, token: Token) -> CreateTopologyReq:
        pb = CreateTopologyReq()
        if not token is None:
            pb.token = token._to_message()
        pb.sequence.components.extend(self._sequence)
        return pb

    def get_request(self, token: Token, full_response=False) -> GetTopologyReq:
        pb = GetTopologyReq()
        if not token is None:
            pb.token = token._to_message()
        if not self._id is None:
            pb.id = self._id
        else:
            pb.sequence.components.extend(self._sequence)
        pb.return_full_object = full_response
        return pb

