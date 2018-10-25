from ..dynagatewaytypes.action_pb2 import CreateActionReq, GetActionReq, RenameActionReq, DeleteActionReq
from ..dynagatewaytypes.action_pb2 import ActionRes
from ..dynagatewaytypes.query_pb2 import ActionFetchRes
from .auth import Token

class Action:
    def __init__(self, id=None, name=None):
        self._id = id
        self._name = name

    def log(self):
        id = 0
        name = ""
        if not self._id is None:
            id = self._id
        if not self._name is None:
            name = self._name
        print("ACTION({0}): {1}".format(id, name))

    def set_id(self, id: int):
        self._id = id

    def get_id(self) -> int:
        return self._id

    def set_name(self, name: str):
        self._name = name

    def get_name(self) -> str:
        return self._name

    @staticmethod
    def from_action_response(resp: ActionRes):
        id = resp.id
        name = resp.name
        return Action(id, name)

    @staticmethod
    def from_query_fetch(resp: ActionFetchRes):
        res = []
        for action in resp.actions:
            res.append(Action(action.id, action.name))
        return res



    def create_request(self, token: Token) -> CreateActionReq:
        pb = CreateActionReq()
        if not token is None:
            pb.token = token._to_message()
        pb.name = self._name
        return pb

    def get_request(self, token: Token, full_response=False) -> GetActionReq:
        pb = GetActionReq()
        if not token is None:
            pb.token = token._to_message()
        if not self._id is None:
            pb.id = self._id
        else:
            pb.name = self._name
        pb.return_full_object = full_response
        return pb

    def rename_request(self, token: Token, new_name: str) -> RenameActionReq:
        pb = RenameActionReq()
        if not token is None:
            pb.token = token._to_message()
        pb.id = self._id
        pb.name = new_name
        return pb

    def delete_request(self, token: Token, cascade=False) -> DeleteActionReq:
        pb = DeleteActionReq()
        if not token is None:
            pb.token = token._to_message()
        pb.id = self._id
        pb.cascade = cascade
        return pb

