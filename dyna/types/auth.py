from ..dynagatewaytypes.general_types_pb2 import AuthToken

class Token:
    def __init__(self, jwt):
        self._jwt = jwt

    @staticmethod
    def from_auth_token(auth_token: AuthToken):
        return Token(auth_token.jwt)

    @staticmethod
    def _token_to_pb(token) -> AuthToken:
        return token._to_pb()

    def _to_pb(self) -> AuthToken:
        msg = AuthToken()
        msg.jwt = self._jwt
        return msg

    def set_jwt(self, jwt: str):
        self._jwt = jwt

    def get_jwt(self) -> str:
        return self._jwt