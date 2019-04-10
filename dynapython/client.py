import grpc
import csv

#from tables import Table
from enum import IntEnum

from dynagatewaytypes import datatypes_pb2
from dynagatewaytypes import enums_pb2
from dynagatewaytypes import general_types_pb2

from dynagatewaytypes import authentication_pb2_grpc
from dynagatewaytypes import authentication_pb2
from dynagatewaytypes import action_pb2_grpc
from dynagatewaytypes import action_pb2
from dynagatewaytypes import topology_pb2_grpc
from dynagatewaytypes import topology_pb2
from dynagatewaytypes import label_pb2_grpc
from dynagatewaytypes import label_pb2
from dynagatewaytypes import instance_pb2_grpc
from dynagatewaytypes import instance_pb2
from dynagatewaytypes import query_pb2_grpc
from dynagatewaytypes import query_pb2
from dynagatewaytypes import networkquery_pb2_grpc
from dynagatewaytypes import networkquery_pb2

class Service(IntEnum):
    ACTION_SERVICE = 0
    TOPOLOGY_SERVICE = 1
    LABEL_SERVICE = 2
    INSTANCE_SERVICE = 3
    QUERY_SERVICE = 4
    NETWORK_QUERY_SERVICE = 5


class Client:
    def __init__(self, host, port):
        self._channel = grpc.insecure_channel('{0}:{1}'.format(host, port))
        self._authservice = authentication_pb2_grpc.AuthenticateServiceStub(self._channel)
        self._services = [None]*6
        self._services[Service.ACTION_SERVICE] = action_pb2_grpc.ActionServiceStub(self._channel)
        self._services[Service.TOPOLOGY_SERVICE] = topology_pb2_grpc.TopologyServiceStub(self._channel)
        self._services[Service.LABEL_SERVICE] = label_pb2_grpc.LabelServiceStub(self._channel)
        self._services[Service.INSTANCE_SERVICE] = instance_pb2_grpc.InstanceServiceStub(self._channel)
        self._services[Service.QUERY_SERVICE] = query_pb2_grpc.QueryServiceStub(self._channel)
        self._services[Service.NETWORK_QUERY_SERVICE] = networkquery_pb2_grpc.NetworkServiceStub(self._channel)
        self._token = None
        self._metadata = []

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self._channel.close()

    def user_login(self, username, password):
        loginReq = authentication_pb2.GetTokenReq(
            user = authentication_pb2.UserAuth(
                user_name = username,
                password = password
            )
        )
        try:
            tokenResp = self._authservice.GetToken(loginReq)
        except grpc.RpcError as err:
            print(err)
            return False
        self._token = tokenResp.token
        self._metadata = [('authorization', 'Bearer {0}'.format(self._token))]
        return True

    def service_login(self, client_id, secret):
        loginReq = authentication_pb2.GetTokenReq(
            service = authentication_pb2.ServiceAuth(
                client_id = client_id,
                secret = secret
            )
        )
        try:
            tokenResp = self._authservice.GetToken(loginReq)
        except grpc.RpcError as err:
            print(err)
            return False
        self._token = tokenResp.token
        self._metadata = [('authorization', 'Bearer {0}'.format(self._token))]
        return True

    def bearer_login(self, bearer_token):
        loginReq = authentication_pb2.GetTokenReq(
            bearer = authentication_pb2.BearerToken(
                token = bearer_token
            )
        )
        try:
            tokenResp = self._authservice.GetToken(loginReq)
        except grpc.RpcError as err:
            print(err)
            return False
        self._token = tokenResp.token
        self._metadata = [('authorization', 'Bearer {0}'.format(self._token))]
        return True


    def call(self, service_func, arg):
        return service_func(arg, metadata=self._metadata)

    def service(self, service):
        return self._services[service]
