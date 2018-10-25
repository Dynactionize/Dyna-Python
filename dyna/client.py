import grpc

from .common.errors import GRPCError, ServiceError

from .dynagatewaytypes.action_pb2_grpc import ActionServiceStub
from .dynagatewaytypes.topology_pb2_grpc import TopologyServiceStub
from .dynagatewaytypes.instance_pb2_grpc import InstanceServiceStub
from .dynagatewaytypes.label_pb2_grpc import LabelServiceStub
from .dynagatewaytypes.query_pb2_grpc import QueryServiceStub

from .types import *

from typing import Sequence

def service_func(service, method):
    def decorate(func):
        def applicator(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise ServiceError(service, method) from e
        return applicator
    return decorate

class DynizerClient:
    def __init__(self, host='127.0.0.1', port=50022, token: Token=None):
        self._channel = grpc.insecure_channel('{0}:{1}'.format(host, port))
        self._action_service = self.ActionService(self._channel, token)
        self._topology_service = self.TopologyService(self._channel, token)
        self._instance_service = self.InstanceService(self._channel, token)
        self._label_service = self.LabelService(self._channel, token)
        self._query_service = self.QueryService(self._channel, token)
        self._token = token

    @staticmethod
    def _verify_embedded_status(resp):
        if resp.HasField('status'):
            return DynizerClient._verify_status(resp.status)
        return None

    @staticmethod
    def _verify_status(status):
        if status.success != True:
            raise GRPCError(status.code, status.message)
        return None
        
    class ActionService:
        def __init__(self, channel, token):
            self._stub = ActionServiceStub(channel)
            self._token = token

        @service_func('action_service', 'create')
        def create(self, action: Action) -> Action:
            resp = self._stub.Create(action.create_request(self._token))
            DynizerClient._verify_embedded_status(resp)
            return Action.from_action_response(resp)
            
        @service_func('action_service', 'get')
        def get(self, action: Action, full_response=False) -> Action:
            resp = self._stub.Get(action.get_request(self._token, full_response=full_response))
            DynizerClient._verify_embedded_status(resp)
            return Action.from_action_response(resp)

        @service_func('action_service', 'rename')
        def rename(self, action: Action, new_name: str) -> bool:
            resp = self._stub.Rename(action.rename_request(self._token, new_name))
            DynizerClient._verify_status(resp)
            return resp.success

        @service_func('action_service', 'delete')
        def delete(self, action: Action, cascade=False) -> bool:
            resp = self._stub.Delete(action.delete_request(self._token, cascade))
            DynizerClient._verify_status(resp)
            return resp.success


            

    class TopologyService:
        def __init__(self, channel, token):
            self._stub = TopologyServiceStub(channel)
            self._token = token

        @service_func('topology_service', 'create')
        def create(self, topology: Topology) -> Topology:
            resp = self._stub.Create(topology.create_request(self._token))
            DynizerClient._verify_embedded_status(resp)
            return Topology.from_topology_response(resp)

        @service_func('topology_service', 'get')
        def get(self, topology: Topology, full_response=False) -> Topology:
            resp = self._stub.Get(topology.get_request(self._token, full_response=full_response))
            DynizerClient._verify_status(resp)
            return Topology.from_topology_response(resp)

    class InstanceService:
        def __init__(self, channel, token):
            self._stub = InstanceServiceStub(channel)
            self._token = token

        @service_func('instance_service', 'create')
        def create(self, instance: Instance) -> bool:
            resp = self._stub.Create(instance.create_request(self._token))
            DynizerClient._verify_status(resp)
            return resp.success

        @service_func('instance_service', 'batch_create')
        def batch_create(self, instance_batches: InstanceBatches) -> bool:
            batches = instance_batches.batches(self._token)
            for batch in batches:
                resp = self._stub.BatchCreate(batch)
                DynizerClient._verify_status(resp)
                if resp.success == False:
                    return False
            return True

        @service_func('instance_service', 'get')
        def get(self, instance: Instance, return_action_id=False, return_topology_id=False) -> Instance:
            resp = self._stub.Get(instance.get_request(self._token, return_action_id, return_topology_id))
            DynizerClient._verify_embedded_status(resp)
            return Instance.from_instance_response(resp)

        @service_func('instance_service', 'update')
        def update(self, instance: Instance) -> bool:
            resp = self._stub.Update(instance.update_request(self._token))
            DynizerClient._verify_status(resp)
            return resp.success

        @service_func('instance_service', 'delete')
        def delete(self, instance: Instance) -> bool:
            resp = self._stub.Delete(instance.delete_request(self._token))
            DynizerClient._verify_status(resp)
            return resp.success


    class LabelService:
        def __init__(self, channel, token):
            self._stub = LabelServiceStub(channel)
            self._token = token

        @service_func('label_service', 'create')
        def create(self, labels: Labels) -> bool:
            resp = self._stub.Create(labels.create_request(self._token))
            DynizerClient._verify_status(resp)
            return resp.success

        @service_func('label_service', 'get')
        def get(self, labels: Labels) -> Labels:
            resp = self._stub.Get(labels.get_request(self._token))
            DynizerClient._verify_embedded_status(resp)
            return Labels.from_labels_response(resp)

        @service_func('label_service', 'rename')
        def rename(self, labels: Labels, renamed_label: Label) -> bool:
            resp = self._stub.Rename(labels.rename_request(self._token, renamed_label))
            DynizerClient._verify_status(resp)
            return resp.success

        @service_func('label_service', 'delete')
        def delete(self, labels: Labels, position: int) -> bool:
            resp = self._stub.Delete(labels.delete_request(self._token, position))
            DynizerClient._verify_status(resp)
            return resp.success

        @service_func('label_service', 'delete_all')
        def delete_all(self, labels: Labels) -> bool:
            resp = self._stub.DeleteAll(labels.delete_all_request(self._token))
            DynizerClient._verify_status(resp)
            return resp.success


    class QueryService:
        def __init__(self, channel, token):
            self._stub = QueryServiceStub(channel)
            self._token = token

        @service_func('query_service', 'action') 
        def action(self, query: Query) -> Cursor:
            resp = self._stub.Action(query.query_request(self._token))
            DynizerClient._verify_embedded_status(resp)
            return Cursor.from_query_res(resp)

        @service_func('query_service', 'topology')
        def topology(self, query: Query) -> Cursor:
            resp = self._stub.Topology(query.query_request(self._token))
            DynizerClient._verify_embedded_status(resp)
            return Cursor.from_query_res(resp)

        @service_func('query_service', 'instance')
        def instance(self, query: Query) -> Cursor:
            resp = self._stub.Instance(query.query_request(self._token))
            DynizerClient._verify_embedded_status(resp)
            return Cursor.from_query_res(resp)

        @service_func('query_service', 'fetch_actions')
        def fetch_actions(self, fetch: ActionFetchRequest) -> Sequence[Action]:
            resp = self._stub.FetchActions(fetch.fetch_request(self._token))
            DynizerClient._verify_embedded_status(resp)
            return Action.from_query_fetch(resp)

        @service_func('query_service', 'fetch_topologies')
        def fetch_topologies(self, fetch: TopologyFetchRequest) -> Sequence[Topology]:
            resp = self._stub.FetchTopologies(fetch.fetch_request(self._token))
            DynizerClient._verify_embedded_status(resp)
            return Topology.from_query_fetch(resp)

        @service_func('query_service', 'fetch_instances')
        def fetch_instances(self, fetch: InstanceFetchRequest) -> Sequence[Instance]:
            resp = self._stub.FetchInstances(fetch.fetch_request(self._token))
            DynizerClient._verify_embedded_status(resp)
            return Instance.from_query_fetch(resp)

        @service_func('query_service', 'fetch_group_instances')
        def fetch_group_instances(self, fetch: InstanceGroupFetchRequest) -> InstanceGroup:
            resp = self._stub.FetchGroupInstances(fetch.fetch_request(self._token))
            DynizerClient._verify_embedded_status(resp)
            return InstanceGroup.from_group_query_fetch(resp)

        @service_func('query_service', 'fetch_grouped_instances')
        def fetch_grouped_instances(self,
                                    fetch: InstanceGroupedFetchRequest) -> Sequence[InstanceGroup]:
            resp = self._stub.FetchGroupedInstances(fetch.fetch_request(self._token))
            DynizerClient._verify_embedded_status(resp)
            return InstanceGroup.from_grouped_query_fetch(resp)


    def action_service(self):
        return self._action_service

    def topology_service(self):
        return self._topology_service

    def instance_service(self):
        return self._instance_service

    def label_service(self):
        return self._label_service

    def query_service(self):
        return self._query_service