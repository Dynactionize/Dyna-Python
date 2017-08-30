from ..common.decorators import *
from ..common.errors import *
from .types import *
from .filters import *
import http.client
from enum import Enum, IntEnum
import urllib
import re
import json
import base64


class DynizerConnection:
    def __init__(self, address, port=None, endpoint_prefix=None, https=False,
                 key_file=None, cert_file=None, username=None, password=None,
                 auto_reauth=True):
        self.dynizer_address = address
        if port == None:
            self.dynizer_port = 80 if https == False else 443
        else:
            self.dynizer_port = port
        self.endpoint_prefix = '' if endpoint_prefix is None else endpoint_prefix
        self.https = https
        self.key_file = key_file
        self.cert_file = cert_file
        self.username = username
        self.password = password
        self._headers = {
            'cache-control': 'no-cache',
            'content-type': 'application/json'
        }
        self.connection = None
        self.token = None

    def __del__(self):
        self.close()

    def connect(self, reconnect=False):
        if not self.connection is None:
            if not reconnect:
                return
            else:
                self.close()

        if self.https:
            if not self.key_file is None and not self.cert_file is None:
                self.connection = http.client.HTTPSConnection(
                        self.dynizer_address, self.dynizer_port,
                        key_file=self.key_file, cert_file=self.cert_file)
            elif not self.username is None and not self.password is None:
                self.connection = http.client.HTTPSConnection(
                        self.dynizer_address, self.dynizer_port)
                self._login(self.username, self.password)
            else:
                self.connection = http.client.HTTPSConnection(
                        self.dynizer_address, self.dynizer_port)
        else:
            self.connection = http.client.HTTPConnection(self.dynizer_address, self.dynizer_port)
            if not self.username is None and not self.password is None:
                self._login(self.username, self.password)


    def _login(self, user, password):
        user = user.lower()
        content = json.dumps({
                'userName': user,
                'userPassword': base64.b64encode(password.encode()).decode()
                })
        url = '/ui-backend/v1/users/Login'
        data = self.__POST(url, content, dict, success_code=200)
        self.token = data['jwtToken']['value']
        self._headers['x_dynizer_authkey'] = user
        self._headers['x_dynizer_session'] = self.token
        return True



    def close(self):
        if not self.connection is None:
            self.connection.close()
            self.connection = None



    # Functions that operate on partially of fully populated objects
    def analyze(self, dictionary, corpus, text):
        return self.__analyze(dictionary, corpus, text)

    def create(self, obj):
        f = self.__get_function_handle_for_obj('create', obj)
        return f(obj)

    def batch_create(self, obj_arr):
        f = self.__get_function_handle_for_obj('batch_create', obj_arr[0])
        return f(obj_arr)

    def read(self, obj):
        f = self.__get_function_handle_for_obj('read', obj)
        return f(obj)

    def update(self, obj):
        f = self.__get_function_handle_for_obj('update', obj)
        return f(obj)

    def delete(self, obj):
        f = self.__get_function_handle_for_obj('delete', obj)
        return f(obj)

    def link_actiontopology(self, action, topology, labels=None):
        if labels:
            topology.labels = labels
        return self.__link_ActionTopology(action, topology)

    def update_actiontopology(self, action, topology):
        return self.__update_ActionTopology(action, topology)

    # Functions that operate based on classes
    def list(self, type, field_filters=None, pagination_filter=None):
        f = self.__get_function_handle_for_class('list', type)
        return f(field_filters, pagination_filter)

    def list_linked(self, obj, pagination_filter=None):
        if type(obj) == Action:
            return self.__list_linked_Topologies(obj, pagination_filter)
        elif type(obj) == Topology:
            return self.__list_linked_Actions(obj, pagination_filter)
        else:
            raise DispatchError(obj, 'list_linked')

    # Query functions
    def query(self, query, pagination_filter=None):
        f = self.__get_function_handle_for_obj('query', query)
        return f(query, pagination_filter)



    def __get_function_handle_for_obj(self, op, obj):
        func_name = '_{0}__{1}_{2}'.format(self.__class__.__name__, op, obj.__class__.__name__)
        return self.__get_dispatch_func(func_name)

    def __get_function_handle_for_class(self, op, cls):
        _regex=re.compile('ys$')
        _replace='ies'
        func_name = _regex.sub(_replace, '_{0}__{1}_{2}s'.format(
            self.__class__.__name__, op, cls.__name__))
        return self.__get_dispatch_func(func_name)

    def __get_dispatch_func(self, func_name):
        func = None
        try:
            func = getattr(self, func_name)
        except Exception as e:
            raise DispatchError(obj, func_name) from e
        return func


    @staticmethod
    def __build_url_with_arguments(cls, url, field_filters=None, pagination_filter=None):
        filters = ''
        pagination = ''
        if field_filters is not None:
            filters = '&'.join(list(map(lambda o: o.compose_filter(cls), field_filters)))
        if pagination_filter is not None:
            pagination = pagination_filter.compose_filter(cls)

        if filters !=  '':
            if pagination != '':
                return '{0}?{1}&{2}'.format(url, filters, pagination)
            else:
                return '{0}?{1}'.format(url, filters)
        elif pagination != '':
                return '{0}?{1}'.format(url, pagination)
        else:
            return url


    def __analyze(self, dictionary, corpus, text):
        #url = '/analyzer/v1/dictionaries/English/wordcorpora/EnglishCorpus/analyze'
        url = '/analyzer/v1/dictionaries/{0}/wordcorpora/{1}/analyze'.format(dictionary, corpus)
        content = json.dumps({
                'split_algorithm': 'group',
                'text': text
                })
        return self.__POST(url, content, list, success_code=200)



    def __create_DataElement(self, obj):
        url = '/data/v1_1/dataelements'
        return self.__POST(url, obj.to_json(), DataElement)

    def __read_DataElement(self, obj):
        url = '/data/v1_1/datalements/{0}'.format(obj.id)
        return self.__GET(url, DataElement)

    def __list_DataElements(self, field_filters, pagination_filter):
        url = DynizerConnection.__build_url_with_arguments(
                DataElement, '/data/v1_1/dataelements', field_filters, pagination_filter)
        return self.__GET(url, DataElement)


    def __create_Action(self, obj):
        url = '/data/v1_1/actions'
        return self.__POST(url, obj.to_json(), Action)

    def __read_Action(self, obj):
        url = '/data/v1_1/actions/{0}'.format('' if obj.id is None else obj.id)
        return self.__GET(url, Action)

    def __list_Actions(self, field_filters, pagination_filter):
        url = DynizerConnection.__build_url_with_arguments(
                Action, '/data/v1_1/actions', field_filters, pagination_filter)
        return self.__GET(url, Action)

    def __create_Topology(self, obj):
        url = '/data/v1_1/topologies'
        return self.__POST(url, obj.to_json(), Topology)

    def __read_Topology(self, obj):
        url = '/data/v1_1/topologies/{0}'.format('' if obj.id is None else obj.id)
        return self.__GET(url, Topology)

    def __list_Topologies(self, field_filters, pagination_filter):
        url = DynizerConnection.__build_url_with_arguments(
                Topology, '/data/v1_1/topologies', field_filters, pagination_filter)
        return self.__GET(url, Topology)

    def __link_ActionTopology(self, action, topology):
        data = topology.to_json(include_components=False,
                                include_labels=True,
                                include_constraining=True,
                                include_applying=True)
        url = '/data/v1_1/actions/{0}/topologies'.format(action.id)
        print(url)
        print(data)
        return self.__POST(url, data, Topology)

    def __update_ActionTopology(self, action, topology):
        data = topology.to_json(include_components=False,
                                include_labels=True,
                                include_constraining=True,
                                include_applying=True)
        url = '/data/v1_1/actions/{0}/topologies/{1}'.format(
                action.id, '' if topology.id is None else topology.id)
        return self.__patch(url, data, Topology)

    def __list_linked_Topologies(self, action, pagination_filter):
        url = DynizerConnection.__build_url_with_arguments(
                Topology,
                '/data/v1_1/actions/{0}/topologies'.format(action.id),
                [], pagination_filter)
        return self.__GET(url, Topology)

    def __list_linked_Actions(self, topology, pagination_filter):
        url = DynizerConnection.__build_url_with_arguments(
                Action,
                '/data/v1_1/topologies/{0}/actions'.format(topology.id),
                [], pagination_filter)
        return self.__GET(url, Action)


    def __create_Instance(self, obj):
        url = '/data/v1_1/instances'
        return self.__POST('/data/v1_1/instances', obj.to_json(), Instance)

    def __batch_create_Instance(self, obj_arr):
        json_arr = list(map(lambda x: x.to_json(), obj_arr))
        data = '['+','.join(json_arr)+']'
        url = '/data/v1_1/instances'
        return self.__POST(url, data, Instance)

    def __read_Instance(self, obj):
        url = '/data/v1_1/instances/{0}'.format('' if obj.id is None else obj.id)
        return self.__GET(url, Instance)

    def __update_Instance(self, obj):
        url = '/data/v1_1/instances/{0}'.format(obj.id)
        return self.__PUT(url, obj.to_json(), Instance)

    def __delete_Instance(self, obj):
        url = '/data/v1_1/instances/{0}'.format(obj.id)
        return self.__DELETE(url, Instance)

    def __list_Instances(self, field_filters, pagination_filter):
        url = DynizerConnection.__build_url_with_arguments(
                Instance, '/data/v1_1/instances', field_filters, pagination_filter)
        return self.__GET(url, Instance)

    def __query_InActionQuery(self, query, pagination_filter):
        actionres = None
        topologyres = None
        instanceres = None
        json = query.to_json()

        if (query.query_results & InActionQueryResult.ACTIONS) == InActionQueryResult.ACTIONS:
            url = DynizerConnection.__build_url_with_arguments(
                    Action, '/data/v1_1/actionquery', None, pagination_filter)
            actionres = self.__POST(url, json, Action, success_code=200)
        if (query.query_results & InActionQueryResult.TOPOLOGIES) == InActionQueryResult.TOPOLOGIES:
            url = DynizerConnection.__build_url_with_arguments(
                    Topology, '/data/v1_1/topologyquery', None, pagination_filter)
            topologyres = self.__POST(url, json, Topology, success_code=200)
        if (query.query_results & InActionQueryResult.INSTANCES) == InActionQueryResult.INSTANCES:
            url = DynizerConnection.__build_url_with_arguments(
                    Instance, '/data/v1_1/instancequery', None, pagination_filter)
            instanceres = self.__POST(url, json, Instance, success_code=200)
        return (actionres, topologyres, instanceres)


    def __query_CorrelationQuery(self, query, pagination_filter):
        json = query.to_json()
        url = '/data/v1_1/correlationquery'
        return self.__POST(url, json, dict, success_code=200)



    def __query_MultiInstanceQuery(self, query, pagination_filter):
        json = query.to_json()
        url = DynizerConnection.__build_url_with_arguments(
                Instance, '/data/v1_1/multiinstancequery', None, pagination_filter)
        return self.__POST(url, json, Instance, success_code=200)




    def __POST(self, endpoint, payload, result_obj=None, success_code=201):
        return self.__REQUEST('POST', endpoint, payload, result_obj, success_code)

    def __PUT(self, endpoint, payload, result_obj=None, success_code=200):
        return self.__REQUEST('PUT', endpoint, payload, result_obj, success_code)

    def __PATCH(self, endpoint, payload, result_obj=None, success_code=200):
        return self.__REQUEST('PATCH', endpoint, payload, result_obj, success_code)

    def __GET(self, endpoint, result_obj=None, success_code=200):
        return self.__REQUEST('GET', endpoint, result_obj=result_obj, success_code=success_code)

    def __DELETE(self, endpoint, result_obj=None, success_code=204):
        return self.__REQUEST('DELETE', endpoint, resultobj=result_obj, success_code=success_code)


    def __REQUEST(self, verb, endpoint, payload=None, result_obj=None, success_code=200,
            reauthenticated=False):
        if self.connection is None:
            raise ConnectionError('Not connected to dynizer. Please issue a connect() call first')

        url = '{0}{1}'.format(self.endpoint_prefix, endpoint)
        response = None
        try:
            if payload is not None:
                self.connection.request(verb, url, body=payload, headers=self._headers)
            else:
                self.connection.request(verb, url, headers=self._headers)
            response = self.connection.getresponse()
        except Exception as e:
            self.connect(True)
            print('{0} {1}'.format(verb, url))
            if not payload is None:
                print(payload)
            print(e)
            raise ConnectionError() from e

        if response.status == 401 and not self.username is None and not self.password is None and self.auto_reauth == True and retry == False:
            # Try to reauthenticate
            self._login(self.username, self.password)
            return self.__REQUEST(verb, endpoint, payload=payload, result_obj=result_obj,
                    success_code=success_code, reauthenticated=True)
        if response.status != success_code:
            print('{0} {1}'.format(verb, url))
            if not payload is None:
                print(payload)
            self.connect(True)
            raise RequestError(response.status, response.reason)

        self.token = response.getheader('x_dynizer_session', None)
        if not self.token is None:
            self._headers['x_dynizer_session'] = self.token


        result = None
        if result_obj is not None:
            try:
                bytestr = response.read()
                json_string = bytestr.decode(response.headers.get_content_charset('utf-8'))
                if result_obj == dict or result_obj == list:
                    result = json.loads(json_string)
                else:
                    result = result_obj.from_json(json_string)
            except Exception as e:
                self.connect(True)
                print('{0} {1}'.format(verb, url))
                if not payload is None:
                    print(payload)
                print(e)
                raise ResponseError() from e

        return result

