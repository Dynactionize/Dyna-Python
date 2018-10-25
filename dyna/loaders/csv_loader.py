from ..dynagatewaytypes.enums_pb2 import ComponentType, DataType, VOID, STRING
from ..common.errors import LoaderError
from ..types import Action, Topology, Label, Labels, Value, Instance, InstanceBatches
from typing import Sequence
from ..client import DynizerClient
import csv

class CSVAbstractElement:
    def __init__(self, value,
                       data_type: DataType,
                       component: ComponentType,
                       label = ''):
        self.value = value
        self.data_type = data_type
        self.component = component
        self.label = label

    def fetch_from_row(self, row, components, data, labels):
        components.append(self.component)
        data.append(Value(self.data_type, self.value))
        labels.append(self.label)
        return True


class CSVFixedElement(CSVAbstractElement):
    def __init__(self, value,
                       data_type: DataType,
                       component: ComponentType,
                       label = ''):
        super().__init__(value, data_type, component, label)



class CSVRowElement(CSVAbstractElement):
    def __init__(self, index: int,
                       data_type: DataType,
                       component: ComponentType,
                       label = '',
                       required = True,
                       default = None,
                       allow_void = True,
                       transform_funcs = [],
                       na_list = ['', 'n/a', 'N/A']):
        super().__init__(None, data_type, component, label)
        self.index = index
        self.required = required
        self.default = default
        self.allow_void = allow_void
        self.transform_funcs = list(transform_funcs)
        self.na_list = list(na_list)

    def fetch_from_row(self, row, components, data, labels):
        if len(row) <= self.index:
            if self.required:
                return self._add_na_value(components, data, labels)
        else:
            value = row[self.index]
            if value in self.na_list:
                if self.required:
                    return self._add_na_value(components, data, labels)
            else:
                for tf in self.transform_funcs:
                    value = tf(value)

                data.append(Value(self.data_type, value))
                components.append(self.component)
                labels.append(self.label)
                return True

    def _add_na_value(self, components, data, labels):
        if self.default is not None:
            data.append(Value(self.data_type, self.default))
        elif self.allow_void:
            data.append(Value(VOID, None))
        else:
            return False

        components.append(self.component)
        labels.append(self.label)
        return True

"""
class CSVDynaTextElement(CSVAbstractElement):
    def __init__(self, index: int,
                       targetaction: str,
                       dictionary: str,
                       wordcorpus: str,
                       component: ComponentType,
                       label = '',
                       required = True,
                       default = None,
                       allow_void = True,
                       trasform_funcs = [],
                       na_list = ['', 'n/a', 'N/A']):
        super().__init__(None, DataType.DYNATEXT, component, label)
        self.index = index
        self.targetaction = targetaction
        self.dictionary = dictionary
        self.wordcorpus = wordcorpus
        self.required = required
        self.default = default
        self.allow_void = allow_void
        self.transform_funcs = list(transform_funcs)
        self.na_list = list(na_list)

    def fetch_from_row(self, row, components, data, labels):
        if len(row) <= self.index:
            if self.required:
                return self._add_na_value(components, data, labels)
        else:
            value = row[self.index]
            if value in self.na_list:
                if self.required:
                    return self._add_na_value(components, data, labels)
            else:
                for tf = self.transform_funcs:
                    value = tf(value)

                data.append(InstanceElement(
                    value=DynaText(text=value,
                                   targetaction=self.targetaction, dictionary=self.dictionary,
                                   wordcorpus=self.wordcorpus), datatype=DataType.DYNATEXT))
                labels.append(self.label)
                return true

    def _add_na_value(self, components, data, labels):
        if self.default is not None:
            data.append(InstanceElement(
                value=DynaText(text="",
                               targetaction=self.targetaction, dictionary=self.dictionary,
                               wordcorpus=self.wordcorpus), datatype=DataType.DYNATEXT))
        elif self.allow_void:
            data.append(InstanceElement())
        else:
            return False

        components.append(self.component)
        labels.append(self.label)
        return True
"""



class CSVStringCombinationElement(CSVAbstractElement):
    def __init__(self, indices: Sequence[int],
                        component: ComponentType,
                        label = '',
                        combinator_func = None,
                        required = True):
        super().__init__(None, STRING, component, label)
        self.indices = indices
        self.combinator_func = combinator_func
        self.required = required

    def fetch_from_row(self, row, components, data, labels):
        tmp_data = []
        for index in self.indices:
            data = ''
            if len(row) > index:
                data = row[index]
            tmp_data.append(data)

        value = self.combinator_func(tmp_data) if self.combinator_func is not None else self._default_combinator(tmp_data)
        if len(value) == 0:
            if self.required:
                data.append(Value(self.data_type, ""))
            else:
                return False
        else:
            data.append(Value(self.data_type, value))

        components.append(self.component)
        labels.append(self.label)

    def _default_combinator(self, tmp_data):
        result = ''
        for elem in tmp_data:
            if len(elem) > 0:
                if len(result) == 0:
                    result = '{0}'.format(elem)
                else:
                    result = '{0} {1}'.format(result, elem)
        return result



class CSVMapping:
    def __init__(self, action: Action,
                       elements: Sequence[CSVAbstractElement],
                       fallback: Sequence[CSVAbstractElement] = [],
                       batch_size = 100):
        self.action = action
        self.elements = elements
        self.fallback = list(fallback)
        self.batch_size = batch_size



class CSVLoader:
    def __init__(self, csv_path: str,
                       mappings: Sequence[CSVMapping] = [],
                       header_count=0,
                       delimiter=',',
                       doublequote=True,
                       escapechar=None,
                       lineterminator='\r\n',
                       quotechar='"',
                       quoting=csv.QUOTE_MINIMAL,
                       skipinitialspace=False,
                       strict=False):
        print("INIT !!!")
        print(mappings)
        self.csv_path = csv_path
        self.mappings = list(mappings)
        self.header_count = header_count
        self.delimiter = delimiter
        self.doublequote = doublequote
        self.escapechar = escapechar
        self.lineterminator = lineterminator
        self.quotechar = quotechar
        self.quoting = quoting
        self.skipinitialspace = skipinitialspace
        self.strict = strict

    def add_mapping(self, mapping: CSVMapping):
        self.mappings.append(mapping)

    def run(self, client: DynizerClient, debug=False):
        try:
            for mapping in self.mappings:
                self.__run_mapping(client, mapping, debug)
        except Exception as e:
            raise e

    def __run_mapping(self, client: DynizerClient,
                            mapping: CSVMapping,
                            debug):
        print('Creating instances for: {0}'.format(mapping.action.get_name()))
        action_obj = client.action_service().create(mapping.action)

        topology_map = {}
        loadlist = []

        self.__run_simple_mapping(client, mapping, action_obj, topology_map, loadlist, debug)

        if len(loadlist) > 0:
            self.__push_batch(client, loadlist)

    def __run_simple_mapping(self, client: DynizerClient,
                                   mapping: CSVMapping,
                                   action_obj, topology_map, loadlist,
                                   debug):
        with open(self.csv_path, newline='') as csvfile:
            row_cnt=0
            csv_rdr = csv.reader(csvfile,
                                 delimiter=self.delimiter,
                                 doublequote=self.doublequote,
                                 escapechar=self.escapechar,
                                 lineterminator=self.lineterminator,
                                 quotechar=self.quotechar,
                                 quoting=self.quoting,
                                 skipinitialspace=self.skipinitialspace,
                                 strict=self.strict)
            for row in csv_rdr:
                row_cnt = row_cnt+1
                if row_cnt <= self.header_count:
                    continue

                status = self.__run_mapping_on_row(row, topology_map, loadlist, action_obj, client, mapping, debug=debug)
                if not status:
                    self.__run_mapping_on_row(row, topology_map, loadlist, action_obj, client, mapping, fallback=True, debug=debug)

            if len(loadlist) >= mapping.batch_size:
                self.__push_batch(client, loadlist)

    def __run_mapping_on_row(self, row, topology_map, loadlist,
                                   action_obj: Action,
                                   client: DynizerClient,
                                   mapping: CSVMapping,
                                   fallback = False,
                                   debug = False):
        components = []
        data = []
        labels = []
        elements = mapping.fallback if fallback else mapping.elements
        if len(elements) == 0:
            return False

        for element in elements:
            if element.fetch_from_row(row, components, data, labels) == False:
                return False

        if len(components) < 2:
            return False
        
        if client is None:
            return True

        top_map_key = ','.join(map(str, components))
        topology_obj = None
        if top_map_key in topology_map:
            topology_obj = topology_map[top_map_key]
        else:
            if topology_obj is None:
                topology = Topology.from_component_array(components)
                topology_obj = client.topology_service().create(topology)

            top_labels = Labels(action_obj, topology_obj)
            for idx, lbl in enumerate(labels):
                top_labels.add_label(Label(idx+1, lbl))

            res = client.label_service().create(top_labels)
            if res == False:
                print("Warning: failed to create labels")

            topology_map[top_map_key] = topology_obj

        inst = Instance(action=action_obj, topology=topology_obj, values=data)
        if debug:
            inst.log()
        loadlist.append(inst)
        return True



    def __push_batch(self, client: DynizerClient,
                           batch: Sequence[Instance]):
        batches = InstanceBatches(batch)
        res = client.instance_service().batch_create(batches)
        if res == False:
           raise LoaderError(CSVLoader, "Failed to push batch of instances") 
        batch.clear()

