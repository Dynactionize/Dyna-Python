from ..dynagatewaytypes.enums_pb2 import ComponentType, DataType, VOID, STRING
from ..common.errors import LoaderError
from ..types import Action, Topology, Label, Labels, Value, Instance, InstanceBatches
from typing import Sequence
from ..client import DynizerClient

import xml.etree.ElementTree as ET
import itertools

class XMLAbstractElement:
    """
    Abstract XML element

    This class represents an abstract XML element, that can be use to build up
    Dynizer instance data.

    Member Variables
    ----------------
    value : any
        The actual value of the xml element

    data_type : DataType
        The Dynizer data type of the xml element

    component : ComponentType
        The Dynizer component type of the xml element

    label : str
        The label for the xml element

    Member Functions
    ----------------
    fetch_from_entity
        Should be overwritten in concrete elements that fetch the value from the
        parsed xml file

    apply_ariables
        Should be overwritten in concrete elements that retrieve the value from
        previously parsed variables out of the xml file

    """
    def __init__(self, value,
                       data_type: DataType,
                       component: ComponentType,
                       label = ''):
        self.value = value
        self.data_type = data_type
        self.component = component
        self.label = label

    def fetch_from_entity(self, entity, components, data, labels, ns):
        components.append(self.component)
        data.append(Value(self.data_type, self.value))
        labels.append(self.label)
        return True

    def apply_variables(self, combinations):
        pass



class XMLFixedElement(XMLAbstractElement):
    """
    Fixed element

    This class represents a Fixed element, that can be use to build up
    Dynizer instance data. A Fixed element acts like a constant and will always
    represent the same value. It can not be altered once provided.
    """
    def __init__(self, value,
                       data_type: DataType,
                       component: ComponentType,
                       label = ''):
        super().__init__(value, data_type, component, label)



class XMLVariableElement(XMLAbstractElement):
    """
    Variable element

    This class represents a Variable element. Variable elements are assigned
    a value based upon a combination of LoopVariables, which have been fetched
    in a pre-parsing loop of the xml file. A full combinatoric off all loop variables
    are created and provided to the variable elements. Based upon the loop_index and
    variable_index the correct element is assigned to the variable element.

    The loop index specifies the loopvariable to access in the combinatorics matrix.
    Since each loop variable can have multiple subvariable references, the variable_index
    refers to the sub-index within the loopvariable.

    See the XmlLoopVariable class for more information
    """
    def __init__(self, loop_index: int,
                       variable_index: int,
                       data_type: DataType,
                       component: ComponentType,
                       label = '',
                       transform_funcs = []):
        super().__init__(None, data_type, component, label)
        self.loop_index = loop_index
        self.variable_index = variable_index
        self.transform_funcs = list(transform_funcs)


    def apply_variables(self, combination):
        self.value = combination[self.loop_index][self.variable_index]
        for tf in self.transform_funcs:
            self.value = tf(self.value)



class XMLExtractionElement(XMLAbstractElement):
    def __init__(self, path: str,
                       data_type: DataType,
                       component: ComponentType,
                       label = '',
                       required = True,
                       default = None,
                       allow_void = True,
                       transform_funcs = []):
        super().__init__(None, data_type, component, label)
        self.path = path
        self.required = required
        self.default = default
        self.allow_void = allow_void
        self.transform_funcs = list(transform_funcs)

    def fetch_from_entity(self, entity, components, data, labels, ns):
        node = entity.findall(self.path, ns)
        if len(node) == 0:
            if self.required:
                if self.default is not None:
                    components.append(self.component)
                    data.append(Value(self.data_type, self.default))
                    labels.append(self.label)
                elif self.allow_void:
                    components.append(self.component)
                    data.append(Value(VOID, None))
                    labels.append(self.label)
                else:
                    return False
        elif len(node) == 1:
            value = node[0].text
            for tf in self.transform_funcs:
                value = tf(value)

            components.append(self.component)
            data.append(Value(self.data_type, value))
            labels.append(self.label)
        else:
            for val in node:
                value = val.text
                for tf in self.transform_funcs:
                    value = tf(value)

                components.append(self.component)
                data.append(Value(self.data_type, value))
                labels.append(self.label)
        return True


"""
class XMLDynaTextElement(XMLAbstractElement):
    def __init__(self, path: str,
                       targetaction: str,
                       dictionary: str,
                       wordcorpus: str,
                       component: ComponentType,
                       label = '',
                       required = True,
                       default = None,
                       allow_void = True,
                       transform_funcs = []):
        super().__init__(None, DataType.DYNATEXT, component, label)
        self.path = path
        self.targetaction = targetaction
        self.dictionary = dictionary
        self.wordcorpus = wordcorpus
        self.required = required
        self.default = default
        self.allow_void = allow_void
        self.transform_funcs = list(transform_funcs)

    def fetch_from_entity(self, entity, components, data, labels, ns):
        node = entity.findall(self.path, ns)
        if len(node) == 0:
            if self.required:
                if self.default is not None:
                    components.append(self.component)
                    data.append(InstanceElement(
                        value=DynaText(text=default,
                                       targetaction=self.targetaction, dictionary=self.dictionary,
                                       wordcorpus=self.wordcorpus), datatype=DataType.DYNATEXT))
                    labels.append(self.label)
                elif self.allow_void:
                    components.append(self.component)
                    data.append(InstanceElement(
                        value=DynaText(text="",
                                       targetaction=self.targetaction, dictionary=self.dictionary,
                                       wordcorpus=self.wordcorpus), datatype=DataType.DYNATEXT))
                    labels.append(self.label)
                else:
                    return False
        elif len(node) == 1:
            value = node[0].text
            for tf in self.tranform_funcs:
                value = tf(value)

            components.append(self.component)
            data.append(InstanceElement(
                value=DynaText(text=value,
                               targetaction=self.targetaction, dictionary=self.dictionary,
                               wordcorpus=self.wordcorpus), datatype=DataType.DYNATEXT))
            labels.append(self.label)
        else:
            for val in node:
                value = val.text
                for tf in self.transform_funcs:
                    value = tf(value)

                components.append(self.component)
                data.append(InstanceElement(
                    value=DynaText(text=value,
                                   targetaction=self.targetaction, dictionary=self.dictionary,
                                   wordcorpus=self.wordcorpus), datatype=DataType.DYNATEXT))
                labels.append(self.label)
        return True
"""



class XMLStringCombinationElement(XMLAbstractElement):
    def __init__(self, paths: Sequence[str],
                 component: ComponentType,
                 label = '',
                 combinator_func = None,
                 required = True,
                 sequence_join_char = ','):
        super().__init__(None, STRING, component, label)
        self.paths = paths
        self.combinator_func = combinator_func
        self.required = required
        self.sequence_join_char = sequence_join_char

    def fetch_from_entity(self, entity, components, data, labels, ns):
        tmp_data=[]
        for path in self.paths:
            node = entity.findall(path, ns)
            val = ''
            if len(node) == 1:
                val = node[0].text
            elif len(node) > 1:
                arr = []
                for n in node:
                    arr.append(n.text)
                val = self.sequence_join_char.join(arr)

            tmp_data.append(val)

        """
        if len(tmp_data) == 0:
            if not self.required:
                return True
            else:
                data.append(InstanceElement())
        else:

            if self.combinator_func is not None:
                data.append(InstanceElement(value=self.combinator_func(tmp_data), datatype=DataType.STRING))
            else:
                data.append(InstanceElement(value=self._default_combinator(tmp_data), datatype=DataType.STRING))
        """
        value = self.combinator_func(tmp_data) if self.combinator_func is not None else self._default_combinator(tmp_data)
        if len(value) == 0:
            if self.required:
                data.append(Value(STRING, ""))
            else:
                return False
        else:
            data.append(Value(STRING, value))

        components.append(self.component)
        labels.append(self.label)

        return True


    def _default_combinator(self, tmp_data):
        result = ''
        for elem in tmp_data:
            if len(elem) > 0:
                if len(result) == 0:
                    result = '{0}'.format(elem)
                else:
                    result = '{0} {1}'.format(result, elem)
        return result



class XMLLoopVariable:
    def __init__(self, path: str, variable_path: Sequence[str]):
        self.path = path
        self.variable_path = variable_path


class XMLMapping:
    def __init__(self, action: Action,
                       root_path: str,
                       variables: Sequence[XMLLoopVariable],
                       elements: Sequence[XMLAbstractElement],
                       fallback: Sequence[XMLAbstractElement] = [],
                       batch_size=100):
        self.action = action
        self.root_path = root_path
        self.variables = variables
        self.elements = elements
        self.fallback = list(fallback)
        self.expanded_variables = []
        self.batch_size = batch_size


class XMLLoader:
    def __init__(self, root_node: ET.Element,
                       mappings: Sequence[XMLMapping] = [],
                       namespaces={}):
        self.root_node = root_node
        self.mappings = list(mappings)
        self.ns = namespaces

    @classmethod
    def parse(cls, xml_file: str, mappings: Sequence[XMLMapping] = [], namespaces={}):
        return cls(ET.parse(xml_file).getroot(), mappings, namespaces)

    @classmethod
    def fromstring(cls, xml_string: str):
        return cls(ET.fromstring(xml_string))

    def add_mapping(self, mapping: XMLMapping):
        self.mappings.append(mapping)

    def run(self, client: DynizerClient, debug=False):
        try:
            for mapping in self.mappings:
                self.__run_mapping(client, mapping, debug)
        except Exception as e:
            raise e


    def __expand_variables(self, root, mapping, variable):
        elements = root.findall(variable.path, self.ns)
        values = []
        for elem in elements:
            v_values = []
            for v_path in variable.variable_path:
                var_elem = elem.findall(v_path, self.ns)
                v_values.append(list(map(lambda x: x.text, var_elem)))
            values = values + list(itertools.product(*v_values))
        mapping.expanded_variables.append(values)


    def __run_mapping(self, client: DynizerClient,
                            mapping: XMLMapping,
                            debug):

        print('Creating instances for: {0}'.format(mapping.action.get_name()))
        action_obj = client.action_service().create(mapping.action)

        topology_map = {}
        loadlist = []

        if len(mapping.variables) == 0:
            # No loopvariables are present
            self.__run_simple_mapping(client, mapping, mapping.root_path, action_obj, topology_map, loadlist, debug)
        else:
            # We have loop variables, resolve them
            for variable in mapping.variables:
                self.__expand_variables(self.root_node, mapping, variable)

            # Make the combinations of the various loop variables and iterate over them
            var_combinations = list(itertools.product(*mapping.expanded_variables))
            for combination in var_combinations:
                # Adjust the root path with the requested loop variables
                current_root = mapping.root_path.format(*combination)
                for elem in mapping.elements:
                    # Apply the current variables to the XMLVariableElements
                    elem.apply_variables(combination)
                for elem in mapping.fallback:
                    elem.apply_variables(combination)

                self.__run_simple_mapping(client, mapping, current_root, action_obj, topology_map, loadlist, debug)

        if len(loadlist) > 0:
            self.__push_batch(client, loadlist)


    def __run_simple_mapping(self, client: DynizerClient,
                                   mapping: XMLMapping,
                                   root_path: str,
                                   action_obj, topology_map, loadlist,
                                   debug):
        # Fetch the root node
        root = None

        try:
            root = self.root_node.findall(root_path, self.ns)
        except Exception as e:
            print(root_path)
            raise e
        if len(root) == 0:
            raise LoaderError(XMLLoader, "Invalid xpath specified for root path: '{0}'".format(root_path))

        # Loop over all entities in the root node and parse the entities
        for entity in root:
            status = self.__run_mapping_on_entity(entity, topology_map, loadlist, action_obj, client, mapping, debug = debug)
            if not status:
                self.__run_mapping_on_entity(entity, topology_map, loadlist, action_obj, client, mapping, fallback=True, debug = debug)


        if len(loadlist) >= mapping.batch_size:
            print('LOADLIST: {0}'.format(len(loadlist)))
            self.__push_batch(client, loadlist)



    def __run_mapping_on_entity(self, entity, topology_map, loadlist,
                                      action_obj: Action,
                                      client: DynizerClient,
                                      mapping: XMLMapping,
                                      fallback = False,
                                      debug = False):
        components = []
        data = []
        labels = []
        elements = mapping.fallback if fallback else mapping.elements
        if len(elements) == 0:
            return False

        # Loop over all elements and fetch them fro mthe entity
        for element in elements:
            if element.fetch_from_entity(entity, components, data, labels, self.ns) == False:
                return False

        if len(components) < 2:
            return False

        # Build the topology
        top_map_key = ','.join(map(str, components))
        topology_obj = None
        if top_map_key in topology_map:
            topology_obj = topology_map[top_map_key]
        else:
            # Check if we have it in the system
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

        # Create the instance and push it onto the load list
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
            raise LoaderError(XMLLoader, "Failed to push batch of instances")
        batch.clear()

