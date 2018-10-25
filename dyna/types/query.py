from ..dynagatewaytypes.query_pb2 import LogicalOperator, AND, OR, AND_NOT, OR_NOT
from ..dynagatewaytypes.query_pb2 import (
    WhereObject, WO_ACTION_ID, WO_ACTION_NAME, WO_TOPOLOGY_ID, WO_TOPOLOGY_SEQUENCE,
    WO_TOPOLOGY_LENGTH, WO_TOPOLOGY_COMPONENT, WO_VALUE, WO_INSTANCE_ID
)
from ..dynagatewaytypes.query_pb2 import (
    WhereOperator, EQUAL, NOT_EQUAL, IN, NOT_IN, LIKE, NOT_LIKE, GREATER_THAN, LESS_THAN,
    GREATER_OR_EQUAL_THAN, LESS_OR_EQUAL_THAN, BETWEEN
)
from ..dynagatewaytypes.query_pb2 import ActionQueryRes, TopologyQueryRes, InstanceQueryRes
from ..dynagatewaytypes.query_pb2 import (
    ActionFetchReq, TopologyFetchReq, InstanceFetchReq, InstanceGroupFetchReq,
    InstanceGroupedFetchReq
)

from ..dynagatewaytypes.query_pb2 import QueryReq
from ..dynagatewaytypes.query_pb2 import WhereClauseGroup as PBWhereClauseGroup
from ..dynagatewaytypes.query_pb2 import WhereClause as PBWhereClause
from ..dynagatewaytypes.query_pb2 import Window as PBWindow
from ..dynagatewaytypes.query_pb2 import ActionSortField as PBActionSortField
from ..dynagatewaytypes.query_pb2 import TopologySortField as PBTopologySortField
from ..dynagatewaytypes.query_pb2 import InstanceSortField as PBInstanceSortField
from ..dynagatewaytypes.query_pb2 import GroupSortField as PBGroupSortField
from ..dynagatewaytypes.query_pb2 import InstanceGroupBy, ACTION_TOPOLOGY, ACTION, Topology

from ..dynagatewaytypes.enums_pb2 import ComponentType, CT_NONE, WHO, WHAT, WHERE, WHEN

from ..common.errors import DynaValueError

from .value import Value
from .auth import Token

from typing import Sequence, Union
from enum import Enum

class WhereClause:
    def __init__(self, object: WhereObject, op: WhereOperator, rhs, component: ComponentType,
                 position: int, label: str):
        self._object = object
        self._op = op
        self._rhs = rhs
        self._component = component
        self._position = position
        self._label = label

    @staticmethod
    def _from_pb(wc: PBWhereClause):
        rhs = None
        if wc.HasField("single"):
            rhs = Value._from_pb(wc.value)
        else:
            rhs = list(map(Value._from_pb, wc.list))
        return WhereClause(wc.object, wc.operator, rhs, wc.component, wc.position, wc.label)

    @staticmethod
    def _where_clause_to_pb(wc) -> PBWhereClause:
        return wc._to_pb()

    def _to_pb(self) -> PBWhereClause:
        pb = PBWhereClause()
        pb.object = self._object
        pb.operator = self._op
        if isinstance(self._rhs, list):
            pb.list.extend(list(map(Value._value_to_pb, self._rhs)))
        else:
            pb.value = self._rhs._to_pb()
        pb.component = self._component
        pb.position = self._position
        pb.label = self._label
        return pb

    def set_object(self, object: WhereObject):
        self._object = object

    def get_object(self) -> WhereObject:
        return self._object

    def set_operator(self, op: WhereOperator):
        self._op = op

    def get_operator(self) -> WhereOperator:
        return self._op

    def set_rhs(self, rhs):
        if isinstance(rhs, Value) or (
            isinstance(rhs, list) and len(rhs) > 0 and isinstance(rhs[0], Value)
        ):
            self._rhs = rhs
        else:
            raise DynaValueError(rhs, "Value or [Value]")

    def get_rhs(self):
        return self._rhs

    def set_component_type(self, component: ComponentType):
        self._component = component
    
    def get_component_type(self) -> ComponentType:
        return self._component

    def set_position(self, position: int):
        self._position = position

    def get_position(self) -> int:
        return self._position

    def set_label(self, label: str):
        self._label = label

    def get_label(self) -> str:
        return self._label



class WhereClauseGroup:
    def __init__(self, op: LogicalOperator, clauses: Sequence[WhereClause]):
        self._op = op
        self._clauses = clauses

    @staticmethod
    def _from_pb(wcg: PBWhereClauseGroup):
        return WhereClauseGroup(wcg.LogicalOperator,
                                list(map(WhereClause._from_pb, wcg._where_clauses)))

    @staticmethod
    def _where_clause_group_to_pb(wcg) -> PBWhereClauseGroup:
        return wcg._to_pb()

    def _to_pb(self) -> PBWhereClauseGroup:
        pb = PBWhereClauseGroup()
        pb.logical_operator = self._op
        pb.where_clauses.extend(list(map(WhereClause._where_clause_to_pb, self._clauses)))
        return pb

    def set_operator(self, op: LogicalOperator):
        self._op = op
    
    def get_operator(self) -> LogicalOperator:
        return self._op

    def add_where_clause(self, clause: WhereClause):
        if self._clauses is None:
            self._clauses = []
        self._clauses.append(clause)

    def extend_where_clauses(self, clauses: Sequence[WhereClause]):
        if self._clauses is None:
            self._clauses = []
        self._clauses.extend(clauses)

    def get_where_clauses(self) -> Sequence[WhereClause]:
        return self._clauses

    def _to_pb(self) -> PBWhereClauseGroup:
        pb = PBWhereClauseGroup()
        pb.LogicalOperator = self._op

class Query:
    def __init__(self, groups: Sequence[WhereClauseGroup]):
        self._groups = groups

    def add_where_clause_group(self, group: WhereClauseGroup):
        if self._groups is None:
            self._groups = []
        self._groups.append(group)

    def extend_where_clause_groups(self, groups: Sequence[WhereClauseGroup]):
        if self._groups is None:
            self._groups = []
        self._groups.extend(groups)

    def get_where_clause_groups(self) -> Sequence[WhereClauseGroup]:
        return self._groups

    def query_request(self, token: Token):
        pb = QueryReq()
        if not token is None:
            pb.token = token._to_message()
        pb.clause_groups.extend(list(map(WhereClauseGroup._where_clause_group_to_pb, self._groups)))
        return pb

class CursorType(Enum):
    UNKNOWN_CURSOR = 0
    ACTION_CURSOR = 1
    TOPOLOGY_CURSOR = 2
    INSTANCE_CURSOR = 3

class Cursor:
    def __init__(self):
        self._type = CursorType.UNKNOWN_CURSOR
        self._cursor = None
        self._count = None

        # INSTANCE_CURSOR specific fields
        self._number_action_topologies = None
        self._number_actions = None
        self._number_topologies = None

    def get_count(self) -> int:
        return self._count

    def get_number_action_topologies(self) -> int:
        return self._number_action_topologies

    def get_number_actions(self) -> int:
        return self._number_actions

    def get_number_topologies(self) -> int:
        return self._njumber_topologies

    def __str__(self):
        return str(self._cursor)

    @staticmethod
    def from_query_res(res):
        cursor = Cursor()
        cursor._cursor = res.cursor
        cursor._count = res.count

        if isinstance(res, InstanceQueryRes):
            cursor._number_action_topologies = res.number_action_topologies
            cursor._number_actions = res.number_action_topologies
            cursor._number_topologies = res.number_topologies

        return cursor


class Window:
    def __init__(self, offset: int, size: int):
        self._offset = offset
        self._size = size

    def get_offset(self) -> int:
        return self._offset

    def get_size(self) -> int:
        return self._size

    def set_offset(self, offset: int):
        self._offset = offset

    def set_Size(self, size) -> int:
        self._size = size

    @staticmethod
    def _from_pb(pb: PBWindow):
        return Window(pb.offset, pb.size)

    @staticmethod
    def _window_to_pb(window) -> PBWindow:
        return window._to_pb()

    def _to_pb(self) -> _window_to_pb:
        pb = PBWindow()
        pb.offset = self._offset
        pb._size = self.size
        return pb


class SortField:
    def __init__(self, descending: bool=False):
        self._descending = descending

    @staticmethod
    def _sortfield_to_pb(field):
        return field._to_pb()

class ActionSortField(SortField):
    def __init__(self, id: bool=False, name: bool=False, descending: bool=False):
        super().__init__(descending)
        self._id = id
        self._name = name

    def _to_pb(self) -> PBActionSortField:
        if self._id == False and self._name == False:
            return None

        pb = PBActionSortField()
        pb.id = self._id
        pb.name = self._name
        pb.descending = self._descending
        return pb

class TopologySortField(SortField):
    def __init__(self, id: bool=False, topology_length: bool=False, descending: bool=False):
        super().__init__(descending)
        self._id = id
        self._topology_length = topology_length

    def _to_pb(self):
        if self._id == False and self._topology_length == False:
            return None

        pb = PBTopologySortField()
        pb.id = self._id
        pb.topology_length = self._topology_length
        return pb

class InstanceSortField(SortField):
    def __init__(self, id: bool=False, action_name: bool=False, topology_length: bool=False,            
                       topology_position: int=None, descending: bool=False):
        super().__init__(secending)
        self._id = id
        self._action_name = action_name
        self._topology_length = topology_length
        self._topology_position = topology_position

    def _to_pb(self) -> PBInstanceSortField:
        if (self.id == False and self._action_name == False and self._topology_length == False and
            self._topology_position is None):
            return None

        pb = PBInstanceSortField()
        pb.id = self._id
        pb.action_name = self._action_name
        pb.topology_length = self._topology_length
        if not self._topology_position is None:
            pb.topology_position = self._topology_position
        return pb

class GroupSortField(SortField):
    def __init__(self, action_id: bool=False, action_name: bool=False, toplogy_id: bool=False,
                 topology_length: bool=False, descending: bool=False):
        super().__init__(descending)
        self._action_id = action_id
        self._aciton_name = action_name
        self._topology_id = _topology_id
        self._topology_length = topology_length

    def _to_pb(self) -> PBGroupSortField:
        if (self.action_id == False and self._action_name == False and
            self._topology_id == False and self._topology_length == False):
            return None
        
        pb = PBGroupSortField()
        pb.action_id = self._action_id
        pb.action_name = self._aciton_name
        pb.topology_id = self._topology_id
        pb.topology_length = self._topology_length
        return pb

class FetchRequest:
    def __init__(self, cursor_string: str,
                 sortfields: Union[
                     Sequence[ActionSortField],
                     Sequence[TopologySortField],
                     Sequence[InstanceSortField]] = [],
                 group_by: InstanceGroupBy=None,
                 group_sorting: Sequence[GroupSortField] = [],
                 current_group: int = 1,
                 window: Window = None,
                 omit_labels: bool=False):
        self._cursor = cursor_string
        self._sortfields = sortfields
        self._group_by = group_by
        self._group_sorting = group_sorting
        self._current_group = current_group
        self._window = window
        self._omit_labels = omit_labels

    def get_cursor(self) -> str:
        return self._cursor

    def get_sortfields(self) -> Sequence[SortField]:
        return self._sortfields

    def get_groupby(self) -> InstanceGroupBy:
        return self._group_by

    def get_groupsorting(self) -> Sequence[GroupSortField]:
        return self._group_sorting

    def get_currentgroup(self) -> int:
        return self._current_group

    def get_window(self) -> Window:
        return self._window

    def get_omitlabels(self) -> bool:
        return self._omit_labels

    def set_cursor(self, cursor: str):
        self._cursor = cursor

    def set_sortfields(self,
                       sortfields: Union[
                           Sequence[ActionSortField],
                           Sequence[TopologySortField],
                           Sequence[InstanceSortField]
                       ]):
        self._sortfields = sortfields

    def set_groupby(self, group_by: InstanceGroupBy):
        self._group_by = group_by

    def set_groupsorting(self, group_sorting: Sequence[GroupSortField]):
        self._group_sorting = group_sorting

    def set_currentgroup(self, current_group: int):
        self._current_group = current_group

    def set_window(self, window: Window):
        self._window = window

    def set_omitlabels(self, omit_labels: bool):
        self._omit_labels = omit_labels



class ActionFetchRequest(FetchRequest):
    def __init__(self, cursor_string: str,
                 sortfields: Sequence[ActionSortField] = [],
                 window: Window = None):
        super().__init__(cursor_string,
                         sortfields = sortfields,
                         window = window)
                 

    def fetch_request(self, token: Token) -> ActionFetchReq:
        pb = ActionFetchReq()
        if not token is None:
            pb.token = token._to_message()
        pb.cursor = self._cursor
        pb.sorting.extend(list(map(ActionSortField._sortfield_to_pb, self._sortfields)))
        pb.window = self._window._to_pb()
        return pb

class TopologyFetchRequest(FetchRequest):
    def __init__(self, cursor_string: str,
                 sortfields: Sequence[TopologySortField] = [],
                 window: Window = None):
        super().__init__(cursor_string,
                         sortfields = sortfields,
                         window = window)

    def fetch_request(self, token: Token) -> TopologyFetchReq:
        pb = TopologyFetchReq()
        if not token is None:
            pb.token = token._to_message()
        pb.cursor = self._cursor
        pb.sorting.extend(list(map(TopologySortField._sortfield_to_pb, self._sortfields)))
        pb.window = self._window._to_pb()
        return pb

class InstanceFetchRequest(FetchRequest):
    def __init__(self, cursor_string: str,
                 sortfields: Sequence[InstanceSortField] = [],
                 window: Window = None):
        super().__init__(cursor_string,
                         sortfields = sortfields,
                         window = window)

    def fetch_request(self, token: Token) -> TopologyFetchReq:
        pb = InstanceFetchReq()
        if not token is None:
            pb.token = token._to_message()
        pb.cursor = self._cursor
        pb.sorting.extend(list(map(InstanceSortField._sortfield_to_pb, self._sortfields)))
        pb.window = self._window._to_pb()
        return pb

class InstanceGroupFetchRequest(FetchRequest):
    def __init__(self, cursor_string: str,
                 sortfields: Sequence[InstanceSortField] = [],
                 group_by: InstanceGroupBy = None,
                 group_sorting: Sequence[GroupSortField] = [],
                 current_group: int = 1,
                 window: Window = None,
                 omit_labels: bool = False):
        super().__init__(cursor_string,
                         sortfields = sortfields,
                         group_by = group_by,
                         group_sorting = group_sorting,
                         current_group = current_group,
                         window = window,
                         omit_labels = omit_labels)

    def fetch_request(self, token: Token) -> InstanceGroupFetchReq:
        pb = InstanceGroupFetchReq()
        if not token is None:
            pb.token = token._to_message()
        pb.cursor = self._cursor
        pb.sorting.extend(list(map(InstanceSortField._sortfield_to_pb, self._sortfields)))
        pb.group_by = self._group_by
        pb.group_sorting = list(map(GroupSortField._sortfield_to_pb, self._group_sorting))
        pb.current_group = self._current_group
        pb.window = self._window.to_pb()
        pb.omit_labels = self._omit_labels
        return pb

class InstanceGroupedFetchRequest(FetchRequest):
    def __init__(self, cursor_string: str,
                 sortfields: Sequence[InstanceSortField] = [],
                 group_by: InstanceGroupBy = None,
                 group_sorting: Sequence[GroupSortField] = [],
                 windows: Sequence[Window] = [],
                 omit_labels: bool = False):
        super().__init__(cursor_string,
                         sortfields = sortfields,
                         group_by = group_by,
                         group_sorting = group_sorting,
                         omit_labels = omit_labels)
        self._windows = windows

    def get_windows(self) -> Sequence[Window]:
        return self._windows

    def set_windows(self, windows: Sequence[Window]):
        self._windows = windows

    def fetch_request(self, token: Token) -> InstanceGroupedFetchReq:
        pb = InstanceGroupFetchReq()
        if not token is None:
            pb.token = token._to_message()
        pb.cursor = self._cursor
        pb.sorting.extend(list(map(InstanceSortField._sortfield_to_pb, self._sortfields)))
        pb.group_by = self._group_by
        pb.group_sorting.extend(list(map(GroupSortField._sortfield_to_pb, self._group_sorting)))
        pb.group_windows.extend(list(map(Window._window_to_pb, self._windows)))
        pb.omit_labels = self._omit_labels
        return pb

