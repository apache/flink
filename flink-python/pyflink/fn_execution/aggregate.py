################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from apache_beam.coders import PickleCoder

from pyflink.common.state import ListState, MapState
from pyflink.fn_execution.coders import from_proto
from pyflink.fn_execution.operation_utils import is_built_in_function, load_aggregate_function
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend
from pyflink.table import FunctionContext
from pyflink.table.data_view import ListView, MapView


def extract_data_view_specs_from_accumulator(current_index, accumulator):
    # for built in functions we extract the data view specs from their accumulator
    i = -1
    extracted_specs = []
    for field in accumulator:
        i += 1
        # TODO: infer the coder from the input types and output type of the built-in functions
        if isinstance(field, MapView):
            extracted_specs.append(MapViewSpec(
                "builtInAgg%df%d" % (current_index, i), i, PickleCoder(), PickleCoder()))
        elif isinstance(field, ListView):
            extracted_specs.append(ListViewSpec(
                "builtInAgg%df%d" % (current_index, i), i, PickleCoder()))
    return extracted_specs


def extract_data_view_specs(udfs):
    extracted_udf_data_view_specs = []
    current_index = -1
    for udf in udfs:
        current_index += 1
        udf_data_view_specs_proto = udf.specs
        if not udf_data_view_specs_proto:
            if is_built_in_function(udf.payload):
                built_in_function = load_aggregate_function(udf.payload)
                accumulator = built_in_function.create_accumulator()
                extracted_udf_data_view_specs.append(
                    extract_data_view_specs_from_accumulator(current_index, accumulator))
            else:
                extracted_udf_data_view_specs.append([])
        else:
            extracted_specs = []
            for spec_proto in udf_data_view_specs_proto:
                state_id = spec_proto.name
                field_index = spec_proto.field_index
                if spec_proto.HasField("list_view"):
                    element_coder = from_proto(spec_proto.list_view.element_type)
                    extracted_specs.append(ListViewSpec(state_id, field_index, element_coder))
                elif spec_proto.HasField("map_view"):
                    key_coder = from_proto(spec_proto.map_view.key_type)
                    value_coder = from_proto(spec_proto.map_view.value_type)
                    extracted_specs.append(
                        MapViewSpec(state_id, field_index, key_coder, value_coder))
                else:
                    raise Exception("Unsupported data view spec type: " + spec_proto.type)
            extracted_udf_data_view_specs.append(extracted_specs)
    if all([len(i) == 0 for i in extracted_udf_data_view_specs]):
        return []
    return extracted_udf_data_view_specs


class StateListView(ListView):

    def __init__(self, list_state: ListState):
        super().__init__()
        self._list_state = list_state

    def get(self):
        return self._list_state.get()

    def add(self, value):
        self._list_state.add(value)

    def add_all(self, values):
        self._list_state.add_all(values)

    def clear(self):
        self._list_state.clear()

    def __hash__(self) -> int:
        return hash([i for i in self.get()])


class StateMapView(MapView):

    def __init__(self, map_state: MapState):
        super().__init__()
        self._map_state = map_state

    def get(self, key):
        return self._map_state.get(key)

    def put(self, key, value) -> None:
        self._map_state.put(key, value)

    def put_all(self, dict_value) -> None:
        self._map_state.put_all(dict_value)

    def remove(self, key) -> None:
        self._map_state.remove(key)

    def contains(self, key) -> bool:
        return self._map_state.contains(key)

    def items(self):
        return self._map_state.items()

    def keys(self):
        return self._map_state.keys()

    def values(self):
        return self._map_state.values()

    def is_empty(self) -> bool:
        return self._map_state.is_empty()

    def clear(self) -> None:
        return self._map_state.clear()


class DataViewSpec(object):

    def __init__(self, state_id, field_index):
        self.state_id = state_id
        self.field_index = field_index


class ListViewSpec(DataViewSpec):

    def __init__(self, state_id, field_index, element_coder):
        super(ListViewSpec, self).__init__(state_id, field_index)
        self.element_coder = element_coder


class MapViewSpec(DataViewSpec):

    def __init__(self, state_id, field_index, key_coder, value_coder):
        super(MapViewSpec, self).__init__(state_id, field_index)
        self.key_coder = key_coder
        self.value_coder = value_coder


class StateDataViewStore(object):
    """
    The class used to manage the DataViews used in :class:`AggsHandleFunction`. Currently
    DataView is not supported so it is just a wrapper of the :class:`FunctionContext`.
    """

    def __init__(self,
                 function_context: FunctionContext,
                 keyed_state_backend: RemoteKeyedStateBackend):
        self._function_context = function_context
        self._keyed_state_backend = keyed_state_backend

    def get_runtime_context(self):
        return self._function_context

    def get_state_list_view(self, state_name, element_coder):
        return StateListView(self._keyed_state_backend.get_list_state(state_name, element_coder))

    def get_state_map_view(self, state_name, key_coder, value_coder):
        return StateMapView(
            self._keyed_state_backend.get_map_state(state_name, key_coder, value_coder))
