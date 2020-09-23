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
import collections

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker.bundle_processor import SynchronousBagRuntimeState
from apache_beam.transforms import userstate

from pyflink.common.state import ValueState


class LRUCache(object):
    """
    A simple LRUCache implementation used to manage the internal runtime state.
    An internal runtime state is used to handle the data under a specific key of a "public" state.
    So the number of the internal runtime states may keep growing during the streaming task
    execution. To prevent the OOM caused by the unlimited growth, we introduce this LRUCache
    to evict the inactive internal runtime states.
    """

    def __init__(self, max_entries, default_entry):
        self._max_entries = max_entries
        self._default_entry = default_entry
        self._cache = collections.OrderedDict()
        self._on_evict = None

    def get(self, key):
        value = self._cache.pop(key, self._default_entry)
        if value != self._default_entry:
            self._cache[key] = value
        return value

    def put(self, key, value):
        self._cache[key] = value
        while len(self._cache) > self._max_entries:
            name, value = self._cache.popitem(last=False)
            if self._on_evict is not None:
                self._on_evict(name, value)

    def evict(self, key):
        value = self._cache.pop(key, self._default_entry)
        if self._on_evict is not None:
            self._on_evict(key, value)

    def evict_all(self):
        if self._on_evict is not None:
            for item in self._cache.items():
                self._on_evict(*item)
        self._cache.clear()

    def set_on_evict(self, func):
        self._on_evict = func

    def __len__(self):
        return len(self._cache)


class SynchronousValueRuntimeState(ValueState):
    """
    The runtime ValueState implementation backed by a :class:`SynchronousBagRuntimeState`.
    """

    def __init__(self, internal_state: SynchronousBagRuntimeState):
        self._internal_state = internal_state

    def value(self):
        for i in self._internal_state.read():
            return i
        return None

    def update(self, value) -> None:
        self._internal_state.clear()
        self._internal_state.add(value)

    def clear(self) -> None:
        self._internal_state.clear()


class RemoteKeyedStateBackend(object):
    """
    A keyed state backend provides methods for managing keyed state.
    """

    def __init__(self, state_handler, key_coder, state_cache_size):
        self._state_handler = state_handler

        try:
            from pyflink.fn_execution import coder_impl_fast
            is_fast = True if coder_impl_fast else False
        except:
            is_fast = False
        if not is_fast:
            self._key_coder_impl = key_coder.get_impl()
        else:
            from pyflink.fn_execution.coders import FlattenRowCoder
            self._key_coder_impl = FlattenRowCoder(key_coder._field_coders).get_impl()
        self._state_cache_size = state_cache_size
        self._all_states = {}
        self._all_internal_states = LRUCache(self._state_cache_size, None)
        self._all_internal_states.set_on_evict(lambda k, v: v.commit())
        self._current_key = None
        self._encoded_current_key = None

    def get_value_state(self, name, value_coder):
        if name in self._all_states:
            self.validate_state(name, value_coder)
            return self._all_states[name]
        internal_bag_state = self._get_internal_bag_state(name, value_coder)
        value_state = SynchronousValueRuntimeState(internal_bag_state)
        self._all_states[name] = value_state
        return value_state

    def validate_state(self, name, coder):
        if name in self._all_states:
            state = self._all_states[name]
            if state._internal_state._value_coder != coder:
                raise ValueError("State name corrupted: %s" % name)

    def _get_internal_bag_state(self, name, element_coder):
        cached_state = self._all_internal_states.get((name, self._current_key))
        if cached_state is not None:
            return cached_state
        state_spec = userstate.BagStateSpec(name, element_coder)
        internal_state = self._create_state(state_spec)
        self._all_internal_states.put((name, self._current_key), internal_state)
        return internal_state

    def _create_state(self, state_spec: userstate.StateSpec) -> userstate.AccumulatingRuntimeState:
        if isinstance(state_spec, userstate.BagStateSpec):
            bag_state = SynchronousBagRuntimeState(
                self._state_handler,
                state_key=beam_fn_api_pb2.StateKey(
                    bag_user_state=beam_fn_api_pb2.StateKey.BagUserState(
                        transform_id="",
                        user_state_id=state_spec.name,
                        key=self._encoded_current_key)),
                value_coder=state_spec.coder)
            return bag_state
        else:
            raise NotImplementedError(state_spec)

    def set_current_key(self, key):
        self._current_key = key
        self._encoded_current_key = self._key_coder_impl.encode_nested(self._current_key)
        for state_name, state_obj in self._all_states.items():
            state_obj._internal_state = \
                self._get_internal_bag_state(state_name, state_obj._internal_state._value_coder)

    def get_current_key(self):
        return self._current_key

    def commit(self):
        self._all_internal_states.evict_all()
        self._all_states = {}

    def reset(self):
        self._all_internal_states.evict_all()
        self._all_states = {}
