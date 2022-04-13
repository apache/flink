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
import base64
import collections
from abc import ABC, abstractmethod
from enum import Enum
from functools import partial
from io import BytesIO

from apache_beam.coders import coder_impl
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker.bundle_processor import SynchronousBagRuntimeState
from apache_beam.transforms import userstate
from typing import List, Tuple, Any, Dict, Collection

from pyflink.datastream import ReduceFunction
from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.state import StateTtlConfig
from pyflink.fn_execution.beam.beam_coders import FlinkCoder
from pyflink.fn_execution.coders import FieldCoder
from pyflink.fn_execution.internal_state import InternalKvState, N, InternalValueState, \
    InternalListState, InternalReducingState, InternalMergingState, InternalAggregatingState, \
    InternalMapState


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
            # update the last access time
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

    def __iter__(self):
        return iter(self._cache.values())

    def __contains__(self, key):
        return key in self._cache


class SynchronousKvRuntimeState(InternalKvState, ABC):
    """
    Base Class for partitioned State implementation.
    """

    def __init__(self, name: str, remote_state_backend: 'RemoteKeyedStateBackend'):
        self.name = name
        self._remote_state_backend = remote_state_backend
        self._internal_state = None
        self.namespace = None
        self._ttl_config = None
        self._cache_type = SynchronousKvRuntimeState.CacheType.ENABLE_READ_WRITE_CACHE

    def set_current_namespace(self, namespace: N) -> None:
        if namespace == self.namespace:
            return
        if self.namespace is not None:
            self._remote_state_backend.cache_internal_state(
                self._remote_state_backend._encoded_current_key, self)
        self.namespace = namespace
        self._internal_state = None

    def enable_time_to_live(self, ttl_config: StateTtlConfig):
        self._ttl_config = ttl_config
        if ttl_config.get_state_visibility() == StateTtlConfig.StateVisibility.NeverReturnExpired:
            self._cache_type = SynchronousKvRuntimeState.CacheType.DISABLE_CACHE
        elif ttl_config.get_update_type() == StateTtlConfig.UpdateType.OnReadAndWrite:
            self._cache_type = SynchronousKvRuntimeState.CacheType.ENABLE_WRITE_CACHE

        if self._cache_type != SynchronousKvRuntimeState.CacheType.ENABLE_READ_WRITE_CACHE:
            # disable read cache
            self._remote_state_backend._state_handler._state_cache._cache._max_entries = 0

    @abstractmethod
    def get_internal_state(self):
        pass

    class CacheType(Enum):
        DISABLE_CACHE = 0
        ENABLE_WRITE_CACHE = 1
        ENABLE_READ_WRITE_CACHE = 2


class SynchronousBagKvRuntimeState(SynchronousKvRuntimeState, ABC):
    """
    Base Class for State implementation backed by a :class:`SynchronousBagRuntimeState`.
    """
    def __init__(self, name: str, value_coder, remote_state_backend: 'RemoteKeyedStateBackend'):
        super(SynchronousBagKvRuntimeState, self).__init__(name, remote_state_backend)
        self._value_coder = value_coder

    def get_internal_state(self):
        if self._internal_state is None:
            self._internal_state = self._remote_state_backend._get_internal_bag_state(
                self.name, self.namespace, self._value_coder, self._ttl_config)
        return self._internal_state

    def _maybe_clear_write_cache(self):
        if self._cache_type == SynchronousKvRuntimeState.CacheType.DISABLE_CACHE or \
                self._remote_state_backend._state_cache_size <= 0:
            self._internal_state.commit()
            self._internal_state._cleared = False
            self._internal_state._added_elements = []


class SynchronousValueRuntimeState(SynchronousBagKvRuntimeState, InternalValueState):
    """
    The runtime ValueState implementation backed by a :class:`SynchronousBagRuntimeState`.
    """

    def __init__(self, name: str, value_coder, remote_state_backend: 'RemoteKeyedStateBackend'):
        super(SynchronousValueRuntimeState, self).__init__(name, value_coder, remote_state_backend)

    def value(self):
        for i in self.get_internal_state().read():
            return i
        return None

    def update(self, value) -> None:
        self.get_internal_state()
        self._internal_state.clear()
        self._internal_state.add(value)
        self._maybe_clear_write_cache()

    def clear(self) -> None:
        self.get_internal_state().clear()


class SynchronousMergingRuntimeState(SynchronousBagKvRuntimeState, InternalMergingState, ABC):
    """
    Base Class for MergingState implementation.
    """

    def __init__(self, name: str, value_coder, remote_state_backend: 'RemoteKeyedStateBackend'):
        super(SynchronousMergingRuntimeState, self).__init__(
            name, value_coder, remote_state_backend)

    def merge_namespaces(self, target: N, sources: Collection[N]) -> None:
        self._remote_state_backend.merge_namespaces(self, target, sources, self._ttl_config)


class SynchronousListRuntimeState(SynchronousMergingRuntimeState, InternalListState):
    """
    The runtime ListState implementation backed by a :class:`SynchronousBagRuntimeState`.
    """

    def __init__(self, name: str, value_coder, remote_state_backend: 'RemoteKeyedStateBackend'):
        super(SynchronousListRuntimeState, self).__init__(name, value_coder, remote_state_backend)

    def add(self, v):
        self.get_internal_state().add(v)
        self._maybe_clear_write_cache()

    def get(self):
        return self.get_internal_state().read()

    def add_all(self, values):
        self.get_internal_state()._added_elements.extend(values)
        self._maybe_clear_write_cache()

    def update(self, values):
        self.clear()
        self.add_all(values)
        self._maybe_clear_write_cache()

    def clear(self):
        self.get_internal_state().clear()


class SynchronousReducingRuntimeState(SynchronousMergingRuntimeState, InternalReducingState):
    """
    The runtime ReducingState implementation backed by a :class:`SynchronousBagRuntimeState`.
    """

    def __init__(self,
                 name: str,
                 value_coder,
                 remote_state_backend: 'RemoteKeyedStateBackend',
                 reduce_function: ReduceFunction):
        super(SynchronousReducingRuntimeState, self).__init__(
            name, value_coder, remote_state_backend)
        self._reduce_function = reduce_function

    def add(self, v):
        current_value = self.get()
        if current_value is None:
            self._internal_state.add(v)
        else:
            self._internal_state.clear()
            self._internal_state.add(self._reduce_function.reduce(current_value, v))
        self._maybe_clear_write_cache()

    def get(self):
        for i in self.get_internal_state().read():
            return i
        return None

    def clear(self):
        self.get_internal_state().clear()


class SynchronousAggregatingRuntimeState(SynchronousMergingRuntimeState, InternalAggregatingState):
    """
    The runtime AggregatingState implementation backed by a :class:`SynchronousBagRuntimeState`.
    """

    def __init__(self,
                 name: str,
                 value_coder,
                 remote_state_backend: 'RemoteKeyedStateBackend',
                 agg_function: AggregateFunction):
        super(SynchronousAggregatingRuntimeState, self).__init__(
            name, value_coder, remote_state_backend)
        self._agg_function = agg_function

    def add(self, v):
        if v is None:
            self.clear()
            return
        accumulator = self._get_accumulator()
        if accumulator is None:
            accumulator = self._agg_function.create_accumulator()
        accumulator = self._agg_function.add(v, accumulator)
        self._internal_state.clear()
        self._internal_state.add(accumulator)
        self._maybe_clear_write_cache()

    def get(self):
        accumulator = self._get_accumulator()
        if accumulator is None:
            return None
        else:
            return self._agg_function.get_result(accumulator)

    def _get_accumulator(self):
        for i in self.get_internal_state().read():
            return i
        return None

    def clear(self):
        self.get_internal_state().clear()


class CachedMapState(LRUCache):

    def __init__(self, max_entries):
        super(CachedMapState, self).__init__(max_entries, None)
        self._all_data_cached = False
        self._cached_keys = set()

        def on_evict(key, value):
            self._cached_keys.remove(key)
            self._all_data_cached = False

        self.set_on_evict(on_evict)

    def set_all_data_cached(self):
        self._all_data_cached = True

    def is_all_data_cached(self):
        return self._all_data_cached

    def put(self, key, exists_and_value):
        if exists_and_value[0]:
            self._cached_keys.add(key)
        super(CachedMapState, self).put(key, exists_and_value)

    def get_cached_keys(self):
        return self._cached_keys


class IterateType(Enum):
    ITEMS = 0
    KEYS = 1
    VALUES = 2


class IteratorToken(Enum):
    """
    The token indicates the status of current underlying iterator. It can also be a UUID,
    which represents an iterator on the Java side.
    """
    NOT_START = 0
    FINISHED = 1


def create_cache_iterator(cache_dict, iterate_type, iterated_keys=None):
    if iterated_keys is None:
        iterated_keys = []
    if iterate_type == IterateType.KEYS:
        for key, (exists, value) in cache_dict.items():
            if not exists or key in iterated_keys:
                continue
            yield key, key
    elif iterate_type == IterateType.VALUES:
        for key, (exists, value) in cache_dict.items():
            if not exists or key in iterated_keys:
                continue
            yield key, value
    elif iterate_type == IterateType.ITEMS:
        for key, (exists, value) in cache_dict.items():
            if not exists or key in iterated_keys:
                continue
            yield key, (key, value)
    else:
        raise Exception("Unsupported iterate type: %s" % iterate_type)


class CachingMapStateHandler(object):
    # GET request flags
    GET_FLAG = 0
    ITERATE_FLAG = 1
    CHECK_EMPTY_FLAG = 2
    # GET response flags
    EXIST_FLAG = 0
    IS_NONE_FLAG = 1
    NOT_EXIST_FLAG = 2
    IS_EMPTY_FLAG = 3
    NOT_EMPTY_FLAG = 4
    # APPEND request flags
    DELETE = 0
    SET_NONE = 1
    SET_VALUE = 2

    def __init__(self, caching_state_handler, max_cached_map_key_entries):
        self._state_cache = caching_state_handler._state_cache
        self._underlying = caching_state_handler._underlying
        self._context = caching_state_handler._context
        self._max_cached_map_key_entries = max_cached_map_key_entries
        self._cached_iterator_num = 0

    def _get_cache_token(self):
        if not self._state_cache.is_cache_enabled():
            return None
        if self._context.user_state_cache_token:
            return self._context.user_state_cache_token
        else:
            return self._context.bundle_cache_token

    def blocking_get(self, state_key, map_key, map_key_encoder, map_value_decoder):
        cache_token = self._get_cache_token()
        if not cache_token:
            # cache disabled / no cache token, request from remote directly
            return self._get_raw(state_key, map_key, map_key_encoder, map_value_decoder)

        # lookup cache first
        cache_state_key = self._convert_to_cache_key(state_key)
        cached_map_state = self._state_cache.get(cache_state_key, cache_token)
        if cached_map_state is None:
            # request from remote
            exists, value = self._get_raw(state_key, map_key, map_key_encoder, map_value_decoder)
            cached_map_state = CachedMapState(self._max_cached_map_key_entries)
            cached_map_state.put(map_key, (exists, value))
            self._state_cache.put(cache_state_key, cache_token, cached_map_state)
            return exists, value
        else:
            cached_value = cached_map_state.get(map_key)
            if cached_value is None:
                if cached_map_state.is_all_data_cached():
                    return False, None

                # request from remote
                exists, value = self._get_raw(
                    state_key, map_key, map_key_encoder, map_value_decoder)
                cached_map_state.put(map_key, (exists, value))
                return exists, value
            else:
                return cached_value

    def lazy_iterator(self, state_key, iterate_type, map_key_decoder, map_value_decoder,
                      iterated_keys):
        cache_token = self._get_cache_token()
        if cache_token:
            # check if the data in the read cache can be used
            cache_state_key = self._convert_to_cache_key(state_key)
            cached_map_state = self._state_cache.get(cache_state_key, cache_token)
            if cached_map_state and cached_map_state.is_all_data_cached():
                return create_cache_iterator(
                    cached_map_state._cache, iterate_type, iterated_keys)

        # request from remote
        last_iterator_token = IteratorToken.NOT_START
        current_batch, iterator_token = self._iterate_raw(
            state_key, iterate_type,
            last_iterator_token,
            map_key_decoder,
            map_value_decoder)

        if cache_token and \
                iterator_token == IteratorToken.FINISHED and \
                iterate_type != IterateType.KEYS and \
                self._max_cached_map_key_entries >= len(current_batch):
            # Special case: all the data of the map state is contained in current batch,
            # and can be stored in the cached map state.
            cached_map_state = CachedMapState(self._max_cached_map_key_entries)
            cache_state_key = self._convert_to_cache_key(state_key)
            for key, value in current_batch.items():
                cached_map_state.put(key, (True, value))
            cached_map_state.set_all_data_cached()
            self._state_cache.put(cache_state_key, cache_token, cached_map_state)

        return self._lazy_remote_iterator(
            state_key,
            iterate_type,
            map_key_decoder,
            map_value_decoder,
            iterated_keys,
            iterator_token,
            current_batch)

    def _lazy_remote_iterator(
            self,
            state_key,
            iterate_type,
            map_key_decoder,
            map_value_decoder,
            iterated_keys,
            iterator_token,
            current_batch):
        if iterate_type == IterateType.KEYS:
            while True:
                for key in current_batch:
                    if key in iterated_keys:
                        continue
                    yield key, key
                if iterator_token == IteratorToken.FINISHED:
                    break
                current_batch, iterator_token = self._iterate_raw(
                    state_key,
                    iterate_type,
                    iterator_token,
                    map_key_decoder,
                    map_value_decoder)
        elif iterate_type == IterateType.VALUES:
            while True:
                for key, value in current_batch.items():
                    if key in iterated_keys:
                        continue
                    yield key, value
                if iterator_token == IteratorToken.FINISHED:
                    break
                current_batch, iterator_token = self._iterate_raw(
                    state_key,
                    iterate_type,
                    iterator_token,
                    map_key_decoder,
                    map_value_decoder)
        elif iterate_type == IterateType.ITEMS:
            while True:
                for key, value in current_batch.items():
                    if key in iterated_keys:
                        continue
                    yield key, (key, value)
                if iterator_token == IteratorToken.FINISHED:
                    break
                current_batch, iterator_token = self._iterate_raw(
                    state_key,
                    iterate_type,
                    iterator_token,
                    map_key_decoder,
                    map_value_decoder)
        else:
            raise Exception("Unsupported iterate type: %s" % iterate_type)

    def extend(self, state_key, items: List[Tuple[int, Any, Any]],
               map_key_encoder, map_value_encoder):
        cache_token = self._get_cache_token()
        if cache_token:
            # Cache lookup
            cache_state_key = self._convert_to_cache_key(state_key)
            cached_map_state = self._state_cache.get(cache_state_key, cache_token)
            if cached_map_state is None:
                cached_map_state = CachedMapState(self._max_cached_map_key_entries)
                self._state_cache.put(cache_state_key, cache_token, cached_map_state)
            for request_flag, map_key, map_value in items:
                if request_flag == self.DELETE:
                    cached_map_state.put(map_key, (False, None))
                elif request_flag == self.SET_NONE:
                    cached_map_state.put(map_key, (True, None))
                elif request_flag == self.SET_VALUE:
                    cached_map_state.put(map_key, (True, map_value))
                else:
                    raise Exception("Unknown flag: " + str(request_flag))
        return self._append_raw(
            state_key,
            items,
            map_key_encoder,
            map_value_encoder)

    def check_empty(self, state_key):
        cache_token = self._get_cache_token()
        if cache_token:
            # Cache lookup
            cache_state_key = self._convert_to_cache_key(state_key)
            cached_map_state = self._state_cache.get(cache_state_key, cache_token)
            if cached_map_state is not None:
                if cached_map_state.is_all_data_cached() and \
                        len(cached_map_state.get_cached_keys()) == 0:
                    return True
                elif len(cached_map_state.get_cached_keys()) > 0:
                    return False
        return self._check_empty_raw(state_key)

    def clear(self, state_key):
        self.clear_read_cache(state_key)
        return self._underlying.clear(state_key)

    def clear_read_cache(self, state_key):
        cache_token = self._get_cache_token()
        if cache_token:
            cache_key = self._convert_to_cache_key(state_key)
            self._state_cache.evict(cache_key, cache_token)

    def get_cached_iterators_num(self):
        return self._cached_iterator_num

    def _inc_cached_iterators_num(self):
        self._cached_iterator_num += 1

    def _dec_cached_iterators_num(self):
        self._cached_iterator_num -= 1

    def reset_cached_iterators_num(self):
        self._cached_iterator_num = 0

    def _check_empty_raw(self, state_key):
        output_stream = coder_impl.create_OutputStream()
        output_stream.write_byte(self.CHECK_EMPTY_FLAG)
        continuation_token = output_stream.get()
        data, response_token = self._underlying.get_raw(state_key, continuation_token)
        if data[0] == self.IS_EMPTY_FLAG:
            return True
        elif data[0] == self.NOT_EMPTY_FLAG:
            return False
        else:
            raise Exception("Unknown response flag: " + str(data[0]))

    def _get_raw(self, state_key, map_key, map_key_encoder, map_value_decoder):
        output_stream = coder_impl.create_OutputStream()
        output_stream.write_byte(self.GET_FLAG)
        map_key_encoder(map_key, output_stream)
        continuation_token = output_stream.get()
        data, response_token = self._underlying.get_raw(state_key, continuation_token)
        input_stream = coder_impl.create_InputStream(data)
        result_flag = input_stream.read_byte()
        if result_flag == self.EXIST_FLAG:
            return True, map_value_decoder(input_stream)
        elif result_flag == self.IS_NONE_FLAG:
            return True, None
        elif result_flag == self.NOT_EXIST_FLAG:
            return False, None
        else:
            raise Exception("Unknown response flag: " + str(result_flag))

    def _iterate_raw(self, state_key, iterate_type, iterator_token,
                     map_key_decoder, map_value_decoder):
        output_stream = coder_impl.create_OutputStream()
        output_stream.write_byte(self.ITERATE_FLAG)
        output_stream.write_byte(iterate_type.value)
        if not isinstance(iterator_token, IteratorToken):
            # The iterator token represents a Java iterator
            output_stream.write_bigendian_int32(len(iterator_token))
            output_stream.write(iterator_token)
        else:
            output_stream.write_bigendian_int32(0)
        continuation_token = output_stream.get()
        data, response_token = self._underlying.get_raw(state_key, continuation_token)
        if len(response_token) != 0:
            # The new iterator token is an UUID which represents a cached iterator at Java
            # side.
            new_iterator_token = response_token
            if iterator_token == IteratorToken.NOT_START:
                # This is the first request but not the last request of current state.
                # It means there is a new iterator has been created and cached at Java side.
                self._inc_cached_iterators_num()
        else:
            new_iterator_token = IteratorToken.FINISHED
            if iterator_token != IteratorToken.NOT_START:
                # This is not the first request but the last request of current state.
                # It means the cached iterator created at Java side has been removed as
                # current iteration has finished.
                self._dec_cached_iterators_num()
        input_stream = coder_impl.create_InputStream(data)
        if iterate_type == IterateType.ITEMS or iterate_type == IterateType.VALUES:
            # decode both key and value
            current_batch = {}
            while input_stream.size() > 0:
                key = map_key_decoder(input_stream)
                is_not_none = input_stream.read_byte()
                if is_not_none:
                    value = map_value_decoder(input_stream)
                else:
                    value = None
                current_batch[key] = value
        else:
            # only decode key
            current_batch = []
            while input_stream.size() > 0:
                key = map_key_decoder(input_stream)
                current_batch.append(key)
        return current_batch, new_iterator_token

    def _append_raw(self, state_key, items, map_key_encoder, map_value_encoder):
        output_stream = coder_impl.create_OutputStream()
        output_stream.write_bigendian_int32(len(items))
        for request_flag, map_key, map_value in items:
            output_stream.write_byte(request_flag)
            # Not all the coder impls will serialize the length of bytes when we set the "nested"
            # param to "True", so we need to encode the length of bytes manually.
            tmp_out = coder_impl.create_OutputStream()
            map_key_encoder(map_key, tmp_out)
            serialized_data = tmp_out.get()
            output_stream.write_bigendian_int32(len(serialized_data))
            output_stream.write(serialized_data)
            if request_flag == self.SET_VALUE:
                tmp_out = coder_impl.create_OutputStream()
                map_value_encoder(map_value, tmp_out)
                serialized_data = tmp_out.get()
                output_stream.write_bigendian_int32(len(serialized_data))
                output_stream.write(serialized_data)
        return self._underlying.append_raw(state_key, output_stream.get())

    @staticmethod
    def _convert_to_cache_key(state_key):
        return state_key.SerializeToString()


class RemovableConcatIterator(collections.abc.Iterator):

    def __init__(self, internal_map_state, first, second):
        self._first = first
        self._second = second
        self._first_not_finished = True
        self._internal_map_state = internal_map_state
        self._mod_count = self._internal_map_state._mod_count
        self._last_key = None

    def __next__(self):
        self._check_modification()
        if self._first_not_finished:
            try:
                self._last_key, element = next(self._first)
                return element
            except StopIteration:
                self._first_not_finished = False
                return self.__next__()
        else:
            self._last_key, element = next(self._second)
            return element

    def remove(self):
        """
        Remove the the last element returned by this iterator.
        """
        if self._last_key is None:
            raise Exception("You need to call the '__next__' method before calling "
                            "this method.")
        self._check_modification()
        # Bypass the 'remove' method of the map state to avoid triggering the commit of the write
        # cache.
        if self._internal_map_state._cleared:
            del self._internal_map_state._write_cache[self._last_key]
            if len(self._internal_map_state._write_cache) == 0:
                self._internal_map_state._is_empty = True
        else:
            self._internal_map_state._write_cache[self._last_key] = (False, None)
        self._mod_count += 1
        self._internal_map_state._mod_count += 1
        self._last_key = None

    def _check_modification(self):
        if self._mod_count != self._internal_map_state._mod_count:
            raise Exception("Concurrent modification detected. "
                            "You can not modify the map state when iterating it except using the "
                            "'remove' method of this iterator.")


class InternalSynchronousMapRuntimeState(object):

    def __init__(self,
                 map_state_handler: CachingMapStateHandler,
                 state_key,
                 map_key_coder,
                 map_value_coder,
                 max_write_cache_entries):
        self._map_state_handler = map_state_handler
        self._state_key = state_key
        self._map_key_coder = map_key_coder
        if isinstance(map_key_coder, FieldCoder):
            map_key_coder_impl = FlinkCoder(map_key_coder).get_impl()
        else:
            map_key_coder_impl = map_key_coder.get_impl()
        self._map_key_encoder, self._map_key_decoder = \
            self._get_encoder_and_decoder(map_key_coder_impl)
        self._map_value_coder = map_value_coder
        if isinstance(map_value_coder, FieldCoder):
            map_value_coder_impl = FlinkCoder(map_value_coder).get_impl()
        else:
            map_value_coder_impl = map_value_coder.get_impl()
        self._map_value_encoder, self._map_value_decoder = \
            self._get_encoder_and_decoder(map_value_coder_impl)
        self._write_cache = dict()
        self._max_write_cache_entries = max_write_cache_entries
        self._is_empty = None
        self._cleared = False
        self._mod_count = 0

    def get(self, map_key):
        if self._is_empty:
            return None
        if map_key in self._write_cache:
            exists, value = self._write_cache[map_key]
            if exists:
                return value
            else:
                return None
        if self._cleared:
            return None
        exists, value = self._map_state_handler.blocking_get(
            self._state_key, map_key, self._map_key_encoder, self._map_value_decoder)
        if exists:
            return value
        else:
            return None

    def put(self, map_key, map_value):
        self._write_cache[map_key] = (True, map_value)
        self._is_empty = False
        self._mod_count += 1
        if len(self._write_cache) >= self._max_write_cache_entries:
            self.commit()

    def put_all(self, dict_value):
        for map_key, map_value in dict_value:
            self._write_cache[map_key] = (True, map_value)
        self._is_empty = False
        self._mod_count += 1
        if len(self._write_cache) >= self._max_write_cache_entries:
            self.commit()

    def remove(self, map_key):
        if self._is_empty:
            return
        if self._cleared:
            del self._write_cache[map_key]
            if len(self._write_cache) == 0:
                self._is_empty = True
        else:
            self._write_cache[map_key] = (False, None)
            self._is_empty = None
        self._mod_count += 1
        if len(self._write_cache) >= self._max_write_cache_entries:
            self.commit()

    def contains(self, map_key):
        if self._is_empty:
            return False
        if self.get(map_key) is None:
            return False
        else:
            return True

    def is_empty(self):
        if self._is_empty is None:
            if len(self._write_cache) > 0:
                self.commit()
            self._is_empty = self._map_state_handler.check_empty(self._state_key)
        return self._is_empty

    def clear(self):
        self._cleared = True
        self._is_empty = True
        self._mod_count += 1
        self._write_cache.clear()

    def items(self):
        return RemovableConcatIterator(
            self,
            self.write_cache_iterator(IterateType.ITEMS),
            self.remote_data_iterator(IterateType.ITEMS))

    def keys(self):
        return RemovableConcatIterator(
            self,
            self.write_cache_iterator(IterateType.KEYS),
            self.remote_data_iterator(IterateType.KEYS))

    def values(self):
        return RemovableConcatIterator(
            self,
            self.write_cache_iterator(IterateType.VALUES),
            self.remote_data_iterator(IterateType.VALUES))

    def commit(self):
        to_await = None
        if self._cleared:
            to_await = self._map_state_handler.clear(self._state_key)
        if self._write_cache:
            append_items = []
            for map_key, (exists, value) in self._write_cache.items():
                if exists:
                    if value is not None:
                        append_items.append(
                            (CachingMapStateHandler.SET_VALUE, map_key, value))
                    else:
                        append_items.append((CachingMapStateHandler.SET_NONE, map_key, None))
                else:
                    append_items.append((CachingMapStateHandler.DELETE, map_key, None))
            self._write_cache.clear()
            to_await = self._map_state_handler.extend(
                self._state_key, append_items, self._map_key_encoder, self._map_value_encoder)
        if to_await:
            to_await.get()
        self._write_cache.clear()
        self._cleared = False
        self._mod_count += 1

    def write_cache_iterator(self, iterate_type):
        return create_cache_iterator(self._write_cache, iterate_type)

    def remote_data_iterator(self, iterate_type):
        if self._cleared or self._is_empty:
            return iter([])
        else:
            return self._map_state_handler.lazy_iterator(
                self._state_key,
                iterate_type,
                self._map_key_decoder,
                self._map_value_decoder,
                self._write_cache)

    @staticmethod
    def _get_encoder_and_decoder(coder):
        encoder = partial(coder.encode_to_stream, nested=True)
        decoder = partial(coder.decode_from_stream, nested=True)
        return encoder, decoder


class SynchronousMapRuntimeState(SynchronousKvRuntimeState, InternalMapState):

    def __init__(self,
                 name: str,
                 map_key_coder,
                 map_value_coder,
                 remote_state_backend: 'RemoteKeyedStateBackend'):
        super(SynchronousMapRuntimeState, self).__init__(name, remote_state_backend)
        self._map_key_coder = map_key_coder
        self._map_value_coder = map_value_coder

    def get_internal_state(self):
        if self._internal_state is None:
            self._internal_state = self._remote_state_backend._get_internal_map_state(
                self.name,
                self.namespace,
                self._map_key_coder,
                self._map_value_coder,
                self._ttl_config,
                self._cache_type)
        return self._internal_state

    def get(self, key):
        return self.get_internal_state().get(key)

    def put(self, key, value):
        self.get_internal_state().put(key, value)

    def put_all(self, dict_value):
        self.get_internal_state().put_all(dict_value)

    def remove(self, key):
        self.get_internal_state().remove(key)

    def contains(self, key):
        return self.get_internal_state().contains(key)

    def items(self):
        return self.get_internal_state().items()

    def keys(self):
        return self.get_internal_state().keys()

    def values(self):
        return self.get_internal_state().values()

    def is_empty(self):
        return self.get_internal_state().is_empty()

    def clear(self):
        self.get_internal_state().clear()


class RemoteKeyedStateBackend(object):
    """
    A keyed state backend provides methods for managing keyed state.
    """

    MERGE_NAMESAPCES_MARK = "merge_namespaces"

    def __init__(self,
                 state_handler,
                 key_coder,
                 namespace_coder,
                 state_cache_size,
                 map_state_read_cache_size,
                 map_state_write_cache_size):
        self._state_handler = state_handler
        self._map_state_handler = CachingMapStateHandler(
            state_handler, map_state_read_cache_size)
        self._key_coder_impl = key_coder.get_impl()
        self.namespace_coder = namespace_coder
        if namespace_coder:
            self._namespace_coder_impl = namespace_coder.get_impl()
        else:
            self._namespace_coder_impl = None
        self._state_cache_size = state_cache_size
        self._map_state_write_cache_size = map_state_write_cache_size
        self._all_states = {}  # type: Dict[str, SynchronousKvRuntimeState]
        self._internal_state_cache = LRUCache(self._state_cache_size, None)
        self._internal_state_cache.set_on_evict(
            lambda key, value: self.commit_internal_state(value))
        self._current_key = None
        self._encoded_current_key = None
        self._clear_iterator_mark = beam_fn_api_pb2.StateKey(
            multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                transform_id="clear_iterators",
                side_input_id="clear_iterators",
                key=self._encoded_current_key))

    def get_list_state(self, name, element_coder, ttl_config=None):
        return self._wrap_internal_bag_state(
            name,
            element_coder,
            SynchronousListRuntimeState,
            SynchronousListRuntimeState,
            ttl_config)

    def get_value_state(self, name, value_coder, ttl_config=None):
        return self._wrap_internal_bag_state(
            name,
            value_coder,
            SynchronousValueRuntimeState,
            SynchronousValueRuntimeState,
            ttl_config)

    def get_map_state(self, name, map_key_coder, map_value_coder, ttl_config=None):
        if name in self._all_states:
            self.validate_map_state(name, map_key_coder, map_value_coder)
            return self._all_states[name]
        map_state = SynchronousMapRuntimeState(name, map_key_coder, map_value_coder, self)
        if ttl_config is not None:
            map_state.enable_time_to_live(ttl_config)
        self._all_states[name] = map_state
        return map_state

    def get_reducing_state(self, name, coder, reduce_function, ttl_config=None):
        return self._wrap_internal_bag_state(
            name,
            coder,
            SynchronousReducingRuntimeState,
            partial(SynchronousReducingRuntimeState, reduce_function=reduce_function),
            ttl_config)

    def get_aggregating_state(self, name, coder, agg_function, ttl_config=None):
        return self._wrap_internal_bag_state(
            name,
            coder,
            SynchronousAggregatingRuntimeState,
            partial(SynchronousAggregatingRuntimeState, agg_function=agg_function),
            ttl_config)

    def validate_state(self, name, coder, expected_type):
        if name in self._all_states:
            state = self._all_states[name]
            if not isinstance(state, expected_type):
                raise Exception("The state name '%s' is already in use and not a %s."
                                % (name, expected_type))
            if state._value_coder != coder:
                raise Exception("State name corrupted: %s" % name)

    def validate_map_state(self, name, map_key_coder, map_value_coder):
        if name in self._all_states:
            state = self._all_states[name]
            if not isinstance(state, SynchronousMapRuntimeState):
                raise Exception("The state name '%s' is already in use and not a map state."
                                % name)
            if state._map_key_coder != map_key_coder or \
                    state._map_value_coder != map_value_coder:
                raise Exception("State name corrupted: %s" % name)

    def _wrap_internal_bag_state(
            self, name, element_coder, wrapper_type, wrap_method, ttl_config):
        if name in self._all_states:
            self.validate_state(name, element_coder, wrapper_type)
            return self._all_states[name]
        wrapped_state = wrap_method(name, element_coder, self)
        if ttl_config is not None:
            wrapped_state.enable_time_to_live(ttl_config)
        self._all_states[name] = wrapped_state
        return wrapped_state

    def _get_internal_bag_state(self, name, namespace, element_coder, ttl_config):
        encoded_namespace = self._encode_namespace(namespace)
        cached_state = self._internal_state_cache.get(
            (name, self._encoded_current_key, encoded_namespace))
        if cached_state is not None:
            return cached_state
        # The created internal state would not be put into the internal state cache
        # at once. The internal state cache is only updated when the current key changes.
        # The reason is that the state cache size may be smaller that the count of activated
        # state (i.e. the state with current key).
        if isinstance(element_coder, FieldCoder):
            element_coder = FlinkCoder(element_coder)
        state_spec = userstate.BagStateSpec(name, element_coder)
        internal_state = self._create_bag_state(state_spec, encoded_namespace, ttl_config)
        return internal_state

    def _get_internal_map_state(
            self, name, namespace, map_key_coder, map_value_coder, ttl_config, cache_type):
        encoded_namespace = self._encode_namespace(namespace)
        cached_state = self._internal_state_cache.get(
            (name, self._encoded_current_key, encoded_namespace))
        if cached_state is not None:
            return cached_state
        internal_map_state = self._create_internal_map_state(
            name, encoded_namespace, map_key_coder, map_value_coder, ttl_config, cache_type)
        return internal_map_state

    def _create_bag_state(self, state_spec: userstate.StateSpec, encoded_namespace, ttl_config) \
            -> userstate.AccumulatingRuntimeState:
        if isinstance(state_spec, userstate.BagStateSpec):
            bag_state = SynchronousBagRuntimeState(
                self._state_handler,
                state_key=self.get_bag_state_key(
                    state_spec.name, self._encoded_current_key, encoded_namespace, ttl_config),
                value_coder=state_spec.coder)
            return bag_state
        else:
            raise NotImplementedError(state_spec)

    def _create_internal_map_state(
            self, name, encoded_namespace, map_key_coder, map_value_coder, ttl_config, cache_type):
        # Currently the `beam_fn_api.proto` does not support MapState, so we use the
        # the `MultimapSideInput` message to mark the state as a MapState for now.
        from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor
        state_proto = StateDescriptor()
        state_proto.state_name = name
        if ttl_config is not None:
            state_proto.state_ttl_config.CopyFrom(ttl_config._to_proto())
        state_key = beam_fn_api_pb2.StateKey(
            multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                transform_id="",
                window=encoded_namespace,
                side_input_id=base64.b64encode(state_proto.SerializeToString()),
                key=self._encoded_current_key))
        if cache_type == SynchronousKvRuntimeState.CacheType.DISABLE_CACHE:
            write_cache_size = 0
        else:
            write_cache_size = self._map_state_write_cache_size
        return InternalSynchronousMapRuntimeState(
            self._map_state_handler,
            state_key,
            map_key_coder,
            map_value_coder,
            write_cache_size)

    def _encode_namespace(self, namespace):
        if namespace is not None:
            encoded_namespace = self._namespace_coder_impl.encode(namespace)
        else:
            encoded_namespace = b''
        return encoded_namespace

    def cache_internal_state(self, encoded_key, internal_kv_state: SynchronousKvRuntimeState):
        encoded_old_namespace = self._encode_namespace(internal_kv_state.namespace)
        self._internal_state_cache.put(
            (internal_kv_state.name, encoded_key, encoded_old_namespace),
            internal_kv_state.get_internal_state())

    def set_current_key(self, key):
        if key == self._current_key:
            return
        encoded_old_key = self._encoded_current_key
        for state_name, state_obj in self._all_states.items():
            if self._state_cache_size > 0:
                # cache old internal state
                self.cache_internal_state(encoded_old_key, state_obj)
            state_obj.namespace = None
            state_obj._internal_state = None
        self._current_key = key
        self._encoded_current_key = self._key_coder_impl.encode(self._current_key)

    def get_current_key(self):
        return self._current_key

    def commit(self):
        for internal_state in self._internal_state_cache:
            self.commit_internal_state(internal_state)
        for name, state in self._all_states.items():
            if (name, self._encoded_current_key, self._encode_namespace(state.namespace)) \
                    not in self._internal_state_cache:
                self.commit_internal_state(state._internal_state)

    def clear_cached_iterators(self):
        if self._map_state_handler.get_cached_iterators_num() > 0:
            self._clear_iterator_mark.multimap_side_input.key = self._encoded_current_key
            self._map_state_handler.clear(self._clear_iterator_mark)

    def merge_namespaces(self, state: SynchronousMergingRuntimeState, target, sources, ttl_config):
        for source in sources:
            state.set_current_namespace(source)
            self.commit_internal_state(state.get_internal_state())
        state.set_current_namespace(target)
        self.commit_internal_state(state.get_internal_state())
        encoded_target_namespace = self._encode_namespace(target)
        encoded_namespaces = [self._encode_namespace(source) for source in sources]
        self.clear_state_cache(state, [encoded_target_namespace] + encoded_namespaces)

        state_key = self.get_bag_state_key(
            state.name, self._encoded_current_key, encoded_target_namespace, ttl_config)
        state_key.bag_user_state.transform_id = self.MERGE_NAMESAPCES_MARK

        encoded_namespaces_writer = BytesIO()
        encoded_namespaces_writer.write(len(sources).to_bytes(4, 'big'))
        for encoded_namespace in encoded_namespaces:
            encoded_namespaces_writer.write(encoded_namespace)
        sources_bytes = encoded_namespaces_writer.getvalue()
        to_await = self._map_state_handler._underlying.append_raw(state_key, sources_bytes)
        if to_await:
            to_await.get()

    def clear_state_cache(self, state: SynchronousMergingRuntimeState, encoded_namespaces):
        name = state.name
        for encoded_namespace in encoded_namespaces:
            if (name, self._encoded_current_key, encoded_namespace) in self._internal_state_cache:
                # commit and clear the write cache
                self._internal_state_cache.evict(
                    (name, self._encoded_current_key, encoded_namespace))
                # currently all the SynchronousMergingRuntimeState is based on bag state
                state_key = self.get_bag_state_key(
                    name, self._encoded_current_key, encoded_namespace, None)
                # clear the read cache, the read cache is shared between map state handler and bag
                # state handler. So we can use the map state handler instead.
                self._map_state_handler.clear_read_cache(state_key)

    def get_bag_state_key(self, name, encoded_key, encoded_namespace, ttl_config):
        from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor
        state_proto = StateDescriptor()
        state_proto.state_name = name
        if ttl_config is not None:
            state_proto.state_ttl_config.CopyFrom(ttl_config._to_proto())
        return beam_fn_api_pb2.StateKey(
            bag_user_state=beam_fn_api_pb2.StateKey.BagUserState(
                transform_id="",
                window=encoded_namespace,
                user_state_id=base64.b64encode(state_proto.SerializeToString()),
                key=encoded_key))

    @staticmethod
    def commit_internal_state(internal_state):
        if internal_state is not None:
            internal_state.commit()
        # reset the status of the internal state to reuse the object cross bundle
        if isinstance(internal_state, SynchronousBagRuntimeState):
            internal_state._cleared = False
            internal_state._added_elements = []
