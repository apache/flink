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
from enum import Enum

from apache_beam.coders import coder_impl
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker.bundle_processor import SynchronousBagRuntimeState
from apache_beam.transforms import userstate
from typing import List, Tuple, Any

from pyflink.common.state import ValueState, ListState, MapState


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


class SynchronousListRuntimeState(ListState):
    """
    The runtime ListState implementation backed by a :class:`SynchronousBagRuntimeState`.
    """

    def __init__(self, internal_state: SynchronousBagRuntimeState):
        self._internal_state = internal_state

    def add(self, v):
        self._internal_state.add(v)

    def get(self):
        return self._internal_state.read()

    def add_all(self, values):
        self._internal_state._added_elements.extend(values)

    def update(self, values):
        self.clear()
        self.add_all(values)

    def clear(self):
        self._internal_state.clear()


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

    def blocking_get(self, state_key, map_key, map_key_coder, map_value_coder):
        cache_token = self._get_cache_token()
        if not cache_token:
            # cache disabled / no cache token, request from remote directly
            return self._get_raw(state_key, map_key, map_key_coder, map_value_coder)

        # lookup cache first
        cache_state_key = self._convert_to_cache_key(state_key)
        cached_map_state = self._state_cache.get(cache_state_key, cache_token)
        if cached_map_state is None:
            # request from remote
            exists, value = self._get_raw(state_key, map_key, map_key_coder, map_value_coder)
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
                exists, value = self._get_raw(state_key, map_key, map_key_coder, map_value_coder)
                cached_map_state.put(map_key, (exists, value))
                return exists, value
            else:
                return cached_value

    def lazy_iterator(self, state_key, iterate_type, map_key_coder, map_value_coder, iterated_keys):
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
            state_key, iterate_type, last_iterator_token, map_key_coder, map_value_coder)

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
            map_key_coder,
            map_value_coder,
            iterated_keys,
            iterator_token,
            current_batch)

    def _lazy_remote_iterator(
            self,
            state_key,
            iterate_type,
            map_key_coder,
            map_value_coder,
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
                    state_key, iterate_type, iterator_token, map_key_coder, map_value_coder)
        elif iterate_type == IterateType.VALUES:
            while True:
                for key, value in current_batch.items():
                    if key in iterated_keys:
                        continue
                    yield key, value
                if iterator_token == IteratorToken.FINISHED:
                    break
                current_batch, iterator_token = self._iterate_raw(
                    state_key, iterate_type, iterator_token, map_key_coder, map_value_coder)
        elif iterate_type == IterateType.ITEMS:
            while True:
                for key, value in current_batch.items():
                    if key in iterated_keys:
                        continue
                    yield key, (key, value)
                if iterator_token == IteratorToken.FINISHED:
                    break
                current_batch, iterator_token = self._iterate_raw(
                    state_key, iterate_type, iterator_token, map_key_coder, map_value_coder)
        else:
            raise Exception("Unsupported iterate type: %s" % iterate_type)

    def extend(self, state_key, items: List[Tuple[int, Any, Any]], map_key_coder, map_value_coder):
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
            map_key_coder,
            map_value_coder)

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
        cache_token = self._get_cache_token()
        if cache_token:
            cache_key = self._convert_to_cache_key(state_key)
            self._state_cache.evict(cache_key, cache_token)
        return self._underlying.clear(state_key)

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

    def _get_raw(self, state_key, map_key, map_key_coder, map_value_coder):
        output_stream = coder_impl.create_OutputStream()
        output_stream.write_byte(self.GET_FLAG)
        map_key_coder.encode_to_stream(map_key, output_stream, True)
        continuation_token = output_stream.get()
        data, response_token = self._underlying.get_raw(state_key, continuation_token)
        input_stream = coder_impl.create_InputStream(data)
        result_flag = input_stream.read_byte()
        if result_flag == self.EXIST_FLAG:
            return True, map_value_coder.decode_from_stream(input_stream, True)
        elif result_flag == self.IS_NONE_FLAG:
            return True, None
        elif result_flag == self.NOT_EXIST_FLAG:
            return False, None
        else:
            raise Exception("Unknown response flag: " + str(result_flag))

    def _iterate_raw(self, state_key, iterate_type, iterator_token, map_key_coder, map_value_coder):
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
                key = map_key_coder.decode_from_stream(input_stream, True)
                is_not_none = input_stream.read_byte()
                if is_not_none:
                    value = map_value_coder.decode_from_stream(input_stream, True)
                else:
                    value = None
                current_batch[key] = value
        else:
            # only decode key
            current_batch = []
            while input_stream.size() > 0:
                key = map_key_coder.decode_from_stream(input_stream, True)
                current_batch.append(key)
        return current_batch, new_iterator_token

    def _append_raw(self, state_key, items, map_key_coder, map_value_coder):
        output_stream = coder_impl.create_OutputStream()
        output_stream.write_bigendian_int32(len(items))
        for request_flag, map_key, map_value in items:
            output_stream.write_byte(request_flag)
            # Not all the coder impls will serialize the length of bytes when we set the "nested"
            # param to "True", so we need to encode the length of bytes manually.
            tmp_out = coder_impl.create_OutputStream()
            map_key_coder.encode_to_stream(map_key, tmp_out, True)
            serialized_data = tmp_out.get()
            output_stream.write_bigendian_int32(len(serialized_data))
            output_stream.write(serialized_data)
            if request_flag == self.SET_VALUE:
                tmp_out = coder_impl.create_OutputStream()
                map_value_coder.encode_to_stream(map_value, tmp_out, True)
                serialized_data = tmp_out.get()
                output_stream.write_bigendian_int32(len(serialized_data))
                output_stream.write(serialized_data)
        return self._underlying.append_raw(state_key, output_stream.get())

    @staticmethod
    def _convert_to_cache_key(state_key):
        return state_key.SerializeToString()


class RemovableConcatIterator(collections.Iterator):

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
        self._map_key_coder_impl = map_key_coder._create_impl()
        self._map_value_coder = map_value_coder
        self._map_value_coder_impl = map_value_coder._create_impl()
        self._write_cache = dict()
        self._max_write_cache_entries = max_write_cache_entries
        self._is_empty = None
        self._cleared = False
        self._mod_count = 0

    def get(self, map_key):
        if self._is_empty:
            raise KeyError("Map key %s not found!" % str(map_key))
        if map_key in self._write_cache:
            exists, value = self._write_cache[map_key]
            if exists:
                return value
            else:
                raise KeyError("Map key %s not found!" % str(map_key))
        if self._cleared:
            raise KeyError("Map key %s not found!" % str(map_key))
        exists, value = self._map_state_handler.blocking_get(
            self._state_key, map_key, self._map_key_coder_impl, self._map_value_coder_impl)
        if exists:
            return value
        else:
            raise KeyError("Map key %s not found!" % str(map_key))

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
        try:
            self.get(map_key)
            return True
        except KeyError:
            return False

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
                self._state_key, append_items, self._map_key_coder_impl, self._map_value_coder_impl)
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
                self._map_key_coder_impl,
                self._map_value_coder_impl,
                self._write_cache)


class SynchronousMapRuntimeState(MapState):

    def __init__(self, internal_state: InternalSynchronousMapRuntimeState):
        self._internal_state = internal_state

    def get(self, key):
        return self._internal_state.get(key)

    def put(self, key, value):
        self._internal_state.put(key, value)

    def put_all(self, dict_value):
        self._internal_state.put_all(dict_value)

    def remove(self, key):
        self._internal_state.remove(key)

    def contains(self, key):
        return self._internal_state.contains(key)

    def items(self):
        return self._internal_state.items()

    def keys(self):
        return self._internal_state.keys()

    def values(self):
        return self._internal_state.values()

    def is_empty(self):
        return self._internal_state.is_empty()

    def clear(self):
        self._internal_state.clear()


class RemoteKeyedStateBackend(object):
    """
    A keyed state backend provides methods for managing keyed state.
    """

    def __init__(self,
                 state_handler,
                 key_coder,
                 state_cache_size,
                 map_state_read_cache_size,
                 map_state_write_cache_size):
        self._state_handler = state_handler
        self._map_state_handler = CachingMapStateHandler(
            state_handler, map_state_read_cache_size)

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
        self._map_state_write_cache_size = map_state_write_cache_size
        self._all_states = {}
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

    def get_list_state(self, name, element_coder):
        if name in self._all_states:
            self.validate_list_state(name, element_coder)
            return self._all_states[name]
        internal_bag_state = self._get_internal_bag_state(name, element_coder)
        list_state = SynchronousListRuntimeState(internal_bag_state)
        self._all_states[name] = list_state
        return list_state

    def get_value_state(self, name, value_coder):
        if name in self._all_states:
            self.validate_value_state(name, value_coder)
            return self._all_states[name]
        internal_bag_state = self._get_internal_bag_state(name, value_coder)
        value_state = SynchronousValueRuntimeState(internal_bag_state)
        self._all_states[name] = value_state
        return value_state

    def get_map_state(self, name, map_key_coder, map_value_coder):
        if name in self._all_states:
            self.validate_map_state(name, map_key_coder, map_value_coder)
            return self._all_states[name]
        internal_map_state = self._get_internal_map_state(name, map_key_coder, map_value_coder)
        map_state = SynchronousMapRuntimeState(internal_map_state)
        self._all_states[name] = map_state
        return map_state

    def validate_value_state(self, name, coder):
        if name in self._all_states:
            state = self._all_states[name]
            if not isinstance(state, SynchronousValueRuntimeState):
                raise Exception("The state name '%s' is already in use and not a value state."
                                % name)
            if state._internal_state._value_coder != coder:
                raise Exception("State name corrupted: %s" % name)

    def validate_list_state(self, name, coder):
        if name in self._all_states:
            state = self._all_states[name]
            if not isinstance(state, SynchronousListRuntimeState):
                raise Exception("The state name '%s' is already in use and not a list state."
                                % name)
            if state._internal_state._value_coder != coder:
                raise Exception("State name corrupted: %s" % name)

    def validate_map_state(self, name, map_key_coder, map_value_coder):
        if name in self._all_states:
            state = self._all_states[name]
            if not isinstance(state, SynchronousMapRuntimeState):
                raise Exception("The state name '%s' is already in use and not a map state."
                                % name)
            if state._internal_state._map_key_coder != map_key_coder or \
                    state._internal_state._map_value_coder != map_value_coder:
                raise Exception("State name corrupted: %s" % name)

    def _get_internal_bag_state(self, name, element_coder):
        cached_state = self._internal_state_cache.get((name, self._encoded_current_key))
        if cached_state is not None:
            return cached_state
        state_spec = userstate.BagStateSpec(name, element_coder)
        internal_state = self._create_bag_state(state_spec)
        return internal_state

    def _get_internal_map_state(self, name, map_key_coder, map_value_coder):
        cached_state = self._internal_state_cache.get((name, self._encoded_current_key))
        if cached_state is not None:
            return cached_state
        internal_map_state = self._create_internal_map_state(name, map_key_coder, map_value_coder)
        return internal_map_state

    def _create_bag_state(self, state_spec: userstate.StateSpec) \
            -> userstate.AccumulatingRuntimeState:
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

    def _create_internal_map_state(self, name, map_key_coder, map_value_coder):
        # Currently the `beam_fn_api.proto` does not support MapState, so we use the
        # the `MultimapSideInput` message to mark the state as a MapState for now.
        state_key = beam_fn_api_pb2.StateKey(
            multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
                transform_id="",
                side_input_id=name,
                key=self._encoded_current_key))
        return InternalSynchronousMapRuntimeState(
            self._map_state_handler,
            state_key,
            map_key_coder,
            map_value_coder,
            self._map_state_write_cache_size)

    def set_current_key(self, key):
        if key == self._current_key:
            return
        encoded_old_key = self._encoded_current_key
        self._current_key = key
        self._encoded_current_key = self._key_coder_impl.encode_nested(self._current_key)
        for state_name, state_obj in self._all_states.items():
            if self._state_cache_size > 0:
                # cache old internal state
                self._internal_state_cache.put(
                    (state_name, encoded_old_key), state_obj._internal_state)
            if isinstance(state_obj, (SynchronousValueRuntimeState, SynchronousListRuntimeState)):
                state_obj._internal_state = self._get_internal_bag_state(
                    state_name, state_obj._internal_state._value_coder)
            elif isinstance(state_obj, SynchronousMapRuntimeState):
                state_obj._internal_state = self._get_internal_map_state(
                    state_name,
                    state_obj._internal_state._map_key_coder,
                    state_obj._internal_state._map_value_coder)
            else:
                raise Exception("Unknown internal state '%s': %s" % (state_name, state_obj))

    def get_current_key(self):
        return self._current_key

    def commit(self):
        for internal_state in self._internal_state_cache:
            self.commit_internal_state(internal_state)
        for name, state in self._all_states.items():
            if (name, self._encoded_current_key) not in self._internal_state_cache:
                self.commit_internal_state(state._internal_state)

    def clear_cached_iterators(self):
        if self._map_state_handler.get_cached_iterators_num() > 0:
            self._clear_iterator_mark.multimap_side_input.key = self._encoded_current_key
            self._map_state_handler.clear(self._clear_iterator_mark)

    @staticmethod
    def commit_internal_state(internal_state):
        internal_state.commit()
        # reset the status of the internal state to reuse the object cross bundle
        if isinstance(internal_state, SynchronousBagRuntimeState):
            internal_state._cleared = False
            internal_state._added_elements = []
