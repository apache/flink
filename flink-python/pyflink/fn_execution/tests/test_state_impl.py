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

import contextlib
import unittest
from unittest import mock

from apache_beam.coders import VarIntCoder, StrUtf8Coder
from apache_beam.runners.worker.sdk_worker import GlobalCachingStateHandler, StateHandler
from apache_beam.runners.worker.statecache import StateCache

from pyflink.common import Time, Types
from pyflink.datastream.state import StateTtlConfig, MapStateDescriptor
from pyflink.fn_execution.state_impl import RemoteKeyedStateBackend, RemoteOperatorStateBackend, \
    CachingMapStateHandler


class StateTtlReadCacheTests(unittest.TestCase):

    @staticmethod
    def _ttl_configs():
        for update_type in (StateTtlConfig.UpdateType.OnCreateAndWrite,
                            StateTtlConfig.UpdateType.OnReadAndWrite):
            yield StateTtlConfig.new_builder(Time.seconds(5)) \
                .set_update_type(update_type) \
                .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
                .build()
        yield StateTtlConfig.new_builder(Time.seconds(5)) \
            .update_ttl_on_read_and_write() \
            .set_state_visibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) \
            .build()

    @staticmethod
    def _create_backend(cache_size=1 << 20):
        # Only remote I/O is mocked. Both Beam caches and Flink runtime states are real.
        remote = mock.Mock(spec=StateHandler)
        remote.process_instruction_id.return_value = contextlib.nullcontext()
        remote.append_raw.return_value = None
        remote.clear.return_value = None
        handler = GlobalCachingStateHandler(StateCache(cache_size), remote)
        backend = RemoteKeyedStateBackend(
            handler, StrUtf8Coder(), StrUtf8Coder(), 16, 16, 16)
        backend.set_current_key('key')
        return backend, handler, remote

    @staticmethod
    def _encode(value):
        return VarIntCoder().get_impl().encode_nested(value)

    @classmethod
    def _map_entry(cls, key, value):
        return cls._encode(key) + b'\x01' + cls._encode(value)

    def test_value_ttl_bypasses_read_cache_without_invalidating_other_states(self):
        for ttl in self._ttl_configs():
            with self.subTest(ttl=ttl):
                backend, handler, remote = self._create_backend()
                cache = handler._state_cache
                remote.get_raw.side_effect = [
                    (self._encode(42), b''), (self._encode(100), b''), (b'', b'')]
                with handler.process_instruction_id('bundle', []):
                    cached = backend.get_value_state('cached', VarIntCoder())
                    self.assertEqual(cached.value(), 42)
                    state = backend.get_value_state('ttl', VarIntCoder(), ttl)
                    self.assertEqual(state.value(), 100)
                    # An empty backend response represents expiration, without a timing dependency.
                    self.assertIsNone(state.value())
                    self.assertEqual(cached.value(), 42)
                self.assertEqual(remote.get_raw.call_count, 3)
                self.assertIs(backend._state_handler, handler)
                self.assertIs(handler._state_cache, cache)
                self.assertTrue(cache.is_cache_enabled())
                self.assertIs(cached.get_internal_state()._state_handler, handler)
                self.assertIsNot(state.get_internal_state()._state_handler, handler)

    def test_cache_permitting_states_keep_read_cache(self):
        ttl = StateTtlConfig.new_builder(Time.seconds(5)) \
            .set_state_visibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) \
            .build()
        for config in (None, ttl):
            with self.subTest(ttl=config):
                backend, handler, remote = self._create_backend()
                remote.get_raw.side_effect = [(self._encode(100), b'')]
                with handler.process_instruction_id('bundle', []):
                    state = backend.get_value_state('value', VarIntCoder(), config)
                    self.assertEqual(state.value(), 100)
                    self.assertEqual(state.value(), 100)
                remote.get_raw.assert_called_once()
                self.assertIs(state.get_internal_state()._state_handler, handler)

    def test_ttl_write_cache_policy_is_preserved(self):
        for ttl in self._ttl_configs():
            with self.subTest(ttl=ttl):
                backend, handler, remote = self._create_backend()
                with handler.process_instruction_id('bundle', []):
                    value = backend.get_value_state('value', VarIntCoder(), ttl)
                    map_state = backend.get_map_state('map', VarIntCoder(), VarIntCoder(), ttl)
                    value.update(100)
                    map_state.put(1, 100)
                    if ttl.get_state_visibility() == \
                            StateTtlConfig.StateVisibility.NeverReturnExpired:
                        self.assertEqual(remote.append_raw.call_count, 2)
                    else:
                        remote.append_raw.assert_not_called()
                    backend.commit()
                    self.assertEqual(remote.append_raw.call_count, 2)

    def test_map_ttl_bypasses_read_cache_without_invalidating_other_states(self):
        for ttl in self._ttl_configs():
            with self.subTest(ttl=ttl):
                backend, handler, remote = self._create_backend()
                exists = bytes([CachingMapStateHandler.EXIST_FLAG])
                missing = bytes([CachingMapStateHandler.NOT_EXIST_FLAG])
                remote.get_raw.side_effect = [
                    (exists + self._encode(42), b''),
                    (exists + self._encode(100), b''), (missing, b'')]
                with handler.process_instruction_id('bundle', []):
                    cached = backend.get_map_state('cached', VarIntCoder(), VarIntCoder())
                    self.assertEqual(cached.get(1), 42)
                    state = backend.get_map_state('ttl', VarIntCoder(), VarIntCoder(), ttl)
                    self.assertEqual(state.get(1), 100)
                    self.assertIsNone(state.get(1))
                    self.assertEqual(cached.get(1), 42)
                self.assertEqual(remote.get_raw.call_count, 3)
                self.assertIs(cached.get_internal_state()._map_state_handler._state_cache,
                              handler._state_cache)
                self.assertFalse(state.get_internal_state()._map_state_handler
                                 ._state_cache.is_cache_enabled())

    def test_map_ttl_is_empty_after_committed_value_expires(self):
        for ttl in self._ttl_configs():
            with self.subTest(ttl=ttl):
                backend, handler, remote = self._create_backend()
                remote.get_raw.side_effect = [
                    (bytes([CachingMapStateHandler.NOT_EXIST_FLAG]), b''),
                    (bytes([CachingMapStateHandler.IS_EMPTY_FLAG]), b'')]
                with handler.process_instruction_id('bundle', []):
                    state = backend.get_map_state('ttl', VarIntCoder(), VarIntCoder(), ttl)
                    state.put(1, 100)
                    backend.commit()
                    self.assertIsNone(state.get(1))
                    self.assertTrue(state.is_empty())
                self.assertEqual(remote.get_raw.call_count, 2)

    def test_map_ttl_is_empty_refreshes_within_bundle(self):
        for ttl in self._ttl_configs():
            with self.subTest(ttl=ttl):
                backend, handler, remote = self._create_backend()
                remote.get_raw.side_effect = [
                    (bytes([CachingMapStateHandler.NOT_EMPTY_FLAG]), b''),
                    (bytes([CachingMapStateHandler.IS_EMPTY_FLAG]), b''),
                    (bytes([CachingMapStateHandler.NOT_EMPTY_FLAG]), b'')]
                with handler.process_instruction_id('bundle', []):
                    state = backend.get_map_state('ttl', VarIntCoder(), VarIntCoder(), ttl)
                    mod_count = state.get_internal_state()._mod_count
                    self.assertFalse(state.is_empty())
                    self.assertTrue(state.is_empty())
                    self.assertEqual(state.get_internal_state()._mod_count, mod_count)
                    self.assertTrue(state.is_empty())
                    self.assertEqual(remote.get_raw.call_count, 2)
                    state.put(1, 100)
                    self.assertFalse(state.is_empty())
                self.assertEqual(remote.get_raw.call_count, 3)

    def test_map_ttl_is_empty_flushes_pending_mutations(self):
        # This TTL policy retains the write cache while bypassing the read cache.
        ttl = list(self._ttl_configs())[-1]
        for operation in ('put', 'remove', 'clear_then_put', 'clear'):
            with self.subTest(operation=operation):
                backend, handler, remote = self._create_backend()
                with handler.process_instruction_id('bundle', []):
                    state = backend.get_map_state('ttl', VarIntCoder(), VarIntCoder(), ttl)
                    if operation == 'remove':
                        state.put(1, 100)
                        backend.commit()
                        remote.reset_mock()
                        state.remove(1)
                    elif operation == 'clear':
                        state.clear()
                    else:
                        if operation == 'clear_then_put':
                            state.clear()
                        state.put(1, 100)
                    if operation == 'clear':
                        self.assertTrue(state.is_empty())
                        remote.get_raw.assert_not_called()
                        remote.clear.assert_not_called()
                        backend.commit()
                        remote.clear.assert_called_once()
                        continue
                    empty = operation == 'remove'
                    response = CachingMapStateHandler.IS_EMPTY_FLAG if empty else \
                        CachingMapStateHandler.NOT_EMPTY_FLAG
                    remote.get_raw.return_value = (bytes([response]), b'')
                    self.assertEqual(state.is_empty(), empty)
                    expected_calls = ['append_raw', 'get_raw']
                    if operation == 'clear_then_put':
                        expected_calls.insert(0, 'clear')
                    self.assertEqual([call[0] for call in remote.method_calls
                                      if call[0] != 'process_instruction_id'], expected_calls)

    def test_cache_permitting_map_states_keep_is_empty_cache(self):
        ttl = StateTtlConfig.new_builder(Time.seconds(5)) \
            .set_state_visibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) \
            .build()
        for config in (None, ttl):
            with self.subTest(ttl=config):
                backend, handler, remote = self._create_backend()
                remote.get_raw.side_effect = [
                    (bytes([CachingMapStateHandler.NOT_EMPTY_FLAG]), b'')]
                with handler.process_instruction_id('bundle', []):
                    state = backend.get_map_state('map', VarIntCoder(), VarIntCoder(), config)
                    self.assertFalse(state.is_empty())
                    self.assertFalse(state.is_empty())
                remote.get_raw.assert_called_once()

    def test_broadcast_map_state_keeps_default_cache_behavior(self):
        _, handler, remote = self._create_backend()
        backend = RemoteOperatorStateBackend(handler, 16, 16, 16)
        with handler.process_instruction_id('bundle', []):
            state = backend.get_broadcast_state(
                MapStateDescriptor('broadcast', Types.INT(), Types.INT()))
            state.put(1, 100)
            backend.commit()
            self.assertFalse(state._internal_map_state.is_empty())
            self.assertEqual(state.get(1), 100)
        remote.get_raw.assert_not_called()

    def test_map_ttl_iteration_and_unfinished_iterator_cleanup(self):
        ttl = next(self._ttl_configs())
        backend, handler, remote = self._create_backend()
        remote.get_raw.side_effect = [
            (self._map_entry(1, 100), b''), (b'', b''),
            (self._map_entry(1, 100), b'next-page'),
            (self._map_entry(2, 200), b''),
            (self._map_entry(1, 100), b'unfinished')]
        with handler.process_instruction_id('bundle', []):
            state = backend.get_map_state('ttl', VarIntCoder(), VarIntCoder(), ttl)
            self.assertEqual(list(state.items()), [(1, 100)])
            self.assertEqual(list(state.items()), [])
            self.assertEqual(list(state.items()), [(1, 100), (2, 200)])
            self.assertIn(b'next-page', remote.get_raw.call_args_list[3].args[1])
            self.assertEqual(next(state.items()), (1, 100))
            self.assertEqual(backend._map_state_handler.get_cached_iterators_num(), 0)
            backend.clear_cached_iterators()
        remote.clear.assert_called_once()
        clear_key = remote.clear.call_args.args[0].multimap_side_input
        self.assertEqual(clear_key.transform_id, 'clear_iterators')
        self.assertEqual(clear_key.key, b'key')

    def test_ttl_handler_is_preserved_when_reusing_key_and_namespace(self):
        ttl = next(self._ttl_configs())
        backend, handler, remote = self._create_backend()
        remote.get_raw.side_effect = [
            (self._encode(100), b''), (self._encode(200), b''), (b'', b'')]
        with handler.process_instruction_id('bundle', []):
            state = backend.get_value_state('ttl', VarIntCoder(), ttl)
            state.set_current_namespace('first')
            self.assertEqual(state.value(), 100)
            first_internal_state = state.get_internal_state()
            backend.set_current_key('other')
            state.set_current_namespace('second')
            self.assertEqual(state.value(), 200)
            backend.set_current_key('key')
            state.set_current_namespace('first')
            self.assertIs(state.get_internal_state(), first_internal_state)
            self.assertIsNone(state.value())
        first_key, second_key, reused_key = [
            call.args[0] for call in remote.get_raw.call_args_list]
        self.assertEqual(first_key, reused_key)
        self.assertNotEqual(first_key, second_key)

    def test_repeated_state_registration_with_disabled_cache(self):
        ttl = next(self._ttl_configs())
        backend, handler, remote = self._create_backend(cache_size=0)
        remote.get_raw.side_effect = [(self._encode(100), b''), (b'', b'')]
        with handler.process_instruction_id('bundle', []):
            state = backend.get_value_state('ttl', VarIntCoder(), ttl)
            self.assertEqual(state.value(), 100)
            self.assertIs(backend.get_value_state('ttl', VarIntCoder(), ttl), state)
            self.assertIsNone(state.value())
        self.assertEqual(remote.get_raw.call_count, 2)
        self.assertFalse(handler._state_cache.is_cache_enabled())


if __name__ == '__main__':
    unittest.main()
