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

from apache_beam.coders import StrUtf8Coder, VarIntCoder
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker.sdk_worker import GlobalCachingStateHandler, StateHandler
from apache_beam.runners.worker.statecache import StateCache

from pyflink.common import Time
from pyflink.datastream.state import StateTtlConfig
from pyflink.fn_execution.state_impl import CachingMapStateHandler, \
    InternalSynchronousMapRuntimeState, RemoteKeyedStateBackend


class StateTtlReadCacheTests(unittest.TestCase):

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

    def test_merge_namespaces_invalidates_ttl_read_cache(self):
        ttl = StateTtlConfig.new_builder(Time.seconds(5)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .set_state_visibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) \
            .build()
        for config in (None, ttl):
            for initial_target in ([], [0]):
                with self.subTest(ttl=config, initial_target=initial_target):
                    backend, handler, remote = self._create_backend()
                    values = {'target': initial_target, 'source-a': [1], 'source-b': [2],
                              'unrelated': [42]}
                    remote.get_raw.side_effect = lambda key, continuation_token=None: (
                        b''.join(self._encode(v)
                                 for v in values[key.bag_user_state.window.decode()]), b'')
                    with handler.process_instruction_id('bundle', []):
                        state = backend.get_list_state('list', VarIntCoder(), config)
                        for namespace in ('unrelated', 'source-a', 'source-b', 'target'):
                            state.set_current_namespace(namespace)
                            self.assertEqual(list(state.get()), values[namespace])
                        self.assertEqual(list(state.get()), initial_target)
                        self.assertEqual(remote.get_raw.call_count, 4)

                        state.merge_namespaces('target', ['source-a', 'source-b'])
                        remote.append_raw.assert_called_once()
                        remote.clear.assert_not_called()
                        # The remote response changes after the merge; all cached namespaces
                        # participating in it must be read again, including an empty target.
                        values.update({'target': initial_target + [1, 2],
                                       'source-a': [], 'source-b': []})
                        self.assertEqual(list(state.get()), initial_target + [1, 2])
                        for namespace in ('source-a', 'source-b'):
                            state.set_current_namespace(namespace)
                            self.assertEqual(list(state.get()), [])
                        self.assertEqual(remote.get_raw.call_count, 7)

                        state.set_current_namespace('unrelated')
                        self.assertEqual(list(state.get()), [42])
                        self.assertEqual(remote.get_raw.call_count, 7)


class InternalSynchronousMapRuntimeStateTests(unittest.TestCase):

    def setUp(self):
        self.state = InternalSynchronousMapRuntimeState(
            InMemoryMapStateHandler(), beam_fn_api_pb2.StateKey(),
            StrUtf8Coder(), StrUtf8Coder(), 16)

    def test_contains_distinguishes_missing_keys_from_none_values(self):
        values = {'none': None, 'empty': '', 'value': 'value'}
        for key, value in values.items():
            self.state.put(key, value)

        for committed in (False, True):
            if committed:
                self.state.commit()
            with self.subTest(committed=committed):
                for key, value in values.items():
                    self.assertEqual(self.state.get(key), value)
                    self.assertTrue(self.state.contains(key))
                self.assertIsNone(self.state.get('missing'))
                self.assertFalse(self.state.contains('missing'))

    def test_contains_after_remove(self):
        self.state.put('x', None)
        self.state.commit()
        self.state.remove('x')

        self.assertFalse(self.state.contains('x'))
        self.state.commit()
        self.assertFalse(self.state.contains('x'))

    def test_contains_after_clear_and_put(self):
        self.state.put('x', 'old')
        self.state.commit()
        self.state.clear()
        self.assertFalse(self.state.contains('x'))

        self.state.put('y', None)

        self.assertFalse(self.state.contains('x'))
        self.assertTrue(self.state.contains('y'))
        self.state.commit()
        self.assertFalse(self.state.contains('x'))
        self.assertTrue(self.state.contains('y'))


class KeySwitchWithoutStateCacheTests(unittest.TestCase):

    def setUp(self):
        remote = InMemoryStateHandler()
        self.handler = GlobalCachingStateHandler(StateCache(0), remote)
        self.backend = RemoteKeyedStateBackend(
            self.handler, StrUtf8Coder(), StrUtf8Coder(), 0, 16, 16)

    def test_clear_is_flushed_before_key_switch(self):
        with self.handler.process_instruction_id('bundle', []):
            self.backend.set_current_key('a')
            value_state = self.backend.get_value_state('value', VarIntCoder())
            list_state = self.backend.get_list_state('list', VarIntCoder())
            value_state.update(100)
            list_state.add(100)
            self.backend.commit()

            value_state.clear()
            list_state.clear()
            self.assertIsNone(value_state.value())
            self.assertEqual(list(list_state.get()), [])

            self.backend.set_current_key('b')
            self.assertIsNone(value_state.value())
            self.assertEqual(list(list_state.get()), [])

            self.backend.set_current_key('a')
            self.assertIsNone(value_state.value())
            self.assertEqual(list(list_state.get()), [])


class SynchronousValueRuntimeStateTests(unittest.TestCase):

    @staticmethod
    def _create_backend(cache_size, remote=None):
        remote = remote if remote is not None else InMemoryStateHandler()
        handler = GlobalCachingStateHandler(StateCache(1 << 20), remote)
        backend = RemoteKeyedStateBackend(
            handler, StrUtf8Coder(), StrUtf8Coder(), cache_size, 16, 16)
        backend.set_current_key('key')
        return backend, handler, remote

    def test_update_none_clears_pending_and_committed_values(self):
        for cache_size in (0, 16):
            for committed in (False, True):
                with self.subTest(cache_size=cache_size, committed=committed):
                    backend, handler, remote = self._create_backend(cache_size)
                    with handler.process_instruction_id('bundle', []):
                        state = backend.get_value_state('value', VarIntCoder())
                        state.update(42)
                        if committed:
                            backend.commit()

                        state.update(None)
                        self.assertIsNone(state.value())
                        backend.commit()
                        self.assertEqual(remote.values, {})

                    restored, restored_handler, _ = self._create_backend(cache_size, remote)
                    with restored_handler.process_instruction_id('next-bundle', []):
                        self.assertIsNone(restored.get_value_state('value', VarIntCoder()).value())

    def test_update_none_on_empty_state_is_repeatable(self):
        for cache_size in (0, 16):
            with self.subTest(cache_size=cache_size):
                backend, handler, remote = self._create_backend(cache_size)
                with handler.process_instruction_id('bundle', []):
                    state = backend.get_value_state('value', VarIntCoder())
                    for _ in range(2):
                        state.update(None)
                        self.assertIsNone(state.value())
                        backend.commit()
                        self.assertEqual(remote.values, {})

    def test_update_after_none_preserves_new_value(self):
        for cache_size in (0, 16):
            with self.subTest(cache_size=cache_size):
                backend, handler, remote = self._create_backend(cache_size)
                with handler.process_instruction_id('bundle', []):
                    state = backend.get_value_state('value', VarIntCoder())
                    state.update(1)
                    backend.commit()
                    state.update(None)
                    backend.commit()
                    state.update(0)
                    self.assertEqual(state.value(), 0)
                    backend.commit()

                restored, restored_handler, _ = self._create_backend(cache_size, remote)
                with restored_handler.process_instruction_id('next-bundle', []):
                    self.assertEqual(restored.get_value_state('value', VarIntCoder()).value(), 0)

    def test_non_none_updates_preserve_zero_and_replacement(self):
        for cache_size in (0, 16):
            with self.subTest(cache_size=cache_size):
                backend, handler, remote = self._create_backend(cache_size)
                with handler.process_instruction_id('bundle', []):
                    state = backend.get_value_state('value', VarIntCoder())
                    for value in (42, 0, -1):
                        state.update(value)
                        self.assertEqual(state.value(), value)
                        backend.commit()
                        self.assertEqual(list(remote.values.values()),
                                         [VarIntCoder().get_impl().encode_nested(value)])


class InMemoryStateHandler:

    def __init__(self):
        self.values = {}

    @contextlib.contextmanager
    def process_instruction_id(self, bundle_id):
        yield

    def get_raw(self, state_key, continuation_token=None):
        return self.values.get(state_key.SerializeToString(), b''), None

    def append_raw(self, state_key, data):
        key = state_key.SerializeToString()
        self.values[key] = self.values.get(key, b'') + data

    def clear(self, state_key):
        self.values.pop(state_key.SerializeToString(), None)


class InMemoryMapStateHandler:

    def __init__(self):
        self.values = {}

    def blocking_get(self, state_key, map_key, map_key_encoder, map_value_decoder):
        return map_key in self.values, self.values.get(map_key)

    def extend(self, state_key, items, map_key_encoder, map_value_encoder):
        for flag, key, value in items:
            if flag == CachingMapStateHandler.DELETE:
                self.values.pop(key, None)
            elif flag == CachingMapStateHandler.SET_NONE:
                self.values[key] = None
            elif flag == CachingMapStateHandler.SET_VALUE:
                self.values[key] = value
            else:
                raise ValueError('Unknown map state update flag: %s' % flag)

    def clear(self, state_key):
        self.values.clear()


if __name__ == '__main__':
    unittest.main()
