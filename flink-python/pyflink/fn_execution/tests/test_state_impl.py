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

from apache_beam.coders import StrUtf8Coder, VarIntCoder
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners.worker.sdk_worker import GlobalCachingStateHandler
from apache_beam.runners.worker.statecache import StateCache

from pyflink.fn_execution.state_impl import CachingMapStateHandler, \
    InternalSynchronousMapRuntimeState, RemoteKeyedStateBackend


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
