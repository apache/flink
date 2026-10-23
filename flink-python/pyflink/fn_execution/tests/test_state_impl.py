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

import unittest

from apache_beam.coders import StrUtf8Coder
from apache_beam.portability.api import beam_fn_api_pb2

from pyflink.fn_execution.state_impl import CachingMapStateHandler, \
    InternalSynchronousMapRuntimeState


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
