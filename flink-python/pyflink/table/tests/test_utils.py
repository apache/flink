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

import datetime
import os
import pickle
import time
import unittest
from unittest.mock import patch

from pyflink.common import Row
from pyflink.table import DataTypes
from pyflink.table.utils import pickled_bytes_to_python_converter


class PickledBytesConverterTests(unittest.TestCase):

    def test_null_marker(self):
        # PythonBridgeUtils encodes NULL as byte[0], including nested fields.
        for field_type in (DataTypes.STRING(), DataTypes.INT(),
                           DataTypes.ARRAY(DataTypes.STRING()),
                           DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                           DataTypes.ROW([DataTypes.FIELD('value', DataTypes.STRING())])):
            with self.subTest(field_type=field_type):
                self.assertIsNone(pickled_bytes_to_python_converter(b'', field_type))

    def test_nullable_row_fields(self):
        result = pickled_bytes_to_python_converter(
            [b'\x00', b'', pickle.dumps([pickle.dumps('row')])],
            DataTypes.ROW([DataTypes.FIELD('text', DataTypes.STRING()),
                           DataTypes.FIELD('tags', DataTypes.ARRAY(DataTypes.STRING()))]))
        self.assertEqual(Row(None, ['row']), result)

    def test_array_of_nullable_rows(self):
        result = pickled_bytes_to_python_converter(
            pickle.dumps([b'', [b'\x00', b''], [b'\x00', pickle.dumps('ok')]]),
            DataTypes.ARRAY(DataTypes.ROW([DataTypes.FIELD('value', DataTypes.STRING())])))
        self.assertEqual([None, Row(None), Row('ok')], result)

    def test_nullable_map_values(self):
        for field_type in (DataTypes.INT(),
                           DataTypes.ROW([DataTypes.FIELD('value', DataTypes.STRING())])):
            with self.subTest(field_type=field_type):
                result = pickled_bytes_to_python_converter(
                    pickle.dumps([[pickle.dumps('key')], [b'']]),
                    DataTypes.MAP(DataTypes.STRING(), field_type))
                self.assertEqual({'key': None}, result)

    def test_empty_values_are_not_null(self):
        for value, field_type, expected in (
                ('', DataTypes.STRING(), ''), (b'', DataTypes.BYTES(), b''),
                ([], DataTypes.ARRAY(DataTypes.STRING()), []),
                ([[], []], DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), {})):
            with self.subTest(field_type=field_type):
                self.assertEqual(expected, pickled_bytes_to_python_converter(
                    pickle.dumps(value), field_type))

    def test_zero_and_false_are_not_null(self):
        for value, field_type in ((0, DataTypes.INT()), (False, DataTypes.BOOLEAN())):
            with self.subTest(field_type=field_type):
                self.assertEqual(value, pickled_bytes_to_python_converter(
                    pickle.dumps(value), field_type))

    def test_invalid_nonempty_payload_is_rejected(self):
        with self.assertRaises(pickle.UnpicklingError):
            pickled_bytes_to_python_converter(b'not-a-pickle', DataTypes.STRING())

    def test_timestamp_ignores_local_time_zone(self):
        patcher = patch.dict(os.environ, {'TZ': 'America/Los_Angeles'})
        patcher.start()
        self.addCleanup(time.tzset)
        self.addCleanup(patcher.stop)
        time.tzset()
        # Microseconds since 1970-01-01T00:00 of the wall clock, as PythonBridgeUtils sends them.
        for micros, expected in (
                (1583634600000000, datetime.datetime(2020, 3, 8, 2, 30)),  # DST gap
                (-2208988800000000, datetime.datetime(1900, 1, 1)),
                (-5364662400000000, datetime.datetime(1800, 1, 1)),
                (1593604800123456, datetime.datetime(2020, 7, 1, 12, 0, 0, 123456)),
                (-500000, datetime.datetime(1969, 12, 31, 23, 59, 59, 500000)),
                (-62135596800000000, datetime.datetime(1, 1, 1)),
                (253402300799999999, datetime.datetime(9999, 12, 31, 23, 59, 59, 999999))):
            with self.subTest(expected=expected):
                self.assertEqual(expected, pickled_bytes_to_python_converter(
                    pickle.dumps(micros), DataTypes.TIMESTAMP(6)))


if __name__ == '__main__':
    unittest.main()
