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

from pyflink.common import Row, RowKind


class RowTests(unittest.TestCase):

    def test_named_row_hash(self):
        for row_kind in RowKind:
            for other in [Row(b=2, a=1), Row('b', 'a')(2, 1)]:
                with self.subTest(row_kind=row_kind, other=other):
                    row = Row(a=1, b=2)
                    row.set_row_kind(row_kind)
                    other.set_row_kind(row_kind)
                    self.assertEqual(row, other)
                    self.assertEqual(hash(row), hash(other))
                    self.assertEqual(tuple(other), (2, 1))
                    self.assertEqual(list(other.as_dict()), ['b', 'a'])

    def test_named_row_set(self):
        self.assertEqual(len({Row(a=1, b=2), Row(b=2, a=1)}), 1)

    def test_named_row_dict(self):
        values = {Row(a=1, b=2): 'value'}
        self.assertEqual(values[Row(b=2, a=1)], 'value')
        values[Row(b=2, a=1)] = 'updated'
        self.assertEqual(len(values), 1)
        self.assertEqual(values[Row(a=1, b=2)], 'updated')

    def test_positional_row_hash(self):
        for values in [(), (1, 2), (2, 1)]:
            with self.subTest(values=values):
                self.assertEqual(hash(Row(*values)), hash(values))
                self.assertEqual(len({Row(*values), Row(*values)}), 1)
        self.assertEqual(len({Row(1, 2), Row(2, 1)}), 2)

    def test_nested_row_hash(self):
        row = Row(a=Row(x=1, y=2), b=3)
        other = Row(b=3, a=Row(y=2, x=1))
        self.assertEqual(row, other)
        self.assertEqual(hash(row), hash(other))
        self.assertEqual(len({row, other}), 1)

    def test_unhashable_row_fields(self):
        for value in [[1, 2], {'a': 1}]:
            for row in [Row(value), Row(a=value)]:
                with self.subTest(row=row):
                    with self.assertRaises(TypeError):
                        hash(row)


if __name__ == '__main__':
    unittest.main()
