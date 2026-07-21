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

import pyflink.dataframe as pf
from pyflink.table import DataTypes


class DataTypeTests(unittest.TestCase):
    def test_public_factory_surface(self):
        public_methods = {
            name for name in dir(pf.DataType) if not name.startswith("_")
        }

        self.assertEqual(public_methods, {"int64", "string"})

    def test_int64_maps_to_table_bigint(self):
        self.assertEqual(pf.DataType.int64()._to_table_data_type(), DataTypes.BIGINT())

    def test_string_maps_to_table_string(self):
        self.assertEqual(pf.DataType.string()._to_table_data_type(), DataTypes.STRING())

    def test_logically_equal_types_compare_and_hash_equally(self):
        first_int = pf.DataType.int64()
        second_int = pf.DataType.int64()
        string = pf.DataType.string()

        self.assertEqual(first_int, second_int)
        self.assertNotEqual(first_int, string)
        self.assertEqual(len({first_int, second_int, string}), 2)

    def test_nullability_modifiers_are_not_exposed(self):
        for data_type in [pf.DataType.int64(), pf.DataType.string()]:
            with self.subTest(data_type=data_type):
                self.assertFalse(hasattr(data_type, "not_null"))
                self.assertFalse(hasattr(data_type, "nullable"))


if __name__ == "__main__":
    unittest.main()
