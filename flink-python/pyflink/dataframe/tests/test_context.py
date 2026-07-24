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
from typing import get_type_hints, Optional

import pyflink.dataframe as pf
from pyflink.table import StreamTableEnvironment
from pyflink.testing.test_case_utils import PyFlinkUTTestCase


class TableEnvironmentContextValidationTests(unittest.TestCase):
    def test_public_type_hints_are_resolvable(self):
        self.assertEqual(
            get_type_hints(pf.set_table_environment),
            {
                "t_env": Optional[StreamTableEnvironment],
                "return": type(None),
            },
        )
        self.assertEqual(
            get_type_hints(pf.get_table_environment),
            {"return": Optional[StreamTableEnvironment]},
        )
        self.assertEqual(
            get_type_hints(pf.get_or_create_table_environment),
            {"return": StreamTableEnvironment},
        )

    def test_set_environment_rejects_invalid_type_without_changing_state(self):
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)

        with self.assertRaisesRegex(
            TypeError, "t_env must be a StreamTableEnvironment or None"
        ):
            pf.set_table_environment(object())

        self.assertIs(pf.get_table_environment(), previous_environment)


class TableEnvironmentContextTests(PyFlinkUTTestCase):
    def setUp(self):
        super().setUp()
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)

    def test_set_environment_makes_it_retrievable(self):
        pf.set_table_environment(self.t_env)

        self.assertIs(pf.get_table_environment(), self.t_env)

    def test_set_none_clears_the_environment(self):
        pf.set_table_environment(self.t_env)

        pf.set_table_environment(None)

        self.assertIsNone(pf.get_table_environment())

    def test_created_environment_is_retrievable(self):
        pf.set_table_environment(None)

        created_environment = pf.get_or_create_table_environment()

        self.assertIs(created_environment, pf.get_table_environment())


if __name__ == "__main__":
    unittest.main()
