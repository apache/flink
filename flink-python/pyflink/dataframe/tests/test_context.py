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

import pyflink.dataframe as pf
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class TableEnvironmentContextTests(PyFlinkStreamTableTestCase):
    def tearDown(self):
        try:
            super().tearDown()
        finally:
            pf.set_table_environment(None)

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
    import unittest

    unittest.main()
