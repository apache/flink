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

from pyflink.common import Configuration
from pyflink.table import TableConfig, SqlDialect
from pyflink.testing.test_case_utils import PyFlinkTestCase


class TableConfigTests(PyFlinkTestCase):

    def test_get_set_idle_state_retention_time(self):
        table_config = TableConfig.get_default()

        table_config.set_idle_state_retention_time(
            datetime.timedelta(days=1), datetime.timedelta(days=2))

        self.assertEqual(2 * 24 * 3600 * 1000, table_config.get_max_idle_state_retention_time())
        self.assertEqual(24 * 3600 * 1000, table_config.get_min_idle_state_retention_time())

    def test_get_set_decimal_context(self):
        table_config = TableConfig.get_default()

        table_config.set_decimal_context(20, "UNNECESSARY")
        self.assertEqual((20, "UNNECESSARY"), table_config.get_decimal_context())
        table_config.set_decimal_context(20, "HALF_EVEN")
        self.assertEqual((20, "HALF_EVEN"), table_config.get_decimal_context())
        table_config.set_decimal_context(20, "HALF_DOWN")
        self.assertEqual((20, "HALF_DOWN"), table_config.get_decimal_context())
        table_config.set_decimal_context(20, "HALF_UP")
        self.assertEqual((20, "HALF_UP"), table_config.get_decimal_context())
        table_config.set_decimal_context(20, "FLOOR")
        self.assertEqual((20, "FLOOR"), table_config.get_decimal_context())
        table_config.set_decimal_context(20, "CEILING")
        self.assertEqual((20, "CEILING"), table_config.get_decimal_context())
        table_config.set_decimal_context(20, "DOWN")
        self.assertEqual((20, "DOWN"), table_config.get_decimal_context())
        table_config.set_decimal_context(20, "UP")
        self.assertEqual((20, "UP"), table_config.get_decimal_context())

    def test_get_set_local_timezone(self):
        table_config = TableConfig.get_default()

        table_config.set_local_timezone("Asia/Shanghai")
        timezone = table_config.get_local_timezone()

        self.assertEqual(timezone, "Asia/Shanghai")

    def test_get_set_max_generated_code_length(self):
        table_config = TableConfig.get_default()

        table_config.set_max_generated_code_length(32000)
        max_generated_code_length = table_config.get_max_generated_code_length()

        self.assertEqual(max_generated_code_length, 32000)

    def test_get_set_null_check(self):
        table_config = TableConfig.get_default()

        null_check = table_config.get_null_check()
        self.assertTrue(null_check)

        table_config.set_null_check(False)
        null_check = table_config.get_null_check()

        self.assertFalse(null_check)

    def test_get_configuration(self):
        table_config = TableConfig.get_default()

        table_config.get_configuration().set_string("k1", "v1")

        self.assertEqual(table_config.get_configuration().get_string("k1", ""), "v1")

    def test_add_configuration(self):
        table_config = TableConfig.get_default()
        configuration = Configuration()
        configuration.set_string("k1", "v1")

        table_config.add_configuration(configuration)

        self.assertEqual(table_config.get_configuration().get_string("k1", ""), "v1")

    def test_get_set_sql_dialect(self):
        table_config = TableConfig.get_default()

        sql_dialect = table_config.get_sql_dialect()
        self.assertEqual(sql_dialect, SqlDialect.DEFAULT)

        table_config.set_sql_dialect(SqlDialect.HIVE)
        sql_dialect = table_config.get_sql_dialect()
        self.assertEqual(sql_dialect, SqlDialect.HIVE)
