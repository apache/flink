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


from pyflink.table import DataTypes
from pyflink.table.udf import ScalarFunction, udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase
import unittest


class SubtractOne(ScalarFunction, unittest.TestCase):

    def open(self, function_context):
        mg = function_context.get_metric_group()
        # test metric group info which is passed from Java to Python
        self.assertTrue('.aaa' in mg.get_metric_identifier('aaa'))
        self.assertTrue(len(mg.get_all_variables()) > 0)
        self.assertTrue(len(mg.get_scope_components()) > 0)

    def eval(self, i):
        return i - 1


class UserDefinedFunctionTests(object):

    def test_chaining_scalar_function(self):
        self.t_env.register_function(
            "subtract_one", udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(
            ['a'],
            [DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        t = self.t_env.from_elements([(1, 2, 1), (2, 5, 2), (3, 1, 3)], ['a', 'b', 'c'])
        t.select("subtract_one(b)") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1", "4", "0"])


class MetricITTests(UserDefinedFunctionTests, PyFlinkStreamTableTestCase):
    pass
