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
import pandas as pd
from pandas.util.testing import assert_frame_equal

from pyflink.table import DataTypes
from pyflink.table.udf import AggregateFunction, udaf
from pyflink.testing.test_case_utils import PyFlinkBlinkStreamTableTestCase


class CountAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        return [0]

    def accumulate(self, accumulator, *args):
        accumulator[0] = accumulator[0] + 1

    def retract(self, accumulator, *args):
        accumulator[0] = accumulator[0] - 1

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] = accumulator[0] + other_acc[0]

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.BIGINT())

    def get_result_type(self):
        return DataTypes.BIGINT()


class SumAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        return [0]

    def accumulate(self, accumulator, *args):
        accumulator[0] = accumulator[0] + args[0]

    def retract(self, accumulator, *args):
        accumulator[0] = accumulator[0] - args[0]

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] = accumulator[0] + other_acc[0]

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.BIGINT())

    def get_result_type(self):
        return DataTypes.BIGINT()


class StreamTableAggregateTests(PyFlinkBlinkStreamTableTestCase):

    def test_double_aggregate(self):
        self.t_env.register_function("my_count", CountAggregateFunction())
        self.t_env.create_temporary_function("my_sum", SumAggregateFunction())
        # trigger the finish bundle more frequently to ensure testing the communication
        # between RemoteKeyedStateBackend and the StateGrpcService.
        self.t_env.get_config().get_configuration().set_string(
            "python.fn-execution.bundle.size", "2")
        # trigger the cache eviction in a bundle.
        self.t_env.get_config().get_configuration().set_string(
            "python.state.cache.size", "1")
        t = self.t_env.from_elements([(1, 'Hi', 'Hello'),
                                      (3, 'Hi', 'hi'),
                                      (3, 'Hi2', 'hi'),
                                      (3, 'Hi', 'hi2'),
                                      (2, 'Hi', 'Hello')], ['a', 'b', 'c'])
        result = t.group_by(t.c).select("my_count(a) as a, my_sum(a) as b, c") \
            .select("my_count(a) as a, my_sum(b) as b")
        assert_frame_equal(result.to_pandas(), pd.DataFrame([[3, 12]], columns=['a', 'b']))

    def test_using_decorator(self):
        my_count = udaf(CountAggregateFunction(),
                        accumulator_type=DataTypes.ARRAY(DataTypes.INT()),
                        result_type=DataTypes.INT())
        t = self.t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c'])
        result = t.group_by(t.c) \
            .select(my_count(t.a).alias("a"), t.c.alias("b"))
        plan = result.explain()
        result_type = result.get_schema().get_field_data_type(0)
        self.assertTrue(plan.find("PythonGroupAggregate(groupBy=[c], ") >= 0)
        self.assertEqual(result_type, DataTypes.INT())


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
