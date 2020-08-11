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
import decimal

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeySelector
from pyflink.datastream.functions import MapFunction, FlatMapFunction
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.testing.test_case_utils import PyFlinkTestCase


class DataStreamTests(PyFlinkTestCase):

    def setUp(self) -> None:
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.test_sink = DataStreamTestSinkFunction()

    def test_data_stream_name(self):
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')])
        test_name = 'test_name'
        ds.name(test_name)
        self.assertEqual(test_name, ds.get_name())

    def test_set_parallelism(self):
        parallelism = 3
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')])
        ds.set_parallelism(parallelism)
        ds.add_sink(self.test_sink)
        plan = eval(str(self.env.get_execution_plan()))
        self.assertEqual(parallelism, plan['nodes'][0]['parallelism'])

    def test_set_max_parallelism(self):
        max_parallelism = 4
        self.env.set_parallelism(8)
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')])
        ds.set_parallelism(max_parallelism).add_sink(self.test_sink)
        plan = eval(str(self.env.get_execution_plan()))
        self.assertEqual(max_parallelism, plan['nodes'][0]['parallelism'])

    def test_force_non_parallel(self):
        self.env.set_parallelism(8)
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')])
        ds.force_non_parallel().add_sink(self.test_sink)
        plan = eval(str(self.env.get_execution_plan()))
        self.assertEqual(1, plan['nodes'][0]['parallelism'])

    def test_reduce_function_without_data_types(self):
        ds = self.env.from_collection([(1, 'a'), (2, 'a'), (3, 'a'), (4, 'b')],
                                      type_info=Types.ROW([Types.INT(), Types.STRING()]))
        ds.key_by(lambda a: a[1]).reduce(lambda a, b: (a[0] + b[0], b[1])).add_sink(self.test_sink)
        self.env.execute('reduce_function_test')
        result = self.test_sink.get_results()
        expected = ["1,a", "3,a", "6,a", "4,b"]
        expected.sort()
        result.sort()
        self.assertEqual(expected, result)

    def test_map_function_without_data_types(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection([('ab', decimal.Decimal(1)),
                                       ('bdc', decimal.Decimal(2)),
                                       ('cfgs', decimal.Decimal(3)),
                                       ('deeefg', decimal.Decimal(4))],
                                      type_info=Types.ROW([Types.STRING(), Types.BIG_DEC()]))
        ds.map(MyMapFunction()).add_sink(self.test_sink)
        self.env.execute('map_function_test')
        results = self.test_sink.get_results(True)
        expected = ["('ab', 2, Decimal('1'))", "('bdc', 3, Decimal('2'))",
                    "('cfgs', 4, Decimal('3'))", "('deeefg', 6, Decimal('4'))"]
        expected.sort()
        results.sort()
        self.assertEqual(expected, results)

    def test_map_function_with_data_types(self):
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.TUPLE([Types.STRING(), Types.INT()]))

        def map_func(value):
            result = (value[0], len(value[0]), value[1])
            return result

        ds.map(map_func, type_info=Types.ROW([Types.STRING(), Types.INT(), Types.INT()]))\
            .add_sink(self.test_sink)
        self.env.execute('map_function_test')
        results = self.test_sink.get_results(False)
        expected = ['ab,2,1', 'bdc,3,2', 'cfgs,4,3', 'deeefg,6,4']
        expected.sort()
        results.sort()
        self.assertEqual(expected, results)

    def test_map_function_with_data_types_and_function_object(self):
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        ds.map(MyMapFunction(), type_info=Types.ROW([Types.STRING(), Types.INT(), Types.INT()]))\
            .add_sink(self.test_sink)
        self.env.execute('map_function_test')
        results = self.test_sink.get_results(False)
        expected = ['ab,2,1', 'bdc,3,2', 'cfgs,4,3', 'deeefg,6,4']
        expected.sort()
        results.sort()
        self.assertEqual(expected, results)

    def test_flat_map_function(self):
        ds = self.env.from_collection([('a', 0), ('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.flat_map(MyFlatMapFunction(), type_info=Types.ROW([Types.STRING(), Types.INT()]))\
            .add_sink(self.test_sink)

        self.env.execute('flat_map_test')
        results = self.test_sink.get_results(False)
        expected = ['a,0', 'bdc,2', 'deeefg,4']
        results.sort()
        expected.sort()

        self.assertEqual(expected, results)

    def test_flat_map_function_with_function_object(self):
        ds = self.env.from_collection([('a', 0), ('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        def flat_map(value):
            if value[1] % 2 == 0:
                yield value

        ds.flat_map(flat_map, type_info=Types.ROW([Types.STRING(), Types.INT()]))\
            .add_sink(self.test_sink)
        self.env.execute('flat_map_test')
        results = self.test_sink.get_results(False)
        expected = ['a,0', 'bdc,2', 'deeefg,4']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_add_sink(self):
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.add_sink(self.test_sink)
        self.env.execute("test_add_sink")
        results = self.test_sink.get_results(False)
        expected = ['deeefg,4', 'bdc,2', 'ab,1', 'cfgs,3']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_key_by_map(self):
        ds = self.env.from_collection([('a', 0), ('b', 0), ('c', 1), ('d', 1), ('e', 2)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        keyed_stream = ds.key_by(MyKeySelector(), key_type_info=Types.INT())

        with self.assertRaises(Exception):
            keyed_stream.name("keyed stream")

        class AssertKeyMapFunction(MapFunction):
            def __init__(self):
                self.pre = None

            def map(self, value):
                if value[0] == 'b':
                    assert self.pre == 'a'
                if value[0] == 'd':
                    assert self.pre == 'c'
                self.pre = value[0]
                return value

        keyed_stream.map(AssertKeyMapFunction()).add_sink(self.test_sink)
        self.env.execute('key_by_test')
        results = self.test_sink.get_results(True)
        expected = ["<Row('e', 2)>", "<Row('a', 0)>", "<Row('b', 0)>", "<Row('c', 1)>",
                    "<Row('d', 1)>"]
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def tearDown(self) -> None:
        self.test_sink.get_results()


class MyMapFunction(MapFunction):

    def map(self, value):
        result = (value[0], len(value[0]), value[1])
        return result


class MyFlatMapFunction(FlatMapFunction):

    def flat_map(self, value):
        if value[1] % 2 == 0:
            yield value


class MyKeySelector(KeySelector):
    def get_key(self, value):
        return value[1]
