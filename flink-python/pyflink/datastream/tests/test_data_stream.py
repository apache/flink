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
from pyflink.datastream.functions import FilterFunction
from pyflink.datastream.functions import KeySelector
from pyflink.datastream.functions import MapFunction, FlatMapFunction
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.java_gateway import get_gateway
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
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')]).map(lambda x: x)
        ds.set_parallelism(parallelism).add_sink(self.test_sink)
        plan = eval(str(self.env.get_execution_plan()))
        self.assertEqual(parallelism, plan['nodes'][1]['parallelism'])

    def test_set_max_parallelism(self):
        max_parallelism = 4
        self.env.set_parallelism(8)
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')]).map(lambda x: x)
        ds.set_parallelism(max_parallelism).add_sink(self.test_sink)
        plan = eval(str(self.env.get_execution_plan()))
        self.assertEqual(max_parallelism, plan['nodes'][1]['parallelism'])

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

    def test_filter_without_data_types(self):
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')])
        ds.filter(MyFilterFunction()).add_sink(self.test_sink)
        self.env.execute("test filter")
        results = self.test_sink.get_results(True)
        expected = ["(2, 'Hello', 'Hi')"]
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_filter_with_data_types(self):
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')],
                                      type_info=Types.ROW(
                                          [Types.INT(), Types.STRING(), Types.STRING()])
                                      )
        ds.filter(lambda x: x[0] % 2 == 0).add_sink(self.test_sink)
        self.env.execute("test filter")
        results = self.test_sink.get_results(False)
        expected = ['2,Hello,Hi']
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

    def test_multi_key_by(self):
        ds = self.env.from_collection([('a', 0), ('b', 0), ('c', 1), ('d', 1), ('e', 2)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.key_by(MyKeySelector(), key_type_info=Types.INT()).key_by(lambda x: x[0])\
            .add_sink(self.test_sink)

        self.env.execute("test multi key by")
        results = self.test_sink.get_results(False)
        expected = ['d,1', 'c,1', 'a,0', 'b,0', 'e,2']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_print_without_align_output(self):
        # No need to align output typeinfo since we have specified the type info of the DataStream.
        self.env.set_parallelism(1)
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.print()
        plan = eval(str(self.env.get_execution_plan()))
        self.assertEqual("Sink: Print to Std. Out", plan['nodes'][1]['type'])

    def test_print_with_align_output(self):
        # need to align output type before print, therefore the plan will contain three nodes
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)])
        ds.print()
        plan = eval(str(self.env.get_execution_plan()))
        self.assertEqual(3, len(plan['nodes']))
        self.assertEqual("Sink: Print to Std. Out", plan['nodes'][2]['type'])

    def test_union_stream(self):
        ds_1 = self.env.from_collection([1, 2, 3])
        ds_2 = self.env.from_collection([4, 5, 6])
        ds_3 = self.env.from_collection([7, 8, 9])

        united_stream = ds_3.union(ds_1, ds_2)

        united_stream.map(lambda x: x + 1).add_sink(self.test_sink)
        exec_plan = eval(self.env.get_execution_plan())
        source_ids = []
        union_node_pre_ids = []
        for node in exec_plan['nodes']:
            if node['pact'] == 'Data Source':
                source_ids.append(node['id'])
            if node['pact'] == 'Operator':
                for pre in node['predecessors']:
                    union_node_pre_ids.append(pre['id'])

        source_ids.sort()
        union_node_pre_ids.sort()
        self.assertEqual(source_ids, union_node_pre_ids)

    def test_project(self):
        ds = self.env.from_collection([[1, 2, 3, 4], [5, 6, 7, 8]],
                                      type_info=Types.TUPLE(
                                          [Types.INT(), Types.INT(), Types.INT(), Types.INT()]))
        ds.project(1, 3).map(lambda x: (x[0], x[1] + 1)).add_sink(self.test_sink)
        exec_plan = eval(self.env.get_execution_plan())
        self.assertEqual(exec_plan['nodes'][1]['type'], 'Projection')

    def test_broadcast(self):
        ds_1 = self.env.from_collection([1, 2, 3])
        ds_1.broadcast().map(lambda x: x + 1).set_parallelism(3).add_sink(self.test_sink)
        exec_plan = eval(self.env.get_execution_plan())
        broadcast_node = exec_plan['nodes'][1]
        pre_ship_strategy = broadcast_node['predecessors'][0]['ship_strategy']
        self.assertEqual(pre_ship_strategy, 'BROADCAST')

    def test_rebalance(self):
        ds_1 = self.env.from_collection([1, 2, 3])
        ds_1.rebalance().map(lambda x: x + 1).set_parallelism(3).add_sink(self.test_sink)
        exec_plan = eval(self.env.get_execution_plan())
        rebalance_node = exec_plan['nodes'][1]
        pre_ship_strategy = rebalance_node['predecessors'][0]['ship_strategy']
        self.assertEqual(pre_ship_strategy, 'REBALANCE')

    def test_rescale(self):
        ds_1 = self.env.from_collection([1, 2, 3])
        ds_1.rescale().map(lambda x: x + 1).set_parallelism(3).add_sink(self.test_sink)
        exec_plan = eval(self.env.get_execution_plan())
        rescale_node = exec_plan['nodes'][1]
        pre_ship_strategy = rescale_node['predecessors'][0]['ship_strategy']
        self.assertEqual(pre_ship_strategy, 'RESCALE')

    def test_shuffle(self):
        ds_1 = self.env.from_collection([1, 2, 3])
        ds_1.shuffle().map(lambda x: x + 1).set_parallelism(3).add_sink(self.test_sink)
        exec_plan = eval(self.env.get_execution_plan())
        shuffle_node = exec_plan['nodes'][1]
        pre_ship_strategy = shuffle_node['predecessors'][0]['ship_strategy']
        self.assertEqual(pre_ship_strategy, 'SHUFFLE')

    def test_keyed_stream_partitioning(self):
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)])
        keyed_stream = ds.key_by(lambda x: x[1])
        with self.assertRaises(Exception):
            keyed_stream.shuffle()

        with self.assertRaises(Exception):
            keyed_stream.rebalance()

        with self.assertRaises(Exception):
            keyed_stream.rescale()

        with self.assertRaises(Exception):
            keyed_stream.broadcast()

        with self.assertRaises(Exception):
            keyed_stream.forward()

    def test_slot_sharing_group(self):
        source_operator_name = 'collection source'
        map_operator_name = 'map_operator'
        slot_sharing_group_1 = 'slot_sharing_group_1'
        slot_sharing_group_2 = 'slot_sharing_group_2'
        ds_1 = self.env.from_collection([1, 2, 3]).name(source_operator_name)
        ds_1.slot_sharing_group(slot_sharing_group_1).map(lambda x: x + 1).set_parallelism(3)\
            .name(map_operator_name).slot_sharing_group(slot_sharing_group_2)\
            .add_sink(self.test_sink)

        j_generated_stream_graph = self.env._j_stream_execution_environment \
            .getStreamGraph("test start new_chain", True)

        j_stream_nodes = list(j_generated_stream_graph.getStreamNodes().toArray())
        for j_stream_node in j_stream_nodes:
            if j_stream_node.getOperatorName() == source_operator_name:
                self.assertEqual(j_stream_node.getSlotSharingGroup(), slot_sharing_group_1)
            elif j_stream_node.getOperatorName() == map_operator_name:
                self.assertEqual(j_stream_node.getSlotSharingGroup(), slot_sharing_group_2)

    def test_chaining_strategy(self):
        chained_operator_name_0 = "map_operator_0"
        chained_operator_name_1 = "map_operator_1"
        chained_operator_name_2 = "map_operator_2"

        ds = self.env.from_collection([1, 2, 3])
        ds.map(lambda x: x).set_parallelism(2).name(chained_operator_name_0)\
            .map(lambda x: x).set_parallelism(2).name(chained_operator_name_1)\
            .map(lambda x: x).set_parallelism(2).name(chained_operator_name_2)\
            .add_sink(self.test_sink)

        def assert_chainable(j_stream_graph, expected_upstream_chainable,
                             expected_downstream_chainable):
            j_stream_nodes = list(j_stream_graph.getStreamNodes().toArray())
            for j_stream_node in j_stream_nodes:
                if j_stream_node.getOperatorName() == chained_operator_name_1:
                    JStreamingJobGraphGenerator = get_gateway().jvm \
                        .org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator

                    j_in_stream_edge = j_stream_node.getInEdges().get(0)
                    upstream_chainable = JStreamingJobGraphGenerator.isChainable(j_in_stream_edge,
                                                                                 j_stream_graph)
                    self.assertEqual(expected_upstream_chainable, upstream_chainable)

                    j_out_stream_edge = j_stream_node.getOutEdges().get(0)
                    downstream_chainable = JStreamingJobGraphGenerator.isChainable(
                        j_out_stream_edge, j_stream_graph)
                    self.assertEqual(expected_downstream_chainable, downstream_chainable)

        # The map_operator_1 has the same parallelism with map_operator_0 and map_operator_2, and
        # ship_strategy for map_operator_0 and map_operator_1 is FORWARD, so the map_operator_1
        # can be chained with map_operator_0 and map_operator_2.
        j_generated_stream_graph = self.env._j_stream_execution_environment\
            .getStreamGraph("test start new_chain", True)
        assert_chainable(j_generated_stream_graph, True, True)

        ds = self.env.from_collection([1, 2, 3])
        # Start a new chain for map_operator_1
        ds.map(lambda x: x).set_parallelism(2).name(chained_operator_name_0) \
            .map(lambda x: x).set_parallelism(2).name(chained_operator_name_1).start_new_chain() \
            .map(lambda x: x).set_parallelism(2).name(chained_operator_name_2) \
            .add_sink(self.test_sink)

        j_generated_stream_graph = self.env._j_stream_execution_environment \
            .getStreamGraph("test start new_chain", True)
        # We start a new chain for map operator, therefore, it cannot be chained with upstream
        # operator, but can be chained with downstream operator.
        assert_chainable(j_generated_stream_graph, False, True)

        ds = self.env.from_collection([1, 2, 3])
        # Disable chaining for map_operator_1
        ds.map(lambda x: x).set_parallelism(2).name(chained_operator_name_0) \
            .map(lambda x: x).set_parallelism(2).name(chained_operator_name_1).disable_chaining() \
            .map(lambda x: x).set_parallelism(2).name(chained_operator_name_2) \
            .add_sink(self.test_sink)

        j_generated_stream_graph = self.env._j_stream_execution_environment \
            .getStreamGraph("test start new_chain", True)
        # We disable chaining for map_operator_1, therefore, it cannot be chained with
        # upstream and downstream operators.
        assert_chainable(j_generated_stream_graph, False, False)

    def tearDown(self) -> None:
        self.test_sink.clear()


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


class MyFilterFunction(FilterFunction):

    def filter(self, value):
        return value[0] % 2 == 0
