# ###############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################
from flink.plan.Environment import get_environment
from flink.functions.MapFunction import MapFunction
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.FilterFunction import FilterFunction
from flink.functions.MapPartitionFunction import MapPartitionFunction
from flink.functions.ReduceFunction import ReduceFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.plan.Constants import Order, WriteMode
from flink.plan.Constants import INT, STRING
import struct
from uuid import uuid4
from utils import Id, Verify

if __name__ == "__main__":
    env = get_environment()

    d1 = env.from_elements(1, 6, 12)

    d2 = env.from_elements((1, 0.5, "hello", True), (2, 0.4, "world", False))

    d3 = env.from_elements(("hello",), ("world",))

    d4 = env.from_elements((1, 0.5, "hello", True), (1, 0.4, "hello", False), (1, 0.5, "hello", True), (2, 0.4, "world", False))

    d5 = env.from_elements((1, 2.4), (1, 3.7), (1, 0.4), (1, 5.4))

    d6 = env.from_elements(1, 1, 12)

    d7 = env.generate_sequence(0, 999)

    d8 = env.from_elements((1, (2, 3)), (4, (5, 6)))

    #Generate Sequence Source
    d7.map(Id()).map_partition(Verify(range(1000), "Sequence")).output()

    #Zip with Index
    #check that IDs (first field of each element) are consecutive
    d7.zip_with_index()\
        .map(lambda x: x[0])\
        .map_partition(Verify(range(1000), "ZipWithIndex")).output()

    #CSV Source/Sink
    csv_data = env.read_csv("src/test/python/org/apache/flink/python/api/data_csv", (INT, INT, STRING))

    out = "flink_python_" + str(uuid4())
    csv_data.write_csv("/tmp/flink/" + out, line_delimiter="\n", field_delimiter="|", write_mode=WriteMode.OVERWRITE)

    out = "flink_python_" + str(uuid4())
    d8.write_csv("/tmp/flink/" + out, line_delimiter="\n", field_delimiter="|", write_mode=WriteMode.OVERWRITE)

    #Text Source/Sink
    text_data = env.read_text("src/test/python/org/apache/flink/python/api/data_text")

    out = "flink_python_" + str(uuid4())
    text_data.write_text("/tmp/flink/" + out, WriteMode.OVERWRITE)

    #Types
    env.from_elements(bytearray(b"hello"), bytearray(b"world"))\
        .map(Id()).map_partition(Verify([bytearray(b"hello"), bytearray(b"world")], "Byte")).output()

    env.from_elements(1, 2, 3, 4, 5)\
        .map(Id()).map_partition(Verify([1,2,3,4,5], "Int")).output()

    env.from_elements(True, True, False)\
        .map(Id()).map_partition(Verify([True, True, False], "Bool")).output()

    env.from_elements(1.4, 1.7, 12312.23)\
        .map(Id()).map_partition(Verify([1.4, 1.7, 12312.23], "Float")).output()

    env.from_elements("hello", "world")\
        .map(Id()).map_partition(Verify(["hello", "world"], "String")).output()

    #Custom Serialization
    class Ext(MapPartitionFunction):
        def map_partition(self, iterator, collector):
            for value in iterator:
                collector.collect(value.value)

    class MyObj(object):
        def __init__(self, i):
            self.value = i

    class MySerializer(object):
        def serialize(self, value):
            return struct.pack(">i", value.value)

    class MyDeserializer(object):
        def deserialize(self, read):
            i = struct.unpack(">i", read(4))[0]
            return MyObj(i)

    env.register_type(MyObj, MySerializer(), MyDeserializer())

    env.from_elements(MyObj(2), MyObj(4)) \
        .map(Id()).map_partition(Ext()) \
        .map_partition(Verify([2, 4], "CustomTypeSerialization")).output()

    #Map
    class Mapper(MapFunction):
        def map(self, value):
            return value * value
    d1 \
        .map((lambda x: x * x)).map(Mapper()) \
        .map_partition(Verify([1, 1296, 20736], "Map")).output()

    #FlatMap
    class FlatMap(FlatMapFunction):
        def flat_map(self, value, collector):
            collector.collect(value)
            collector.collect(value * 2)
    d1 \
        .flat_map(FlatMap()).flat_map(FlatMap()) \
        .map_partition(Verify([1, 2, 2, 4, 6, 12, 12, 24, 12, 24, 24, 48], "FlatMap")).output()

    #MapPartition
    class MapPartition(MapPartitionFunction):
        def map_partition(self, iterator, collector):
            for value in iterator:
                collector.collect(value * 2)
    d1 \
        .map_partition(MapPartition()) \
        .map_partition(Verify([2, 12, 24], "MapPartition")).output()

    #Filter
    class Filter(FilterFunction):
        def __init__(self, limit):
            super(Filter, self).__init__()
            self.limit = limit

        def filter(self, value):
            return value > self.limit
    d1 \
        .filter(Filter(5)).filter(Filter(8)) \
        .map_partition(Verify([12], "Filter")).output()

    #Reduce
    class Reduce(ReduceFunction):
        def reduce(self, value1, value2):
            return value1 + value2

    class Reduce2(ReduceFunction):
        def reduce(self, value1, value2):
            return (value1[0] + value2[0], value1[1] + value2[1], value1[2], value1[3] or value2[3])
    d1 \
        .reduce(Reduce()) \
        .map_partition(Verify([19], "AllReduce")).output()

    d4 \
        .group_by(2).reduce(Reduce2()) \
        .map_partition(Verify([(3, 1.4, "hello", True), (2, 0.4, "world", False)], "GroupedReduce")).output()

    #GroupReduce
    class GroupReduce(GroupReduceFunction):
        def reduce(self, iterator, collector):
            if iterator.has_next():
                i, f, s, b = iterator.next()
                for value in iterator:
                    i += value[0]
                    f += value[1]
                    b |= value[3]
                collector.collect((i, f, s, b))

    class GroupReduce2(GroupReduceFunction):
        def reduce(self, iterator, collector):
            for value in iterator:
                collector.collect(value)

    d4 \
        .reduce_group(GroupReduce2()) \
        .map_partition(Verify([(1, 0.5, "hello", True), (1, 0.4, "hello", False), (1, 0.5, "hello", True), (2, 0.4, "world", False)], "AllGroupReduce")).output()
    d4 \
        .group_by(lambda x: x[2]).reduce_group(GroupReduce(), combinable=False) \
        .map_partition(Verify([(3, 1.4, "hello", True), (2, 0.4, "world", False)], "GroupReduceWithKeySelector")).output()
    d4 \
        .group_by(2).reduce_group(GroupReduce()) \
        .map_partition(Verify([(3, 1.4, "hello", True), (2, 0.4, "world", False)], "GroupReduce")).output()
    d5 \
        .group_by(0).sort_group(1, Order.ASCENDING).reduce_group(GroupReduce2(), combinable=True) \
        .map_partition(Verify([(1, 0.4), (1, 2.4), (1, 3.7), (1, 5.4)], "SortedGroupReduceAsc")).output()
    d5 \
        .group_by(0).sort_group(1, Order.DESCENDING).reduce_group(GroupReduce2(), combinable=True) \
        .map_partition(Verify([(1, 5.4), (1, 3.7), (1, 2.4), (1, 0.4)], "SortedGroupReduceDes")).output()
    d5 \
        .group_by(lambda x: x[0]).sort_group(1, Order.DESCENDING).reduce_group(GroupReduce2(), combinable=True) \
        .map_partition(Verify([(1, 5.4), (1, 3.7), (1, 2.4), (1, 0.4)], "SortedGroupReduceKeySelG")).output()
    d5 \
        .group_by(0).sort_group(lambda x: x[1], Order.DESCENDING).reduce_group(GroupReduce2(), combinable=True) \
        .map_partition(Verify([(1, 5.4), (1, 3.7), (1, 2.4), (1, 0.4)], "SortedGroupReduceKeySelS")).output()
    d5 \
        .group_by(lambda x: x[0]).sort_group(lambda x: x[1], Order.DESCENDING).reduce_group(GroupReduce2(), combinable=True) \
        .map_partition(Verify([(1, 5.4), (1, 3.7), (1, 2.4), (1, 0.4)], "SortedGroupReduceKeySelGS")).output()

    #Execution
    env.set_parallelism(1)

    env.execute(local=True)
