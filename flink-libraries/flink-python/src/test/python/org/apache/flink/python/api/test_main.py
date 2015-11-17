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
from flink.functions.CrossFunction import CrossFunction
from flink.functions.JoinFunction import JoinFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.CoGroupFunction import CoGroupFunction
from flink.plan.Constants import INT, STRING, FLOAT, BOOL, CUSTOM, Order
import struct


class Mapper(MapFunction):
    def map(self, value):
        return value * value


class Filter(FilterFunction):
    def __init__(self, limit):
        super(Filter, self).__init__()
        self.limit = limit

    def filter(self, value):
        return value > self.limit


class FlatMap(FlatMapFunction):
    def flat_map(self, value, collector):
        collector.collect(value)
        collector.collect(value * 2)


class MapPartition(MapPartitionFunction):
    def map_partition(self, iterator, collector):
        for value in iterator:
            collector.collect(value * 2)


class Reduce(ReduceFunction):
    def reduce(self, value1, value2):
        return value1 + value2


class Reduce2(ReduceFunction):
    def reduce(self, value1, value2):
        return (value1[0] + value2[0], value1[1] + value2[1], value1[2], value1[3] or value2[3])


class Cross(CrossFunction):
    def cross(self, value1, value2):
        return (value1, value2[3])


class MapperBcv(MapFunction):
    def map(self, value):
        factor = self.context.get_broadcast_variable("test")[0][0]
        return value * factor


class Join(JoinFunction):
    def join(self, value1, value2):
        if value1[3]:
            return value2[0] + str(value1[0])
        else:
            return value2[0] + str(value1[1])


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


class GroupReduce3(GroupReduceFunction):
    def reduce(self, iterator, collector):
        collector.collect(iterator.next())

    def combine(self, iterator, collector):
        if iterator.has_next():
            v1 = iterator.next()
        if iterator.has_next():
            v2 = iterator.next()
        if v1[0] < v2[0]:
            collector.collect(v1)
        else:
            collector.collect(v2)


class CoGroup(CoGroupFunction):
    def co_group(self, iterator1, iterator2, collector):
        while iterator1.has_next() and iterator2.has_next():
            collector.collect((iterator1.next(), iterator2.next()))


class Id(MapFunction):
    def map(self, value):
        return value


class Verify(MapPartitionFunction):
    def __init__(self, expected, name):
        super(Verify, self).__init__()
        self.expected = expected
        self.name = name

    def map_partition(self, iterator, collector):
        index = 0
        for value in iterator:
            if value != self.expected[index]:
                print(self.name + " Test failed. Expected: " + str(self.expected[index]) + " Actual: " + str(value))
                raise Exception(self.name + " failed!")
            index += 1
        collector.collect(self.name + " successful!")


class Verify2(MapPartitionFunction):
    def __init__(self, expected, name):
        super(Verify2, self).__init__()
        self.expected = expected
        self.name = name

    def map_partition(self, iterator, collector):
        for value in iterator:
            if value in self.expected:
                try:
                    self.expected.remove(value)
                except Exception:
                    raise Exception(self.name + " failed!")
        collector.collect(self.name + " successful!")


if __name__ == "__main__":
    env = get_environment()

    d1 = env.from_elements(1, 6, 12)

    d2 = env.from_elements((1, 0.5, "hello", True), (2, 0.4, "world", False))

    d3 = env.from_elements(("hello",), ("world",))

    d4 = env.from_elements((1, 0.5, "hello", True), (1, 0.4, "hello", False), (1, 0.5, "hello", True), (2, 0.4, "world", False))

    d5 = env.from_elements((4.4, 4.3, 1), (4.3, 4.4, 1), (4.2, 4.1, 3), (4.1, 4.1, 3))

    d1 \
        .map((lambda x: x * x), INT).map(Mapper(), INT) \
        .map_partition(Verify([1, 1296, 20736], "Map"), STRING).output()

    d1 \
        .map(Mapper(), INT).map((lambda x: x * x), INT) \
        .map_partition(Verify([1, 1296, 20736], "Chained Lambda"), STRING).output()

    d1 \
        .filter(Filter(5)).filter(Filter(8)) \
        .map_partition(Verify([12], "Filter"), STRING).output()

    d1 \
        .flat_map(FlatMap(), INT).flat_map(FlatMap(), INT) \
        .map_partition(Verify([1, 2, 2, 4, 6, 12, 12, 24, 12, 24, 24, 48], "FlatMap"), STRING).output()

    d1 \
        .map_partition(MapPartition(), INT) \
        .map_partition(Verify([2, 12, 24], "MapPartition"), STRING).output()

    d1 \
        .reduce(Reduce()) \
        .map_partition(Verify([19], "AllReduce"), STRING).output()

    d4 \
        .group_by(2).reduce(Reduce2()) \
        .map_partition(Verify([(3, 1.4, "hello", True), (2, 0.4, "world", False)], "CombineReduce"), STRING).output()

    d4 \
        .map(Id(), (INT, FLOAT, STRING, BOOL)).group_by(2).reduce(Reduce2()) \
        .map_partition(Verify([(3, 1.4, "hello", True), (2, 0.4, "world", False)], "ChainedReduce"), STRING).output()

    d1 \
        .map(MapperBcv(), INT).with_broadcast_set("test", d2) \
        .map_partition(Verify([1, 6, 12], "Broadcast"), STRING).output()

    d1 \
        .cross(d2).using(Cross(), (INT, BOOL)) \
        .map_partition(Verify([(1, True), (1, False), (6, True), (6, False), (12, True), (12, False)], "Cross"), STRING).output()

    d1 \
        .cross(d3) \
        .map_partition(Verify([(1, ("hello",)), (1, ("world",)), (6, ("hello",)), (6, ("world",)), (12, ("hello",)), (12, ("world",))], "Default Cross"), STRING).output()

    d2 \
        .cross(d3).project_second(0).project_first(0, 1) \
        .map_partition(Verify([("hello", 1, 0.5), ("world", 1, 0.5), ("hello", 2, 0.4), ("world", 2, 0.4)], "Project Cross"), STRING).output()

    d2 \
        .join(d3).where(2).equal_to(0).using(Join(), STRING) \
        .map_partition(Verify(["hello1", "world0.4"], "Join"), STRING).output()

    d2 \
        .join(d3).where(2).equal_to(0).project_first(0, 3).project_second(0) \
        .map_partition(Verify([(1, True, "hello"), (2, False, "world")], "Project Join"), STRING).output()

    d2 \
        .join(d3).where(2).equal_to(0) \
        .map_partition(Verify([((1, 0.5, "hello", True), ("hello",)), ((2, 0.4, "world", False), ("world",))], "Default Join"), STRING).output()

    d2 \
        .project(0, 1, 2) \
        .map_partition(Verify([(1, 0.5, "hello"), (2, 0.4, "world")], "Project"), STRING).output()

    d2 \
        .union(d4) \
        .map_partition(Verify2([(1, 0.5, "hello", True), (2, 0.4, "world", False), (1, 0.5, "hello", True), (1, 0.4, "hello", False), (1, 0.5, "hello", True), (2, 0.4, "world", False)], "Union"), STRING).output()

    d4 \
        .group_by(2).reduce_group(GroupReduce(), (INT, FLOAT, STRING, BOOL), combinable=False) \
        .map_partition(Verify([(3, 1.4, "hello", True), (2, 0.4, "world", False)], "AllGroupReduce"), STRING).output()

    d4 \
        .map(Id(), (INT, FLOAT, STRING, BOOL)).group_by(2).reduce_group(GroupReduce(), (INT, FLOAT, STRING, BOOL), combinable=True) \
        .map_partition(Verify([(3, 1.4, "hello", True), (2, 0.4, "world", False)], "ChainedGroupReduce"), STRING).output()

    d4 \
        .group_by(2).reduce_group(GroupReduce(), (INT, FLOAT, STRING, BOOL), combinable=True) \
        .map_partition(Verify([(3, 1.4, "hello", True), (2, 0.4, "world", False)], "CombineGroupReduce"), STRING).output()

    d5 \
        .group_by(2).sort_group(0, Order.DESCENDING).sort_group(1, Order.ASCENDING).reduce_group(GroupReduce3(), (FLOAT, FLOAT, INT), combinable=True) \
        .map_partition(Verify([(4.3, 4.4, 1), (4.1, 4.1, 3)], "ChainedSortedGroupReduce"), STRING).output()

    d4 \
        .co_group(d5).where(0).equal_to(2).using(CoGroup(), ((INT, FLOAT, STRING, BOOL), (FLOAT, FLOAT, INT))) \
        .map_partition(Verify([((1, 0.5, "hello", True), (4.4, 4.3, 1)), ((1, 0.4, "hello", False), (4.3, 4.4, 1))], "CoGroup"), STRING).output()

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
        .map(Id(), CUSTOM).map_partition(Ext(), INT) \
        .map_partition(Verify([2, 4], "CustomTypeSerialization"), STRING).output()

    env.set_degree_of_parallelism(1)

    env.execute(local=True)
