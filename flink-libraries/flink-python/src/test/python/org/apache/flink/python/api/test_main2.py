
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
from flink.functions.CrossFunction import CrossFunction
from flink.functions.JoinFunction import JoinFunction
from flink.functions.CoGroupFunction import CoGroupFunction
from flink.functions.Aggregation import Max, Min, Sum
from utils import Verify, Verify2, Id

if __name__ == "__main__":
    env = get_environment()

    d1 = env.from_elements(1, 6, 12)

    d2 = env.from_elements((1, 0.5, "hello", True), (2, 0.4, "world", False)).map(Id()).map(Id())  # force map chaining

    d3 = env.from_elements(("hello",), ("world",))

    d4 = env.from_elements((1, 0.5, "hello", True), (1, 0.4, "hello", False), (1, 0.5, "hello", True), (2, 0.4, "world", False))

    d5 = env.from_elements((4.4, 4.3, 1), (4.3, 4.4, 1), (4.2, 4.1, 3), (4.1, 4.1, 3))

    d6 = env.from_elements(1, 1, 12)

    #Aggregate
    d4 \
        .group_by(2).aggregate(Sum, 0).and_agg(Max, 1).and_agg(Min, 3) \
        .map_partition(Verify([(3, 0.5, "hello", False), (2, 0.4, "world", False)], "Grouped Aggregate")).output()

    d5 \
        .aggregate(Sum, 0).and_agg(Min, 1).and_agg(Max, 2) \
        .map_partition(Verify([(4.4 + 4.3 + 4.2 + 4.1, 4.1, 3)], "Ungrouped Aggregate")).output()

    #Aggregate syntactic sugar functions
    d4 \
        .group_by(2).sum(0).and_agg(Max, 1).and_agg(Min, 3) \
        .map_partition(Verify([(3, 0.5, "hello", False), (2, 0.4, "world", False)], "Grouped Aggregate")).output()

    d5 \
        .sum(0).and_agg(Min, 1).and_agg(Max, 2) \
        .map_partition(Verify([(4.4 + 4.3 + 4.2 + 4.1, 4.1, 3)], "Ungrouped Aggregate")).output()

    #Join
    class Join(JoinFunction):
        def join(self, value1, value2):
            if value1[3]:
                return value2[0] + str(value1[0])
            else:
                return value2[0] + str(value1[1])
    d2 \
        .join(d3).where(2).equal_to(0).using(Join()) \
        .map_partition(Verify(["hello1", "world0.4"], "Join")).output()
    d2 \
        .join(d3).where(lambda x: x[2]).equal_to(0).using(Join()) \
        .map_partition(Verify(["hello1", "world0.4"], "JoinWithKeySelector")).output()
    d2 \
        .join(d3).where(2).equal_to(0).project_first(0, 3).project_second(0) \
        .map_partition(Verify([(1, True, "hello"), (2, False, "world")], "Project Join")).output()
    d2 \
        .join(d3).where(2).equal_to(0) \
        .map_partition(Verify([((1, 0.5, "hello", True), ("hello",)), ((2, 0.4, "world", False), ("world",))], "Default Join")).output()

    #Cross
    class Cross(CrossFunction):
        def cross(self, value1, value2):
            return (value1, value2[3])
    d1 \
        .cross(d2).using(Cross()) \
        .map_partition(Verify([(1, True), (1, False), (6, True), (6, False), (12, True), (12, False)], "Cross")).output()
    d1 \
        .cross(d3) \
        .map_partition(Verify([(1, ("hello",)), (1, ("world",)), (6, ("hello",)), (6, ("world",)), (12, ("hello",)), (12, ("world",))], "Default Cross")).output()

    d2 \
        .cross(d3).project_second(0).project_first(0, 1) \
        .map_partition(Verify([("hello", 1, 0.5), ("world", 1, 0.5), ("hello", 2, 0.4), ("world", 2, 0.4)], "Project Cross")).output()

    #CoGroup
    class CoGroup(CoGroupFunction):
        def co_group(self, iterator1, iterator2, collector):
            while iterator1.has_next() and iterator2.has_next():
                collector.collect((iterator1.next(), iterator2.next()))
    d4 \
        .co_group(d5).where(0).equal_to(2).using(CoGroup()) \
        .map_partition(Verify([((1, 0.5, "hello", True), (4.4, 4.3, 1)), ((1, 0.4, "hello", False), (4.3, 4.4, 1))], "CoGroup")).output()

    #Broadcast
    class MapperBcv(MapFunction):
        def map(self, value):
            factor = self.context.get_broadcast_variable("test")[0][0]
            return value * factor
    d1 \
        .map(MapperBcv()).with_broadcast_set("test", d2) \
        .map_partition(Verify([1, 6, 12], "Broadcast")).output()

    #Misc
    class Mapper(MapFunction):
        def map(self, value):
            return value * value
    d1 \
        .map(Mapper()).map((lambda x: x * x)) \
        .map_partition(Verify([1, 1296, 20736], "Chained Lambda")).output()
    d2 \
        .project(0, 1, 2) \
        .map_partition(Verify([(1, 0.5, "hello"), (2, 0.4, "world")], "Project")).output()
    d2 \
        .union(d4) \
        .map_partition(Verify2([(1, 0.5, "hello", True), (2, 0.4, "world", False), (1, 0.5, "hello", True), (1, 0.4, "hello", False), (1, 0.5, "hello", True), (2, 0.4, "world", False)], "Union")).output()
    d1 \
        .first(1) \
        .map_partition(Verify([1], "First")).output()
    d4 \
        .group_by(0) \
        .first(1) \
        .map_partition(Verify([(1, 0.5, "hello", True), (2, 0.4, "world", False)], "Grouped First")).output()
    d1 \
        .rebalance()
    d6 \
        .distinct() \
        .map_partition(Verify([1, 12], "Distinct")).output()
    d2 \
        .partition_by_hash(3)


    #Execution
    env.set_parallelism(1)

    env.execute(local=True)
