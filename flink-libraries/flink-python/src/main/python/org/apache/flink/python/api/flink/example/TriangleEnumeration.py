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
from flink.plan.Environment import get_environment
from flink.plan.Constants import Order
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.ReduceFunction import ReduceFunction
from flink.functions.MapFunction import MapFunction
from flink.functions.JoinFunction import JoinFunction


class EdgeDuplicator(FlatMapFunction):
    def flat_map(self, value, collector):
        collector.collect((value[0], value[1]))
        collector.collect((value[1], value[0]))


class DegreeCounter(GroupReduceFunction):
    def reduce(self, iterator, collector):
        other_vertices = []

        data = iterator.next()
        edge = (data[0], data[1])

        group_vertex = edge[0]
        other_vertices.append(edge[1])

        while iterator.has_next():
            data = iterator.next()
            edge = [data[0], data[1]]
            other_vertex = edge[1]

            contained = False
            for v in other_vertices:
                if v == other_vertex:
                    contained = True
                    break
            if not contained and not other_vertex == group_vertex:
                other_vertices.append(other_vertex)

        degree = len(other_vertices)

        for other_vertex in other_vertices:
            if group_vertex < other_vertex:
                output_edge = (group_vertex, degree, other_vertex, 0)
            else:
                output_edge = (other_vertex, 0, group_vertex, degree)
            collector.collect(output_edge)


class DegreeJoiner(ReduceFunction):
    def reduce(self, value1, value2):
        edge1 = [value1[0], value1[1], value1[2], value1[3]]
        edge2 = [value2[0], value2[1], value2[2], value2[3]]

        out_edge = [edge1[0], edge1[1], edge1[2], edge1[3]]
        if edge1[1] == 0 and (not edge1[3] == 0):
            out_edge[1] = edge2[1]
        elif (not edge1[1] == 0) and edge1[3] == 0:
            out_edge[3] = edge2[3]
        return out_edge


class EdgeByDegreeProjector(MapFunction):
    def map(self, value):
        if value[1] > value[3]:
            return (value[2], value[0])
        else:
            return (value[0], value[2])


class EdgeByIdProjector(MapFunction):
    def map(self, value):
        edge = (value[0], value[1])
        if value[0] > value[1]:
            return (value[1], value[0])
        else:
            return (value[0], value[1])


class TriadBuilder(GroupReduceFunction):
    def reduce(self, iterator, collector):
        vertices = []

        y = iterator.next()
        first_edge = [y[0], y[1]]

        vertices.append(first_edge[1])

        while iterator.has_next():
            x = iterator.next()
            second_edge = [x[0], x[1]]
            higher_vertex_id = second_edge[1]

            for lowerVertexId in vertices:
                collector.collect((first_edge[0], lowerVertexId, higher_vertex_id))
            vertices.append(higher_vertex_id)


class TriadFilter(JoinFunction):
    def join(self, value1, value2):
        return value1


if __name__ == "__main__":
    env = get_environment()
    edges = env.from_elements(
        (1, 2), (1, 3), (1, 4), (1, 5), (2, 3), (2, 5), (3, 4), (3, 7), (3, 8), (5, 6), (7, 8))

    edges_with_degrees = edges \
        .flat_map(EdgeDuplicator()) \
        .group_by(0) \
        .sort_group(1, Order.ASCENDING) \
        .reduce_group(DegreeCounter()) \
        .group_by(0, 2) \
        .reduce(DegreeJoiner())

    edges_by_degree = edges_with_degrees \
        .map(EdgeByDegreeProjector())

    edges_by_id = edges_by_degree \
        .map(EdgeByIdProjector())

    triangles = edges_by_degree \
        .group_by(0) \
        .sort_group(1, Order.ASCENDING) \
        .reduce_group(TriadBuilder()) \
        .join(edges_by_id) \
        .where(1, 2) \
        .equal_to(0, 1) \
        .using(TriadFilter())

    triangles.output()

    env.set_parallelism(1)

    env.execute(local=True)