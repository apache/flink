/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.library.clustering.undirected;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.degree.annotate.undirected.EdgeDegreePair;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.asm.result.TertiaryResultBase;
import org.apache.flink.graph.library.clustering.TriangleListingBase;
import org.apache.flink.graph.library.clustering.undirected.TriangleListing.Result;
import org.apache.flink.graph.utils.MurmurHash;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Generates a listing of distinct triangles from the input graph.
 *
 * <p>A triangle is a 3-cycle with vertices A, B, and C connected by edges (A, B), (A, C), and (B,
 * C).
 *
 * <p>The input graph must be a simple, undirected graph containing no duplicate edges or
 * self-loops.
 *
 * <p>Algorithm from "Finding, Counting and Listing all Triangles in Large Graphs, An Experimental
 * Study", Thomas Schank and Dorothea Wagner.
 *
 * <p>See http://i11www.iti.uni-karlsruhe.de/extra/publications/sw-fclt-05_t.pdf
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class TriangleListing<K extends Comparable<K> & CopyableValue<K>, VV, EV>
        extends TriangleListingBase<K, VV, EV, Result<K>> {

    /*
     * Implementation notes:
     *
     * The requirement that "K extends CopyableValue<K>" can be removed when
     *   Flink has a self-join and GenerateTriplets is implemented as such.
     *
     * ProjectTriangles should eventually be replaced by ".projectFirst("*")"
     *   when projections use code generation.
     */

    @Override
    public DataSet<Result<K>> runInternal(Graph<K, VV, EV> input) throws Exception {
        // u, v where u < v
        DataSet<Tuple2<K, K>> filteredByID =
                input.getEdges()
                        .flatMap(new FilterByID<>())
                        .setParallelism(parallelism)
                        .name("Filter by ID");

        // u, v, (edge value, deg(u), deg(v))
        DataSet<Edge<K, Tuple3<EV, LongValue, LongValue>>> pairDegree =
                input.run(new EdgeDegreePair<K, VV, EV>().setParallelism(parallelism));

        // u, v where deg(u) < deg(v) or (deg(u) == deg(v) and u < v)
        DataSet<Tuple2<K, K>> filteredByDegree =
                pairDegree
                        .flatMap(new FilterByDegree<>())
                        .setParallelism(parallelism)
                        .name("Filter by degree");

        // u, v, w where (u, v) and (u, w) are edges in graph, v < w
        DataSet<Tuple3<K, K, K>> triplets =
                filteredByDegree
                        .groupBy(0)
                        .sortGroup(1, Order.ASCENDING)
                        .reduceGroup(new GenerateTriplets<>())
                        .name("Generate triplets");

        // u, v, w where (u, v), (u, w), and (v, w) are edges in graph, v < w
        DataSet<Result<K>> triangles =
                triplets.join(filteredByID, JoinOperatorBase.JoinHint.REPARTITION_HASH_SECOND)
                        .where(1, 2)
                        .equalTo(0, 1)
                        .with(new ProjectTriangles<>())
                        .name("Triangle listing");

        if (permuteResults) {
            triangles = triangles.flatMap(new PermuteResult<>()).name("Permute triangle vertices");
        } else if (sortTriangleVertices.get()) {
            triangles = triangles.map(new SortTriangleVertices<>()).name("Sort triangle vertices");
        }

        return triangles;
    }

    /**
     * Removes edge values while filtering such that only edges where the source vertex ID compares
     * less than the target vertex ID are emitted.
     *
     * <p>Since the input graph is a simple graph this filter removes exactly half of the original
     * edges.
     *
     * @param <T> ID type
     * @param <ET> edge value type
     */
    @ForwardedFields("0; 1")
    private static final class FilterByID<T extends Comparable<T>, ET>
            implements FlatMapFunction<Edge<T, ET>, Tuple2<T, T>> {
        private Tuple2<T, T> edge = new Tuple2<>();

        @Override
        public void flatMap(Edge<T, ET> value, Collector<Tuple2<T, T>> out) throws Exception {
            if (value.f0.compareTo(value.f1) < 0) {
                edge.f0 = value.f0;
                edge.f1 = value.f1;
                out.collect(edge);
            }
        }
    }

    /**
     * Removes edge values while filtering such that edges where the source vertex has lower degree
     * are emitted. If the source and target vertex degrees are equal then the edge is emitted if
     * the source vertex ID compares less than the target vertex ID.
     *
     * <p>Since the input graph is a simple graph this filter removes exactly half of the original
     * edges.
     *
     * @param <T> ID type
     */
    @ForwardedFields("0; 1")
    private static final class FilterByDegree<T extends Comparable<T>, ET>
            implements FlatMapFunction<Edge<T, Tuple3<ET, LongValue, LongValue>>, Tuple2<T, T>> {
        private Tuple2<T, T> edge = new Tuple2<>();

        @Override
        public void flatMap(
                Edge<T, Tuple3<ET, LongValue, LongValue>> value, Collector<Tuple2<T, T>> out)
                throws Exception {
            Tuple3<ET, LongValue, LongValue> degrees = value.f2;
            long sourceDegree = degrees.f1.getValue();
            long targetDegree = degrees.f2.getValue();

            if (sourceDegree < targetDegree
                    || (sourceDegree == targetDegree && value.f0.compareTo(value.f1) < 0)) {
                edge.f0 = value.f0;
                edge.f1 = value.f1;
                out.collect(edge);
            }
        }
    }

    /**
     * Generates the set of triplets by the pairwise enumeration of the open neighborhood for each
     * vertex. The number of triplets is quadratic in the vertex degree; however, data skew is
     * minimized by only generating triplets from the vertex with least degree.
     *
     * @param <T> ID type
     */
    @ForwardedFields("0")
    private static final class GenerateTriplets<T extends CopyableValue<T>>
            implements GroupReduceFunction<Tuple2<T, T>, Tuple3<T, T, T>> {
        private Tuple3<T, T, T> output = new Tuple3<>();

        private List<T> visited = new ArrayList<>();

        @Override
        public void reduce(Iterable<Tuple2<T, T>> values, Collector<Tuple3<T, T, T>> out)
                throws Exception {
            int visitedCount = 0;

            Iterator<Tuple2<T, T>> iter = values.iterator();

            while (true) {
                Tuple2<T, T> edge = iter.next();

                output.f0 = edge.f0;
                output.f2 = edge.f1;

                for (int i = 0; i < visitedCount; i++) {
                    output.f1 = visited.get(i);
                    out.collect(output);
                }

                if (!iter.hasNext()) {
                    break;
                }

                if (visitedCount == visited.size()) {
                    visited.add(edge.f1.copy());
                } else {
                    edge.f1.copyTo(visited.get(visitedCount));
                }

                visitedCount += 1;
            }
        }
    }

    /**
     * Simply project the triplet as a triangle.
     *
     * @param <T> ID type
     */
    @ForwardedFieldsFirst("0->vertexId0; 1->vertexId1; 2->vertexId2")
    @ForwardedFieldsSecond("0->vertexId0; 1->vertexId1")
    private static final class ProjectTriangles<T>
            implements JoinFunction<Tuple3<T, T, T>, Tuple2<T, T>, Result<T>> {
        private Result<T> output = new Result<>();

        @Override
        public Result<T> join(Tuple3<T, T, T> triplet, Tuple2<T, T> edge) throws Exception {
            output.setVertexId0(triplet.f0);
            output.setVertexId1(triplet.f1);
            output.setVertexId2(triplet.f2);
            return output;
        }
    }

    /**
     * Output each input and an additional result for each of the five permutations of the three
     * vertex IDs.
     *
     * @param <T> ID type
     */
    private static class PermuteResult<T> implements FlatMapFunction<Result<T>, Result<T>> {
        @Override
        public void flatMap(Result<T> value, Collector<Result<T>> out) throws Exception {
            T tmp;

            // 0, 1, 2
            out.collect(value);

            tmp = value.getVertexId0();
            value.setVertexId0(value.getVertexId1());
            value.setVertexId1(tmp);

            // 1, 0, 2
            out.collect(value);

            tmp = value.getVertexId1();
            value.setVertexId1(value.getVertexId2());
            value.setVertexId2(tmp);

            // 1, 2, 0
            out.collect(value);

            tmp = value.getVertexId0();
            value.setVertexId0(value.getVertexId2());
            value.setVertexId2(tmp);

            // 0, 2, 1
            out.collect(value);

            tmp = value.getVertexId0();
            value.setVertexId0(value.getVertexId1());
            value.setVertexId1(tmp);

            // 2, 0, 1
            out.collect(value);

            tmp = value.getVertexId1();
            value.setVertexId1(value.getVertexId2());
            value.setVertexId2(tmp);

            // 2, 1, 0
            out.collect(value);
        }
    }

    /**
     * Reorders the vertices of each emitted triangle (K0, K1, K2) into sorted order such that K0 <
     * K1 < K2.
     *
     * @param <T> ID type
     */
    private static final class SortTriangleVertices<T extends Comparable<T>>
            implements MapFunction<Result<T>, Result<T>> {
        @Override
        public Result<T> map(Result<T> value) throws Exception {
            // by the triangle listing algorithm we know f1 < f2
            if (value.getVertexId0().compareTo(value.getVertexId1()) > 0) {
                T tempVal = value.getVertexId0();
                value.setVertexId0(value.getVertexId1());

                if (tempVal.compareTo(value.getVertexId2()) <= 0) {
                    value.setVertexId1(tempVal);
                } else {
                    value.setVertexId1(value.getVertexId2());
                    value.setVertexId2(tempVal);
                }
            }

            return value;
        }
    }

    /**
     * A result for the undirected Triangle Listing algorithm.
     *
     * @param <T> ID type
     */
    public static class Result<T> extends TertiaryResultBase<T> implements PrintableResult {
        @Override
        public String toString() {
            return "(" + getVertexId0() + "," + getVertexId1() + "," + getVertexId2() + ")";
        }

        @Override
        public String toPrintableString() {
            return "1st vertex ID: "
                    + getVertexId0()
                    + ", 2nd vertex ID: "
                    + getVertexId1()
                    + ", 3rd vertex ID: "
                    + getVertexId2();
        }

        // ----------------------------------------------------------------------------------------

        public static final int HASH_SEED = 0xd39fd1ab;

        private transient MurmurHash hasher;

        @Override
        public int hashCode() {
            if (hasher == null) {
                hasher = new MurmurHash(HASH_SEED);
            }

            return hasher.reset()
                    .hash(getVertexId0().hashCode())
                    .hash(getVertexId1().hashCode())
                    .hash(getVertexId2().hashCode())
                    .hash();
        }
    }
}
