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

package org.apache.flink.graph.asm.degree.filter.undirected;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.undirected.VertexDegree;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingGraph;
import org.apache.flink.graph.utils.proxy.OptionalBoolean;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Removes vertices from a graph with degree greater than the given maximum. Any edge with with a
 * source or target vertex with degree greater than the given maximum is also removed.
 *
 * @param <K> ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class MaximumDegree<K, VV, EV> extends GraphAlgorithmWrappingGraph<K, VV, EV, K, VV, EV> {

    // Required configuration
    private long maximumDegree;

    // Optional configuration
    private OptionalBoolean reduceOnTargetId = new OptionalBoolean(false, false);

    private OptionalBoolean broadcastHighDegreeVertices = new OptionalBoolean(false, false);

    /**
     * Filter out vertices with degree greater than the given maximum.
     *
     * @param maximumDegree maximum degree
     */
    public MaximumDegree(long maximumDegree) {
        Preconditions.checkArgument(maximumDegree > 0, "Maximum degree must be greater than zero");

        this.maximumDegree = maximumDegree;
    }

    /**
     * The degree can be counted from either the edge source or target IDs. By default the source
     * IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is
     * sorted by target ID.
     *
     * @param reduceOnTargetId set to {@code true} if the input edge list is sorted by target ID
     * @return this
     */
    public MaximumDegree<K, VV, EV> setReduceOnTargetId(boolean reduceOnTargetId) {
        this.reduceOnTargetId.set(reduceOnTargetId);

        return this;
    }

    /**
     * After filtering high-degree vertices this algorithm must perform joins on the original
     * graph's vertex set and on both the source and target IDs of the edge set. These joins can be
     * performed without shuffling data over the network if the high-degree vertices are distributed
     * by a broadcast-hash.
     *
     * @param broadcastHighDegreeVertices set to {@code true} if the high-degree vertices should be
     *     broadcast when joining
     * @return this
     */
    public MaximumDegree<K, VV, EV> setBroadcastHighDegreeVertices(
            boolean broadcastHighDegreeVertices) {
        this.broadcastHighDegreeVertices.set(broadcastHighDegreeVertices);

        return this;
    }

    @Override
    protected boolean canMergeConfigurationWith(GraphAlgorithmWrappingBase other) {
        if (!super.canMergeConfigurationWith(other)) {
            return false;
        }

        MaximumDegree rhs = (MaximumDegree) other;

        return maximumDegree == rhs.maximumDegree;
    }

    @Override
    protected void mergeConfiguration(GraphAlgorithmWrappingBase other) {
        super.mergeConfiguration(other);

        MaximumDegree rhs = (MaximumDegree) other;

        reduceOnTargetId.mergeWith(rhs.reduceOnTargetId);
        broadcastHighDegreeVertices.mergeWith(rhs.broadcastHighDegreeVertices);
    }

    /*
     * Implementation notes:
     *
     * The three leftOuterJoin below could be implemented more efficiently
     *   as an anti-join when available in Flink.
     */

    @Override
    public Graph<K, VV, EV> runInternal(Graph<K, VV, EV> input) throws Exception {
        // u, d(u)
        DataSet<Vertex<K, LongValue>> vertexDegree =
                input.run(
                        new VertexDegree<K, VV, EV>()
                                .setReduceOnTargetId(reduceOnTargetId.get())
                                .setParallelism(parallelism));

        // u, d(u) if d(u) > maximumDegree
        DataSet<Tuple1<K>> highDegreeVertices =
                vertexDegree
                        .flatMap(new DegreeFilter<>(maximumDegree))
                        .setParallelism(parallelism)
                        .name("Filter high-degree vertices");

        JoinHint joinHint =
                broadcastHighDegreeVertices.get()
                        ? JoinHint.BROADCAST_HASH_SECOND
                        : JoinHint.REPARTITION_HASH_SECOND;

        // Vertices
        DataSet<Vertex<K, VV>> vertices =
                input.getVertices()
                        .leftOuterJoin(highDegreeVertices, joinHint)
                        .where(0)
                        .equalTo(0)
                        .with(new ProjectVertex<>())
                        .setParallelism(parallelism)
                        .name("Project low-degree vertices");

        // Edges
        DataSet<Edge<K, EV>> edges =
                input.getEdges()
                        .leftOuterJoin(highDegreeVertices, joinHint)
                        .where(reduceOnTargetId.get() ? 1 : 0)
                        .equalTo(0)
                        .with(new ProjectEdge<>())
                        .setParallelism(parallelism)
                        .name(
                                "Project low-degree edges by "
                                        + (reduceOnTargetId.get() ? "target" : "source"))
                        .leftOuterJoin(highDegreeVertices, joinHint)
                        .where(reduceOnTargetId.get() ? 0 : 1)
                        .equalTo(0)
                        .with(new ProjectEdge<>())
                        .setParallelism(parallelism)
                        .name(
                                "Project low-degree edges by "
                                        + (reduceOnTargetId.get() ? "source" : "target"));

        // Graph
        return Graph.fromDataSet(vertices, edges, input.getContext());
    }

    /**
     * Emit vertices with degree greater than the given maximum.
     *
     * @param <K> ID type
     */
    @ForwardedFields("0")
    private static class DegreeFilter<K>
            implements FlatMapFunction<Vertex<K, LongValue>, Tuple1<K>> {
        private long maximumDegree;

        private Tuple1<K> output = new Tuple1<>();

        public DegreeFilter(long maximumDegree) {
            this.maximumDegree = maximumDegree;
        }

        @Override
        public void flatMap(Vertex<K, LongValue> value, Collector<Tuple1<K>> out) throws Exception {
            if (value.f1.getValue() > maximumDegree) {
                output.f0 = value.f0;
                out.collect(output);
            }
        }
    }

    /**
     * Project vertex.
     *
     * @param <T> ID type
     * @param <VT> vertex value type
     */
    @ForwardedFieldsFirst("0; 1")
    private static class ProjectVertex<T, VT>
            implements FlatJoinFunction<Vertex<T, VT>, Tuple1<T>, Vertex<T, VT>> {
        @Override
        public void join(Vertex<T, VT> vertex, Tuple1<T> id, Collector<Vertex<T, VT>> out)
                throws Exception {
            if (id == null) {
                out.collect(vertex);
            }
        }
    }

    /**
     * Project edge.
     *
     * @param <T> ID type
     * @param <ET> edge value type
     */
    @ForwardedFieldsFirst("0; 1; 2")
    private static class ProjectEdge<T, ET>
            implements FlatJoinFunction<Edge<T, ET>, Tuple1<T>, Edge<T, ET>> {
        @Override
        public void join(Edge<T, ET> edge, Tuple1<T> id, Collector<Edge<T, ET>> out)
                throws Exception {
            if (id == null) {
                out.collect(edge);
            }
        }
    }
}
