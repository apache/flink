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

package org.apache.flink.graph.bipartite;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

/**
 * The vertices of a bipartite graph are divided into two disjoint sets, referenced by the names
 * "top" and "bottom". Top and bottom vertices with the same key value represent distinct entities
 * and must be specially handled when projecting to a simple {@link Graph}. Edges can only exist
 * between a pair of vertices from different vertices sets. E.g. there can be no vertices between a
 * pair of top vertices.
 *
 * <p>Bipartite graphs are useful to represent graphs with two sets of objects, like researchers and
 * their publications, where an edge represents that a particular publication was authored by a
 * particular author.
 *
 * <p>Bipartite interface is different from {@link Graph} interface, so to apply algorithms that
 * work on a regular graph a bipartite graph should be first converted into a {@link Graph}
 * instance. This can be achieved by using the projection methods.
 *
 * @param <KT> the key type of top vertices
 * @param <KB> the key type of bottom vertices
 * @param <VVT> the vertex value type of top vertices
 * @param <VVB> the vertex value type of bottom vertices
 * @param <EV> the edge value type
 */
public class BipartiteGraph<KT, KB, VVT, VVB, EV> {

    private final ExecutionEnvironment context;
    private final DataSet<Vertex<KT, VVT>> topVertices;
    private final DataSet<Vertex<KB, VVB>> bottomVertices;
    private final DataSet<BipartiteEdge<KT, KB, EV>> edges;

    private BipartiteGraph(
            DataSet<Vertex<KT, VVT>> topVertices,
            DataSet<Vertex<KB, VVB>> bottomVertices,
            DataSet<BipartiteEdge<KT, KB, EV>> edges,
            ExecutionEnvironment context) {
        this.topVertices = topVertices;
        this.bottomVertices = bottomVertices;
        this.edges = edges;
        this.context = context;
    }

    /**
     * Create bipartite graph from datasets.
     *
     * @param topVertices dataset of top vertices in the graph
     * @param bottomVertices dataset of bottom vertices in the graph
     * @param edges dataset of edges between vertices
     * @param context Flink execution context
     * @return new bipartite graph created from provided datasets
     */
    public static <KT, KB, VVT, VVB, EV> BipartiteGraph<KT, KB, VVT, VVB, EV> fromDataSet(
            DataSet<Vertex<KT, VVT>> topVertices,
            DataSet<Vertex<KB, VVB>> bottomVertices,
            DataSet<BipartiteEdge<KT, KB, EV>> edges,
            ExecutionEnvironment context) {
        return new BipartiteGraph<>(topVertices, bottomVertices, edges, context);
    }

    /**
     * Get dataset with top vertices.
     *
     * @return dataset with top vertices
     */
    public DataSet<Vertex<KT, VVT>> getTopVertices() {
        return topVertices;
    }

    /**
     * Get dataset with bottom vertices.
     *
     * @return dataset with bottom vertices
     */
    public DataSet<Vertex<KB, VVB>> getBottomVertices() {
        return bottomVertices;
    }

    /**
     * Get dataset with graph edges.
     *
     * @return dataset with graph edges
     */
    public DataSet<BipartiteEdge<KT, KB, EV>> getEdges() {
        return edges;
    }

    /**
     * Convert a bipartite graph into an undirected graph that contains only top vertices. An edge
     * between two vertices in the new graph will exist only if the original bipartite graph
     * contains a bottom vertex they are both connected to.
     *
     * <p>The simple projection performs a single join and returns edges containing the bipartite
     * edge values.
     *
     * <p>Note: KT must override .equals(). This requirement may be removed in a future release.
     *
     * @return simple top projection of the bipartite graph
     */
    public Graph<KT, VVT, Tuple2<EV, EV>> projectionTopSimple() {
        DataSet<Edge<KT, Tuple2<EV, EV>>> newEdges =
                edges.join(edges)
                        .where(1)
                        .equalTo(1)
                        .with(new ProjectionTopSimple<>())
                        .name("Simple top projection");

        return Graph.fromDataSet(topVertices, newEdges, context);
    }

    @ForwardedFieldsFirst("0; 2->2.0")
    @ForwardedFieldsSecond("0->1; 2->2.1")
    private static class ProjectionTopSimple<KT, KB, EV>
            implements FlatJoinFunction<
                    BipartiteEdge<KT, KB, EV>,
                    BipartiteEdge<KT, KB, EV>,
                    Edge<KT, Tuple2<EV, EV>>> {
        private Tuple2<EV, EV> edgeValues = new Tuple2<>();

        private Edge<KT, Tuple2<EV, EV>> edge = new Edge<>(null, null, edgeValues);

        @Override
        public void join(
                BipartiteEdge<KT, KB, EV> first,
                BipartiteEdge<KT, KB, EV> second,
                Collector<Edge<KT, Tuple2<EV, EV>>> out)
                throws Exception {
            if (!first.f0.equals(second.f0)) {
                edge.f0 = first.f0;
                edge.f1 = second.f0;

                edgeValues.f0 = first.f2;
                edgeValues.f1 = second.f2;

                out.collect(edge);
            }
        }
    }

    /**
     * Convert a bipartite graph into an undirected graph that contains only bottom vertices. An
     * edge between two vertices in the new graph will exist only if the original bipartite graph
     * contains a top vertex they are both connected to.
     *
     * <p>The simple projection performs a single join and returns edges containing the bipartite
     * edge values.
     *
     * <p>Note: KB must override .equals(). This requirement may be removed in a future release.
     *
     * @return simple bottom projection of the bipartite graph
     */
    public Graph<KB, VVB, Tuple2<EV, EV>> projectionBottomSimple() {
        DataSet<Edge<KB, Tuple2<EV, EV>>> newEdges =
                edges.join(edges)
                        .where(0)
                        .equalTo(0)
                        .with(new ProjectionBottomSimple<>())
                        .name("Simple bottom projection");

        return Graph.fromDataSet(bottomVertices, newEdges, context);
    }

    @ForwardedFieldsFirst("1->0; 2->2.0")
    @ForwardedFieldsSecond("1; 2->2.1")
    private static class ProjectionBottomSimple<KT, KB, EV>
            implements FlatJoinFunction<
                    BipartiteEdge<KT, KB, EV>,
                    BipartiteEdge<KT, KB, EV>,
                    Edge<KB, Tuple2<EV, EV>>> {
        private Tuple2<EV, EV> edgeValues = new Tuple2<>();

        private Edge<KB, Tuple2<EV, EV>> edge = new Edge<>(null, null, edgeValues);

        @Override
        public void join(
                BipartiteEdge<KT, KB, EV> first,
                BipartiteEdge<KT, KB, EV> second,
                Collector<Edge<KB, Tuple2<EV, EV>>> out)
                throws Exception {
            if (!first.f1.equals(second.f1)) {
                edge.f0 = first.f1;
                edge.f1 = second.f1;

                edgeValues.f0 = first.f2;
                edgeValues.f1 = second.f2;

                out.collect(edge);
            }
        }
    }

    /**
     * Convert a bipartite graph into a graph that contains only top vertices. An edge between two
     * vertices in the new graph will exist only if the original bipartite graph contains at least
     * one bottom vertex they both connect to.
     *
     * <p>The full projection performs three joins and returns edges containing the connecting
     * vertex ID and value, both top vertex values, and both bipartite edge values.
     *
     * <p>Note: KT must override .equals(). This requirement may be removed in a future release.
     *
     * @return full top projection of the bipartite graph
     */
    public Graph<KT, VVT, Projection<KB, VVB, VVT, EV>> projectionTopFull() {
        DataSet<Tuple5<KT, KB, EV, VVT, VVB>> edgesWithVertices = joinEdgeWithVertices();

        DataSet<Edge<KT, Projection<KB, VVB, VVT, EV>>> newEdges =
                edgesWithVertices
                        .join(edgesWithVertices)
                        .where(1)
                        .equalTo(1)
                        .with(new ProjectionTopFull<>())
                        .name("Full top projection");

        return Graph.fromDataSet(topVertices, newEdges, context);
    }

    private DataSet<Tuple5<KT, KB, EV, VVT, VVB>> joinEdgeWithVertices() {
        return edges.join(topVertices, JoinHint.REPARTITION_HASH_SECOND)
                .where(0)
                .equalTo(0)
                .projectFirst(0, 1, 2)
                .<Tuple4<KT, KB, EV, VVT>>projectSecond(1)
                .name("Edge with vertex")
                .join(bottomVertices, JoinHint.REPARTITION_HASH_SECOND)
                .where(1)
                .equalTo(0)
                .projectFirst(0, 1, 2, 3)
                .<Tuple5<KT, KB, EV, VVT, VVB>>projectSecond(1)
                .name("Edge with vertices");
    }

    @ForwardedFieldsFirst("0; 1->2.0; 2->2.4; 3->2.2; 4->2.1")
    @ForwardedFieldsSecond("0->1; 2->2.5; 3->2.3")
    private static class ProjectionTopFull<KT, KB, EV, VVT, VVB>
            implements FlatJoinFunction<
                    Tuple5<KT, KB, EV, VVT, VVB>,
                    Tuple5<KT, KB, EV, VVT, VVB>,
                    Edge<KT, Projection<KB, VVB, VVT, EV>>> {
        private Projection<KB, VVB, VVT, EV> projection = new Projection<>();

        private Edge<KT, Projection<KB, VVB, VVT, EV>> edge = new Edge<>(null, null, projection);

        @Override
        public void join(
                Tuple5<KT, KB, EV, VVT, VVB> first,
                Tuple5<KT, KB, EV, VVT, VVB> second,
                Collector<Edge<KT, Projection<KB, VVB, VVT, EV>>> out)
                throws Exception {
            if (!first.f0.equals(second.f0)) {
                edge.f0 = first.f0;
                edge.f1 = second.f0;

                projection.f0 = first.f1;
                projection.f1 = first.f4;
                projection.f2 = first.f3;
                projection.f3 = second.f3;
                projection.f4 = first.f2;
                projection.f5 = second.f2;

                out.collect(edge);
            }
        }
    }

    /**
     * Convert a bipartite graph into a graph that contains only bottom vertices. An edge between
     * two vertices in the new graph will exist only if the original bipartite graph contains at
     * least one top vertex they both connect to.
     *
     * <p>The full projection performs three joins and returns edges containing the connecting
     * vertex ID and value, both bottom vertex values, and both bipartite edge values.
     *
     * <p>Note: KB must override .equals(). This requirement may be removed in a future release.
     *
     * @return full bottom projection of the bipartite graph
     */
    public Graph<KB, VVB, Projection<KT, VVT, VVB, EV>> projectionBottomFull() {
        DataSet<Tuple5<KT, KB, EV, VVT, VVB>> edgesWithVertices = joinEdgeWithVertices();

        DataSet<Edge<KB, Projection<KT, VVT, VVB, EV>>> newEdges =
                edgesWithVertices
                        .join(edgesWithVertices)
                        .where(0)
                        .equalTo(0)
                        .with(new ProjectionBottomFull<>())
                        .name("Full bottom projection");

        return Graph.fromDataSet(bottomVertices, newEdges, context);
    }

    @ForwardedFieldsFirst("1->0; 2->2.4; 3->2.1; 4->2.2")
    @ForwardedFieldsSecond("1; 2->2.5; 4->2.3")
    private static class ProjectionBottomFull<KT, KB, EV, VVT, VVB>
            implements FlatJoinFunction<
                    Tuple5<KT, KB, EV, VVT, VVB>,
                    Tuple5<KT, KB, EV, VVT, VVB>,
                    Edge<KB, Projection<KT, VVT, VVB, EV>>> {
        private Projection<KT, VVT, VVB, EV> projection = new Projection<>();

        private Edge<KB, Projection<KT, VVT, VVB, EV>> edge = new Edge<>(null, null, projection);

        @Override
        public void join(
                Tuple5<KT, KB, EV, VVT, VVB> first,
                Tuple5<KT, KB, EV, VVT, VVB> second,
                Collector<Edge<KB, Projection<KT, VVT, VVB, EV>>> out)
                throws Exception {
            if (!first.f1.equals(second.f1)) {
                edge.f0 = first.f1;
                edge.f1 = second.f1;

                projection.f0 = first.f0;
                projection.f1 = first.f3;
                projection.f2 = first.f4;
                projection.f3 = second.f4;
                projection.f4 = first.f2;
                projection.f5 = second.f2;

                out.collect(edge);
            }
        }
    }
}
