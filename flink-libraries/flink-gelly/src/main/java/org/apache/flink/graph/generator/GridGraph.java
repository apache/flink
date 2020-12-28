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

package org.apache.flink.graph.generator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * @see <a href="http://mathworld.wolfram.com/GridGraph.html">Grid Graph at Wolfram MathWorld</a>
 */
public class GridGraph extends GraphGeneratorBase<LongValue, NullValue, NullValue> {

    // Required to create the DataSource
    private final ExecutionEnvironment env;

    // Required configuration
    private List<Tuple2<Long, Boolean>> dimensions = new ArrayList<>();

    private long vertexCount = 1;

    /**
     * An undirected {@code Graph} connecting vertices in a regular tiling in one or more dimensions
     * and where the endpoints are optionally connected.
     *
     * @param env the Flink execution environment
     */
    public GridGraph(ExecutionEnvironment env) {
        this.env = env;
    }

    /**
     * Required configuration for each dimension of the graph.
     *
     * @param size number of vertices; dimensions of size 1 are prohibited due to having no effect
     *     on the generated graph
     * @param wrapEndpoints whether to connect first and last vertices; this has no effect on
     *     dimensions of size 2
     * @return this
     */
    public GridGraph addDimension(long size, boolean wrapEndpoints) {
        Preconditions.checkArgument(size >= 2, "Dimension size must be at least 2");

        vertexCount = Math.multiplyExact(vertexCount, size);

        // prevent duplicate edges
        if (size == 2) {
            wrapEndpoints = false;
        }

        dimensions.add(new Tuple2<>(size, wrapEndpoints));

        return this;
    }

    @Override
    public Graph<LongValue, NullValue, NullValue> generate() {
        Preconditions.checkState(!dimensions.isEmpty(), "No dimensions added to GridGraph");

        // Vertices
        DataSet<Vertex<LongValue, NullValue>> vertices =
                GraphGeneratorUtils.vertexSequence(env, parallelism, vertexCount);

        // Edges
        LongValueSequenceIterator iterator = new LongValueSequenceIterator(0, this.vertexCount - 1);

        DataSet<Edge<LongValue, NullValue>> edges =
                env.fromParallelCollection(iterator, LongValue.class)
                        .setParallelism(parallelism)
                        .name("Edge iterators")
                        .flatMap(new LinkVertexToNeighbors(vertexCount, dimensions))
                        .setParallelism(parallelism)
                        .name("Grid graph edges");

        // Graph
        return Graph.fromDataSet(vertices, edges, env);
    }

    @ForwardedFields("*->f0")
    private static class LinkVertexToNeighbors
            implements FlatMapFunction<LongValue, Edge<LongValue, NullValue>> {

        private long vertexCount;

        private List<Tuple2<Long, Boolean>> dimensions;

        private LongValue target = new LongValue();

        private Edge<LongValue, NullValue> edge = new Edge<>(null, target, NullValue.getInstance());

        public LinkVertexToNeighbors(long vertexCount, List<Tuple2<Long, Boolean>> dimensions) {
            this.vertexCount = vertexCount;
            this.dimensions = dimensions;
        }

        @Override
        public void flatMap(LongValue source, Collector<Edge<LongValue, NullValue>> out)
                throws Exception {
            edge.f0 = source;
            long val = source.getValue();

            // the distance between neighbors in a given iteration
            long increment = vertexCount;

            // the value in the remaining dimensions
            long remainder = val;

            for (Tuple2<Long, Boolean> dimension : dimensions) {
                increment /= dimension.f0;

                // the index within this dimension
                long index = remainder / increment;

                if (index > 0) {
                    target.setValue(val - increment);
                    out.collect(edge);
                } else if (dimension.f1) {
                    target.setValue(val + increment * (dimension.f0 - 1));
                    out.collect(edge);
                }

                if (index < dimension.f0 - 1) {
                    target.setValue(val + increment);
                    out.collect(edge);
                } else if (dimension.f1) {
                    target.setValue(val - increment * (dimension.f0 - 1));
                    out.collect(edge);
                }

                remainder %= increment;
            }
        }
    }
}
