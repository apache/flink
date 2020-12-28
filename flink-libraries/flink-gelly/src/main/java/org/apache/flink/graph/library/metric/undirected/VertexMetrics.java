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

package org.apache.flink.graph.library.metric.undirected;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.AnalyticHelper;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalyticBase;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.undirected.VertexDegree;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.library.metric.undirected.VertexMetrics.Result;
import org.apache.flink.types.LongValue;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;
import java.text.NumberFormat;

/**
 * Compute the following vertex metrics in an undirected graph. - number of vertices - number of
 * edges - average degree - number of triplets - maximum degree - maximum number of triplets
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class VertexMetrics<K extends Comparable<K>, VV, EV>
        extends GraphAnalyticBase<K, VV, EV, Result> {

    private static final String VERTEX_COUNT = "vertexCount";

    private static final String EDGE_COUNT = "edgeCount";

    private static final String TRIPLET_COUNT = "tripletCount";

    private static final String MAXIMUM_DEGREE = "maximumDegree";

    private static final String MAXIMUM_TRIPLETS = "maximumTriplets";

    private VertexMetricsHelper<K> vertexMetricsHelper;

    // Optional configuration
    private boolean includeZeroDegreeVertices = false;

    private boolean reduceOnTargetId = false;

    /**
     * By default only the edge set is processed for the computation of degree. When this flag is
     * set an additional join is performed against the vertex set in order to output vertices with a
     * degree of zero.
     *
     * @param includeZeroDegreeVertices whether to output vertices with a degree of zero
     * @return this
     */
    public VertexMetrics<K, VV, EV> setIncludeZeroDegreeVertices(
            boolean includeZeroDegreeVertices) {
        this.includeZeroDegreeVertices = includeZeroDegreeVertices;

        return this;
    }

    /**
     * The degree can be counted from either the edge source or target IDs. By default the source
     * IDs are counted. Reducing on target IDs may optimize the algorithm if the input edge list is
     * sorted by target ID.
     *
     * @param reduceOnTargetId set to {@code true} if the input edge list is sorted by target ID
     * @return this
     */
    public VertexMetrics<K, VV, EV> setReduceOnTargetId(boolean reduceOnTargetId) {
        this.reduceOnTargetId = reduceOnTargetId;

        return this;
    }

    @Override
    public VertexMetrics<K, VV, EV> run(Graph<K, VV, EV> input) throws Exception {
        super.run(input);

        DataSet<Vertex<K, LongValue>> vertexDegree =
                input.run(
                        new VertexDegree<K, VV, EV>()
                                .setIncludeZeroDegreeVertices(includeZeroDegreeVertices)
                                .setReduceOnTargetId(reduceOnTargetId)
                                .setParallelism(parallelism));

        vertexMetricsHelper = new VertexMetricsHelper<>();

        vertexDegree.output(vertexMetricsHelper).name("Vertex metrics");

        return this;
    }

    @Override
    public Result getResult() {
        long vertexCount = vertexMetricsHelper.getAccumulator(env, VERTEX_COUNT);
        long edgeCount = vertexMetricsHelper.getAccumulator(env, EDGE_COUNT);
        long tripletCount = vertexMetricsHelper.getAccumulator(env, TRIPLET_COUNT);
        long maximumDegree = vertexMetricsHelper.getAccumulator(env, MAXIMUM_DEGREE);
        long maximumTriplets = vertexMetricsHelper.getAccumulator(env, MAXIMUM_TRIPLETS);

        // each edge is counted twice, once from each vertex, so must be halved
        return new Result(vertexCount, edgeCount / 2, tripletCount, maximumDegree, maximumTriplets);
    }

    /**
     * Helper class to collect vertex metrics.
     *
     * @param <T> ID type
     */
    private static class VertexMetricsHelper<T> extends AnalyticHelper<Vertex<T, LongValue>> {
        private long vertexCount;
        private long edgeCount;
        private long tripletCount;
        private long maximumDegree;
        private long maximumTriplets;

        @Override
        public void writeRecord(Vertex<T, LongValue> record) throws IOException {
            long degree = record.f1.getValue();
            long triplets = degree * (degree - 1) / 2;

            vertexCount++;
            edgeCount += degree;
            tripletCount += triplets;
            maximumDegree = Math.max(maximumDegree, degree);
            maximumTriplets = Math.max(maximumTriplets, triplets);
        }

        @Override
        public void close() throws IOException {
            addAccumulator(VERTEX_COUNT, new LongCounter(vertexCount));
            addAccumulator(EDGE_COUNT, new LongCounter(edgeCount));
            addAccumulator(TRIPLET_COUNT, new LongCounter(tripletCount));
            addAccumulator(MAXIMUM_DEGREE, new LongMaximum(maximumDegree));
            addAccumulator(MAXIMUM_TRIPLETS, new LongMaximum(maximumTriplets));
        }
    }

    /** Wraps vertex metrics. */
    public static class Result implements PrintableResult {
        private long vertexCount;
        private long edgeCount;
        private long tripletCount;
        private long maximumDegree;
        private long maximumTriplets;

        public Result(
                long vertexCount,
                long edgeCount,
                long tripletCount,
                long maximumDegree,
                long maximumTriplets) {
            this.vertexCount = vertexCount;
            this.edgeCount = edgeCount;
            this.tripletCount = tripletCount;
            this.maximumDegree = maximumDegree;
            this.maximumTriplets = maximumTriplets;
        }

        /**
         * Get the number of vertices.
         *
         * @return number of vertices
         */
        public long getNumberOfVertices() {
            return vertexCount;
        }

        /**
         * Get the number of edges. Each edge is counted once even though Gelly stores undirected
         * edges twice, once in each direction.
         *
         * @return number of edges
         */
        public long getNumberOfEdges() {
            return edgeCount;
        }

        /**
         * Get the average degree, the average number of edges per vertex.
         *
         * <p>A result of {@code Float.NaN} is returned for an empty graph for which both the number
         * of edges and number of vertices is zero.
         *
         * @return average degree
         */
        public double getAverageDegree() {
            // each edge is incident on two vertices
            return vertexCount == 0 ? Double.NaN : 2 * edgeCount / (double) vertexCount;
        }

        /**
         * Get the density, the ratio of actual to potential edges between vertices.
         *
         * <p>A result of {@code Float.NaN} is returned for a graph with fewer than two vertices for
         * which the number of edges is zero.
         *
         * @return density
         */
        public double getDensity() {
            return vertexCount <= 1
                    ? Double.NaN
                    : edgeCount / (double) (vertexCount * (vertexCount - 1) / 2);
        }

        /**
         * Get the number of triplets.
         *
         * @return number of triplets
         */
        public long getNumberOfTriplets() {
            return tripletCount;
        }

        /**
         * Get the maximum degree.
         *
         * @return maximum degree
         */
        public long getMaximumDegree() {
            return maximumDegree;
        }

        /**
         * Get the maximum triplets.
         *
         * @return maximum triplets
         */
        public long getMaximumTriplets() {
            return maximumTriplets;
        }

        @Override
        public String toString() {
            return toPrintableString();
        }

        @Override
        public String toPrintableString() {
            NumberFormat nf = NumberFormat.getInstance();

            // format for very small fractional numbers
            NumberFormat ff = NumberFormat.getInstance();
            ff.setMaximumFractionDigits(8);

            return "vertex count: "
                    + nf.format(vertexCount)
                    + "; edge count: "
                    + nf.format(edgeCount)
                    + "; average degree: "
                    + nf.format(getAverageDegree())
                    + "; density: "
                    + ff.format(getDensity())
                    + "; triplet count: "
                    + nf.format(tripletCount)
                    + "; maximum degree: "
                    + nf.format(maximumDegree)
                    + "; maximum triplets: "
                    + nf.format(maximumTriplets);
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder()
                    .append(vertexCount)
                    .append(edgeCount)
                    .append(tripletCount)
                    .append(maximumDegree)
                    .append(maximumTriplets)
                    .hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (obj == this) {
                return true;
            }

            if (obj.getClass() != getClass()) {
                return false;
            }

            Result rhs = (Result) obj;

            return new EqualsBuilder()
                    .append(vertexCount, rhs.vertexCount)
                    .append(edgeCount, rhs.edgeCount)
                    .append(tripletCount, rhs.tripletCount)
                    .append(maximumDegree, rhs.maximumDegree)
                    .append(maximumTriplets, rhs.maximumTriplets)
                    .isEquals();
        }
    }
}
