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

package org.apache.flink.graph.library.metric.directed;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.LongMaximum;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.AnalyticHelper;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalyticBase;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees.Degrees;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.library.metric.directed.VertexMetrics.Result;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;
import java.text.NumberFormat;

/**
 * Compute the following vertex metrics in a directed graph. - number of vertices - number of edges
 * - number of unidirectional edges - number of bidirectional edges - average degree - number of
 * triplets - maximum degree - maximum out degree - maximum in degree - maximum number of triplets
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class VertexMetrics<K extends Comparable<K>, VV, EV>
        extends GraphAnalyticBase<K, VV, EV, Result> {

    private static final String VERTEX_COUNT = "vertexCount";

    private static final String UNIDIRECTIONAL_EDGE_COUNT = "unidirectionalEdgeCount";

    private static final String BIDIRECTIONAL_EDGE_COUNT = "bidirectionalEdgeCount";

    private static final String TRIPLET_COUNT = "tripletCount";

    private static final String MAXIMUM_DEGREE = "maximumDegree";

    private static final String MAXIMUM_OUT_DEGREE = "maximumOutDegree";

    private static final String MAXIMUM_IN_DEGREE = "maximumInDegree";

    private static final String MAXIMUM_TRIPLETS = "maximumTriplets";

    private VertexMetricsHelper<K> vertexMetricsHelper;

    // Optional configuration
    private boolean includeZeroDegreeVertices = false;

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

    @Override
    public VertexMetrics<K, VV, EV> run(Graph<K, VV, EV> input) throws Exception {
        super.run(input);

        DataSet<Vertex<K, Degrees>> vertexDegree =
                input.run(
                        new VertexDegrees<K, VV, EV>()
                                .setIncludeZeroDegreeVertices(includeZeroDegreeVertices)
                                .setParallelism(parallelism));

        vertexMetricsHelper = new VertexMetricsHelper<>();

        vertexDegree.output(vertexMetricsHelper).name("Vertex metrics");

        return this;
    }

    @Override
    public Result getResult() {
        long vertexCount = vertexMetricsHelper.getAccumulator(env, VERTEX_COUNT);
        long unidirectionalEdgeCount =
                vertexMetricsHelper.getAccumulator(env, UNIDIRECTIONAL_EDGE_COUNT);
        long bidirectionalEdgeCount =
                vertexMetricsHelper.getAccumulator(env, BIDIRECTIONAL_EDGE_COUNT);
        long tripletCount = vertexMetricsHelper.getAccumulator(env, TRIPLET_COUNT);
        long maximumDegree = vertexMetricsHelper.getAccumulator(env, MAXIMUM_DEGREE);
        long maximumOutDegree = vertexMetricsHelper.getAccumulator(env, MAXIMUM_OUT_DEGREE);
        long maximumInDegree = vertexMetricsHelper.getAccumulator(env, MAXIMUM_IN_DEGREE);
        long maximumTriplets = vertexMetricsHelper.getAccumulator(env, MAXIMUM_TRIPLETS);

        // each edge is counted twice, once from each vertex, so must be halved
        return new Result(
                vertexCount,
                unidirectionalEdgeCount / 2,
                bidirectionalEdgeCount / 2,
                tripletCount,
                maximumDegree,
                maximumOutDegree,
                maximumInDegree,
                maximumTriplets);
    }

    /**
     * Helper class to collect vertex metrics.
     *
     * @param <T> ID type
     */
    private static class VertexMetricsHelper<T> extends AnalyticHelper<Vertex<T, Degrees>> {
        private long vertexCount;
        private long unidirectionalEdgeCount;
        private long bidirectionalEdgeCount;
        private long tripletCount;
        private long maximumDegree;
        private long maximumOutDegree;
        private long maximumInDegree;
        private long maximumTriplets;

        @Override
        public void writeRecord(Vertex<T, Degrees> record) throws IOException {
            long degree = record.f1.getDegree().getValue();
            long outDegree = record.f1.getOutDegree().getValue();
            long inDegree = record.f1.getInDegree().getValue();

            long bidirectionalEdges = outDegree + inDegree - degree;
            long triplets = degree * (degree - 1) / 2;

            vertexCount++;
            unidirectionalEdgeCount += degree - bidirectionalEdges;
            bidirectionalEdgeCount += bidirectionalEdges;
            tripletCount += triplets;
            maximumDegree = Math.max(maximumDegree, degree);
            maximumOutDegree = Math.max(maximumOutDegree, outDegree);
            maximumInDegree = Math.max(maximumInDegree, inDegree);
            maximumTriplets = Math.max(maximumTriplets, triplets);
        }

        @Override
        public void close() throws IOException {
            addAccumulator(VERTEX_COUNT, new LongCounter(vertexCount));
            addAccumulator(UNIDIRECTIONAL_EDGE_COUNT, new LongCounter(unidirectionalEdgeCount));
            addAccumulator(BIDIRECTIONAL_EDGE_COUNT, new LongCounter(bidirectionalEdgeCount));
            addAccumulator(TRIPLET_COUNT, new LongCounter(tripletCount));
            addAccumulator(MAXIMUM_DEGREE, new LongMaximum(maximumDegree));
            addAccumulator(MAXIMUM_OUT_DEGREE, new LongMaximum(maximumOutDegree));
            addAccumulator(MAXIMUM_IN_DEGREE, new LongMaximum(maximumInDegree));
            addAccumulator(MAXIMUM_TRIPLETS, new LongMaximum(maximumTriplets));
        }
    }

    /** Wraps vertex metrics. */
    public static class Result implements PrintableResult {
        private long vertexCount;
        private long unidirectionalEdgeCount;
        private long bidirectionalEdgeCount;
        private long tripletCount;
        private long maximumDegree;
        private long maximumOutDegree;
        private long maximumInDegree;
        private long maximumTriplets;

        public Result(
                long vertexCount,
                long unidirectionalEdgeCount,
                long bidirectionalEdgeCount,
                long tripletCount,
                long maximumDegree,
                long maximumOutDegree,
                long maximumInDegree,
                long maximumTriplets) {
            this.vertexCount = vertexCount;
            this.unidirectionalEdgeCount = unidirectionalEdgeCount;
            this.bidirectionalEdgeCount = bidirectionalEdgeCount;
            this.tripletCount = tripletCount;
            this.maximumDegree = maximumDegree;
            this.maximumOutDegree = maximumOutDegree;
            this.maximumInDegree = maximumInDegree;
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
         * Get the number of edges.
         *
         * @return number of edges
         */
        public long getNumberOfEdges() {
            return unidirectionalEdgeCount + 2 * bidirectionalEdgeCount;
        }

        /**
         * Get the number of unidirectional edges.
         *
         * @return number of unidirectional edges
         */
        public long getNumberOfDirectedEdges() {
            return unidirectionalEdgeCount;
        }

        /**
         * Get the number of bidirectional edges.
         *
         * @return number of bidirectional edges
         */
        public long getNumberOfUndirectedEdges() {
            return bidirectionalEdgeCount;
        }

        /**
         * Get the average degree, the average number of in- plus out-edges per vertex.
         *
         * <p>A result of {@code Float.NaN} is returned for an empty graph for which both the number
         * of edges and number of vertices is zero.
         *
         * @return average degree
         */
        public double getAverageDegree() {
            return vertexCount == 0 ? Double.NaN : getNumberOfEdges() / (double) vertexCount;
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
                    : getNumberOfEdges() / (double) (vertexCount * (vertexCount - 1));
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
         * Get the maximum out degree.
         *
         * @return maximum out degree
         */
        public long getMaximumOutDegree() {
            return maximumOutDegree;
        }

        /**
         * Get the maximum in degree.
         *
         * @return maximum in degree
         */
        public long getMaximumInDegree() {
            return maximumInDegree;
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
                    + nf.format(getNumberOfEdges())
                    + "; unidirectional edge count: "
                    + nf.format(unidirectionalEdgeCount)
                    + "; bidirectional edge count: "
                    + nf.format(bidirectionalEdgeCount)
                    + "; average degree: "
                    + nf.format(getAverageDegree())
                    + "; density: "
                    + ff.format(getDensity())
                    + "; triplet count: "
                    + nf.format(tripletCount)
                    + "; maximum degree: "
                    + nf.format(maximumDegree)
                    + "; maximum out degree: "
                    + nf.format(maximumOutDegree)
                    + "; maximum in degree: "
                    + nf.format(maximumInDegree)
                    + "; maximum triplets: "
                    + nf.format(maximumTriplets);
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder()
                    .append(vertexCount)
                    .append(unidirectionalEdgeCount)
                    .append(bidirectionalEdgeCount)
                    .append(tripletCount)
                    .append(maximumDegree)
                    .append(maximumOutDegree)
                    .append(maximumInDegree)
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
                    .append(unidirectionalEdgeCount, rhs.unidirectionalEdgeCount)
                    .append(bidirectionalEdgeCount, rhs.bidirectionalEdgeCount)
                    .append(tripletCount, rhs.tripletCount)
                    .append(maximumDegree, rhs.maximumDegree)
                    .append(maximumOutDegree, rhs.maximumOutDegree)
                    .append(maximumInDegree, rhs.maximumInDegree)
                    .append(maximumTriplets, rhs.maximumTriplets)
                    .isEquals();
        }
    }
}
