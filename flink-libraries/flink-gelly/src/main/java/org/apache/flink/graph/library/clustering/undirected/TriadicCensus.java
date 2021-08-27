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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalytic;
import org.apache.flink.graph.GraphAnalyticBase;
import org.apache.flink.graph.asm.dataset.Count;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.library.clustering.undirected.TriadicCensus.Result;
import org.apache.flink.graph.library.metric.undirected.VertexMetrics;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.math.BigInteger;
import java.text.NumberFormat;

/**
 * A triad is formed by three connected or unconnected vertices in a graph. The triadic census
 * counts the occurrences of each type of triad.
 *
 * <p>The four types of undirected triads are formed with 0, 1, 2, or 3 connecting edges.
 *
 * <p>See http://vlado.fmf.uni-lj.si/pub/networks/doc/triads/triads.pdf
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class TriadicCensus<K extends Comparable<K> & CopyableValue<K>, VV, EV>
        extends GraphAnalyticBase<K, VV, EV, Result> {

    private Count<TriangleListing.Result<K>> triangleCount;

    private GraphAnalytic<K, VV, EV, VertexMetrics.Result> vertexMetrics;

    @Override
    public TriadicCensus<K, VV, EV> run(Graph<K, VV, EV> input) throws Exception {
        super.run(input);

        triangleCount = new Count<>();

        DataSet<TriangleListing.Result<K>> triangles =
                input.run(
                        new TriangleListing<K, VV, EV>()
                                .setSortTriangleVertices(false)
                                .setParallelism(parallelism));

        triangleCount.run(triangles);

        vertexMetrics = new VertexMetrics<K, VV, EV>().setParallelism(parallelism);

        input.run(vertexMetrics);

        return this;
    }

    @Override
    public Result getResult() {
        // vertex metrics
        BigInteger bigVertexCount =
                BigInteger.valueOf(vertexMetrics.getResult().getNumberOfVertices());
        BigInteger bigEdgeCount = BigInteger.valueOf(vertexMetrics.getResult().getNumberOfEdges());
        BigInteger bigTripletCount =
                BigInteger.valueOf(vertexMetrics.getResult().getNumberOfTriplets());

        // triangle count
        BigInteger bigTriangleCount = BigInteger.valueOf(triangleCount.getResult());

        BigInteger one = BigInteger.ONE;
        BigInteger two = BigInteger.valueOf(2);
        BigInteger three = BigInteger.valueOf(3);
        BigInteger six = BigInteger.valueOf(6);

        // counts as ordered in TriadicCensus.Result
        BigInteger[] counts = new BigInteger[4];

        // triads with three connecting edges = closed triplet = triangle
        counts[3] = bigTriangleCount;

        // triads with two connecting edges = open triplet;
        // deduct each triplet having been counted three times per triangle
        counts[2] = bigTripletCount.subtract(bigTriangleCount.multiply(three));

        // triads with one connecting edge; each edge pairs with `vertex count - 2` vertices
        // then deduct twice for each open triplet and three times for each triangle
        counts[1] =
                bigEdgeCount
                        .multiply(bigVertexCount.subtract(two))
                        .subtract(counts[2].multiply(two))
                        .subtract(counts[3].multiply(three));

        // triads with zero connecting edges;
        // (vertex count choose 3) minus earlier counts
        counts[0] =
                bigVertexCount
                        .multiply(bigVertexCount.subtract(one))
                        .multiply(bigVertexCount.subtract(two))
                        .divide(six)
                        .subtract(counts[1])
                        .subtract(counts[2])
                        .subtract(counts[3]);

        return new Result(counts);
    }

    /** Wraps triadic census metrics. */
    public static class Result implements PrintableResult {
        private final BigInteger[] counts;

        public Result(BigInteger... counts) {
            Preconditions.checkArgument(
                    counts.length == 4, "Expected 4 counts but received " + counts.length);

            this.counts = counts;
        }

        public Result(long... counts) {
            Preconditions.checkArgument(
                    counts.length == 4, "Expected 4 counts but received " + counts.length);

            this.counts = new BigInteger[counts.length];

            for (int i = 0; i < counts.length; i++) {
                this.counts[i] = BigInteger.valueOf(counts[i]);
            }
        }

        /**
         * Get the count of "03" triads which have zero connecting vertices.
         *
         * @return count of "03" triads
         */
        public BigInteger getCount03() {
            return counts[0];
        }

        /**
         * Get the count of "12" triads which have one edge among the vertices.
         *
         * @return count of "12" triads
         */
        public BigInteger getCount12() {
            return counts[1];
        }

        /**
         * Get the count of "21" triads which have two edges among the vertices and form a open
         * triplet.
         *
         * @return count of "21" triads
         */
        public BigInteger getCount21() {
            return counts[2];
        }

        /**
         * Get the count of "30" triads which have three edges among the vertices and form a closed
         * triplet, a triangle.
         *
         * @return count of "30" triads
         */
        public BigInteger getCount30() {
            return counts[3];
        }

        /**
         * Get the array of counts.
         *
         * <p>The order of the counts is from least to most connected: 03, 12, 21, 30
         *
         * @return array of counts
         */
        public BigInteger[] getCounts() {
            return counts;
        }

        @Override
        public String toString() {
            return toPrintableString();
        }

        @Override
        public String toPrintableString() {
            NumberFormat nf = NumberFormat.getInstance();

            return "03: "
                    + nf.format(getCount03())
                    + "; 12: "
                    + nf.format(getCount12())
                    + "; 21: "
                    + nf.format(getCount21())
                    + "; 30: "
                    + nf.format(getCount30());
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(counts).hashCode();
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

            return new EqualsBuilder().append(counts, rhs.counts).isEquals();
        }
    }
}
