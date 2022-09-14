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

package org.apache.flink.graph.library.clustering.directed;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees.Degrees;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.asm.result.UnaryResultBase;
import org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient.Result;
import org.apache.flink.graph.utils.MurmurHash;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.graph.utils.proxy.OptionalBoolean;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

/**
 * The local clustering coefficient measures the connectedness of each vertex's neighborhood. Scores
 * range from 0.0 (no edges between neighbors) to 1.0 (neighborhood is a clique).
 *
 * <p>An edge between a vertex's neighbors is a triangle. Counting edges between neighbors is
 * equivalent to counting the number of triangles which include the vertex.
 *
 * <p>The input graph must be a simple graph containing no duplicate edges or self-loops.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class LocalClusteringCoefficient<K extends Comparable<K> & CopyableValue<K>, VV, EV>
        extends GraphAlgorithmWrappingDataSet<K, VV, EV, Result<K>> {

    // Optional configuration
    private OptionalBoolean includeZeroDegreeVertices = new OptionalBoolean(true, true);

    /**
     * By default the vertex set is checked for zero degree vertices. When this flag is disabled
     * only clustering coefficient scores for vertices with a degree of a least one will be
     * produced.
     *
     * @param includeZeroDegreeVertices whether to output scores for vertices with a degree of zero
     * @return this
     */
    public LocalClusteringCoefficient<K, VV, EV> setIncludeZeroDegreeVertices(
            boolean includeZeroDegreeVertices) {
        this.includeZeroDegreeVertices.set(includeZeroDegreeVertices);

        return this;
    }

    @Override
    protected boolean canMergeConfigurationWith(GraphAlgorithmWrappingBase other) {
        if (!super.canMergeConfigurationWith(other)) {
            return false;
        }

        LocalClusteringCoefficient rhs = (LocalClusteringCoefficient) other;

        return !includeZeroDegreeVertices.conflictsWith(rhs.includeZeroDegreeVertices);
    }

    @Override
    protected void mergeConfiguration(GraphAlgorithmWrappingBase other) {
        super.mergeConfiguration(other);

        LocalClusteringCoefficient rhs = (LocalClusteringCoefficient) other;

        includeZeroDegreeVertices.mergeWith(rhs.includeZeroDegreeVertices);
    }

    /*
     * Implementation notes:
     *
     * The requirement that "K extends CopyableValue<K>" can be removed when
     *   removed from TriangleListing.
     *
     * CountVertices can be replaced by ".sum(1)" when Flink aggregators use
     *   code generation.
     */

    @Override
    public DataSet<Result<K>> runInternal(Graph<K, VV, EV> input) throws Exception {
        // u, v, w, bitmask
        DataSet<TriangleListing.Result<K>> triangles =
                input.run(new TriangleListing<K, VV, EV>().setParallelism(parallelism));

        // u, edge count
        DataSet<Tuple2<K, LongValue>> triangleVertices =
                triangles.flatMap(new SplitTriangles<>()).name("Split triangle vertices");

        // u, triangle count
        DataSet<Tuple2<K, LongValue>> vertexTriangleCount =
                triangleVertices
                        .groupBy(0)
                        .reduce(new CountTriangles<>())
                        .setCombineHint(CombineHint.HASH)
                        .name("Count triangles")
                        .setParallelism(parallelism);

        // u, deg(u)
        DataSet<Vertex<K, Degrees>> vertexDegree =
                input.run(
                        new VertexDegrees<K, VV, EV>()
                                .setIncludeZeroDegreeVertices(includeZeroDegreeVertices.get())
                                .setParallelism(parallelism));

        // u, deg(u), triangle count
        return vertexDegree
                .leftOuterJoin(vertexTriangleCount)
                .where(0)
                .equalTo(0)
                .with(new JoinVertexDegreeWithTriangleCount<>())
                .setParallelism(parallelism)
                .name("Clustering coefficient");
    }

    /**
     * Emits the three vertex IDs comprising each triangle along with an initial count.
     *
     * @param <T> ID type
     */
    private static class SplitTriangles<T>
            implements FlatMapFunction<TriangleListing.Result<T>, Tuple2<T, LongValue>> {
        private LongValue one = new LongValue(1);

        private LongValue two = new LongValue(2);

        private Tuple2<T, LongValue> output = new Tuple2<>();

        @Override
        public void flatMap(TriangleListing.Result<T> value, Collector<Tuple2<T, LongValue>> out)
                throws Exception {
            byte bitmask = value.getBitmask().getValue();

            output.f0 = value.getVertexId0();
            output.f1 = ((bitmask & 0b000011) == 0b000011) ? two : one;
            out.collect(output);

            output.f0 = value.getVertexId1();
            output.f1 = ((bitmask & 0b001100) == 0b001100) ? two : one;
            out.collect(output);

            output.f0 = value.getVertexId2();
            output.f1 = ((bitmask & 0b110000) == 0b110000) ? two : one;
            out.collect(output);
        }
    }

    /**
     * Sums the triangle count for each vertex ID.
     *
     * @param <T> ID type
     */
    @ForwardedFields("0")
    private static class CountTriangles<T> implements ReduceFunction<Tuple2<T, LongValue>> {
        @Override
        public Tuple2<T, LongValue> reduce(Tuple2<T, LongValue> left, Tuple2<T, LongValue> right)
                throws Exception {
            left.f1.setValue(left.f1.getValue() + right.f1.getValue());
            return left;
        }
    }

    /**
     * Joins the vertex and degree with the vertex's triangle count.
     *
     * @param <T> ID type
     */
    @ForwardedFieldsFirst("0->vertexId0; 1.0->degree")
    @ForwardedFieldsSecond("0->vertexId0")
    private static class JoinVertexDegreeWithTriangleCount<T>
            implements JoinFunction<Vertex<T, Degrees>, Tuple2<T, LongValue>, Result<T>> {
        private LongValue zero = new LongValue(0);

        private Result<T> output = new Result<>();

        @Override
        public Result<T> join(
                Vertex<T, Degrees> vertexAndDegree, Tuple2<T, LongValue> vertexAndTriangleCount)
                throws Exception {
            output.setVertexId0(vertexAndDegree.f0);
            output.setDegree(vertexAndDegree.f1.f0);
            output.setTriangleCount(
                    (vertexAndTriangleCount == null) ? zero : vertexAndTriangleCount.f1);

            return output;
        }
    }

    /**
     * A result for the directed Local Clustering Coefficient algorithm.
     *
     * @param <T> ID type
     */
    public static class Result<T> extends UnaryResultBase<T> implements PrintableResult {
        private LongValue degree;

        private LongValue triangleCount;

        /**
         * Get the vertex degree.
         *
         * @return vertex degree
         */
        public LongValue getDegree() {
            return degree;
        }

        /**
         * Set the vertex degree.
         *
         * @param degree vertex degree
         */
        public void setDegree(LongValue degree) {
            this.degree = degree;
        }

        /**
         * Get the number of triangles containing this vertex; equivalently, this is the number of
         * edges between neighbors of this vertex.
         *
         * @return triangle count
         */
        public LongValue getTriangleCount() {
            return triangleCount;
        }

        /**
         * Set the number of triangles containing this vertex; equivalently, this is the number of
         * edges between neighbors of this vertex.
         *
         * @param triangleCount triangle count
         */
        public void setTriangleCount(LongValue triangleCount) {
            this.triangleCount = triangleCount;
        }

        /**
         * Get the local clustering coefficient score. This is computed as the number of edges
         * between neighbors, equal to the triangle count, divided by the number of potential edges
         * between neighbors.
         *
         * <p>A score of {@code Double.NaN} is returned for a vertex with degree 1 for which both
         * the triangle count and number of neighbors are zero.
         *
         * @return local clustering coefficient score
         */
        public double getLocalClusteringCoefficientScore() {
            long degree = getDegree().getValue();
            long neighborPairs = degree * (degree - 1);

            return (neighborPairs == 0)
                    ? Double.NaN
                    : getTriangleCount().getValue() / (double) neighborPairs;
        }

        @Override
        public String toString() {
            return "(" + getVertexId0() + "," + getDegree() + "," + getTriangleCount() + ")";
        }

        /**
         * Format values into a human-readable string.
         *
         * @return verbose string
         */
        @Override
        public String toPrintableString() {
            return "Vertex ID: "
                    + getVertexId0()
                    + ", vertex degree: "
                    + getDegree()
                    + ", triangle count: "
                    + getTriangleCount()
                    + ", local clustering coefficient: "
                    + getLocalClusteringCoefficientScore();
        }

        // ----------------------------------------------------------------------------------------

        public static final int HASH_SEED = 0x37a208c4;

        private transient MurmurHash hasher;

        @Override
        public int hashCode() {
            if (hasher == null) {
                hasher = new MurmurHash(HASH_SEED);
            }

            return hasher.reset()
                    .hash(getVertexId0().hashCode())
                    .hash(degree.getValue())
                    .hash(triangleCount.getValue())
                    .hash();
        }
    }
}
