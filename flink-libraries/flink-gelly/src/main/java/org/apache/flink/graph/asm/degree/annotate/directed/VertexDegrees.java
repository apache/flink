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

package org.apache.flink.graph.asm.degree.annotate.directed;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeOrder;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexDegrees.Degrees;
import org.apache.flink.graph.utils.MurmurHash;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingBase;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.graph.utils.proxy.OptionalBoolean;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

/**
 * Annotates vertices of a directed graph with the degree, out-, and in-degree.
 *
 * @param <K> graph label type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class VertexDegrees<K, VV, EV>
        extends GraphAlgorithmWrappingDataSet<K, VV, EV, Vertex<K, Degrees>> {

    // Optional configuration
    private OptionalBoolean includeZeroDegreeVertices = new OptionalBoolean(false, true);

    /**
     * By default only the edge set is processed for the computation of degree. When this flag is
     * set an additional join is performed against the vertex set in order to output vertices with
     * an in-degree of zero.
     *
     * @param includeZeroDegreeVertices whether to output vertices with an in-degree of zero
     * @return this
     */
    public VertexDegrees<K, VV, EV> setIncludeZeroDegreeVertices(
            boolean includeZeroDegreeVertices) {
        this.includeZeroDegreeVertices.set(includeZeroDegreeVertices);

        return this;
    }

    @Override
    protected boolean canMergeConfigurationWith(GraphAlgorithmWrappingBase other) {
        if (!super.canMergeConfigurationWith(other)) {
            return false;
        }

        VertexDegrees rhs = (VertexDegrees) other;

        return !includeZeroDegreeVertices.conflictsWith(rhs.includeZeroDegreeVertices);
    }

    @Override
    protected void mergeConfiguration(GraphAlgorithmWrappingBase other) {
        super.mergeConfiguration(other);

        VertexDegrees rhs = (VertexDegrees) other;

        includeZeroDegreeVertices.mergeWith(rhs.includeZeroDegreeVertices);
    }

    @Override
    public DataSet<Vertex<K, Degrees>> runInternal(Graph<K, VV, EV> input) throws Exception {
        // s, t, bitmask
        DataSet<Tuple2<K, ByteValue>> vertexWithEdgeOrder =
                input.getEdges()
                        .flatMap(new EmitAndFlipEdge<>())
                        .setParallelism(parallelism)
                        .name("Emit and flip edge")
                        .groupBy(0, 1)
                        .reduceGroup(new ReduceBitmask<>())
                        .setParallelism(parallelism)
                        .name("Reduce bitmask");

        // s, d(s)
        DataSet<Vertex<K, Degrees>> vertexDegrees =
                vertexWithEdgeOrder
                        .groupBy(0)
                        .reduceGroup(new DegreeCount<>())
                        .setParallelism(parallelism)
                        .name("Degree count");

        if (includeZeroDegreeVertices.get()) {
            vertexDegrees =
                    input.getVertices()
                            .leftOuterJoin(vertexDegrees)
                            .where(0)
                            .equalTo(0)
                            .with(new JoinVertexWithVertexDegrees<>())
                            .setParallelism(parallelism)
                            .name("Zero degree vertices");
        }

        return vertexDegrees;
    }

    /**
     * Emit each vertex both forward and reversed with the associated bitmask.
     *
     * @param <T> ID type
     * @param <TV> vertex value type
     */
    private static class EmitAndFlipEdge<T, TV>
            implements FlatMapFunction<Edge<T, TV>, Tuple3<T, T, ByteValue>> {
        private Tuple3<T, T, ByteValue> forward =
                new Tuple3<>(null, null, new ByteValue(EdgeOrder.FORWARD.getBitmask()));

        private Tuple3<T, T, ByteValue> reverse =
                new Tuple3<>(null, null, new ByteValue(EdgeOrder.REVERSE.getBitmask()));

        @Override
        public void flatMap(Edge<T, TV> value, Collector<Tuple3<T, T, ByteValue>> out)
                throws Exception {
            forward.f0 = value.f0;
            forward.f1 = value.f1;
            out.collect(forward);

            reverse.f0 = value.f1;
            reverse.f1 = value.f0;
            out.collect(reverse);
        }
    }

    /**
     * Reduce bitmasks to a single value using bitwise-or.
     *
     * @param <T> ID type
     */
    @ForwardedFields("0")
    private static final class ReduceBitmask<T>
            implements GroupReduceFunction<Tuple3<T, T, ByteValue>, Tuple2<T, ByteValue>> {
        private Tuple2<T, ByteValue> output = new Tuple2<>(null, new ByteValue());

        @Override
        public void reduce(
                Iterable<Tuple3<T, T, ByteValue>> values, Collector<Tuple2<T, ByteValue>> out)
                throws Exception {
            byte bitmask = 0;

            for (Tuple3<T, T, ByteValue> value : values) {
                output.f0 = value.f0;
                bitmask |= value.f2.getValue();
            }

            output.f1.setValue(bitmask);
            out.collect(output);
        }
    }

    /**
     * Sum vertex degree by counting over mutual, out-, and in-edges.
     *
     * @param <T> ID type
     */
    @ForwardedFields("0")
    private static class DegreeCount<T>
            implements GroupReduceFunction<Tuple2<T, ByteValue>, Vertex<T, Degrees>> {
        private Vertex<T, Degrees> output = new Vertex<>(null, new Degrees());

        @Override
        public void reduce(Iterable<Tuple2<T, ByteValue>> values, Collector<Vertex<T, Degrees>> out)
                throws Exception {
            long degree = 0;
            long outDegree = 0;
            long inDegree = 0;

            for (Tuple2<T, ByteValue> edge : values) {
                output.f0 = edge.f0;

                byte bitmask = edge.f1.getValue();

                degree++;

                if (bitmask == EdgeOrder.FORWARD.getBitmask()) {
                    outDegree++;
                } else if (bitmask == EdgeOrder.REVERSE.getBitmask()) {
                    inDegree++;
                } else {
                    outDegree++;
                    inDegree++;
                }
            }

            output.f1.getDegree().setValue(degree);
            output.f1.getOutDegree().setValue(outDegree);
            output.f1.getInDegree().setValue(inDegree);

            out.collect(output);
        }
    }

    /**
     * Performs a left outer join to apply a zero count for vertices with out- and in-degree of
     * zero.
     *
     * @param <T> ID type
     * @param <TV> vertex value type
     */
    @ForwardedFieldsFirst("0")
    @ForwardedFieldsSecond("0")
    private static class JoinVertexWithVertexDegrees<T, TV>
            implements JoinFunction<Vertex<T, TV>, Vertex<T, Degrees>, Vertex<T, Degrees>> {
        private Vertex<T, Degrees> output = new Vertex<>(null, new Degrees());

        @Override
        public Vertex<T, Degrees> join(Vertex<T, TV> vertex, Vertex<T, Degrees> vertexDegree)
                throws Exception {
            if (vertexDegree == null) {
                output.f0 = vertex.f0;
                return output;
            } else {
                return vertexDegree;
            }
        }
    }

    /** Wraps the vertex degree, out-degree, and in-degree. */
    public static class Degrees extends Tuple3<LongValue, LongValue, LongValue> {
        private static final int HASH_SEED = 0x3a12fc31;

        private MurmurHash hasher = new MurmurHash(HASH_SEED);

        public Degrees() {
            this(new LongValue(), new LongValue(), new LongValue());
        }

        public Degrees(LongValue value0, LongValue value1, LongValue value2) {
            super(value0, value1, value2);
        }

        public LongValue getDegree() {
            return f0;
        }

        public LongValue getOutDegree() {
            return f1;
        }

        public LongValue getInDegree() {
            return f2;
        }

        @Override
        public int hashCode() {
            return hasher.reset()
                    .hash(f0.getValue())
                    .hash(f1.getValue())
                    .hash(f2.getValue())
                    .hash();
        }
    }
}
