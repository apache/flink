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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests for chaining the source operators / inputs. */
@SuppressWarnings("serial")
public class SourceNAryInputChainingITCase extends TestLogger {

    private static final int PARALLELISM = 4;

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    // ------------------------------------------------------------------------

    // We currently only have tests for chaining sources in N-ary Input Operators,
    // because those are the only ones where the runtime supports chaining at the moment.

    @Test
    public void testDirectSourcesOnlyChainCreation() throws Exception {
        final DataStream<Long> stream = createProgramWithSourcesOnly();
        final JobGraph jobGraph = sinkAndCompileJobGraph(stream);

        assertEquals(1, jobGraph.getNumberOfVertices());
    }

    @Test
    public void testDirectSourcesOnlyExecution() throws Exception {
        final DataStream<Long> stream = createProgramWithSourcesOnly();
        final List<Long> result =
                DataStreamUtils.collectBoundedStream(stream, "N-Ary Source Chaining Test Program");

        verifySequence(result, 1L, 30L);
    }

    @Test
    public void testMixedInputsChainCreation() throws Exception {
        final DataStream<Long> stream = createProgramWithMixedInputs();
        final JobGraph jobGraph = sinkAndCompileJobGraph(stream);

        assertEquals(3, jobGraph.getNumberOfVertices());
    }

    @Test
    public void testMixedInputsExecution() throws Exception {
        final DataStream<Long> stream = createProgramWithMixedInputs();
        final List<Long> result =
                DataStreamUtils.collectBoundedStream(stream, "N-Ary Source Chaining Test Program");

        verifySequence(result, 1L, 30L);
    }

    @Test
    public void testMixedInputsWithUnionChainCreation() throws Exception {
        final DataStream<Long> stream = createProgramWithUnionInput();
        final JobGraph jobGraph = sinkAndCompileJobGraph(stream);

        assertEquals(4, jobGraph.getNumberOfVertices());
    }

    @Test
    public void testMixedInputsWithUnionExecution() throws Exception {
        final DataStream<Long> stream = createProgramWithUnionInput();
        final List<Long> result =
                DataStreamUtils.collectBoundedStream(stream, "N-Ary Source Chaining Test Program");

        verifySequence(result, 1L, 40L);
    }

    @Test
    public void testMixedInputsWithMultipleUnionsChainCreation() throws Exception {
        final DataStream<Long> stream = createProgramWithMultipleUnionInputs();
        final JobGraph jobGraph = sinkAndCompileJobGraph(stream);

        assertEquals(6, jobGraph.getNumberOfVertices());
    }

    @Test
    public void testMixedInputsWithMultipleUnionsExecution() throws Exception {
        final DataStream<Long> stream = createProgramWithMultipleUnionInputs();
        final List<Long> result =
                DataStreamUtils.collectBoundedStream(stream, "N-Ary Source Chaining Test Program");

        verifySequence(result, 1L, 60L);
    }

    /**
     * Creates a DataStream program as shown below.
     *
     * <pre>
     *               +--------------+
     *   (src 1) --> |              |
     *               |     N-Ary    |
     *   (src 2) --> |              |
     *               |   Operator   |
     *   (src 3) --> |              |
     *               +--------------+
     * </pre>
     */
    private DataStream<Long> createProgramWithSourcesOnly() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.getConfig().enableObjectReuse();

        final DataStream<Long> source1 =
                env.fromSource(
                        new NumberSequenceSource(1L, 10L),
                        WatermarkStrategy.noWatermarks(),
                        "source-1");

        final DataStream<Long> source2 =
                env.fromSource(
                        new NumberSequenceSource(11L, 20L),
                        WatermarkStrategy.noWatermarks(),
                        "source-2");

        final DataStream<Long> source3 =
                env.fromSource(
                        new NumberSequenceSource(21L, 30L),
                        WatermarkStrategy.noWatermarks(),
                        "source-3");

        return nAryInputStreamOperation(source1, source2, source3);
    }

    /**
     * Creates a DataStream program as shown below.
     *
     * <pre>
     *                         +--------------+
     *   (src 1) --> (map) --> |              |
     *                         |     N-Ary    |
     *             (src 2) --> |              |
     *                         |   Operator   |
     *   (src 3) --> (map) --> |              |
     *                         +--------------+
     * </pre>
     */
    private DataStream<Long> createProgramWithMixedInputs() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.getConfig().enableObjectReuse();

        final DataStream<Long> source1 =
                env.fromSource(
                        new NumberSequenceSource(1L, 10L),
                        WatermarkStrategy.noWatermarks(),
                        "source-1");

        final DataStream<Long> source2 =
                env.fromSource(
                        new NumberSequenceSource(11L, 20L),
                        WatermarkStrategy.noWatermarks(),
                        "source-2");

        final DataStream<Long> source3 =
                env.fromSource(
                        new NumberSequenceSource(21L, 30L),
                        WatermarkStrategy.noWatermarks(),
                        "source-3");

        final DataStream<Long> stream1 = source1.map(v -> v);
        final DataStream<Long> stream3 = source3.map(v -> v);

        return nAryInputStreamOperation(stream1, source2, stream3);
    }

    /**
     * Creates a DataStream program as shown below.
     *
     * <pre>
     *                                   +--------------+
     *             (src 1) --> (map) --> |              |
     *                                   |              |
     *            (src 2) --+            |    N-Ary     |
     *                      +-- UNION -> |   Operator   |
     *   (src 3) -> (map) --+            |              |
     *                                   |              |
     *                       (src 4) --> |              |
     *                                   +--------------+
     * </pre>
     */
    private DataStream<Long> createProgramWithUnionInput() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.getConfig().enableObjectReuse();

        final DataStream<Long> source1 =
                env.fromSource(
                        new NumberSequenceSource(1L, 10L),
                        WatermarkStrategy.noWatermarks(),
                        "source-1");

        final DataStream<Long> source2 =
                env.fromSource(
                        new NumberSequenceSource(11L, 20L),
                        WatermarkStrategy.noWatermarks(),
                        "source-2");

        final DataStream<Long> source3 =
                env.fromSource(
                        new NumberSequenceSource(21L, 30L),
                        WatermarkStrategy.noWatermarks(),
                        "source-3");

        final DataStream<Long> source4 =
                env.fromSource(
                        new NumberSequenceSource(31L, 40L),
                        WatermarkStrategy.noWatermarks(),
                        "source-4");

        return nAryInputStreamOperation(source1.map((v) -> v), source2.union(source3), source4);
    }

    /**
     * Creates a DataStream program as shown below.
     *
     * <pre>
     *                                   +--------------+
     *             (src 1) --> (map) --> |              |
     *                                   |              |
     *           (src 2) --+             |              |
     *                     +-- UNION --> |              |
     *           (src 3) --+             |    N-Ary     |
     *                                   |   Operator   |
     *   (src 4) -> (map) --+            |              |
     *                      +-- UNION -> |              |
     *   (src 5) -> (map) --+            |              |
     *                                   |              |
     *                       (src 6) --> |              |
     *                                   +--------------+
     * </pre>
     */
    private DataStream<Long> createProgramWithMultipleUnionInputs() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.getConfig().enableObjectReuse();

        final DataStream<Long> source1 =
                env.fromSource(
                        new NumberSequenceSource(1L, 10L),
                        WatermarkStrategy.noWatermarks(),
                        "source-1");

        final DataStream<Long> source2 =
                env.fromSource(
                        new NumberSequenceSource(11L, 20L),
                        WatermarkStrategy.noWatermarks(),
                        "source-2");

        final DataStream<Long> source3 =
                env.fromSource(
                        new NumberSequenceSource(21L, 30L),
                        WatermarkStrategy.noWatermarks(),
                        "source-3");

        final DataStream<Long> source4 =
                env.fromSource(
                        new NumberSequenceSource(31L, 40L),
                        WatermarkStrategy.noWatermarks(),
                        "source-4");

        final DataStream<Long> source5 =
                env.fromSource(
                        new NumberSequenceSource(41L, 50L),
                        WatermarkStrategy.noWatermarks(),
                        "source-5");

        final DataStream<Long> source6 =
                env.fromSource(
                        new NumberSequenceSource(51L, 60L),
                        WatermarkStrategy.noWatermarks(),
                        "source-6");

        return nAryInputStreamOperation(
                source1.map((v) -> v),
                source2.union(source3),
                source4.map((v) -> v).union(source5.map((v) -> v)),
                source6);
    }

    private static DataStream<Long> nAryInputStreamOperation(final DataStream<?>... inputs) {

        final StreamExecutionEnvironment env = inputs[0].getExecutionEnvironment();

        // this is still clumsy due to the raw API

        final MultipleInputTransformation<Long> transform =
                new MultipleInputTransformation<>(
                        "MultipleInputOperator",
                        new NAryUnionOpFactory(inputs.length),
                        Types.LONG,
                        env.getParallelism());
        for (DataStream<?> input : inputs) {
            transform.addInput(input.getTransformation());
        }
        transform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);
        env.addOperator(transform);

        return new MultipleConnectedStreams(env).transform(transform);
    }

    private static JobGraph sinkAndCompileJobGraph(DataStream<?> stream) {
        stream.addSink(new DiscardingSink<>());

        final StreamGraph streamGraph = stream.getExecutionEnvironment().getStreamGraph();
        return StreamingJobGraphGenerator.createJobGraph(streamGraph);
    }

    private static void verifySequence(final List<Long> sequence, final long from, final long to) {
        if (sequence.size() != to - from + 1) {
            fail(String.format("Expected: Sequence [%d, %d]. Found: %s", from, to, sequence));
        }

        final ArrayList<Long> list =
                new ArrayList<>(sequence); // copy to be safe against immutable lists, etc.
        list.sort(Long::compareTo);

        int pos = 0;
        for (long value = from; value <= to; value++, pos++) {
            if (value != list.get(pos)) {
                fail(String.format("Expected: Sequence [%d, %d]. Found: %s", from, to, list));
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Tests mocks for N-ary input operator
    // ------------------------------------------------------------------------

    private static final class NAryUnionOp<T> extends AbstractStreamOperatorV2<T>
            implements MultipleInputStreamOperator<T> {

        private final int numInputs;

        public NAryUnionOp(StreamOperatorParameters<T> parameters, int numInputs) {
            super(parameters, numInputs);
            this.numInputs = numInputs;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public List<Input> getInputs() {
            final ArrayList<Input> inputs = new ArrayList<>();
            for (int i = 1; i <= numInputs; i++) {
                inputs.add(new PassThoughInput<>(this, i));
            }
            return inputs;
        }
    }

    /** Factory for {@link MultipleInputITCase.SumAllInputOperator}. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final class NAryUnionOpFactory extends AbstractStreamOperatorFactory<Long> {

        private final int numInputs;

        NAryUnionOpFactory(int numInputs) {
            this.numInputs = numInputs;
        }

        @Override
        public <T extends StreamOperator<Long>> T createStreamOperator(
                StreamOperatorParameters<Long> parameters) {
            final StreamOperator<Long> operator = new NAryUnionOp<>(parameters, numInputs);
            return (T) operator;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return NAryUnionOp.class;
        }
    }

    private static final class PassThoughInput<T> extends AbstractInput<T, T> {

        PassThoughInput(AbstractStreamOperatorV2<T> owner, int inputId) {
            super(owner, inputId);
        }

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            output.collect(element);
        }
    }
}
