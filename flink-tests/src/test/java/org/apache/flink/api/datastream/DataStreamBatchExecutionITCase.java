/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.datastream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.CollectionUtil.iteratorToList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * Integration test for {@link RuntimeExecutionMode#BATCH} execution on the DataStream API.
 *
 * <p>We use a {@link MiniClusterWithClientResource} with a single TaskManager with 1 slot to verify
 * that programs in BATCH execution mode can be executed in stages.
 */
public class DataStreamBatchExecutionITCase {
    private static final int DEFAULT_PARALLELISM = 1;

    @ClassRule
    public static MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                            .build());

    /**
     * We induce a failure in the last mapper. In BATCH execution mode the part of the pipeline
     * before the key-by should not be re-executed. Only the part after that will restart. We check
     * that by suffixing the attempt number to records and asserting the correct number.
     */
    @Test
    public void batchFailoverWithKeyByBarrier() throws Exception {

        final StreamExecutionEnvironment env = getExecutionEnvironment();

        DataStreamSource<String> source = env.fromElements("foo", "bar");

        SingleOutputStreamOperator<String> mapped =
                source.map(new SuffixAttemptId("a"))
                        .map(new SuffixAttemptId("b"))
                        .keyBy(in -> in)
                        .map(new SuffixAttemptId("c"))
                        .map(new OnceFailingMapper("d"));

        try (CloseableIterator<String> result = mapped.executeAndCollect()) {

            // only the operators after the key-by "barrier" are restarted and will have the
            // "attempt 1" suffix
            assertThat(
                    iteratorToList(result),
                    containsInAnyOrder("foo-a0-b0-c1-d1", "bar-a0-b0-c1-d1"));
        }
    }

    /**
     * We induce a failure in the last mapper. In BATCH execution mode the part of the pipeline
     * before the rebalance should not be re-executed. Only the part after that will restart. We
     * check that by suffixing the attempt number to records and asserting the correct number.
     */
    @Test
    public void batchFailoverWithRebalanceBarrier() throws Exception {

        final StreamExecutionEnvironment env = getExecutionEnvironment();

        DataStreamSource<String> source = env.fromElements("foo", "bar");

        SingleOutputStreamOperator<String> mapped =
                source.map(new SuffixAttemptId("a"))
                        .map(new SuffixAttemptId("b"))
                        .rebalance()
                        .map(new SuffixAttemptId("c"))
                        .map(new OnceFailingMapper("d"));

        try (CloseableIterator<String> result = mapped.executeAndCollect()) {

            // only the operators after the rebalance "barrier" are restarted and will have the
            // "attempt 1" suffix
            assertThat(
                    iteratorToList(result),
                    containsInAnyOrder("foo-a0-b0-c1-d1", "bar-a0-b0-c1-d1"));
        }
    }

    /**
     * We induce a failure in the last mapper. In BATCH execution mode the part of the pipeline
     * before the rescale should not be re-executed. Only the part after that will restart. We check
     * that by suffixing the attempt number to records and asserting the correct number.
     */
    @Test
    public void batchFailoverWithRescaleBarrier() throws Exception {

        final StreamExecutionEnvironment env = getExecutionEnvironment();

        DataStreamSource<String> source = env.fromElements("foo", "bar");
        env.setParallelism(1);

        SingleOutputStreamOperator<String> mapped =
                source.map(new SuffixAttemptId("a"))
                        .map(new SuffixAttemptId("b"))
                        .rescale()
                        .map(new SuffixAttemptId("c"))
                        .setParallelism(2)
                        .map(new OnceFailingMapper("d"))
                        .setParallelism(2);

        try (CloseableIterator<String> result = mapped.executeAndCollect()) {

            // only the operators after the rescale "barrier" are restarted and will have the
            // "attempt 1" suffix
            assertThat(
                    iteratorToList(result),
                    containsInAnyOrder("foo-a0-b0-c1-d1", "bar-a0-b0-c1-d1"));
        }
    }

    @Test
    public void batchReduceSingleResultPerKey() throws Exception {
        StreamExecutionEnvironment env = getExecutionEnvironment();
        DataStreamSource<Long> numbers = env.fromSequence(0, 10);

        // send all records into a single reducer
        KeyedStream<Long, Long> stream = numbers.keyBy(i -> i % 2);
        DataStream<Long> sums = stream.reduce(Long::sum);

        try (CloseableIterator<Long> sumsIterator = sums.executeAndCollect()) {
            List<Long> results = CollectionUtil.iteratorToList(sumsIterator);
            assertThat(results, equalTo(Arrays.asList(30L, 25L)));
        }
    }

    @Test
    public void batchSumSingleResultPerKey() throws Exception {
        StreamExecutionEnvironment env = getExecutionEnvironment();
        DataStreamSource<Long> numbers = env.fromSequence(0, 10);

        // send all records into a single reducer
        KeyedStream<Long, Long> stream = numbers.keyBy(i -> i % 2);
        DataStream<Long> sums = stream.sum(0);

        try (CloseableIterator<Long> sumsIterator = sums.executeAndCollect()) {
            List<Long> results = CollectionUtil.iteratorToList(sumsIterator);
            assertThat(results, equalTo(Arrays.asList(30L, 25L)));
        }
    }

    /**
     * Verifies that all regular input is processed before keyed input.
     *
     * <p>Here, the first input is keyed while the second input is not keyed.
     */
    @Test
    public void batchKeyedNonKeyedTwoInputOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Tuple2<String, Integer>> keyedInput =
                env.fromElements(
                                Tuple2.of("regular2", 4),
                                Tuple2.of("regular1", 3),
                                Tuple2.of("regular1", 2),
                                Tuple2.of("regular2", 1))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1));

        DataStream<Tuple2<String, Integer>> regularInput =
                env.fromElements(
                                Tuple2.of("regular4", 4),
                                Tuple2.of("regular3", 3),
                                Tuple2.of("regular3", 2),
                                Tuple2.of("regular4", 1))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1));

        DataStream<String> result =
                keyedInput
                        .keyBy(in -> in.f0)
                        .connect(regularInput)
                        .transform(
                                "operator",
                                BasicTypeInfo.STRING_TYPE_INFO,
                                new TwoInputIdentityOperator());

        try (CloseableIterator<String> resultIterator = result.executeAndCollect()) {
            List<String> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(
                    results,
                    equalTo(
                            Arrays.asList(
                                    "(regular4,4)",
                                    "(regular3,3)",
                                    "(regular3,2)",
                                    "(regular4,1)",
                                    "(regular1,2)",
                                    "(regular1,3)",
                                    "(regular2,1)",
                                    "(regular2,4)")));
        }
    }

    /**
     * Verifies that all regular input is processed before keyed input.
     *
     * <p>Here, the first input is not keyed while the second input is keyed.
     */
    @Test
    public void batchNonKeyedKeyedTwoInputOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Tuple2<String, Integer>> keyedInput =
                env.fromElements(
                                Tuple2.of("regular2", 4),
                                Tuple2.of("regular1", 3),
                                Tuple2.of("regular1", 2),
                                Tuple2.of("regular2", 1))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1));

        DataStream<Tuple2<String, Integer>> regularInput =
                env.fromElements(
                                Tuple2.of("regular4", 4),
                                Tuple2.of("regular3", 3),
                                Tuple2.of("regular3", 2),
                                Tuple2.of("regular4", 1))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1));

        DataStream<String> result =
                regularInput
                        .connect(keyedInput.keyBy(in -> in.f0))
                        .transform(
                                "operator",
                                BasicTypeInfo.STRING_TYPE_INFO,
                                new TwoInputIdentityOperator());

        try (CloseableIterator<String> resultIterator = result.executeAndCollect()) {
            List<String> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(
                    results,
                    equalTo(
                            Arrays.asList(
                                    "(regular4,4)",
                                    "(regular3,3)",
                                    "(regular3,2)",
                                    "(regular4,1)",
                                    "(regular1,2)",
                                    "(regular1,3)",
                                    "(regular2,1)",
                                    "(regular2,4)")));
        }
    }

    /** Verifies that all broadcast input is processed before keyed input. */
    @Test
    public void batchKeyedBroadcastExecution() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Tuple2<String, Integer>> bcInput =
                env.fromElements(Tuple2.of("bc1", 1), Tuple2.of("bc2", 2), Tuple2.of("bc3", 3))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1));

        DataStream<Tuple2<String, Integer>> regularInput =
                env.fromElements(
                                Tuple2.of("regular1", 1),
                                Tuple2.of("regular1", 2),
                                Tuple2.of("regular2", 2),
                                Tuple2.of("regular1", 3),
                                Tuple2.of("regular1", 4),
                                Tuple2.of("regular1", 3),
                                Tuple2.of("regular2", 5),
                                Tuple2.of("regular1", 5),
                                Tuple2.of("regular2", 3),
                                Tuple2.of("regular1", 3))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1));

        BroadcastStream<Tuple2<String, Integer>> broadcastStream =
                bcInput.broadcast(STATE_DESCRIPTOR);

        DataStream<String> result =
                regularInput
                        .keyBy((input) -> input.f0)
                        .connect(broadcastStream)
                        .process(new TestKeyedBroadcastFunction());

        try (CloseableIterator<String> resultIterator = result.executeAndCollect()) {
            List<String> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(
                    results,
                    equalTo(
                            Arrays.asList(
                                    "(regular1,1): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular1,2): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular1,3): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular1,3): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular1,3): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular1,4): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular1,5): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular2,2): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular2,3): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular2,5): [bc2=bc2, bc1=bc1, bc3=bc3]")));
        }
    }

    /** Verifies that all broadcast input is processed before regular input. */
    @Test
    public void batchBroadcastExecution() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Tuple2<String, Integer>> bcInput =
                env.fromElements(Tuple2.of("bc1", 1), Tuple2.of("bc2", 2), Tuple2.of("bc3", 3))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1));

        DataStream<Tuple2<String, Integer>> regularInput =
                env.fromElements(
                                Tuple2.of("regular1", 1),
                                Tuple2.of("regular1", 2),
                                Tuple2.of("regular1", 3),
                                Tuple2.of("regular1", 4),
                                Tuple2.of("regular1", 3),
                                Tuple2.of("regular1", 5),
                                Tuple2.of("regular1", 3))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1));

        BroadcastStream<Tuple2<String, Integer>> broadcastStream =
                bcInput.broadcast(STATE_DESCRIPTOR);

        DataStream<String> result =
                regularInput.connect(broadcastStream).process(new TestBroadcastFunction());

        try (CloseableIterator<String> resultIterator = result.executeAndCollect()) {
            List<String> results = CollectionUtil.iteratorToList(resultIterator);
            // regular, that is non-keyed input is not sorted by timestamp. For keyed inputs
            // this is a by-product of the grouping/sorting we use to get the keyed groups.
            assertThat(
                    results,
                    equalTo(
                            Arrays.asList(
                                    "(regular1,1): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular1,2): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular1,3): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular1,4): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular1,3): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular1,5): [bc2=bc2, bc1=bc1, bc3=bc3]",
                                    "(regular1,3): [bc2=bc2, bc1=bc1, bc3=bc3]")));
        }
    }

    @Test
    public void batchMixedKeyedAndNonKeyedTwoInputOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Tuple2<String, Integer>> bcInput =
                env.fromElements(Tuple2.of("bc3", 3), Tuple2.of("bc2", 2), Tuple2.of("bc1", 1))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1))
                        .broadcast();

        DataStream<Tuple2<String, Integer>> regularInput =
                env.fromElements(
                                Tuple2.of("regular1", 1),
                                Tuple2.of("regular1", 2),
                                Tuple2.of("regular1", 3),
                                Tuple2.of("regular1", 4),
                                Tuple2.of("regular2", 3),
                                Tuple2.of("regular2", 5),
                                Tuple2.of("regular1", 3))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1))
                        .keyBy(input -> input.f0);

        TwoInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>, String>
                twoInputTransformation =
                        new TwoInputTransformation<>(
                                regularInput.getTransformation(),
                                bcInput.getTransformation(),
                                "operator",
                                new TestMixedTwoInputOperator(),
                                BasicTypeInfo.STRING_TYPE_INFO,
                                1);

        twoInputTransformation.setStateKeyType(BasicTypeInfo.STRING_TYPE_INFO);
        twoInputTransformation.setStateKeySelectors(input -> input.f0, null);

        DataStream<String> result = new DataStream<>(env, twoInputTransformation);

        try (CloseableIterator<String> resultIterator = result.executeAndCollect()) {
            List<String> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(
                    results,
                    equalTo(
                            Arrays.asList(
                                    "(regular1,1): [bc3, bc2, bc1]",
                                    "(regular1,2): [bc3, bc2, bc1]",
                                    "(regular1,3): [bc3, bc2, bc1]",
                                    "(regular1,3): [bc3, bc2, bc1]",
                                    "(regular1,4): [bc3, bc2, bc1]",
                                    "(regular2,3): [bc3, bc2, bc1]",
                                    "(regular2,5): [bc3, bc2, bc1]")));
        }
    }

    @Test
    public void batchMixedKeyedAndNonKeyedMultiInputOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Tuple2<String, Integer>> bc1Input =
                env.fromElements(Tuple2.of("bc3", 3), Tuple2.of("bc2", 2))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1))
                        .broadcast();
        DataStream<Tuple2<String, Integer>> bc2Input =
                env.fromElements(Tuple2.of("bc1", 1))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1))
                        .broadcast();
        DataStream<Tuple2<String, Integer>> regularInput =
                env.fromElements(
                                Tuple2.of("regular1", 1),
                                Tuple2.of("regular1", 2),
                                Tuple2.of("regular1", 3),
                                Tuple2.of("regular1", 4),
                                Tuple2.of("regular2", 3),
                                Tuple2.of("regular2", 5),
                                Tuple2.of("regular1", 3))
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                                        .withTimestampAssigner((in, ts) -> in.f1))
                        .keyBy(input -> input.f0);

        KeyedMultipleInputTransformation<String> multipleInputTransformation =
                new KeyedMultipleInputTransformation<>(
                        "operator",
                        mixedInputsOperatorFactory,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        1,
                        BasicTypeInfo.STRING_TYPE_INFO);
        multipleInputTransformation.addInput(
                regularInput.getTransformation(), input -> ((Tuple2<String, Integer>) input).f0);
        multipleInputTransformation.addInput(bc1Input.getTransformation(), null);
        multipleInputTransformation.addInput(bc2Input.getTransformation(), null);

        DataStream<String> result =
                new MultipleConnectedStreams(env).transform(multipleInputTransformation);

        try (CloseableIterator<String> resultIterator = result.executeAndCollect()) {
            List<String> results = CollectionUtil.iteratorToList(resultIterator);
            assertThat(
                    results,
                    equalTo(
                            Arrays.asList(
                                    "(regular1,1): [bc3, bc2, bc1]",
                                    "(regular1,2): [bc3, bc2, bc1]",
                                    "(regular1,3): [bc3, bc2, bc1]",
                                    "(regular1,3): [bc3, bc2, bc1]",
                                    "(regular1,4): [bc3, bc2, bc1]",
                                    "(regular2,3): [bc3, bc2, bc1]",
                                    "(regular2,5): [bc3, bc2, bc1]")));
        }
    }

    private StreamExecutionEnvironment getExecutionEnvironment() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        // trick the collecting sink into working even in the face of failures 🙏
        env.enableCheckpointing(42);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.milliseconds(1)));

        return env;
    }

    /** Adds the attempt number as a suffix. */
    public static class SuffixAttemptId extends RichMapFunction<String, String> {
        private final String suffix;

        public SuffixAttemptId(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public String map(String value) {
            return value + "-" + suffix + getRuntimeContext().getAttemptNumber();
        }
    }

    /**
     * Adds the attempt number as a suffix.
     *
     * <p>Also fails by throwing an exception on the first attempt.
     */
    public static class OnceFailingMapper extends RichMapFunction<String, String> {
        private final String suffix;

        public OnceFailingMapper(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public String map(String value) throws Exception {
            if (getRuntimeContext().getAttemptNumber() <= 0) {
                throw new RuntimeException("FAILING");
            }
            return value + "-" + suffix + getRuntimeContext().getAttemptNumber();
        }
    }

    static final MapStateDescriptor<String, String> STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    "bc-input", StringSerializer.INSTANCE, StringSerializer.INSTANCE);

    static final ValueStateDescriptor<String> KEYED_STATE_DESCRIPTOR =
            new ValueStateDescriptor<>("keyed-state", StringSerializer.INSTANCE);

    static final ListStateDescriptor<String> LIST_STATE_DESCRIPTOR =
            new ListStateDescriptor<>("bc-list-input", StringSerializer.INSTANCE);

    private static class TestKeyedBroadcastFunction
            extends KeyedBroadcastProcessFunction<
                    String, Tuple2<String, Integer>, Tuple2<String, Integer>, String> {
        @Override
        public void processElement(
                Tuple2<String, Integer> value, ReadOnlyContext ctx, Collector<String> out)
                throws Exception {
            ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(STATE_DESCRIPTOR);

            out.collect(value + ": " + state.immutableEntries().toString());
        }

        @Override
        public void processBroadcastElement(
                Tuple2<String, Integer> value, Context ctx, Collector<String> out)
                throws Exception {
            BroadcastState<String, String> state = ctx.getBroadcastState(STATE_DESCRIPTOR);
            state.put(value.f0, value.f0);

            // iterating over keys is a no-op in BATCH execution mode
            ctx.applyToKeyedState(
                    KEYED_STATE_DESCRIPTOR,
                    (key, state1) -> {
                        throw new RuntimeException("Shouldn't happen");
                    });
        }
    }

    private static class TestBroadcastFunction
            extends BroadcastProcessFunction<
                    Tuple2<String, Integer>, Tuple2<String, Integer>, String> {

        @Override
        public void processElement(
                Tuple2<String, Integer> value, ReadOnlyContext ctx, Collector<String> out)
                throws Exception {
            ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(STATE_DESCRIPTOR);

            out.collect(value + ": " + state.immutableEntries().toString());
        }

        @Override
        public void processBroadcastElement(
                Tuple2<String, Integer> value, Context ctx, Collector<String> out)
                throws Exception {
            BroadcastState<String, String> state = ctx.getBroadcastState(STATE_DESCRIPTOR);
            state.put(value.f0, value.f0);
        }
    }

    private static class TwoInputIdentityOperator extends AbstractStreamOperator<String>
            implements TwoInputStreamOperator<
                    Tuple2<String, Integer>, Tuple2<String, Integer>, String> {
        @Override
        public void processElement1(StreamRecord<Tuple2<String, Integer>> element)
                throws Exception {
            output.collect(
                    new StreamRecord<>(element.getValue().toString(), element.getTimestamp()));
        }

        @Override
        public void processElement2(StreamRecord<Tuple2<String, Integer>> element)
                throws Exception {
            output.collect(
                    new StreamRecord<>(element.getValue().toString(), element.getTimestamp()));
        }
    }

    private static final class TestMixedTwoInputOperator extends AbstractStreamOperator<String>
            implements TwoInputStreamOperator<
                    Tuple2<String, Integer>, Tuple2<String, Integer>, String> {
        @Override
        public void processElement1(StreamRecord<Tuple2<String, Integer>> element)
                throws Exception {
            ListState<String> operatorState =
                    getOperatorStateBackend().getListState(LIST_STATE_DESCRIPTOR);

            output.collect(
                    new StreamRecord<>(element.getValue() + ": " + operatorState.get().toString()));
        }

        @Override
        public void processElement2(StreamRecord<Tuple2<String, Integer>> element)
                throws Exception {
            ListState<String> operatorState =
                    getOperatorStateBackend().getListState(LIST_STATE_DESCRIPTOR);
            operatorState.add(element.getValue().f0);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final AbstractStreamOperatorFactory<String> mixedInputsOperatorFactory =
            new AbstractStreamOperatorFactory<String>() {
                @Override
                public <T extends StreamOperator<String>> T createStreamOperator(
                        StreamOperatorParameters<String> parameters) {
                    return (T) new TestMixedMultipleInputOperator(parameters);
                }

                @Override
                public Class<? extends StreamOperator> getStreamOperatorClass(
                        ClassLoader classLoader) {
                    return TestMixedMultipleInputOperator.class;
                }
            };

    private static class TestMixedMultipleInputOperator extends AbstractStreamOperatorV2<String>
            implements MultipleInputStreamOperator<String> {

        public TestMixedMultipleInputOperator(StreamOperatorParameters<String> parameters) {
            super(parameters, 3);
        }

        @Override
        @SuppressWarnings({"rawtypes"})
        public List<Input> getInputs() {
            return Arrays.asList(
                    new AbstractInput<Tuple2<String, Integer>, String>(this, 1) {
                        @Override
                        public void processElement(StreamRecord<Tuple2<String, Integer>> element)
                                throws Exception {
                            ListState<String> operatorState =
                                    getOperatorStateBackend().getListState(LIST_STATE_DESCRIPTOR);

                            output.collect(
                                    new StreamRecord<>(
                                            element.getValue()
                                                    + ": "
                                                    + operatorState.get().toString()));
                        }
                    },
                    new AbstractInput<Tuple2<String, Integer>, String>(this, 2) {
                        @Override
                        public void processElement(StreamRecord<Tuple2<String, Integer>> element)
                                throws Exception {
                            ListState<String> operatorState =
                                    getOperatorStateBackend().getListState(LIST_STATE_DESCRIPTOR);
                            operatorState.add(element.getValue().f0);
                        }
                    },
                    new AbstractInput<Tuple2<String, Integer>, String>(this, 3) {
                        @Override
                        public void processElement(StreamRecord<Tuple2<String, Integer>> element)
                                throws Exception {
                            ListState<String> operatorState =
                                    getOperatorStateBackend().getListState(LIST_STATE_DESCRIPTOR);
                            operatorState.add(element.getValue().f0);
                        }
                    });
        }
    }
}
