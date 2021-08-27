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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SplittableIterator;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** An end to end test for sorted inputs for a keyed operator with bounded inputs. */
public class SortingBoundedInputITCase extends AbstractTestBase {

    @Test
    public void testOneInputOperator() {
        long numberOfRecords = 1_000_000;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(config, this.getClass().getClassLoader());

        DataStreamSource<Tuple2<Integer, byte[]>> elements =
                env.fromParallelCollection(
                        new InputGenerator(numberOfRecords),
                        new TupleTypeInfo<>(
                                BasicTypeInfo.INT_TYPE_INFO,
                                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));

        SingleOutputStreamOperator<Long> counts =
                elements.keyBy(element -> element.f0)
                        .transform(
                                "Asserting operator",
                                BasicTypeInfo.LONG_TYPE_INFO,
                                new AssertingOperator());

        long sum =
                CollectionUtil.iteratorToList(DataStreamUtils.collect(counts)).stream()
                        .mapToLong(l -> l)
                        .sum();

        assertThat(sum, equalTo(numberOfRecords));
    }

    @Test
    public void testTwoInputOperator() {
        long numberOfRecords = 500_000;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(config, this.getClass().getClassLoader());

        DataStreamSource<Tuple2<Integer, byte[]>> elements1 =
                env.fromParallelCollection(
                        new InputGenerator(numberOfRecords),
                        new TupleTypeInfo<>(
                                BasicTypeInfo.INT_TYPE_INFO,
                                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));

        DataStreamSource<Tuple2<Integer, byte[]>> elements2 =
                env.fromParallelCollection(
                        new InputGenerator(numberOfRecords),
                        new TupleTypeInfo<>(
                                BasicTypeInfo.INT_TYPE_INFO,
                                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        SingleOutputStreamOperator<Long> counts =
                elements1
                        .connect(elements2)
                        .keyBy(element -> element.f0, element -> element.f0)
                        .transform(
                                "Asserting operator",
                                BasicTypeInfo.LONG_TYPE_INFO,
                                new AssertingTwoInputOperator());

        long sum =
                CollectionUtil.iteratorToList(DataStreamUtils.collect(counts)).stream()
                        .mapToLong(l -> l)
                        .sum();

        assertThat(sum, equalTo(numberOfRecords * 2));
    }

    @Test
    public void testThreeInputOperator() {
        long numberOfRecords = 500_000;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(config, this.getClass().getClassLoader());

        KeyedStream<Tuple2<Integer, byte[]>, Object> elements1 =
                env.fromParallelCollection(
                                new InputGenerator(numberOfRecords),
                                new TupleTypeInfo<>(
                                        BasicTypeInfo.INT_TYPE_INFO,
                                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO))
                        .keyBy(el -> el.f0);

        KeyedStream<Tuple2<Integer, byte[]>, Object> elements2 =
                env.fromParallelCollection(
                                new InputGenerator(numberOfRecords),
                                new TupleTypeInfo<>(
                                        BasicTypeInfo.INT_TYPE_INFO,
                                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO))
                        .keyBy(el -> el.f0);

        KeyedStream<Tuple2<Integer, byte[]>, Object> elements3 =
                env.fromParallelCollection(
                                new InputGenerator(numberOfRecords),
                                new TupleTypeInfo<>(
                                        BasicTypeInfo.INT_TYPE_INFO,
                                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO))
                        .keyBy(el -> el.f0);

        KeyedMultipleInputTransformation<Long> assertingTransformation =
                new KeyedMultipleInputTransformation<>(
                        "Asserting operator",
                        new AssertingThreeInputOperatorFactory(),
                        BasicTypeInfo.LONG_TYPE_INFO,
                        -1,
                        BasicTypeInfo.INT_TYPE_INFO);
        assertingTransformation.addInput(elements1.getTransformation(), elements1.getKeySelector());
        assertingTransformation.addInput(elements2.getTransformation(), elements2.getKeySelector());
        assertingTransformation.addInput(elements3.getTransformation(), elements3.getKeySelector());

        env.addOperator(assertingTransformation);
        DataStream<Long> counts = new DataStream<>(env, assertingTransformation);

        long sum =
                CollectionUtil.iteratorToList(DataStreamUtils.collect(counts)).stream()
                        .mapToLong(l -> l)
                        .sum();

        assertThat(sum, equalTo(numberOfRecords * 3));
    }

    @Test
    public void testBatchExecutionWithTimersOneInput() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // set parallelism to 1 to have consistent order of results

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(config, this.getClass().getClassLoader());

        WatermarkStrategy<Tuple2<Integer, Integer>> watermarkStrategy =
                WatermarkStrategy.forGenerator(ctx -> GENERATE_WATERMARK_AFTER_4_14_TIMESTAMP)
                        .withTimestampAssigner((r, previousTimestamp) -> r.f1);
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> elements =
                env.fromElements(
                                Tuple2.of(1, 3),
                                Tuple2.of(1, 1),
                                Tuple2.of(2, 1),
                                Tuple2.of(1, 4),
                                // emit watermark = 5
                                Tuple2.of(2, 3), // late element
                                Tuple2.of(1, 2), // late element
                                Tuple2.of(1, 13),
                                Tuple2.of(1, 11),
                                Tuple2.of(2, 14),
                                // emit watermark = 15
                                Tuple2.of(1, 11) // late element
                                )
                        .assignTimestampsAndWatermarks(watermarkStrategy);

        OutputTag<Integer> lateElements =
                new OutputTag<>("late_elements", BasicTypeInfo.INT_TYPE_INFO);
        SingleOutputStreamOperator<Tuple3<Long, Integer, Integer>> sums =
                elements.map(element -> element.f0)
                        .keyBy(element -> element)
                        .process(
                                new KeyedProcessFunction<
                                        Integer, Integer, Tuple3<Long, Integer, Integer>>() {

                                    private MapState<Long, Integer> countState;
                                    private ValueState<Long> previousTimestampState;

                                    @Override
                                    public void open(Configuration parameters) {
                                        countState =
                                                getRuntimeContext()
                                                        .getMapState(
                                                                new MapStateDescriptor<>(
                                                                        "sum",
                                                                        BasicTypeInfo
                                                                                .LONG_TYPE_INFO,
                                                                        BasicTypeInfo
                                                                                .INT_TYPE_INFO));
                                        previousTimestampState =
                                                getRuntimeContext()
                                                        .getState(
                                                                new ValueStateDescriptor<>(
                                                                        "previousTimestamp",
                                                                        BasicTypeInfo
                                                                                .LONG_TYPE_INFO));
                                    }

                                    @Override
                                    public void processElement(
                                            Integer value,
                                            Context ctx,
                                            Collector<Tuple3<Long, Integer, Integer>> out)
                                            throws Exception {

                                        Long elementTimestamp = ctx.timestamp();
                                        long nextTen = ((elementTimestamp + 10) / 10) * 10;
                                        ctx.timerService().registerEventTimeTimer(nextTen);

                                        if (elementTimestamp
                                                < ctx.timerService().currentWatermark()) {
                                            ctx.output(lateElements, value);
                                        } else {
                                            Long previousTimestamp =
                                                    Optional.ofNullable(
                                                                    previousTimestampState.value())
                                                            .orElse(0L);
                                            assertThat(
                                                    elementTimestamp,
                                                    greaterThanOrEqualTo(previousTimestamp));
                                            previousTimestampState.update(elementTimestamp);

                                            Integer currentCount =
                                                    Optional.ofNullable(countState.get(nextTen))
                                                            .orElse(0);
                                            countState.put(nextTen, currentCount + 1);
                                        }
                                    }

                                    @Override
                                    public void onTimer(
                                            long timestamp,
                                            OnTimerContext ctx,
                                            Collector<Tuple3<Long, Integer, Integer>> out)
                                            throws Exception {
                                        out.collect(
                                                Tuple3.of(
                                                        timestamp,
                                                        ctx.getCurrentKey(),
                                                        countState.get(timestamp)));
                                        countState.remove(timestamp);

                                        // this would go in infinite loop if we did not quiesce the
                                        // timer service.
                                        ctx.timerService().registerEventTimeTimer(timestamp + 1);
                                    }
                                });

        DataStream<Integer> lateStream = sums.getSideOutput(lateElements);
        List<Integer> lateRecordsCollected =
                CollectionUtil.iteratorToList(DataStreamUtils.collect(lateStream));
        List<Tuple3<Long, Integer, Integer>> sumsCollected =
                CollectionUtil.iteratorToList(DataStreamUtils.collect(sums));

        assertTrue(lateRecordsCollected.isEmpty());
        assertThat(
                sumsCollected,
                equalTo(
                        Arrays.asList(
                                Tuple3.of(10L, 1, 4),
                                Tuple3.of(20L, 1, 3),
                                Tuple3.of(10L, 2, 2),
                                Tuple3.of(20L, 2, 1))));
    }

    @Test
    public void testBatchExecutionWithTimersTwoInput() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // set parallelism to 1 to have consistent order of results

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(config, this.getClass().getClassLoader());

        WatermarkStrategy<Tuple2<Integer, Integer>> watermarkStrategy =
                WatermarkStrategy.forGenerator(ctx -> GENERATE_WATERMARK_AFTER_4_14_TIMESTAMP)
                        .withTimestampAssigner((r, previousTimestamp) -> r.f1);
        SingleOutputStreamOperator<Integer> elements1 =
                env.fromElements(
                                Tuple2.of(1, 3),
                                Tuple2.of(1, 1),
                                Tuple2.of(2, 1),
                                Tuple2.of(1, 4),
                                // emit watermark = 5
                                Tuple2.of(2, 3), // late element
                                Tuple2.of(1, 2), // late element
                                Tuple2.of(1, 13),
                                Tuple2.of(1, 11),
                                Tuple2.of(2, 14),
                                // emit watermark = 15
                                Tuple2.of(1, 11) // late element
                                )
                        .assignTimestampsAndWatermarks(watermarkStrategy)
                        .map(element -> element.f0);

        SingleOutputStreamOperator<Integer> elements2 =
                env.fromElements(
                                Tuple2.of(1, 3),
                                Tuple2.of(1, 1),
                                Tuple2.of(2, 1),
                                Tuple2.of(1, 4),
                                // emit watermark = 5
                                Tuple2.of(2, 3), // late element
                                Tuple2.of(1, 2), // late element
                                Tuple2.of(1, 13),
                                Tuple2.of(1, 11),
                                Tuple2.of(2, 14),
                                // emit watermark = 15
                                Tuple2.of(1, 11) // late element
                                )
                        .assignTimestampsAndWatermarks(watermarkStrategy)
                        .map(element -> element.f0);

        OutputTag<Integer> lateElements =
                new OutputTag<>("late_elements", BasicTypeInfo.INT_TYPE_INFO);
        SingleOutputStreamOperator<Tuple3<Long, Integer, Integer>> sums =
                elements1
                        .connect(elements2)
                        .keyBy(element -> element, element -> element)
                        .process(
                                new KeyedCoProcessFunction<
                                        Integer,
                                        Integer,
                                        Integer,
                                        Tuple3<Long, Integer, Integer>>() {

                                    private MapState<Long, Integer> countState;
                                    private ValueState<Long> previousTimestampState;

                                    @Override
                                    public void open(Configuration parameters) {
                                        countState =
                                                getRuntimeContext()
                                                        .getMapState(
                                                                new MapStateDescriptor<>(
                                                                        "sum",
                                                                        BasicTypeInfo
                                                                                .LONG_TYPE_INFO,
                                                                        BasicTypeInfo
                                                                                .INT_TYPE_INFO));
                                        previousTimestampState =
                                                getRuntimeContext()
                                                        .getState(
                                                                new ValueStateDescriptor<>(
                                                                        "previousTimestamp",
                                                                        BasicTypeInfo
                                                                                .LONG_TYPE_INFO));
                                    }

                                    @Override
                                    public void processElement1(
                                            Integer value,
                                            Context ctx,
                                            Collector<Tuple3<Long, Integer, Integer>> out)
                                            throws Exception {
                                        processElement(value, ctx);
                                    }

                                    @Override
                                    public void processElement2(
                                            Integer value,
                                            Context ctx,
                                            Collector<Tuple3<Long, Integer, Integer>> out)
                                            throws Exception {
                                        processElement(value, ctx);
                                    }

                                    private void processElement(Integer value, Context ctx)
                                            throws Exception {
                                        Long elementTimestamp = ctx.timestamp();
                                        long nextTen = ((elementTimestamp + 10) / 10) * 10;
                                        ctx.timerService().registerEventTimeTimer(nextTen);
                                        if (elementTimestamp
                                                < ctx.timerService().currentWatermark()) {
                                            ctx.output(lateElements, value);
                                        } else {
                                            Long previousTimestamp =
                                                    Optional.ofNullable(
                                                                    previousTimestampState.value())
                                                            .orElse(0L);
                                            assertThat(
                                                    elementTimestamp,
                                                    greaterThanOrEqualTo(previousTimestamp));
                                            previousTimestampState.update(elementTimestamp);

                                            Integer currentCount =
                                                    Optional.ofNullable(countState.get(nextTen))
                                                            .orElse(0);
                                            countState.put(nextTen, currentCount + 1);
                                        }
                                    }

                                    @Override
                                    public void onTimer(
                                            long timestamp,
                                            OnTimerContext ctx,
                                            Collector<Tuple3<Long, Integer, Integer>> out)
                                            throws Exception {
                                        out.collect(
                                                Tuple3.of(
                                                        timestamp,
                                                        ctx.getCurrentKey(),
                                                        countState.get(timestamp)));
                                        countState.remove(timestamp);

                                        // this would go in infinite loop if we did not quiesce the
                                        // timer service.
                                        ctx.timerService().registerEventTimeTimer(timestamp + 1);
                                    }
                                });

        DataStream<Integer> lateStream = sums.getSideOutput(lateElements);
        List<Integer> lateRecordsCollected =
                CollectionUtil.iteratorToList(DataStreamUtils.collect(lateStream));
        List<Tuple3<Long, Integer, Integer>> sumsCollected =
                CollectionUtil.iteratorToList(DataStreamUtils.collect(sums));

        assertTrue(lateRecordsCollected.isEmpty());
        assertThat(
                sumsCollected,
                equalTo(
                        Arrays.asList(
                                Tuple3.of(10L, 1, 8),
                                Tuple3.of(20L, 1, 6),
                                Tuple3.of(10L, 2, 4),
                                Tuple3.of(20L, 2, 2))));
    }

    private static final WatermarkGenerator<Tuple2<Integer, Integer>>
            GENERATE_WATERMARK_AFTER_4_14_TIMESTAMP =
                    new WatermarkGenerator<Tuple2<Integer, Integer>>() {
                        @Override
                        public void onEvent(
                                Tuple2<Integer, Integer> event,
                                long eventTimestamp,
                                WatermarkOutput output) {
                            if (eventTimestamp == 4) {
                                output.emitWatermark(new Watermark(5));
                            } else if (eventTimestamp == 14) {
                                output.emitWatermark(new Watermark(15));
                            }
                        }

                        @Override
                        public void onPeriodicEmit(WatermarkOutput output) {}
                    };

    private static class AssertingOperator extends AbstractStreamOperator<Long>
            implements OneInputStreamOperator<Tuple2<Integer, byte[]>, Long>, BoundedOneInput {
        private final Set<Integer> seenKeys = new HashSet<>();
        private long seenRecords = 0;
        private Integer currentKey = null;

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, byte[]>> element) throws Exception {
            this.seenRecords++;
            Integer incomingKey = element.getValue().f0;
            if (!Objects.equals(incomingKey, currentKey)) {
                if (!seenKeys.add(incomingKey)) {
                    Assert.fail("Received an out of order key: " + incomingKey);
                }
                this.currentKey = incomingKey;
            }
        }

        @Override
        public void endInput() {
            output.collect(new StreamRecord<>(seenRecords));
        }
    }

    private static class AssertingTwoInputOperator extends AbstractStreamOperator<Long>
            implements TwoInputStreamOperator<
                            Tuple2<Integer, byte[]>, Tuple2<Integer, byte[]>, Long>,
                    BoundedMultiInput {
        private final Set<Integer> seenKeys = new HashSet<>();
        private long seenRecords = 0;
        private Integer currentKey = null;
        private boolean input1Finished = false;
        private boolean input2Finished = false;

        @Override
        public void processElement1(StreamRecord<Tuple2<Integer, byte[]>> element) {
            processElement(element);
        }

        @Override
        public void processElement2(StreamRecord<Tuple2<Integer, byte[]>> element) {
            processElement(element);
        }

        private void processElement(StreamRecord<Tuple2<Integer, byte[]>> element) {
            this.seenRecords++;
            Integer incomingKey = element.getValue().f0;
            if (!Objects.equals(incomingKey, currentKey)) {
                if (!seenKeys.add(incomingKey)) {
                    Assert.fail("Received an out of order key: " + incomingKey);
                }
                this.currentKey = incomingKey;
            }
        }

        @Override
        public void endInput(int inputId) {
            if (inputId == 1) {
                input1Finished = true;
            }

            if (inputId == 2) {
                input2Finished = true;
            }

            if (input1Finished && input2Finished) {
                output.collect(new StreamRecord<>(seenRecords));
            }
        }
    }

    private static class AssertingThreeInputOperator extends AbstractStreamOperatorV2<Long>
            implements MultipleInputStreamOperator<Long>, BoundedMultiInput {
        private final Set<Integer> seenKeys = new HashSet<>();
        private long seenRecords = 0;
        private Integer currentKey = null;
        private boolean input1Finished = false;
        private boolean input2Finished = false;
        private boolean input3Finished = false;

        public AssertingThreeInputOperator(
                StreamOperatorParameters<Long> parameters, int numberOfInputs) {
            super(parameters, 3);
            assert numberOfInputs == 3;
        }

        private void processElement(Tuple2<Integer, byte[]> element) {
            this.seenRecords++;
            Integer incomingKey = element.f0;
            if (!Objects.equals(incomingKey, currentKey)) {
                if (!seenKeys.add(incomingKey)) {
                    Assert.fail("Received an out of order key: " + incomingKey);
                }
                this.currentKey = incomingKey;
            }
        }

        @Override
        public void endInput(int inputId) {
            if (inputId == 1) {
                input1Finished = true;
            }

            if (inputId == 2) {
                input2Finished = true;
            }

            if (inputId == 3) {
                input3Finished = true;
            }

            if (input1Finished && input2Finished && input3Finished) {
                output.collect(new StreamRecord<>(seenRecords));
            }
        }

        @Override
        public List<Input> getInputs() {
            return Arrays.asList(
                    new SingleInput(this::processElement),
                    new SingleInput(this::processElement),
                    new SingleInput(this::processElement));
        }
    }

    private static class AssertingThreeInputOperatorFactory implements StreamOperatorFactory<Long> {
        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Long>> T createStreamOperator(
                StreamOperatorParameters<Long> parameters) {
            return (T) new AssertingThreeInputOperator(parameters, 3);
        }

        @Override
        public void setChainingStrategy(ChainingStrategy strategy) {}

        @Override
        public ChainingStrategy getChainingStrategy() {
            return ChainingStrategy.NEVER;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return AssertingThreeInputOperator.class;
        }
    }

    private static class SingleInput implements Input<Tuple2<Integer, byte[]>> {

        private final Consumer<Tuple2<Integer, byte[]>> recordConsumer;

        private SingleInput(Consumer<Tuple2<Integer, byte[]>> recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, byte[]>> element) throws Exception {
            recordConsumer.accept(element.getValue());
        }

        @Override
        public void processWatermark(org.apache.flink.streaming.api.watermark.Watermark mark) {}

        @Override
        public void processLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void setKeyContextElement(StreamRecord<Tuple2<Integer, byte[]>> record) {}

        @Override
        public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {}
    }

    private static class InputGenerator extends SplittableIterator<Tuple2<Integer, byte[]>> {

        private final long numberOfRecords;
        private long generatedRecords;
        private final Random rnd = new Random();
        private final byte[] bytes = new byte[500];

        private InputGenerator(long numberOfRecords) {
            this.numberOfRecords = numberOfRecords;
            rnd.nextBytes(bytes);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Iterator<Tuple2<Integer, byte[]>>[] split(int numPartitions) {
            long numberOfRecordsPerPartition = numberOfRecords / numPartitions;
            long remainder = numberOfRecords % numPartitions;
            Iterator<Tuple2<Integer, byte[]>>[] iterators = new Iterator[numPartitions];

            for (int i = 0; i < numPartitions - 1; i++) {
                iterators[i] = new InputGenerator(numberOfRecordsPerPartition);
            }

            iterators[numPartitions - 1] =
                    new InputGenerator(numberOfRecordsPerPartition + remainder);

            return iterators;
        }

        @Override
        public int getMaximumNumberOfSplits() {
            return (int) Math.min(numberOfRecords, Integer.MAX_VALUE);
        }

        @Override
        public boolean hasNext() {
            return generatedRecords < numberOfRecords;
        }

        @Override
        public Tuple2<Integer, byte[]> next() {
            if (hasNext()) {
                generatedRecords++;
                return Tuple2.of(rnd.nextInt(10), bytes);
            }

            return null;
        }
    }
}
