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

package org.apache.flink.test.streaming.api.datastream.extension.window;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.connector.dsv2.WrappedSource;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.builtin.BuiltinFuncs;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.window.context.OneInputWindowContext;
import org.apache.flink.datastream.api.extension.window.context.TwoInputWindowContext;
import org.apache.flink.datastream.api.extension.window.function.OneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.function.TwoInputNonBroadcastWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.function.TwoOutputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.strategy.WindowStrategy;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test the Window extension on DataStream V2. */
class WindowITCase implements Serializable {

    private static final long NUM_ELEMENT_PER_SOURCE = 10_000;
    private static final long EXPECTED_ELEMENT_SUM_PER_SOURCE =
            (0 + (NUM_ELEMENT_PER_SOURCE - 1)) * NUM_ELEMENT_PER_SOURCE / 2;

    private transient ExecutionEnvironment env;

    @BeforeEach
    void before() throws Exception {
        env = ExecutionEnvironment.getInstance();
    }

    @AfterEach
    void after() throws Exception {
        TestCollectResultProcessFunction.elementSum = 0;
        TestCollectResultProcessFunction.elementCount = 0;
    }

    // =================== one input ==========================

    @Test
    void testOneInputWindowFunctionWithEventTimeGlobalWindow() throws Exception {
        NonKeyedPartitionStream<ValueWithTimestamp> source = getSource("source", 0, 0, true);

        OneInputStreamProcessFunction<ValueWithTimestamp, ValueWithTimestamp>
                windowProcessFunction =
                        BuiltinFuncs.window(
                                WindowStrategy.global(),
                                new OneInputWindowStreamProcessFunction<
                                        ValueWithTimestamp, ValueWithTimestamp>() {
                                    @Override
                                    public void onTrigger(
                                            Collector<ValueWithTimestamp> output,
                                            PartitionedContext<ValueWithTimestamp> ctx,
                                            OneInputWindowContext<ValueWithTimestamp> windowContext)
                                            throws Exception {
                                        Iterable<ValueWithTimestamp> allRecords =
                                                windowContext.getAllRecords();
                                        int recordCount = 0;
                                        for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                            output.collect(valueWithTimestamp);
                                            recordCount++;
                                        }
                                        assertThat(recordCount)
                                                .isEqualTo(NUM_ELEMENT_PER_SOURCE / 2);
                                    }
                                });

        source.keyBy(x -> x.value % 2)
                .process(windowProcessFunction)
                .process(new TestCollectResultProcessFunction())
                .withParallelism(1)
                .toSink(new WrappedSink<>(new DiscardingSink<>()));
        env.execute("testOneInputWindowFunctionWithEventTimeGlobalWindow");

        assertThat(TestCollectResultProcessFunction.elementSum)
                .isEqualTo(EXPECTED_ELEMENT_SUM_PER_SOURCE);
        assertThat(TestCollectResultProcessFunction.elementCount).isEqualTo(NUM_ELEMENT_PER_SOURCE);
    }

    @Test
    void testOneInputWindowFunctionWithEventTimeTumblingWindow() throws Exception {
        NonKeyedPartitionStream<ValueWithTimestamp> source = getSource("source", 0, 0, true);

        OneInputStreamProcessFunction<ValueWithTimestamp, ValueWithTimestamp>
                windowProcessFunction =
                        BuiltinFuncs.window(
                                WindowStrategy.tumbling(Duration.ofSeconds(10)),
                                new OneInputWindowStreamProcessFunction<
                                        ValueWithTimestamp, ValueWithTimestamp>() {
                                    @Override
                                    public void onTrigger(
                                            Collector<ValueWithTimestamp> output,
                                            PartitionedContext<ValueWithTimestamp> ctx,
                                            OneInputWindowContext<ValueWithTimestamp> windowContext)
                                            throws Exception {
                                        Iterable<ValueWithTimestamp> allRecords =
                                                windowContext.getAllRecords();
                                        int recordCount = 0;
                                        for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                            output.collect(valueWithTimestamp);
                                            recordCount++;
                                        }
                                        assertThat(recordCount).isEqualTo(5);
                                    }
                                });

        source.keyBy(x -> x.value % 2)
                .process(windowProcessFunction)
                .process(new TestCollectResultProcessFunction())
                .withParallelism(1)
                .toSink(new WrappedSink<>(new DiscardingSink<>()));
        env.execute("testOneInputWindowFunctionWithEventTimeTumblingWindow");

        assertThat(TestCollectResultProcessFunction.elementSum)
                .isEqualTo(EXPECTED_ELEMENT_SUM_PER_SOURCE);
        assertThat(TestCollectResultProcessFunction.elementCount).isEqualTo(NUM_ELEMENT_PER_SOURCE);
    }

    @Test
    void testOneInputWindowFunctionWithEventTimeSlidingWindow() throws Exception {
        NonKeyedPartitionStream<ValueWithTimestamp> source = getSource("source", 0, 0, true);

        OneInputStreamProcessFunction<ValueWithTimestamp, ValueWithTimestamp>
                windowProcessFunction =
                        BuiltinFuncs.window(
                                WindowStrategy.sliding(
                                        Duration.ofSeconds(10), Duration.ofSeconds(1)),
                                new OneInputWindowStreamProcessFunction<
                                        ValueWithTimestamp, ValueWithTimestamp>() {
                                    long preWindowStartTime = -1;

                                    @Override
                                    public void onTrigger(
                                            Collector<ValueWithTimestamp> output,
                                            PartitionedContext<ValueWithTimestamp> ctx,
                                            OneInputWindowContext<ValueWithTimestamp> windowContext)
                                            throws Exception {
                                        Iterable<ValueWithTimestamp> allRecords =
                                                windowContext.getAllRecords();
                                        for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                            output.collect(valueWithTimestamp);
                                        }
                                        if (preWindowStartTime != -1) {
                                            assertThat(
                                                            windowContext.getStartTime()
                                                                    - preWindowStartTime)
                                                    .isEqualTo(1000);
                                            preWindowStartTime = windowContext.getStartTime();
                                        }
                                    }
                                });

        source.keyBy(x -> x.value % 2)
                .process(windowProcessFunction)
                .toSink(new WrappedSink<>(new DiscardingSink<>()));
        env.execute("testOneInputWindowFunctionWithEventTimeSlidingWindow");
    }

    @Test
    void testOneInputWindowFunctionWithEventTimeSessionWindow() throws Exception {
        NonKeyedPartitionStream<ValueWithTimestamp> source = getSource("source", 5, 5, true);

        OneInputStreamProcessFunction<ValueWithTimestamp, ValueWithTimestamp>
                windowProcessFunction =
                        BuiltinFuncs.window(
                                WindowStrategy.session(Duration.ofSeconds(3)),
                                new OneInputWindowStreamProcessFunction<
                                        ValueWithTimestamp, ValueWithTimestamp>() {
                                    @Override
                                    public void onTrigger(
                                            Collector<ValueWithTimestamp> output,
                                            PartitionedContext<ValueWithTimestamp> ctx,
                                            OneInputWindowContext<ValueWithTimestamp> windowContext)
                                            throws Exception {
                                        Iterable<ValueWithTimestamp> allRecords =
                                                windowContext.getAllRecords();
                                        int recordCount = 0;
                                        for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                            output.collect(valueWithTimestamp);
                                            recordCount++;
                                        }
                                        assertThat(recordCount).isEqualTo(5);
                                    }
                                });

        source.keyBy(x -> 0)
                .process(windowProcessFunction)
                .process(new TestCollectResultProcessFunction())
                .withParallelism(1)
                .toSink(new WrappedSink<>(new DiscardingSink<>()));
        env.execute("testOneInputWindowFunctionWithEventTimeSessionWindow");

        assertThat(TestCollectResultProcessFunction.elementSum)
                .isEqualTo(EXPECTED_ELEMENT_SUM_PER_SOURCE);
        assertThat(TestCollectResultProcessFunction.elementCount).isEqualTo(NUM_ELEMENT_PER_SOURCE);
    }

    @Test
    void testOneInputWindowFunctionUseWindowStateToAgg() throws Exception {
        NonKeyedPartitionStream<ValueWithTimestamp> source = getSource("source", 0, 0, true);

        OneInputStreamProcessFunction<ValueWithTimestamp, ValueWithTimestamp>
                windowProcessFunction =
                        BuiltinFuncs.window(
                                WindowStrategy.tumbling(Duration.ofSeconds(5)),
                                new OneInputWindowStreamProcessFunction<
                                        ValueWithTimestamp, ValueWithTimestamp>() {

                                    private ValueStateDeclaration<Long> sumStateDeclaration =
                                            StateDeclarations.valueState(
                                                    "sum", TypeDescriptors.LONG);
                                    private ValueStateDeclaration<Long> countStateDeclaration =
                                            StateDeclarations.valueState(
                                                    "count", TypeDescriptors.LONG);

                                    @Override
                                    public Set<StateDeclaration> useWindowStates() {
                                        return Set.of(sumStateDeclaration, countStateDeclaration);
                                    }

                                    @Override
                                    public void onRecord(
                                            ValueWithTimestamp record,
                                            Collector<ValueWithTimestamp> output,
                                            PartitionedContext<ValueWithTimestamp> ctx,
                                            OneInputWindowContext<ValueWithTimestamp> windowContext)
                                            throws Exception {
                                        Optional<ValueState<Long>> countState =
                                                windowContext.getWindowState(countStateDeclaration);
                                        if (countState.isPresent()
                                                && countState.get().value() != null) {
                                            countState.get().update(countState.get().value() + 1);
                                        } else {
                                            countState.get().update(1L);
                                        }

                                        Optional<ValueState<Long>> sumState =
                                                windowContext.getWindowState(sumStateDeclaration);
                                        long sum = 0;
                                        if (sumState.isPresent()
                                                && sumState.get().value() != null) {
                                            sum = sumState.get().value();
                                        }
                                        sumState.get().update(sum + record.getValue());
                                    }

                                    @Override
                                    public void onTrigger(
                                            Collector<ValueWithTimestamp> output,
                                            PartitionedContext<ValueWithTimestamp> ctx,
                                            OneInputWindowContext<ValueWithTimestamp> windowContext)
                                            throws Exception {
                                        Optional<ValueState<Long>> countState =
                                                windowContext.getWindowState(countStateDeclaration);
                                        assertThat(countState.get().value()).isEqualTo(5);

                                        Optional<ValueState<Long>> sumState =
                                                windowContext.getWindowState(sumStateDeclaration);
                                        output.collect(
                                                new ValueWithTimestamp(
                                                        sumState.get().value(), -1L));
                                    }
                                });

        source.keyBy(x -> 0)
                .process(windowProcessFunction)
                .process(new TestCollectResultProcessFunction())
                .withParallelism(1)
                .toSink(new WrappedSink<>(new DiscardingSink<>()));
        env.execute("testOneInputWindowFunctionUseWindowStateToAgg");

        assertThat(TestCollectResultProcessFunction.elementSum)
                .isEqualTo(EXPECTED_ELEMENT_SUM_PER_SOURCE);
        assertThat(TestCollectResultProcessFunction.elementCount)
                .isEqualTo(NUM_ELEMENT_PER_SOURCE / 5);
    }

    @Test
    void testOneInputWindowFunctionWithEventTimeTumblingWindowUseGlobalStream() throws Exception {
        NonKeyedPartitionStream<ValueWithTimestamp> source = getSource("source", 0, 0, true);

        OneInputStreamProcessFunction<ValueWithTimestamp, ValueWithTimestamp>
                windowProcessFunction =
                        BuiltinFuncs.window(
                                WindowStrategy.tumbling(Duration.ofSeconds(5)),
                                new OneInputWindowStreamProcessFunction<
                                        ValueWithTimestamp, ValueWithTimestamp>() {
                                    @Override
                                    public void onTrigger(
                                            Collector<ValueWithTimestamp> output,
                                            PartitionedContext<ValueWithTimestamp> ctx,
                                            OneInputWindowContext<ValueWithTimestamp> windowContext)
                                            throws Exception {
                                        Iterable<ValueWithTimestamp> allRecords =
                                                windowContext.getAllRecords();
                                        int recordCount = 0;
                                        for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                            output.collect(valueWithTimestamp);
                                            recordCount++;
                                        }
                                        assertThat(recordCount).isEqualTo(5);
                                    }
                                });

        source.global()
                .process(windowProcessFunction)
                .process(new TestCollectResultProcessFunction())
                .withParallelism(1)
                .toSink(new WrappedSink<>(new DiscardingSink<>()));
        env.execute("testOneInputWindowFunctionWithEventTimeTumblingWindowUseGlobalStream");

        assertThat(TestCollectResultProcessFunction.elementSum)
                .isEqualTo(EXPECTED_ELEMENT_SUM_PER_SOURCE);
        assertThat(TestCollectResultProcessFunction.elementCount).isEqualTo(NUM_ELEMENT_PER_SOURCE);
    }

    // ======================= two input ==========================

    @Test
    void testTwoInputWindowFunctionWithEventTimeTumblingWindow() throws Exception {
        NonKeyedPartitionStream<ValueWithTimestamp> source1 = getSource("source1", 0, 0, true);
        NonKeyedPartitionStream<ValueWithTimestamp> source2 = getSource("source2", 0, 0, true);

        TwoInputNonBroadcastStreamProcessFunction<
                        ValueWithTimestamp, ValueWithTimestamp, ValueWithTimestamp>
                windowProcessFunction =
                        BuiltinFuncs.window(
                                WindowStrategy.tumbling(Duration.ofSeconds(10)),
                                new TwoInputNonBroadcastWindowStreamProcessFunction<
                                        ValueWithTimestamp,
                                        ValueWithTimestamp,
                                        ValueWithTimestamp>() {

                                    @Override
                                    public void onTrigger(
                                            Collector<ValueWithTimestamp> output,
                                            PartitionedContext<ValueWithTimestamp> ctx,
                                            TwoInputWindowContext<
                                                            ValueWithTimestamp, ValueWithTimestamp>
                                                    windowContext)
                                            throws Exception {
                                        Iterable<ValueWithTimestamp> allRecords1 =
                                                windowContext.getAllRecords1();
                                        int recordCount1 = 0;
                                        for (ValueWithTimestamp valueWithTimestamp : allRecords1) {
                                            output.collect(valueWithTimestamp);
                                            recordCount1++;
                                        }
                                        assertThat(recordCount1).isEqualTo(5);

                                        Iterable<ValueWithTimestamp> allRecords2 =
                                                windowContext.getAllRecords2();
                                        int recordCount2 = 0;
                                        for (ValueWithTimestamp valueWithTimestamp : allRecords2) {
                                            output.collect(valueWithTimestamp);
                                            recordCount2++;
                                        }
                                        assertThat(recordCount2).isEqualTo(5);
                                    }
                                });

        source1.keyBy(x -> x.value % 2)
                .connectAndProcess(source2.keyBy(x -> x.value % 2), windowProcessFunction)
                .process(new TestCollectResultProcessFunction())
                .withParallelism(1)
                .toSink(new WrappedSink<>(new DiscardingSink<>()));
        env.execute("testTwoInputWindowFunctionWithEventTimeTumblingWindow");

        assertThat(TestCollectResultProcessFunction.elementSum)
                .isEqualTo(EXPECTED_ELEMENT_SUM_PER_SOURCE * 2);
        assertThat(TestCollectResultProcessFunction.elementCount)
                .isEqualTo(NUM_ELEMENT_PER_SOURCE * 2);
    }

    // ======================= two output ==========================

    @Test
    void testTwoOutputWindowFunctionWithEventTimeTumblingWindow() throws Exception {
        NonKeyedPartitionStream<ValueWithTimestamp> source = getSource("source", 0, 0, true);

        TwoOutputStreamProcessFunction<ValueWithTimestamp, ValueWithTimestamp, ValueWithTimestamp>
                windowProcessFunction =
                        BuiltinFuncs.window(
                                WindowStrategy.tumbling(Duration.ofSeconds(10)),
                                new TwoOutputWindowStreamProcessFunction<
                                        ValueWithTimestamp,
                                        ValueWithTimestamp,
                                        ValueWithTimestamp>() {

                                    @Override
                                    public void onTrigger(
                                            Collector<ValueWithTimestamp> output1,
                                            Collector<ValueWithTimestamp> output2,
                                            TwoOutputPartitionedContext<
                                                            ValueWithTimestamp, ValueWithTimestamp>
                                                    ctx,
                                            OneInputWindowContext<ValueWithTimestamp> windowContext)
                                            throws Exception {
                                        Iterable<ValueWithTimestamp> allRecords =
                                                windowContext.getAllRecords();
                                        int recordCount = 0;
                                        for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                            output1.collect(valueWithTimestamp);
                                            output2.collect(valueWithTimestamp);
                                            recordCount++;
                                        }
                                        assertThat(recordCount).isEqualTo(5);
                                    }
                                });

        NonKeyedPartitionStream.ProcessConfigurableAndTwoNonKeyedPartitionStream<
                        ValueWithTimestamp, ValueWithTimestamp>
                twoOutputStream = source.keyBy(x -> x.value % 2).process(windowProcessFunction);

        twoOutputStream
                .getFirst()
                .process(new TestCollectResultProcessFunction())
                .withParallelism(1)
                .toSink(new WrappedSink<>(new DiscardingSink<>()));

        twoOutputStream
                .getSecond()
                .process(new TestCollectResultProcessFunction())
                .withParallelism(1)
                .toSink(new WrappedSink<>(new DiscardingSink<>()));

        env.execute("testTwoOutputWindowFunctionWithEventTimeTumblingWindow");

        assertThat(TestCollectResultProcessFunction.elementSum)
                .isEqualTo(EXPECTED_ELEMENT_SUM_PER_SOURCE * 2);
        assertThat(TestCollectResultProcessFunction.elementCount)
                .isEqualTo(NUM_ELEMENT_PER_SOURCE * 2);
    }

    public static class ValueWithTimestamp {

        private final long value;

        /** The event time of the element in milliseconds. */
        private final long timestamp;

        public ValueWithTimestamp(long value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        public long getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "ValueWithTimestamp{" + "value=" + value + ", timestamp=" + timestamp + '}';
        }
    }

    /**
     * {@link TestGeneratorFunction} is used to generate test data. The first data he generates is
     * ValueWithTimestamp{value=0, timestamp=0}, each subsequent data than the previous data, value
     * increased by one, timestamp increased by one second.
     */
    private static class TestGeneratorFunction
            implements GeneratorFunction<Long, ValueWithTimestamp> {

        private long curTime = 0L;

        private int curValue = 0;

        // Every {@code countStep} elements, time is incremented by {@code timeStep} seconds. Used
        // to simulate the presence of gaps in the time of elements in the SessionWindow.
        private long countStep;

        private long timeStep;

        private int generatedElementCount = 0;

        public TestGeneratorFunction() {
            this(0, 0);
        }

        public TestGeneratorFunction(long countStep, long timeStep) {
            this.countStep = countStep;
            this.timeStep = timeStep;
        }

        @Override
        public ValueWithTimestamp map(Long value) throws Exception {
            ValueWithTimestamp element = new ValueWithTimestamp(curValue, curTime);
            curValue++;
            curTime = curTime + Duration.ofSeconds(1).toMillis();
            generatedElementCount++;
            if (countStep != 0 && generatedElementCount % countStep == 0) {
                curTime += Duration.ofSeconds(timeStep).toMillis();
            }
            return element;
        }
    }

    private NonKeyedPartitionStream<ValueWithTimestamp> getSource(
            String sourceName, long countStep, long timeStep, boolean isEventTime) {
        NonKeyedPartitionStream<ValueWithTimestamp> stream =
                env.fromSource(
                        new WrappedSource<ValueWithTimestamp>(
                                new DataGeneratorSource<ValueWithTimestamp>(
                                        new TestGeneratorFunction(countStep, timeStep),
                                        NUM_ELEMENT_PER_SOURCE,
                                        TypeInformation.of(ValueWithTimestamp.class))),
                        sourceName);

        if (isEventTime) {
            stream =
                    stream.process(
                            EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                            element -> element.getTimestamp())
                                    .perEventWatermark()
                                    .buildAsProcessFunction());
        }
        return stream;
    }

    private static class TestOneInputWindowStreamProcessFunction
            implements OneInputWindowStreamProcessFunction<ValueWithTimestamp, ValueWithTimestamp> {

        private final boolean checkElementCountWhenTrigger;
        private final int expectElementCountWhenTrigger;
        private final boolean skipCheckWhenFirstTimeTrigger;
        private boolean isFirstTimeTrigger = true;

        private TestOneInputWindowStreamProcessFunction(
                boolean checkElementCountWhenTrigger,
                int expectElementCountWhenTrigger,
                boolean skipCheckWhenFirstTimeTrigger) {
            this.checkElementCountWhenTrigger = checkElementCountWhenTrigger;
            this.expectElementCountWhenTrigger = expectElementCountWhenTrigger;
            this.skipCheckWhenFirstTimeTrigger = skipCheckWhenFirstTimeTrigger;
        }

        @Override
        public void onTrigger(
                Collector<ValueWithTimestamp> output,
                PartitionedContext<ValueWithTimestamp> ctx,
                OneInputWindowContext<ValueWithTimestamp> windowContext)
                throws Exception {
            int elementCount = 0;
            Iterable<ValueWithTimestamp> allRecords = windowContext.getAllRecords();
            for (ValueWithTimestamp record : allRecords) {
                output.collect(record);
                elementCount++;
            }
            if (checkElementCountWhenTrigger
                    && (!skipCheckWhenFirstTimeTrigger || !isFirstTimeTrigger)) {
                assertThat(elementCount).isEqualTo(expectElementCountWhenTrigger);
            }

            isFirstTimeTrigger = false;
        }
    }

    private static class TestCollectResultProcessFunction
            implements OneInputStreamProcessFunction<ValueWithTimestamp, ValueWithTimestamp> {
        public static long elementSum;
        public static long elementCount;

        @Override
        public void processRecord(
                ValueWithTimestamp record,
                Collector<ValueWithTimestamp> output,
                PartitionedContext<ValueWithTimestamp> ctx)
                throws Exception {
            elementCount++;
            elementSum += record.getValue();
        }
    }
}
