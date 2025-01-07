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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateStreamOperatorTest.TestKeySelector;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateStreamOperatorV2;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.ElementOrder;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributesBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.asyncprocessing.AsyncKeyedMultiInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.asyncprocessing.AsyncKeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.BiFunctionWithException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.state.StateBackendTestUtils.buildAsyncStateBackend;
import static org.assertj.core.api.Assertions.assertThat;

/** Basic tests for {@link AbstractAsyncStateStreamOperatorV2}. */
class AbstractAsyncStateStreamOperatorV2Test {

    protected KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
            createTestHarness(
                    int maxParalelism, int numSubtasks, int subtaskIndex, ElementOrder elementOrder)
                    throws Exception {
        KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness =
                        KeyedOneInputStreamOperatorV2TestHarness.create(
                                new TestOperatorFactory(elementOrder),
                                new TestKeySelector(),
                                BasicTypeInfo.INT_TYPE_INFO,
                                maxParalelism,
                                numSubtasks,
                                subtaskIndex);
        testHarness.setStateBackend(buildAsyncStateBackend(new HashMapStateBackend()));
        return testHarness;
    }

    @Test
    void testCreateAsyncExecutionController() throws Exception {
        try (KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            assertThat(testHarness.getBaseOperator())
                    .isInstanceOf(AbstractAsyncStateStreamOperatorV2.class);
            assertThat(
                            ((AbstractAsyncStateStreamOperatorV2) testHarness.getBaseOperator())
                                    .getAsyncExecutionController())
                    .isNotNull();
            assertThat(
                            ((AbstractAsyncStateStreamOperatorV2) testHarness.getBaseOperator())
                                    .getAsyncExecutionController()
                                    .getStateExecutor())
                    .isNotNull();
        }
    }

    @Test
    void testRecordProcessorWithFirstStateOrder() throws Exception {
        try (KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.FIRST_STATE_ORDER)) {
            testHarness.open();
            SingleInputTestOperator testOperator =
                    (SingleInputTestOperator) testHarness.getBaseOperator();
            CompletableFuture<Void> future =
                    testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(5, "5")));

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(1);
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(1);

            // Proceed processing
            testOperator.proceed();
            future.get();
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);
        }
    }

    @Test
    void testRecordProcessorWithRecordOrder() throws Exception {
        try (KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            SingleInputTestOperator testOperator =
                    (SingleInputTestOperator) testHarness.getBaseOperator();
            CompletableFuture<Void> future =
                    testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(5, "5")));

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(1);
            // Why greater than 1:  +1 when enter the processor; +1 when handle the SYNC_POINT
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount())
                    .isGreaterThan(1);

            // Proceed processing
            testOperator.proceed();
            future.get();
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);
        }
    }

    @Test
    void testAsyncProcessWithKey() throws Exception {
        KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness =
                        KeyedOneInputStreamOperatorV2TestHarness.create(
                                new TestDirectAsyncProcessOperatorFactory(
                                        ElementOrder.RECORD_ORDER),
                                new TestKeySelector(),
                                BasicTypeInfo.INT_TYPE_INFO,
                                128,
                                1,
                                0);
        testHarness.setStateBackend(buildAsyncStateBackend(new HashMapStateBackend()));
        try {
            testHarness.open();
            SingleInputTestOperatorDirectAsyncProcess testOperator =
                    (SingleInputTestOperatorDirectAsyncProcess) testHarness.getBaseOperator();
            CompletableFuture<Void> future =
                    testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(5, "5")));

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(0);
            // Why greater than 1:  +1 when handle the SYNC_POINT; then accept +1
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount())
                    .isGreaterThan(1);

            // Proceed processing
            testOperator.proceed();
            future.get();
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);

            // We don't have the mailbox executor actually running, so the new context is blocked
            // and never triggered.
            assertThat(testOperator.getProcessed()).isEqualTo(1);
        } finally {
            testHarness.close();
        }
    }

    @Test
    void testCheckpointDrain() throws Exception {
        try (KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            SingleInputTestOperator testOperator =
                    (SingleInputTestOperator) testHarness.getBaseOperator();
            AsyncExecutionController asyncExecutionController =
                    testOperator.getAsyncExecutionController();
            testOperator.setAsyncKeyedContextElement(
                    new StreamRecord<>(Tuple2.of(5, "5")), new TestKeySelector());
            asyncExecutionController.handleRequest(null, StateRequestType.VALUE_GET, null);
            testOperator.postProcessElement();
            assertThat(asyncExecutionController.getInFlightRecordNum()).isEqualTo(1);
            testHarness.drainStateRequests();
            assertThat(asyncExecutionController.getInFlightRecordNum()).isEqualTo(0);
        }
    }

    @Test
    void testTimerServiceIsAsync() throws Exception {
        try (KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            assertThat(testHarness.getBaseOperator())
                    .isInstanceOf(AbstractAsyncStateStreamOperatorV2.class);
            Triggerable triggerable =
                    new Triggerable() {
                        @Override
                        public void onEventTime(InternalTimer timer) throws Exception {}

                        @Override
                        public void onProcessingTime(InternalTimer timer) throws Exception {}
                    };
            assertThat(
                            ((AbstractAsyncStateStreamOperatorV2) testHarness.getBaseOperator())
                                    .getInternalTimerService(
                                            "test", VoidNamespaceSerializer.INSTANCE, triggerable))
                    .isInstanceOf(InternalTimerServiceAsyncImpl.class);
        }
    }

    @Test
    void testNonRecordProcess() throws Exception {
        try (KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            SingleInputTestOperator testOperator =
                    (SingleInputTestOperator) testHarness.getBaseOperator();
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(5, "5")));
            CompletableFuture<Void> future =
                    testHarness.processRecordAttributesInternal(new RecordAttributes(false));

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(1);
            assertThat(testOperator.getAttributeProcessed()).isEqualTo(0);

            // Proceed processing
            testOperator.proceed();
            future.get();
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);
            assertThat(testOperator.getAttributeProcessed()).isEqualTo(1);
        }
    }

    @Test
    void testWatermark() throws Exception {
        KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness =
                        KeyedOneInputStreamOperatorV2TestHarness.create(
                                new TestWithAsyncProcessTimerOperatorFactory(
                                        ElementOrder.RECORD_ORDER),
                                new TestKeySelector(),
                                BasicTypeInfo.INT_TYPE_INFO,
                                128,
                                1,
                                0);
        testHarness.setStateBackend(buildAsyncStateBackend(new HashMapStateBackend()));

        try {
            testHarness.open();
            ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(1, "1")));
            expectedOutput.add(new StreamRecord<>("EventTimer-1-1"));
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(1, "3")));
            expectedOutput.add(new StreamRecord<>("EventTimer-1-3"));
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(1, "6")));
            expectedOutput.add(new StreamRecord<>("EventTimer-1-6"));
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(1, "9")));
            expectedOutput.add(new StreamRecord<>("EventTimer-1-9"));
            testHarness.processWatermark(10L);
            expectedOutput.add(new Watermark(10L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        } finally {
            testHarness.close();
        }
    }

    @Test
    void testWatermarkHooks() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        KeySelector<Long, Integer> dummyKeySelector = l -> 0;
        List<KeySelector<?, Integer>> keySelectors =
                Arrays.asList(dummyKeySelector, dummyKeySelector, dummyKeySelector);

        WatermarkTestingOperatorFactory factory = new WatermarkTestingOperatorFactory();
        AtomicInteger counter = new AtomicInteger(0);
        factory.setPreProcessFunction(
                (operator, watermark) -> {
                    operator.asyncProcessWithKey(
                            1L,
                            () -> {
                                assertThat(operator.getCurrentKey()).isEqualTo(1L);
                                operator.output(watermark.getTimestamp() + 1000L);
                            });
                    if (counter.incrementAndGet() % 2 == 0) {
                        return null;
                    } else {
                        return new Watermark(watermark.getTimestamp() + 1L);
                    }
                });

        factory.setPostProcessFunction(
                (operator, watermark) -> {
                    operator.output(watermark.getTimestamp() + 100L);
                });
        try (AsyncKeyedMultiInputStreamOperatorTestHarness<Integer, Long> testHarness =
                AsyncKeyedMultiInputStreamOperatorTestHarness.create(
                        factory, BasicTypeInfo.INT_TYPE_INFO, keySelectors, 1, 1, 0)) {
            testHarness.setup();
            testHarness.open();
            testHarness.processElement(0, new StreamRecord<>(1L, 1L));
            testHarness.processElement(0, new StreamRecord<>(3L, 3L));
            testHarness.processElement(0, new StreamRecord<>(4L, 4L));
            testHarness.processWatermark(0, new Watermark(2L));
            testHarness.processWatermark(1, new Watermark(2L));
            testHarness.processWatermark(2, new Watermark(2L));
            expectedOutput.add(new StreamRecord<>(1002L));
            expectedOutput.add(new StreamRecord<>(1L));
            expectedOutput.add(new StreamRecord<>(3L));
            expectedOutput.add(new Watermark(3L));
            expectedOutput.add(new StreamRecord<>(103L));
            testHarness.processWatermark(0, new Watermark(4L));
            testHarness.processWatermark(1, new Watermark(4L));
            testHarness.processWatermark(2, new Watermark(4L));
            expectedOutput.add(new StreamRecord<>(1004L));
            testHarness.processWatermark(0, new Watermark(5L));
            testHarness.processWatermark(1, new Watermark(5L));
            testHarness.processWatermark(2, new Watermark(5L));
            expectedOutput.add(new StreamRecord<>(1005L));
            expectedOutput.add(new StreamRecord<>(4L));
            expectedOutput.add(new Watermark(6L));
            expectedOutput.add(new StreamRecord<>(106L));

            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    void testWatermarkStatus() throws Exception {
        try (KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            SingleInputTestOperator testOperator =
                    (SingleInputTestOperator) testHarness.getBaseOperator();
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(5, "5")));
            testHarness.processWatermarkInternal(new Watermark(205L));
            CompletableFuture<Void> future =
                    testHarness.processWatermarkStatusInternal(WatermarkStatus.IDLE);

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(1);
            assertThat(testOperator.watermarkIndex).isEqualTo(-1);
            assertThat(testOperator.watermarkStatus.isIdle()).isTrue();

            // Proceed processing
            testOperator.proceed();
            future.get();
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);
            assertThat(testOperator.watermarkStatus.isActive()).isFalse();
            assertThat(testHarness.getOutput())
                    .containsExactly(
                            new StreamRecord<>("EventTimer-5-105"),
                            new Watermark(205L),
                            WatermarkStatus.IDLE);
        }
    }

    @Test
    void testIdleWatermarkHandling() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        KeySelector<Long, Integer> dummyKeySelector = l -> 0;
        List<KeySelector<?, Integer>> keySelectors =
                Arrays.asList(dummyKeySelector, dummyKeySelector, dummyKeySelector);
        try (AsyncKeyedMultiInputStreamOperatorTestHarness<Integer, Long> testHarness =
                AsyncKeyedMultiInputStreamOperatorTestHarness.create(
                        new WatermarkTestingOperatorFactory(),
                        BasicTypeInfo.INT_TYPE_INFO,
                        keySelectors,
                        1,
                        1,
                        0)) {
            testHarness.setup();
            testHarness.open();
            testHarness.processElement(0, new StreamRecord<>(1L, 1L));
            testHarness.processElement(0, new StreamRecord<>(3L, 3L));
            testHarness.processElement(0, new StreamRecord<>(4L, 4L));
            testHarness.processWatermark(0, new Watermark(1L));
            assertThat(testHarness.getOutput()).isEmpty();

            testHarness.processWatermarkStatus(1, WatermarkStatus.IDLE);
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
            testHarness.processWatermarkStatus(2, WatermarkStatus.IDLE);
            expectedOutput.add(new StreamRecord<>(1L));
            expectedOutput.add(new Watermark(1L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());

            testHarness.processWatermark(0, new Watermark(3L));
            expectedOutput.add(new StreamRecord<>(3L));
            expectedOutput.add(new Watermark(3L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());

            testHarness.processWatermarkStatus(1, WatermarkStatus.ACTIVE);
            // the other input is active now, we should not emit the watermark
            testHarness.processWatermark(0, new Watermark(4L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    void testIdlenessForwarding() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        KeySelector<Long, Integer> dummyKeySelector = l -> 0;
        List<KeySelector<?, Integer>> keySelectors =
                Arrays.asList(dummyKeySelector, dummyKeySelector, dummyKeySelector);
        try (AsyncKeyedMultiInputStreamOperatorTestHarness<Integer, Long> testHarness =
                AsyncKeyedMultiInputStreamOperatorTestHarness.create(
                        new WatermarkTestingOperatorFactory(),
                        BasicTypeInfo.INT_TYPE_INFO,
                        keySelectors,
                        1,
                        1,
                        0)) {

            testHarness.setup();
            testHarness.open();

            testHarness.processWatermarkStatus(0, WatermarkStatus.IDLE);
            testHarness.processWatermarkStatus(1, WatermarkStatus.IDLE);
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
            testHarness.processWatermarkStatus(2, WatermarkStatus.IDLE);
            expectedOutput.add(WatermarkStatus.IDLE);
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    void testRecordAttributesForwarding() throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        KeySelector<Long, Integer> dummyKeySelector = l -> 0;
        List<KeySelector<?, Integer>> keySelectors =
                Arrays.asList(dummyKeySelector, dummyKeySelector, dummyKeySelector);
        try (AsyncKeyedMultiInputStreamOperatorTestHarness<Integer, Long> testHarness =
                AsyncKeyedMultiInputStreamOperatorTestHarness.create(
                        new WatermarkTestingOperatorFactory(),
                        BasicTypeInfo.INT_TYPE_INFO,
                        keySelectors,
                        1,
                        1,
                        0)) {

            testHarness.setup();
            testHarness.open();

            final RecordAttributes backlogRecordAttributes =
                    new RecordAttributesBuilder(Collections.emptyList()).setBacklog(true).build();
            final RecordAttributes nonBacklogRecordAttributes =
                    new RecordAttributesBuilder(Collections.emptyList()).setBacklog(false).build();

            testHarness.processRecordAttributes(0, backlogRecordAttributes);
            testHarness.processRecordAttributes(1, backlogRecordAttributes);
            testHarness.processRecordAttributes(2, backlogRecordAttributes);
            expectedOutput.add(backlogRecordAttributes);
            expectedOutput.add(backlogRecordAttributes);
            expectedOutput.add(backlogRecordAttributes);

            testHarness.processRecordAttributes(0, nonBacklogRecordAttributes);
            testHarness.processRecordAttributes(1, nonBacklogRecordAttributes);
            testHarness.processRecordAttributes(2, nonBacklogRecordAttributes);
            expectedOutput.add(backlogRecordAttributes);
            expectedOutput.add(backlogRecordAttributes);
            expectedOutput.add(nonBacklogRecordAttributes);

            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    private static class KeyedOneInputStreamOperatorV2TestHarness<K, IN, OUT>
            extends AsyncKeyedOneInputStreamOperatorTestHarness<K, IN, OUT> {
        public static <K, IN, OUT> KeyedOneInputStreamOperatorV2TestHarness<K, IN, OUT> create(
                StreamOperatorFactory<OUT> operatorFactory,
                final KeySelector<IN, K> keySelector,
                TypeInformation<K> keyType,
                int maxParallelism,
                int numSubtasks,
                int subtaskIndex)
                throws Exception {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            CompletableFuture<KeyedOneInputStreamOperatorV2TestHarness<K, IN, OUT>> future =
                    new CompletableFuture<>();
            executorService.execute(
                    () -> {
                        try {
                            future.complete(
                                    new KeyedOneInputStreamOperatorV2TestHarness<>(
                                            executorService,
                                            operatorFactory,
                                            keySelector,
                                            keyType,
                                            maxParallelism,
                                            numSubtasks,
                                            subtaskIndex));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            return future.get();
        }

        public KeyedOneInputStreamOperatorV2TestHarness(
                ExecutorService executorService,
                StreamOperatorFactory<OUT> operatorFactory,
                final KeySelector<IN, K> keySelector,
                TypeInformation<K> keyType,
                int maxParallelism,
                int numSubtasks,
                int subtaskIndex)
                throws Exception {
            super(
                    executorService,
                    operatorFactory,
                    keySelector,
                    keyType,
                    maxParallelism,
                    numSubtasks,
                    subtaskIndex);
        }

        public StreamOperator<OUT> getBaseOperator() {
            return operator;
        }
    }

    private static class TestOperatorFactory extends AbstractStreamOperatorFactory<String> {

        private final ElementOrder elementOrder;

        TestOperatorFactory(ElementOrder elementOrder) {
            this.elementOrder = elementOrder;
        }

        @Override
        public <T extends StreamOperator<String>> T createStreamOperator(
                StreamOperatorParameters<String> parameters) {
            return (T) new SingleInputTestOperator(parameters, elementOrder);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return SingleInputTestOperator.class;
        }
    }

    /**
     * Testing operator that can respond to commands by either setting/deleting state, emitting
     * state or setting timers.
     */
    private static class SingleInputTestOperator extends AbstractAsyncStateStreamOperatorV2<String>
            implements MultipleInputStreamOperator<String>, Triggerable<Integer, VoidNamespace> {

        private static final long serialVersionUID = 1L;

        final AtomicInteger processed = new AtomicInteger(0);
        final AtomicInteger attributeProcessed = new AtomicInteger(0);

        private final ElementOrder elementOrder;

        final Object objectToWait = new Object();

        Input input;

        private WatermarkStatus watermarkStatus = new WatermarkStatus(-1);
        private int watermarkIndex = -1;

        InternalTimerService<VoidNamespace> timerService;

        public SingleInputTestOperator(
                StreamOperatorParameters<String> parameters, ElementOrder elementOrder) {
            super(parameters, 1);
            this.elementOrder = elementOrder;
            input =
                    new AbstractInput<Tuple2<Integer, String>, String>(this, 1) {
                        @Override
                        public void processElement(StreamRecord<Tuple2<Integer, String>> element)
                                throws Exception {
                            processed.incrementAndGet();
                            synchronized (objectToWait) {
                                objectToWait.wait();
                            }
                            timerService.registerEventTimeTimer(
                                    VoidNamespace.INSTANCE, element.getValue().f0 + 100L);
                        }
                    };
        }

        @Override
        public void open() throws Exception {
            super.open();
            this.timerService =
                    getInternalTimerService(
                            "processing timer", VoidNamespaceSerializer.INSTANCE, this);
        }

        @Override
        public List<Input> getInputs() {
            return Collections.singletonList(input);
        }

        @Override
        public ElementOrder getElementOrder() {
            return elementOrder;
        }

        @Override
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            assertThat(getCurrentKey()).isEqualTo(timer.getKey());
            output.collect(
                    new StreamRecord<>(
                            "EventTimer-" + timer.getKey() + "-" + timer.getTimestamp()));
        }

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            assertThat(getCurrentKey()).isEqualTo(timer.getKey());
            output.collect(
                    new StreamRecord<>(
                            "ProcessingTimer-" + timer.getKey() + "-" + timer.getTimestamp()));
        }

        @Override
        public void processRecordAttributes(RecordAttributes recordAttributes, int inputId)
                throws Exception {
            super.processRecordAttributes(recordAttributes, inputId);
            this.attributeProcessed.incrementAndGet();
        }

        @Override
        public void processWatermarkStatus(WatermarkStatus watermarkStatus, int index)
                throws Exception {
            super.processWatermarkStatus(watermarkStatus, index);
            this.watermarkStatus = watermarkStatus;
            this.watermarkIndex = index;
        }

        public int getProcessed() {
            return processed.get();
        }

        public int getAttributeProcessed() {
            return attributeProcessed.get();
        }

        public void proceed() {
            synchronized (objectToWait) {
                objectToWait.notify();
            }
        }
    }

    private static class TestDirectAsyncProcessOperatorFactory
            extends AbstractStreamOperatorFactory<String> {

        private final ElementOrder elementOrder;

        TestDirectAsyncProcessOperatorFactory(ElementOrder elementOrder) {
            this.elementOrder = elementOrder;
        }

        @Override
        public <T extends StreamOperator<String>> T createStreamOperator(
                StreamOperatorParameters<String> parameters) {
            return (T) new SingleInputTestOperatorDirectAsyncProcess(parameters, elementOrder);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return SingleInputTestOperatorDirectAsyncProcess.class;
        }
    }

    private static class SingleInputTestOperatorDirectAsyncProcess extends SingleInputTestOperator {

        SingleInputTestOperatorDirectAsyncProcess(
                StreamOperatorParameters<String> parameters, ElementOrder elementOrder) {
            super(parameters, elementOrder);
            input =
                    new AbstractInput<Tuple2<Integer, String>, String>(this, 1) {
                        @Override
                        public void processElement(StreamRecord<Tuple2<Integer, String>> element)
                                throws Exception {
                            asyncProcessWithKey(
                                    element.getValue().f0,
                                    () -> {
                                        processed.incrementAndGet();
                                    });
                            synchronized (objectToWait) {
                                objectToWait.wait();
                            }
                            processed.incrementAndGet();
                        }
                    };
        }
    }

    private static class TestWithAsyncProcessTimerOperatorFactory
            extends AbstractStreamOperatorFactory<String> {

        private final ElementOrder elementOrder;

        TestWithAsyncProcessTimerOperatorFactory(ElementOrder elementOrder) {
            this.elementOrder = elementOrder;
        }

        @Override
        public <T extends StreamOperator<String>> T createStreamOperator(
                StreamOperatorParameters<String> parameters) {
            return (T) new SingleInputTestOperatorWithAsyncProcessTimer(parameters, elementOrder);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return SingleInputTestOperatorWithAsyncProcessTimer.class;
        }
    }

    private static class SingleInputTestOperatorWithAsyncProcessTimer
            extends SingleInputTestOperator {

        SingleInputTestOperatorWithAsyncProcessTimer(
                StreamOperatorParameters<String> parameters, ElementOrder elementOrder) {
            super(parameters, elementOrder);
            input =
                    new AbstractInput<Tuple2<Integer, String>, String>(this, 1) {
                        @Override
                        public void processElement(StreamRecord<Tuple2<Integer, String>> element)
                                throws Exception {
                            processed.incrementAndGet();
                            timerService.registerEventTimeTimer(
                                    VoidNamespace.INSTANCE, Long.parseLong(element.getValue().f1));
                        }
                    };
        }

        @Override
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            asyncProcessWithKey(timer.getKey(), () -> super.onEventTime(timer));
        }

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            asyncProcessWithKey(timer.getKey(), () -> super.onProcessingTime(timer));
        }
    }

    private static class WatermarkTestingOperatorFactory
            extends AbstractStreamOperatorFactory<Long> {

        private BiFunctionWithException<WatermarkTestingOperator, Watermark, Watermark, Exception>
                preProcessFunction;

        private BiConsumerWithException<WatermarkTestingOperator, Watermark, Exception>
                postProcessFunction;

        public void setPreProcessFunction(
                BiFunctionWithException<WatermarkTestingOperator, Watermark, Watermark, Exception>
                        preProcessFunction) {
            this.preProcessFunction = preProcessFunction;
        }

        public void setPostProcessFunction(
                BiConsumerWithException<WatermarkTestingOperator, Watermark, Exception>
                        postProcessFunction) {
            this.postProcessFunction = postProcessFunction;
        }

        @Override
        public <T extends StreamOperator<Long>> T createStreamOperator(
                StreamOperatorParameters<Long> parameters) {
            return (T)
                    new WatermarkTestingOperator(
                            parameters, preProcessFunction, postProcessFunction);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return WatermarkTestingOperator.class;
        }
    }

    private static class WatermarkTestingOperator extends AbstractAsyncStateStreamOperatorV2<Long>
            implements MultipleInputStreamOperator<Long>, Triggerable<Integer, VoidNamespace> {

        private transient InternalTimerService<VoidNamespace> timerService;

        private BiFunctionWithException<WatermarkTestingOperator, Watermark, Watermark, Exception>
                preProcessFunction;

        private BiConsumerWithException<WatermarkTestingOperator, Watermark, Exception>
                postProcessFunction;

        public WatermarkTestingOperator(
                StreamOperatorParameters<Long> parameters,
                BiFunctionWithException<WatermarkTestingOperator, Watermark, Watermark, Exception>
                        preProcessFunction,
                BiConsumerWithException<WatermarkTestingOperator, Watermark, Exception>
                        postProcessFunction) {
            super(parameters, 3);
            this.preProcessFunction = preProcessFunction;
            this.postProcessFunction = postProcessFunction;
        }

        public void output(Long o) {
            output.collect(new StreamRecord<>(o));
        }

        @Override
        public void open() throws Exception {
            super.open();

            this.timerService =
                    getInternalTimerService("test-timers", VoidNamespaceSerializer.INSTANCE, this);
        }

        @Override
        public Watermark preProcessWatermark(Watermark watermark) throws Exception {
            return preProcessFunction == null
                    ? watermark
                    : preProcessFunction.apply(this, watermark);
        }

        @Override
        public void postProcessWatermark(Watermark watermark) throws Exception {
            if (postProcessFunction != null) {
                postProcessFunction.accept(this, watermark);
            }
        }

        @Override
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            assertThat(getCurrentKey()).isEqualTo(timer.getKey());
            output.collect(new StreamRecord<>(timer.getTimestamp()));
        }

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {
            assertThat(getCurrentKey()).isEqualTo(timer.getKey());
        }

        private Input<Long> createInput(int idx) {
            return new AbstractInput<Long, Long>(this, idx) {
                @Override
                public void processElement(StreamRecord<Long> element) throws Exception {
                    timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, element.getValue());
                }
            };
        }

        @Override
        public List<Input> getInputs() {
            return Arrays.asList(createInput(1), createInput(2), createInput(3));
        }
    }
}
