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

package org.apache.flink.runtime.asyncprocessing.operators;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.SimpleAsyncExecutionController;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordProcessorUtils;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.ElementOrder;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.asyncprocessing.AsyncKeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.asyncprocessing.AsyncOneInputStreamOperatorTestHarness;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.state.StateBackendTestUtils.buildAsyncStateBackend;
import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.MIN_PRIORITY;
import static org.assertj.core.api.Assertions.assertThat;

/** Basic tests for {@link AbstractAsyncRunnableStreamOperator}. */
public class AbstractAsyncRunnableStreamOperatorTest {

    protected AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String>
            createTestHarness(
                    int maxParalelism, int numSubtasks, int subtaskIndex, TestOperator testOperator)
                    throws Exception {
        AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
                AsyncOneInputStreamOperatorTestHarness.create(
                        testOperator, maxParalelism, numSubtasks, subtaskIndex);
        testHarness.setStateBackend(buildAsyncStateBackend(new HashMapStateBackend()));
        return testHarness;
    }

    protected AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String>
            createTestHarness(
                    int maxParalelism, int numSubtasks, int subtaskIndex, ElementOrder elementOrder)
                    throws Exception {
        TestOperator testOperator = new TestOperator(new TestKeySelector(), elementOrder);
        AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
                AsyncOneInputStreamOperatorTestHarness.create(
                        testOperator, maxParalelism, numSubtasks, subtaskIndex);
        testHarness.setStateBackend(buildAsyncStateBackend(new HashMapStateBackend()));
        return testHarness;
    }

    @Test
    void testCreateAsyncExecutionController() throws Exception {
        try (AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
                createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            assertThat(testHarness.getOperator())
                    .isInstanceOf(AbstractAsyncRunnableStreamOperator.class);
            AsyncExecutionController<?, ?> aec =
                    ((AbstractAsyncRunnableStreamOperator) testHarness.getOperator())
                            .getAsyncExecutionController();
            assertThat(aec).isNotNull();
            assertThat(((MailboxExecutorImpl) aec.getMailboxExecutor()).getPriority())
                    .isGreaterThan(MIN_PRIORITY);
            assertThat(aec.getAsyncExecutor()).isNotNull();
        }
    }

    @Test
    void testRecordProcessorWithFirstRequestOrder() throws Exception {
        try (AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
                createTestHarness(128, 1, 0, ElementOrder.FIRST_REQUEST_ORDER)) {
            testHarness.open();
            TestOperator testOperator = (TestOperator) testHarness.getOperator();
            CompletableFuture<Void> future =
                    testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(5, "5")));

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(1);
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(1);

            // Proceed processing
            testOperator.proceed();
            future.get();
            testHarness.drainAsyncRequests();
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);
        }
    }

    @Test
    void testRecordProcessorWithRecordOrder() throws Exception {
        try (AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
                createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            TestOperator testOperator = (TestOperator) testHarness.getOperator();
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
            testHarness.drainAsyncRequests();
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);
        }
    }

    @Test
    void testAsyncProcessWithKey() throws Exception {
        TestOperatorWithAsyncProcessWithKey testOperator =
                new TestOperatorWithAsyncProcessWithKey(
                        new TestKeySelector(), ElementOrder.RECORD_ORDER);
        AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
                AsyncOneInputStreamOperatorTestHarness.create(testOperator, 128, 1, 0);
        testHarness.setStateBackend(buildAsyncStateBackend(new HashMapStateBackend()));
        try {
            testHarness.open();
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
    void testDirectAsyncProcess() throws Exception {
        TestOperatorWithDirectAsyncProcess testOperator =
                new TestOperatorWithDirectAsyncProcess(
                        new TestKeySelector(), ElementOrder.RECORD_ORDER);
        AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
                AsyncOneInputStreamOperatorTestHarness.create(testOperator, 128, 1, 0);
        testHarness.setStateBackend(buildAsyncStateBackend(new HashMapStateBackend()));
        try {
            testHarness.open();
            CompletableFuture<Void> future =
                    testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(5, "5")));

            testHarness.drainAsyncRequests();
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);
            assertThat(testOperator.getProcessed()).isEqualTo(1);
        } finally {
            testHarness.close();
        }
    }

    @Test
    void testManyAsyncProcessWithKey() throws Exception {
        // This test is for verifying StateExecutionController could avoid deadlock for derived
        // processing requests.
        int requests = ExecutionOptions.ASYNC_STATE_TOTAL_BUFFER_SIZE.defaultValue() + 1;
        TestOperatorWithMultipleDirectAsyncProcess testOperator =
                new TestOperatorWithMultipleDirectAsyncProcess(
                        new TestKeySelector(), ElementOrder.RECORD_ORDER, requests);
        AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
                AsyncOneInputStreamOperatorTestHarness.create(testOperator, 128, 1, 0);
        testHarness.setStateBackend(buildAsyncStateBackend(new HashMapStateBackend()));
        try {
            testHarness.open();

            // Repeat twice
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(5, "5")));
            CompletableFuture<Void> future =
                    testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(5, "5")));

            testHarness.drainAsyncRequests();
            // If the AEC could avoid deadlock, there should not be any timeout exception.
            future.get(10000, TimeUnit.MILLISECONDS);
            testOperator.getLastProcessedFuture().get(10000, TimeUnit.MILLISECONDS);

            assertThat(testOperator.getProcessed()).isEqualTo(requests * 2);
            // This ensures the order is correct according to the priority in AEC.
            assertThat(testOperator.getProcessedOrders())
                    .isEqualTo(testOperator.getExpectedProcessedOrders());
        } finally {
            testHarness.close();
        }
    }

    @Test
    void testCheckpointDrain() throws Exception {
        try (AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
                createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            SimpleAsyncExecutionController<String> asyncExecutionController =
                    (SimpleAsyncExecutionController)
                            ((AbstractAsyncRunnableStreamOperator) testHarness.getOperator())
                                    .getAsyncExecutionController();
            ((AbstractAsyncRunnableStreamOperator<String>) testHarness.getOperator())
                    .setAsyncKeyedContextElement(
                            new StreamRecord<>(Tuple2.of(5, "5")), new TestKeySelector());
            ((AbstractAsyncRunnableStreamOperator<String>) testHarness.getOperator())
                    .asyncProcess(
                            () -> {
                                return null;
                            });
            ((AbstractAsyncRunnableStreamOperator<String>) testHarness.getOperator())
                    .postProcessElement();
            assertThat(asyncExecutionController.getInFlightRecordNum()).isEqualTo(1);
            testHarness.drainAsyncRequests();
            assertThat(asyncExecutionController.getInFlightRecordNum()).isEqualTo(0);
        }
    }

    @Test
    void testNonRecordProcess() throws Exception {
        try (AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
                createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            TestOperator testOperator = (TestOperator) testHarness.getOperator();
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(5, "5")));
            CompletableFuture<Void> future =
                    testHarness.processLatencyMarkerInternal(
                            new LatencyMarker(1234, new OperatorID(), 0));

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(1);
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount())
                    .isGreaterThan(1);
            assertThat(testOperator.getLatencyProcessed()).isEqualTo(0);

            // Proceed processing
            testOperator.proceed();
            future.get();
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);
            assertThat(testOperator.getLatencyProcessed()).isEqualTo(1);
        }
    }

    @Test
    void testWatermark() throws Exception {
        TestOperatorWithAsyncProcessTimer testOperator =
                new TestOperatorWithAsyncProcessTimer(
                        new TestKeySelector(), ElementOrder.RECORD_ORDER);
        try (AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
                createTestHarness(128, 1, 0, testOperator)) {
            testHarness.open();
            ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(1, "1")));
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(1, "3")));
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(1, "6")));
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(1, "9")));
            testHarness.processWatermark(10L);
            expectedOutput.add(new Watermark(10L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    void testWatermarkHooks() throws Exception {
        KeySelector<Long, Integer> dummyKeySelector = l -> 0;
        final WatermarkTestingOperator testOperator =
                new WatermarkTestingOperator(dummyKeySelector, dummyKeySelector);

        AtomicInteger counter = new AtomicInteger(0);
        testOperator.setPreProcessFunction(
                (watermark) -> {
                    testOperator.asyncProcessWithKey(
                            1L,
                            () -> {
                                assertThat(testOperator.getCurrentKey()).isEqualTo(1L);
                                testOperator.output(watermark.getTimestamp() + 1000L);
                            });
                    if (counter.incrementAndGet() % 2 == 0) {
                        return null;
                    } else {
                        return new Watermark(watermark.getTimestamp() + 1L);
                    }
                });

        testOperator.setPostProcessFunction(
                (watermark) -> {
                    testOperator.output(watermark.getTimestamp() + 100L);
                });

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        try (AsyncKeyedTwoInputStreamOperatorTestHarness<Integer, Long, Long, Long> testHarness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        testOperator,
                        dummyKeySelector,
                        dummyKeySelector,
                        BasicTypeInfo.INT_TYPE_INFO,
                        1,
                        1,
                        0)) {
            testHarness.setup();
            testHarness.open();
            testHarness.processElement1(1L, 1L);
            testHarness.processElement1(3L, 3L);
            testHarness.processElement1(4L, 4L);
            testHarness.processWatermark1(new Watermark(2L));
            testHarness.processWatermark2(new Watermark(2L));
            expectedOutput.add(new StreamRecord<>(1002L));
            expectedOutput.add(new StreamRecord<>(1L));
            expectedOutput.add(new StreamRecord<>(3L));
            expectedOutput.add(new StreamRecord<>(103L));
            expectedOutput.add(new Watermark(3L));
            testHarness.processWatermark1(new Watermark(4L));
            testHarness.processWatermark2(new Watermark(4L));
            expectedOutput.add(new StreamRecord<>(1004L));
            testHarness.processWatermark1(new Watermark(5L));
            testHarness.processWatermark2(new Watermark(5L));
            expectedOutput.add(new StreamRecord<>(1005L));
            expectedOutput.add(new StreamRecord<>(4L));
            expectedOutput.add(new StreamRecord<>(106L));
            expectedOutput.add(new Watermark(6L));

            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    void testWatermarkStatus() throws Exception {
        try (AsyncOneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
                createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            TestOperator testOperator = (TestOperator) testHarness.getOperator();
            ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> processor =
                    RecordProcessorUtils.getRecordProcessor(testOperator);
            testHarness.processElementInternal(new StreamRecord<>(Tuple2.of(5, "5")));
            testHarness.processWatermarkInternal(new Watermark(205L));
            CompletableFuture<Void> future =
                    testHarness.processWatermarkStatusInternal(WatermarkStatus.IDLE);

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(1);
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount())
                    .isGreaterThan(1);
            assertThat(testOperator.watermarkIndex).isEqualTo(-1);
            assertThat(testOperator.watermarkStatus.isIdle()).isTrue();

            // Proceed processing
            testOperator.proceed();
            future.get();
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);
            assertThat(testOperator.watermarkStatus.isActive()).isFalse();
            assertThat(testHarness.getOutput())
                    .containsExactly(new Watermark(205L), WatermarkStatus.IDLE);
        }
    }

    @Test
    void testIdleWatermarkHandling() throws Exception {
        KeySelector<Long, Integer> dummyKeySelector = l -> 0;
        final WatermarkTestingOperator testOperator =
                new WatermarkTestingOperator(dummyKeySelector, dummyKeySelector);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        try (AsyncKeyedTwoInputStreamOperatorTestHarness<Integer, Long, Long, Long> testHarness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        testOperator,
                        dummyKeySelector,
                        dummyKeySelector,
                        BasicTypeInfo.INT_TYPE_INFO,
                        1,
                        1,
                        0)) {
            testHarness.setup();
            testHarness.open();
            testHarness.processElement1(1L, 1L);
            testHarness.processElement1(3L, 3L);
            testHarness.processElement1(4L, 4L);
            testHarness.processWatermark1(new Watermark(1L));
            assertThat(testHarness.getOutput()).isEmpty();

            testHarness.processWatermarkStatus2(WatermarkStatus.IDLE);
            expectedOutput.add(new StreamRecord<>(1L));
            expectedOutput.add(new Watermark(1L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());

            testHarness.processWatermark1(new Watermark(3L));
            expectedOutput.add(new StreamRecord<>(3L));
            expectedOutput.add(new Watermark(3L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());

            testHarness.processWatermarkStatus2(WatermarkStatus.ACTIVE);
            // the other input is active now, we should not emit the watermark
            testHarness.processWatermark1(new Watermark(4L));
            TestHarnessUtil.assertOutputEquals(
                    "Output was not correct", expectedOutput, testHarness.getOutput());
        }
    }

    /** A simple testing operator. */
    private static class TestOperator extends AbstractAsyncRunnableStreamOperator<String>
            implements OneInputStreamOperator<Tuple2<Integer, String>, String>,
                    Triggerable<Integer, VoidNamespace> {

        private static final long serialVersionUID = 1L;

        private final ElementOrder elementOrder;

        final AtomicInteger processed = new AtomicInteger(0);

        final AtomicInteger latencyProcessed = new AtomicInteger(0);

        final Object objectToWait = new Object();

        private WatermarkStatus watermarkStatus = new WatermarkStatus(-1);
        private int watermarkIndex = -1;

        TestOperator(
                KeySelector<Tuple2<Integer, String>, ?> keySelector, ElementOrder elementOrder) {
            super(keySelector, null, Executors.newSingleThreadExecutor(), 1, 100, 100);
            this.elementOrder = elementOrder;
        }

        @Override
        public void open() throws Exception {
            super.open();
        }

        @Override
        public ElementOrder getElementOrder() {
            return elementOrder;
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, String>> element) throws Exception {
            processed.incrementAndGet();
            synchronized (objectToWait) {
                objectToWait.wait();
            }
            asyncProcess(() -> processed.decrementAndGet())
                    .thenAccept((a) -> processed.incrementAndGet());
        }

        @Override
        public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            super.processLatencyMarker(latencyMarker);
            latencyProcessed.incrementAndGet();
        }

        @Override
        protected void processWatermarkStatus(WatermarkStatus watermarkStatus, int index)
                throws Exception {
            super.processWatermarkStatus(watermarkStatus, index);
            this.watermarkStatus = watermarkStatus;
            this.watermarkIndex = index;
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

        public int getProcessed() {
            return processed.get();
        }

        public int getLatencyProcessed() {
            return latencyProcessed.get();
        }

        public void proceed() {
            synchronized (objectToWait) {
                objectToWait.notify();
            }
        }
    }

    private static class TestOperatorWithAsyncProcessWithKey extends TestOperator {

        TestOperatorWithAsyncProcessWithKey(
                KeySelector<Tuple2<Integer, String>, ?> keySelector, ElementOrder elementOrder) {
            super(keySelector, elementOrder);
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, String>> element) throws Exception {
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
    }

    private static class TestOperatorWithDirectAsyncProcess extends TestOperator {

        TestOperatorWithDirectAsyncProcess(
                KeySelector<Tuple2<Integer, String>, ?> keySelector, ElementOrder elementOrder) {
            super(keySelector, elementOrder);
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, String>> element) throws Exception {
            asyncProcess(processed::decrementAndGet).thenAccept((e) -> processed.addAndGet(2));
        }
    }

    private static class TestOperatorWithMultipleDirectAsyncProcess extends TestOperator {

        private final int numAsyncProcesses;
        private final CompletableFuture<Void> lastProcessedFuture = new CompletableFuture<>();
        private final LinkedList<Integer> processedOrders = new LinkedList<>();
        private final LinkedList<Integer> expectedProcessedOrders = new LinkedList<>();

        TestOperatorWithMultipleDirectAsyncProcess(
                KeySelector<Tuple2<Integer, String>, ?> keySelector,
                ElementOrder elementOrder,
                int numAsyncProcesses) {
            super(keySelector, elementOrder);
            this.numAsyncProcesses = numAsyncProcesses;
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, String>> element) throws Exception {
            for (int i = 0; i < numAsyncProcesses; i++) {
                final int finalI = i;
                if (i < numAsyncProcesses - 1) {
                    asyncProcessWithKey(
                            element.getValue().f0,
                            () -> {
                                processed.incrementAndGet();
                                processedOrders.add(finalI);
                            });
                } else {
                    asyncProcessWithKey(
                            element.getValue().f0,
                            () -> {
                                processed.incrementAndGet();
                                processedOrders.add(finalI);
                                if (!lastProcessedFuture.isDone()) {
                                    lastProcessedFuture.complete(null);
                                }
                            });
                }
                expectedProcessedOrders.add(finalI);
            }
        }

        CompletableFuture<Void> getLastProcessedFuture() {
            return lastProcessedFuture;
        }

        LinkedList<Integer> getProcessedOrders() {
            return processedOrders;
        }

        LinkedList<Integer> getExpectedProcessedOrders() {
            return expectedProcessedOrders;
        }
    }

    private static class TestOperatorWithAsyncProcessTimer extends TestOperator {

        TestOperatorWithAsyncProcessTimer(
                KeySelector<Tuple2<Integer, String>, ?> keySelector, ElementOrder elementOrder) {
            super(keySelector, elementOrder);
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, String>> element) throws Exception {
            processed.incrementAndGet();
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

    private static class WatermarkTestingOperator extends AbstractAsyncRunnableStreamOperator<Long>
            implements TwoInputStreamOperator<Long, Long, Long>,
                    Triggerable<Integer, VoidNamespace> {

        private transient InternalTimerService<VoidNamespace> timerService;

        private FunctionWithException<Watermark, Watermark, Exception> preProcessFunction;

        private ThrowingConsumer<Watermark, Exception> postProcessFunction;

        public WatermarkTestingOperator(
                KeySelector<Long, ?> keySelector1, KeySelector<Long, ?> keySelector2) {
            super(keySelector1, keySelector2, Executors.newSingleThreadExecutor(), 1, 100, 100);
        }

        public void setPreProcessFunction(
                FunctionWithException<Watermark, Watermark, Exception> preProcessFunction) {
            this.preProcessFunction = preProcessFunction;
        }

        public void setPostProcessFunction(
                ThrowingConsumer<Watermark, Exception> postProcessFunction) {
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
            return preProcessFunction == null ? watermark : preProcessFunction.apply(watermark);
        }

        @Override
        public Watermark postProcessWatermark(Watermark watermark) throws Exception {
            if (postProcessFunction != null) {
                postProcessFunction.accept(watermark);
            }
            return watermark;
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

        @Override
        public void processElement1(StreamRecord<Long> element) throws Exception {
            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, element.getValue());
        }

        @Override
        public void processElement2(StreamRecord<Long> element) throws Exception {
            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, element.getValue());
        }
    }

    /** {@link KeySelector} for tests. */
    public static class TestKeySelector implements KeySelector<Tuple2<Integer, String>, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(Tuple2<Integer, String> value) {
            return value.f0;
        }
    }
}
