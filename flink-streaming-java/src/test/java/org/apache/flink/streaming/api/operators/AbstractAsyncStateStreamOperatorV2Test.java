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
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.runtime.io.RecordProcessorUtils;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.ElementOrder;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
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
                        new KeyedOneInputStreamOperatorV2TestHarness<>(
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
            ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> processor =
                    RecordProcessorUtils.getRecordProcessor(testOperator.getInputs().get(0));
            ExecutorService anotherThread = Executors.newSingleThreadExecutor();
            // Trigger the processor
            anotherThread.execute(
                    () -> {
                        try {
                            processor.accept(new StreamRecord<>(Tuple2.of(5, "5")));
                        } catch (Exception e) {
                        }
                    });

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(1);
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(1);

            // Proceed processing
            testOperator.proceed();
            anotherThread.shutdown();
            Thread.sleep(1000);
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
            ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> processor =
                    RecordProcessorUtils.getRecordProcessor(testOperator.getInputs().get(0));
            ExecutorService anotherThread = Executors.newSingleThreadExecutor();
            // Trigger the processor
            anotherThread.execute(
                    () -> {
                        try {
                            processor.accept(new StreamRecord<>(Tuple2.of(5, "5")));
                        } catch (Exception e) {
                        }
                    });

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(1);
            // Why greater than 1:  +1 when enter the processor; +1 when handle the SYNC_POINT
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount())
                    .isGreaterThan(1);

            // Proceed processing
            testOperator.proceed();
            anotherThread.shutdown();
            Thread.sleep(1000);
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);
        }
    }

    @Test
    void testAsyncProcessWithKey() throws Exception {
        KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness =
                        new KeyedOneInputStreamOperatorV2TestHarness<>(
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
            ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> processor =
                    RecordProcessorUtils.getRecordProcessor(testOperator.getInputs().get(0));
            ExecutorService anotherThread = Executors.newSingleThreadExecutor();
            // Trigger the processor
            anotherThread.execute(
                    () -> {
                        try {
                            processor.accept(new StreamRecord<>(Tuple2.of(5, "5")));
                        } catch (Exception e) {
                        }
                    });

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(0);
            // Why greater than 1:  +1 when handle the SYNC_POINT; then accept +1
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount())
                    .isGreaterThan(1);

            // Proceed processing
            testOperator.proceed();
            anotherThread.shutdown();
            Thread.sleep(1000);
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
            CheckpointStorageLocationReference locationReference =
                    CheckpointStorageLocationReference.getDefault();
            AsyncExecutionController asyncExecutionController =
                    testOperator.getAsyncExecutionController();
            testOperator.setAsyncKeyedContextElement(
                    new StreamRecord<>(Tuple2.of(5, "5")), new TestKeySelector());
            asyncExecutionController.handleRequest(null, StateRequestType.VALUE_GET, null);
            testOperator.postProcessElement();
            assertThat(asyncExecutionController.getInFlightRecordNum()).isEqualTo(1);
            testOperator.prepareSnapshotPreBarrier(1);
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
            ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> processor =
                    RecordProcessorUtils.getRecordProcessor(testOperator.getInputs().get(0));
            ExecutorService anotherThread = Executors.newSingleThreadExecutor();
            // Trigger the processor
            anotherThread.execute(
                    () -> {
                        try {
                            processor.accept(new StreamRecord<>(Tuple2.of(5, "5")));
                            testOperator.processRecordAttributes(new RecordAttributes(false), 1);
                        } catch (Exception e) {
                        }
                    });

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(1);
            assertThat(testOperator.getAttributeProcessed()).isEqualTo(0);

            // Proceed processing
            testOperator.proceed();
            Thread.sleep(3000);
            anotherThread.shutdown();
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);
            assertThat(testOperator.getAttributeProcessed()).isEqualTo(1);
        }
    }

    @Test
    void testWatermarkStatus() throws Exception {
        try (KeyedOneInputStreamOperatorV2TestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            SingleInputTestOperator testOperator =
                    (SingleInputTestOperator) testHarness.getBaseOperator();
            ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> processor =
                    RecordProcessorUtils.getRecordProcessor(testOperator.getInputs().get(0));
            ExecutorService anotherThread = Executors.newSingleThreadExecutor();
            // Trigger the processor
            anotherThread.execute(
                    () -> {
                        try {
                            processor.accept(new StreamRecord<>(Tuple2.of(5, "5")));
                            testOperator.processWatermarkStatus(new WatermarkStatus(0), 1);
                        } catch (Exception e) {
                        }
                    });

            Thread.sleep(1000);
            assertThat(testOperator.getProcessed()).isEqualTo(1);
            assertThat(testOperator.watermarkIndex).isEqualTo(-1);
            assertThat(testOperator.watermarkStatus.isIdle()).isTrue();

            // Proceed processing
            testOperator.proceed();
            Thread.sleep(3000);
            anotherThread.shutdown();
            assertThat(testOperator.getCurrentProcessingContext().getReferenceCount()).isEqualTo(0);
            assertThat(testOperator.watermarkStatus.isActive()).isTrue();
            assertThat(testOperator.watermarkIndex).isEqualTo(1);
        }
    }

    private static class KeyedOneInputStreamOperatorV2TestHarness<K, IN, OUT>
            extends KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> {
        public KeyedOneInputStreamOperatorV2TestHarness(
                StreamOperatorFactory<OUT> operatorFactory,
                final KeySelector<IN, K> keySelector,
                TypeInformation<K> keyType,
                int maxParallelism,
                int numSubtasks,
                int subtaskIndex)
                throws Exception {
            super(operatorFactory, keySelector, keyType, maxParallelism, numSubtasks, subtaskIndex);
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
                        }
                    };
        }

        @Override
        public void open() throws Exception {
            super.open();
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
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {}

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer)
                throws Exception {}

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
}
