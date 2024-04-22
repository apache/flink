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

package org.apache.flink.streaming.runtime.operators.asyncprocessing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerServiceAsyncImpl;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.io.RecordProcessorUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.state.StateBackendTestUtils.buildAsyncStateBackend;
import static org.assertj.core.api.Assertions.assertThat;

/** Basic tests for {@link AbstractAsyncStateStreamOperator}. */
class AbstractAsyncStateStreamOperatorTest {

    protected KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
            createTestHarness(
                    int maxParalelism, int numSubtasks, int subtaskIndex, ElementOrder elementOrder)
                    throws Exception {
        TestOperator testOperator = new TestOperator(elementOrder);
        KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                testOperator,
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
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            assertThat(testHarness.getOperator())
                    .isInstanceOf(AbstractAsyncStateStreamOperator.class);
            assertThat(
                            ((AbstractAsyncStateStreamOperator) testHarness.getOperator())
                                    .getAsyncExecutionController())
                    .isNotNull();
            assertThat(
                            ((AbstractAsyncStateStreamOperator) testHarness.getOperator())
                                    .getAsyncExecutionController()
                                    .getStateExecutor())
                    .isNotNull();
        }
    }

    @Test
    void testRecordProcessorWithFirstStateOrder() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.FIRST_STATE_ORDER)) {
            testHarness.open();
            TestOperator testOperator = (TestOperator) testHarness.getOperator();
            ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> processor =
                    RecordProcessorUtils.getRecordProcessor(testOperator);
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
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            TestOperator testOperator = (TestOperator) testHarness.getOperator();
            ThrowingConsumer<StreamRecord<Tuple2<Integer, String>>, Exception> processor =
                    RecordProcessorUtils.getRecordProcessor(testOperator);
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
    void testCheckpointDrain() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            CheckpointStorageLocationReference locationReference =
                    CheckpointStorageLocationReference.getDefault();
            AsyncExecutionController asyncExecutionController =
                    ((AbstractAsyncStateStreamOperator) testHarness.getOperator())
                            .getAsyncExecutionController();
            ((AbstractAsyncStateStreamOperator<String>) testHarness.getOperator())
                    .setAsyncKeyedContextElement(
                            new StreamRecord<>(Tuple2.of(5, "5")), new TestKeySelector());
            asyncExecutionController.handleRequest(null, StateRequestType.VALUE_GET, null);
            ((AbstractAsyncStateStreamOperator<String>) testHarness.getOperator())
                    .postProcessElement();
            assertThat(asyncExecutionController.getInFlightRecordNum()).isEqualTo(1);
            testHarness
                    .getOperator()
                    .snapshotState(
                            1,
                            1,
                            new CheckpointOptions(CheckpointType.CHECKPOINT, locationReference),
                            new JobManagerCheckpointStorage()
                                    .createCheckpointStorage(new JobID())
                                    .resolveCheckpointStorageLocation(1, locationReference));
            assertThat(asyncExecutionController.getInFlightRecordNum()).isEqualTo(0);
        }
    }

    @Disabled("Support Timer for AsyncKeyedStateBackend")
    @Test
    void testTimerServiceIsAsync() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness = createTestHarness(128, 1, 0, ElementOrder.RECORD_ORDER)) {
            testHarness.open();
            assertThat(testHarness.getOperator())
                    .isInstanceOf(AbstractAsyncStateStreamOperator.class);
            Triggerable triggerable =
                    new Triggerable() {
                        @Override
                        public void onEventTime(InternalTimer timer) throws Exception {}

                        @Override
                        public void onProcessingTime(InternalTimer timer) throws Exception {}
                    };
            assertThat(
                            testHarness
                                    .getOperator()
                                    .getInternalTimerService(
                                            "test", VoidNamespaceSerializer.INSTANCE, triggerable))
                    .isInstanceOf(InternalTimerServiceAsyncImpl.class);
        }
    }

    /** A simple testing operator. */
    private static class TestOperator extends AbstractAsyncStateStreamOperator<String>
            implements OneInputStreamOperator<Tuple2<Integer, String>, String>,
                    Triggerable<Integer, VoidNamespace> {

        private static final long serialVersionUID = 1L;

        private final ElementOrder elementOrder;

        final AtomicInteger processed = new AtomicInteger(0);

        final Object objectToWait = new Object();

        TestOperator(ElementOrder elementOrder) {
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
        }

        @Override
        public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) throws Exception {}

        @Override
        public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer)
                throws Exception {}

        public int getProcessed() {
            return processed.get();
        }

        public void proceed() {
            synchronized (objectToWait) {
                objectToWait.notify();
            }
        }
    }

    /** {@link KeySelector} for tests. */
    static class TestKeySelector implements KeySelector<Tuple2<Integer, String>, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(Tuple2<Integer, String> value) {
            return value.f0;
        }
    }
}
