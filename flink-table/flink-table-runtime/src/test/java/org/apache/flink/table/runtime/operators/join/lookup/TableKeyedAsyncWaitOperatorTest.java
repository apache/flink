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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.runtime.operators.TableKeyedAsyncWaitOperator;
import org.apache.flink.table.runtime.operators.TableKeyedAsyncWaitOperatorFactory;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.Epoch;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.TableAsyncExecutionController;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorTest.IgnoreTimeoutLazyAsyncFunction;
import static org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorTest.LazyAsyncFunction;
import static org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorTest.MyAsyncFunction;
import static org.apache.flink.table.runtime.util.AsyncKeyOrderedTestUtils.assertKeyOrdered;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Harness tests for {@link TableKeyedAsyncWaitOperator}. */
public class TableKeyedAsyncWaitOperatorTest {

    private static final long TIMEOUT = 1000L;

    private static final MyAsyncFunction myAsyncFunction = new MyAsyncFunction();

    private static final KeySelector<Integer, Integer> keySelector = value -> value;

    private static LazyAsyncFunction lazyAsyncFunction;

    @BeforeEach
    void beforeEach() throws Exception {
        lazyAsyncFunction = new LazyAsyncFunction();
    }

    @Test
    void testMultiKeysWithEventTime() throws Exception {
        try (final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                createKeyedTestHarness(lazyAsyncFunction, keySelector, TIMEOUT, 10)) {
            testHarness.open();
            lazyAsyncFunction.countDown();

            TableKeyedAsyncWaitOperator<Integer, Integer, Integer> operator =
                    (TableKeyedAsyncWaitOperator<Integer, Integer, Integer>)
                            testHarness.getOperator();
            TableAsyncExecutionController<Integer, Integer, Integer> aec =
                    operator.getAsyncExecutionController();

            final long initialTime = 0L;
            final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

            testHarness.processElement(new StreamRecord<>(0, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(1, initialTime + 2));

            testHarness.processWatermark(new Watermark(initialTime + 2));
            Epoch<Integer> epoch = new Epoch<>(new Watermark(initialTime + 2));
            testHarness.processElement(new StreamRecord<>(0, initialTime + 3));
            epoch.incrementCount();
            testHarness.processElement(new StreamRecord<>(4, initialTime + 4));
            epoch.incrementCount();
            testHarness.processElement(new StreamRecord<>(3, initialTime + 5));
            epoch.incrementCount();
            assertThat(aec.getActiveEpoch()).isEqualTo(epoch);

            testHarness.processWatermark(new Watermark(initialTime + 5));
            epoch = new Epoch<>(new Watermark(initialTime + 5));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 6));
            epoch.incrementCount();
            testHarness.processElement(new StreamRecord<>(1, initialTime + 7));
            epoch.incrementCount();
            testHarness.processElement(new StreamRecord<>(0, initialTime + 8));
            epoch.incrementCount();
            assertThat(aec.getActiveEpoch()).isEqualTo(epoch);

            testHarness.processWatermark(new Watermark(initialTime + 8));

            testHarness.endInput();
            epoch = new Epoch<>(new Watermark(initialTime + 8));
            assertThat(aec.getActiveEpoch()).isEqualTo(epoch);

            expectedOutput.add(new StreamRecord<>(0, initialTime + 1));
            expectedOutput.add(new StreamRecord<>(1, initialTime + 2));
            expectedOutput.add(new Watermark(initialTime + 2));
            expectedOutput.add(new StreamRecord<>(0, initialTime + 3));
            expectedOutput.add(new StreamRecord<>(4, initialTime + 4));
            expectedOutput.add(new StreamRecord<>(3, initialTime + 5));
            expectedOutput.add(new Watermark(initialTime + 5));
            expectedOutput.add(new StreamRecord<>(2, initialTime + 6));
            expectedOutput.add(new StreamRecord<>(1, initialTime + 7));
            expectedOutput.add(new StreamRecord<>(0, initialTime + 8));
            expectedOutput.add(new Watermark(initialTime + 8));

            Queue<Object> expected =
                    new LinkedList<>(
                            Arrays.asList(
                                    new StreamRecord<>(0, initialTime + 1),
                                    new StreamRecord<>(0, initialTime + 3),
                                    new StreamRecord<>(0, initialTime + 8)));
            assertKeyOrdered(testHarness.getOutput(), expected);

            expected =
                    new LinkedList<>(
                            Arrays.asList(
                                    new StreamRecord<>(1, initialTime + 2),
                                    new StreamRecord<>(1, initialTime + 7)));
            assertKeyOrdered(testHarness.getOutput(), expected);

            List<Integer> index = Stream.of(2, 6, 10).collect(Collectors.toList());
            TestHarnessUtil.assertOutputAtIndexEquals(
                    "Output with watermark was not correct.",
                    expectedOutput,
                    testHarness.getOutput(),
                    index);
        }
    }

    @Test
    void testOneKeyWithEventTime() throws Exception {
        try (final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                createKeyedTestHarness(myAsyncFunction, keySelector, TIMEOUT, 10)) {
            final long initialTime = 0L;
            final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
            testHarness.open();

            testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(1, initialTime + 2));
            testHarness.processWatermark(new Watermark(initialTime + 2));
            testHarness.processElement(new StreamRecord<>(1, initialTime + 3));

            testHarness.endInput();

            expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
            expectedOutput.add(new StreamRecord<>(2, initialTime + 2));
            expectedOutput.add(new Watermark(initialTime + 2));
            expectedOutput.add(new StreamRecord<>(2, initialTime + 3));

            TestHarnessUtil.assertOutputEquals(
                    "output of one key with event time is not correct.",
                    expectedOutput,
                    testHarness.getOutput());
        }
    }

    @Test
    void testMultiKeysOnEventTimeWithDifferentCapacity() throws Exception {
        for (int capacity = 1; capacity < 10; capacity++) {
            testMultiKeysMixWatermarkWithEventTime(capacity);
        }
    }

    private void testMultiKeysMixWatermarkWithEventTime(int capacity) throws Exception {
        try (final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                createKeyedTestHarness(myAsyncFunction, keySelector, TIMEOUT, capacity)) {
            testHarness.open();
            final long initialTime = 0L;
            final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

            // the peek of queue under different key could all be watermark but not the same

            testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(1, initialTime + 2));
            testHarness.processWatermark(new Watermark(initialTime + 2));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 3));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 4));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 5));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 6));
            testHarness.processWatermark(new Watermark(initialTime + 7));
            testHarness.processWatermark(new Watermark(initialTime + 8));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 9));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 10));

            // wait until all async collectors in the buffer have been emitted out.

            testHarness.endInput();

            expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
            expectedOutput.add(new StreamRecord<>(2, initialTime + 2));
            expectedOutput.add(new Watermark(initialTime + 2));
            expectedOutput.add(new StreamRecord<>(4, initialTime + 3));
            expectedOutput.add(new StreamRecord<>(6, initialTime + 4));
            expectedOutput.add(new StreamRecord<>(4, initialTime + 5));
            expectedOutput.add(new StreamRecord<>(6, initialTime + 6));
            expectedOutput.add(new Watermark(initialTime + 7));
            expectedOutput.add(new Watermark(initialTime + 8));
            expectedOutput.add(new StreamRecord<>(4, initialTime + 9));
            expectedOutput.add(new StreamRecord<>(6, initialTime + 10));

            Queue<Object> expected =
                    new LinkedList<>(
                            Arrays.asList(
                                    new StreamRecord<>(4, initialTime + 3),
                                    new StreamRecord<>(4, initialTime + 5)));
            assertKeyOrdered(testHarness.getOutput(), expected);
            expected =
                    new LinkedList<>(
                            Arrays.asList(
                                    new StreamRecord<>(6, initialTime + 4),
                                    new StreamRecord<>(6, initialTime + 6)));
            assertKeyOrdered(testHarness.getOutput(), expected);

            List<Integer> index = Stream.of(0, 1, 2, 7, 8).collect(Collectors.toList());
            TestHarnessUtil.assertOutputAtIndexEquals(
                    "Output with watermark was not correct.",
                    expectedOutput,
                    testHarness.getOutput(),
                    index);
        }
    }

    @Test
    void testMultiKeysWithProcessingTime() throws Exception {
        try (final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                createKeyedTestHarness(myAsyncFunction, keySelector, TIMEOUT, 10)) {
            final long initialTime = 0L;
            final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

            testHarness.open();

            testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
            testHarness.processElement(new StreamRecord<>(4, initialTime + 4));
            testHarness.processElement(new StreamRecord<>(5, initialTime + 5));
            testHarness.processElement(new StreamRecord<>(6, initialTime + 6));
            testHarness.processElement(new StreamRecord<>(7, initialTime + 7));
            testHarness.processElement(new StreamRecord<>(8, initialTime + 8));

            expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
            expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
            expectedOutput.add(new StreamRecord<>(6, initialTime + 3));
            expectedOutput.add(new StreamRecord<>(8, initialTime + 4));
            expectedOutput.add(new StreamRecord<>(10, initialTime + 5));
            expectedOutput.add(new StreamRecord<>(12, initialTime + 6));
            expectedOutput.add(new StreamRecord<>(14, initialTime + 7));
            expectedOutput.add(new StreamRecord<>(16, initialTime + 8));

            testHarness.endInput();

            TestHarnessUtil.assertOutputEqualsSorted(
                    "KeyedOrdered Output was not correct.",
                    expectedOutput,
                    testHarness.getOutput(),
                    new StreamRecordComparator());
        }
    }

    @Test
    public void testStateSnapshotAndRestore() throws Exception {
        TestLazyAsyncFunction testLazyAsyncFunction = new TestLazyAsyncFunction();
        // lazy async function is controlled by latch
        TableKeyedAsyncWaitOperatorFactory<Integer, Integer, Integer> factory =
                new TableKeyedAsyncWaitOperatorFactory<>(
                        testLazyAsyncFunction, keySelector, TIMEOUT, 4);

        OperatorID operatorID = new OperatorID(42L, 4711L);

        StreamTaskMailboxTestHarness<Integer> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO, 1, keySelector)
                        .setupOutputForSingletonOperatorChain(factory)
                        .modifyStreamConfig(
                                streamConfig -> {
                                    streamConfig.setStreamOperatorFactory(factory);
                                    streamConfig.setOperatorID(operatorID);
                                })
                        .setKeyType(BasicTypeInfo.INT_TYPE_INFO)
                        .build();

        final TestTaskStateManager taskStateManagerMock = testHarness.getTaskStateManager();

        StreamTask<Integer, ?> task = testHarness.getStreamTask();

        final long initialTime = 0L;

        testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
        testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
        testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
        testHarness.processElement(new StreamRecord<>(4, initialTime + 4));

        assertThat(testHarness.getOutput()).isEmpty();

        final long checkpointId = 1L;
        final long checkpointTimestamp = 1L;

        Future<Boolean> checkpointFuture =
                task.triggerCheckpointAsync(
                        new CheckpointMetaData(checkpointId, checkpointTimestamp),
                        CheckpointOptions.forCheckpointWithDefaultLocation());

        processMailTillCheckpointSucceeds(testHarness, checkpointFuture);

        Assertions.assertEquals(checkpointId, taskStateManagerMock.getReportedCheckpointId());

        testLazyAsyncFunction.countDown();

        testHarness.waitForTaskCompletion();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        expected.add(new StreamRecord<>(1, initialTime + 1));
        expected.add(new StreamRecord<>(2, initialTime + 2));
        expected.add(new StreamRecord<>(3, initialTime + 3));
        expected.add(new StreamRecord<>(4, initialTime + 4));

        testHarness.getOutput().removeIf(record -> record instanceof CheckpointBarrier);

        TestHarnessUtil.assertOutputEqualsSorted(
                "StateAndRestored Test Output was not correct before restore.",
                expected,
                testHarness.getOutput(),
                new StreamRecordComparator());

        // set the operator state from previous attempt into the restored one
        TaskStateSnapshot subtaskStates = taskStateManagerMock.getLastJobManagerTaskStateSnapshot();

        StreamTaskMailboxTestHarness<Integer> restoredTaskHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO, 1, keySelector)
                        .setupOutputForSingletonOperatorChain(
                                new TableKeyedAsyncWaitOperatorFactory<>(
                                        myAsyncFunction, keySelector, TIMEOUT, 6))
                        .setTaskStateSnapshot(checkpointId, subtaskStates)
                        .modifyStreamConfig(
                                streamConfig -> {
                                    streamConfig.setStreamOperatorFactory(factory);
                                    streamConfig.setOperatorID(operatorID);
                                })
                        .setKeyType(BasicTypeInfo.INT_TYPE_INFO)
                        .build();
        // myAsyncFunction.open(new Configuration());

        restoredTaskHarness.processElement(new StreamRecord<>(5, initialTime + 5));
        restoredTaskHarness.processElement(new StreamRecord<>(6, initialTime + 6));
        restoredTaskHarness.processElement(new StreamRecord<>(7, initialTime + 7));

        task = restoredTaskHarness.getStreamTask();
        checkpointFuture =
                task.triggerCheckpointAsync(
                        new CheckpointMetaData(checkpointId, checkpointTimestamp),
                        CheckpointOptions.forCheckpointWithDefaultLocation());

        processMailTillCheckpointSucceeds(restoredTaskHarness, checkpointFuture);

        restoredTaskHarness.processElement(new StreamRecord<>(8, initialTime + 8));

        restoredTaskHarness.waitForTaskCompletion();

        myAsyncFunction.close();
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(1, initialTime + 1));
        expectedOutput.add(new StreamRecord<>(2, initialTime + 2));
        expectedOutput.add(new StreamRecord<>(3, initialTime + 3));
        expectedOutput.add(new StreamRecord<>(4, initialTime + 4));
        expectedOutput.add(new StreamRecord<>(5, initialTime + 5));
        expectedOutput.add(new StreamRecord<>(6, initialTime + 6));
        expectedOutput.add(new StreamRecord<>(7, initialTime + 7));
        expectedOutput.add(new StreamRecord<>(8, initialTime + 8));

        restoredTaskHarness.getOutput().removeIf(record -> record instanceof CheckpointBarrier);

        TestHarnessUtil.assertOutputEqualsSorted(
                "StateAndRestored Test Output was not correct.",
                expectedOutput,
                restoredTaskHarness.getOutput(),
                new StreamRecordComparator());
    }

    @Test
    public void testKeyedAsyncTimeoutFailure() throws Exception {
        testKeyedAsyncTimeout(Optional.of(TimeoutException.class), new StreamRecord<>(2, 5L));
    }

    @Test
    public void testKeyedAsyncTimeoutIgnore() throws Exception {
        lazyAsyncFunction = new IgnoreTimeoutLazyAsyncFunction();
        testKeyedAsyncTimeout(
                Optional.empty(), new StreamRecord<>(3, 0L), new StreamRecord<>(2, 5L));
    }

    private void testKeyedAsyncTimeout(
            Optional<Class<? extends Throwable>> expectedException,
            StreamRecord<Integer>... expectedRecords)
            throws Exception {
        final long timeout = 10L;
        try (final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                createKeyedTestHarness(lazyAsyncFunction, keySelector, timeout, 10)) {
            testHarness.open();
            final MockEnvironment mockEnvironment = testHarness.getEnvironment();
            mockEnvironment.setExpectedExternalFailureCause(Throwable.class);
            final long initialTime = 0L;
            testHarness.setProcessingTime(initialTime);

            testHarness.processElement(new StreamRecord<>(1, initialTime));
            testHarness.setProcessingTime(initialTime + 5L);
            testHarness.processElement(new StreamRecord<>(2, initialTime + 5L));

            // trigger the timeout of the first stream record
            testHarness.setProcessingTime(initialTime + timeout + 1L);

            // allow the second async stream record to be processed
            lazyAsyncFunction.countDown();

            // wait until all async collectors in the buffer have been emitted out.

            testHarness.endInput();

            final ConcurrentLinkedQueue<Object> expectedOutput =
                    new ConcurrentLinkedQueue<>(Arrays.asList(expectedRecords));

            TestHarnessUtil.assertOutputEquals(
                    "Output with watermark was not correct.",
                    expectedOutput,
                    testHarness.getOutput());

            if (expectedException.isPresent()) {
                assertTrue(mockEnvironment.getActualExternalFailureCause().isPresent());
                assertTrue(
                        ExceptionUtils.findThrowable(
                                        mockEnvironment.getActualExternalFailureCause().get(),
                                        expectedException.get())
                                .isPresent());
            }
        }
    }

    private void processMailTillCheckpointSucceeds(
            StreamTaskMailboxTestHarness<Integer> testHarness, Future<Boolean> checkpointFuture)
            throws Exception {
        while (!checkpointFuture.isDone()) {
            testHarness.processSingleStep();
        }
        testHarness.getTaskStateManager().getWaitForReportLatch().await();
    }

    private static <OUT>
            KeyedOneInputStreamOperatorTestHarness<Integer, Integer, OUT> createKeyedTestHarness(
                    AsyncFunction<Integer, OUT> function,
                    KeySelector<Integer, Integer> keySelector,
                    long timeout,
                    int capacity)
                    throws Exception {

        return new KeyedOneInputStreamOperatorTestHarness<>(
                new TableKeyedAsyncWaitOperatorFactory<>(function, keySelector, timeout, capacity),
                keySelector,
                BasicTypeInfo.INT_TYPE_INFO,
                IntSerializer.INSTANCE);
    }

    /** A {@link Comparator} to compare {@link StreamRecord} while sorting them. */
    private static class StreamRecordComparator implements Comparator<Object> {

        @Override
        public int compare(Object o1, Object o2) {
            StreamRecord<Integer> sr0 = (StreamRecord<Integer>) o1;
            StreamRecord<Integer> sr1 = (StreamRecord<Integer>) o2;

            if (sr0.getTimestamp() != sr1.getTimestamp()) {
                return (int) (sr0.getTimestamp() - sr1.getTimestamp());
            }

            int comparison = sr0.getValue().compareTo(sr1.getValue());
            if (comparison != 0) {
                return comparison;
            } else {
                return sr0.getValue() - sr1.getValue();
            }
        }
    }

    // Note: this function is only for testStateSnapshotAndRestore.
    private static class TestLazyAsyncFunction extends LazyAsyncFunction {

        private static CountDownLatch testLatch;

        public TestLazyAsyncFunction() {
            testLatch = new CountDownLatch(1);
        }

        @Override
        public void waitLatch() throws InterruptedException {
            testLatch.await();
        }

        @Override
        public void countDown() {
            testLatch.countDown();
        }
    }
}
