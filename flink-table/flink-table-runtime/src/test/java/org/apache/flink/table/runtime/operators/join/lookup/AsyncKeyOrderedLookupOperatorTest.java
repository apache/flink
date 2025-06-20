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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.asyncprocessing.AsyncOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.asyncprocessing.AsyncOneInputStreamOperatorTestHarnessWithoutEnvCheck;
import org.apache.flink.table.runtime.operators.AsyncKeyOrderedLookupOperator;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.Epoch;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.state.StateBackendTestUtils.buildAsyncStateBackend;
import static org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorTest.LazyAsyncFunction;
import static org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorTest.MyAsyncFunction;
import static org.apache.flink.table.runtime.util.AsyncKeyOrderedTestUtils.assertKeyOrdered;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Harness tests for {@link AsyncKeyOrderedLookupOperator}. */
public class AsyncKeyOrderedLookupOperatorTest {

    private static final long TIMEOUT = 1000L;

    private static final MyAsyncFunction myAsyncFunction = new MyAsyncFunction();

    private static final KeySelector<Integer, Integer> keySelector = value -> value;

    @Test
    void testMultiKeysWithWatermark() throws Exception {
        SynchronizeLazyAsyncFunction lazyAsyncFunction = new SynchronizeLazyAsyncFunction(false);
        try (final AsyncOneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createKeyedTestHarness(lazyAsyncFunction, TIMEOUT, 10, true)) {
            testHarness.open();
            lazyAsyncFunction.countDown();

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

            testHarness.processWatermark(new Watermark(initialTime + 5));
            epoch = new Epoch<>(new Watermark(initialTime + 5));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 6));
            epoch.incrementCount();
            testHarness.processElement(new StreamRecord<>(1, initialTime + 7));
            epoch.incrementCount();
            testHarness.processElement(new StreamRecord<>(0, initialTime + 8));
            epoch.incrementCount();

            testHarness.processWatermark(new Watermark(initialTime + 8));

            testHarness.endInput();

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

            assertWatermarkEquals(
                    "Output watermark was not correct.", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    void testOneKeyWithWatermark() throws Exception {
        try (final AsyncOneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createKeyedTestHarness(myAsyncFunction, TIMEOUT, 10, true)) {
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
    void testMultiKeysWithWatermarkWithDifferentCapacity() throws Exception {
        for (int capacity = 1; capacity < 10; capacity++) {
            testMultiKeysMixWatermark(capacity);
        }
    }

    private void testMultiKeysMixWatermark(int capacity) throws Exception {
        try (final AsyncOneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createKeyedTestHarness(myAsyncFunction, TIMEOUT, capacity, true)) {
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

            assertWatermarkEquals(
                    "Output watermark is not correct.", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    void testMultiKeysWithProcessingTime() throws Exception {
        try (final AsyncOneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createKeyedTestHarness(myAsyncFunction, TIMEOUT, 10, true)) {
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
    public void testKeyedAsyncTimeoutIgnore() throws Exception {
        final long timeout = 10L;
        SynchronizeLazyAsyncFunction lazyAsyncFunction = new SynchronizeLazyAsyncFunction(true);
        try (final AsyncOneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createKeyedTestHarness(lazyAsyncFunction, timeout, 10, true)) {
            testHarness.open();
            final MockEnvironment mockEnvironment = testHarness.getEnvironment();
            mockEnvironment.setExpectedExternalFailureCause(Throwable.class);
            final long initialTime = 0L;
            testHarness.setProcessingTime(initialTime);
            testHarness.processElement(new StreamRecord<>(1, initialTime));

            testHarness.setProcessingTime(initialTime + 5L);
            testHarness.processElement(new StreamRecord<>(2, initialTime + 5L));

            // Wait for actual async invocation in execution thread which means timer for timeout
            // has worked
            lazyAsyncFunction.sync();
            // trigger the timeout of the first stream record
            testHarness.setProcessingTime(initialTime + timeout + 1L);

            // allow the second async stream record to be processed
            lazyAsyncFunction.countDown();

            // wait until all async collectors in the buffer have been emitted out.
            testHarness.endInput();

            final ConcurrentLinkedQueue<Object> expectedOutput =
                    new ConcurrentLinkedQueue<>(
                            Arrays.asList(new StreamRecord<>(1, 0L), new StreamRecord<>(2, 5L)));

            TestHarnessUtil.assertOutputEquals(
                    "Output with watermark was not correct.",
                    expectedOutput,
                    testHarness.getOutput());
        }
    }

    @Test
    public void testKeyedAsyncTimeoutFailure() throws Exception {
        final long timeout = 10L;
        SynchronizeLazyAsyncFunction lazyAsyncFunction = new SynchronizeLazyAsyncFunction(false);
        try (final AsyncOneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createKeyedTestHarness(lazyAsyncFunction, timeout, 10, false)) {
            testHarness.open();
            final MockEnvironment mockEnvironment = testHarness.getEnvironment();
            mockEnvironment.setExpectedExternalFailureCause(Throwable.class);
            final long initialTime = 0L;
            testHarness.setProcessingTime(initialTime);
            testHarness.processElement(new StreamRecord<>(1, initialTime));
            // Wait for actual async invocation in execution thread which means timer for timeout
            // has worked
            lazyAsyncFunction.sync();
            // trigger the timeout
            testHarness.setProcessingTime(initialTime + timeout + 1L);
            lazyAsyncFunction.waitTimeout();

            assertTrue(mockEnvironment.getActualExternalFailureCause().isPresent());
            assertTrue(
                    ExceptionUtils.findThrowable(
                                    mockEnvironment.getActualExternalFailureCause().get(),
                                    TimeoutException.class)
                            .isPresent());
        }
    }

    @Test
    public void testSnapshotAndRestore() throws Exception {
        LazyAsyncFunction testLazyAsyncFunction = new LazyAsyncFunction();
        AsyncOneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createKeyedTestHarness(testLazyAsyncFunction, TIMEOUT, 10, true);

        testHarness.open();

        final long initialTime = 0L;
        testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
        testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
        testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
        testHarness.processElement(new StreamRecord<>(4, initialTime + 4));

        assertThat(testHarness.getOutput()).isEmpty();

        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);

        testLazyAsyncFunction.countDown();
        testHarness.endInput();

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
        testHarness.close();

        testLazyAsyncFunction = new LazyAsyncFunction();
        testHarness = createKeyedTestHarness(testLazyAsyncFunction, TIMEOUT, 10, true);

        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(5, initialTime + 5));
        testHarness.processElement(new StreamRecord<>(6, initialTime + 6));
        testHarness.processElement(new StreamRecord<>(7, initialTime + 7));
        testHarness.processElement(new StreamRecord<>(8, initialTime + 8));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(5, initialTime + 5));
        expectedOutput.add(new StreamRecord<>(6, initialTime + 6));
        expectedOutput.add(new StreamRecord<>(7, initialTime + 7));
        expectedOutput.add(new StreamRecord<>(8, initialTime + 8));

        testLazyAsyncFunction.countDown();
        testHarness.endInput();

        TestHarnessUtil.assertOutputEqualsSorted(
                "StateAndRestored Test Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new StreamRecordComparator());
        testHarness.close();
    }

    @SuppressWarnings("unchecked")
    private static <OUT>
            AsyncOneInputStreamOperatorTestHarness<Integer, Integer> createKeyedTestHarness(
                    AsyncFunction<Integer, OUT> function,
                    long timeout,
                    int capacity,
                    boolean envCheck)
                    throws Exception {

        AsyncKeyOrderedLookupOperator<Integer, Integer, Integer> asyncWaitOperator =
                new AsyncKeyOrderedLookupOperator(
                        function,
                        AsyncKeyOrderedLookupOperatorTest.keySelector,
                        Executors.newCachedThreadPool(),
                        1,
                        10,
                        capacity,
                        timeout,
                        new TestProcessingTimeService());

        AsyncOneInputStreamOperatorTestHarness<Integer, Integer> testHarness;
        if (envCheck) {
            testHarness = AsyncOneInputStreamOperatorTestHarness.create(asyncWaitOperator);
        } else {
            testHarness =
                    AsyncOneInputStreamOperatorTestHarnessWithoutEnvCheck.create(asyncWaitOperator);
        }
        testHarness.setStateBackend(buildAsyncStateBackend(new HashMapStateBackend()));
        return testHarness;
    }

    /** A {@link Comparator} to compare {@link StreamRecord} while sorting them. */
    private static class StreamRecordComparator implements Comparator<Object> {

        @SuppressWarnings("unchecked")
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

    // This function could wait until the actual invocation occurs. Because current async invocation
    // is invoked by non-main thread.
    // It means execution in main thread and async invocation including timer registration in
    // non-main thread executed in parallel.
    // We want a test function to synchronize the actual async invocation.
    private static class SynchronizeLazyAsyncFunction extends LazyAsyncFunction {

        private final CountDownLatch syncLatch;

        private final CountDownLatch timeoutLatch;

        private final boolean ignoreTimeout;

        public SynchronizeLazyAsyncFunction(boolean ignoreTimeout) {
            syncLatch = new CountDownLatch(1);
            timeoutLatch = new CountDownLatch(1);
            this.ignoreTimeout = ignoreTimeout;
        }

        @Override
        protected Runnable getRunnable(
                final Integer input, final ResultFuture<Integer> resultFuture) {
            return () -> {
                try {
                    syncLatch.countDown();
                    waitLatch();
                } catch (InterruptedException e) {
                    // do nothing
                }
                resultFuture.complete(Collections.singletonList(input));
            };
        }

        public void sync() throws InterruptedException {
            // ensure that the input has been submitted to the thread pool and being executed
            syncLatch.await();
        }

        @Override
        public void timeout(Integer input, ResultFuture<Integer> resultFuture) throws Exception {
            if (ignoreTimeout) {
                resultFuture.complete(Collections.singletonList(input));
            } else {
                super.timeout(input, resultFuture);
            }
            timeoutLatch.countDown();
        }

        public void waitTimeout() throws InterruptedException {
            timeoutLatch.await();
        }
    }

    private static <T> void assertWatermarkEquals(
            String message, Queue<T> expected, Queue<T> actual) {
        List<T> expectedList = new ArrayList<>(expected);
        List<T> actualList = new ArrayList<>(actual);

        Function<List<T>, Map<Integer, Watermark>> extractWatermarks =
                list ->
                        IntStream.range(0, list.size())
                                .boxed()
                                .filter(i -> list.get(i) instanceof Watermark)
                                .collect(Collectors.toMap(i -> i, i -> (Watermark) list.get(i)));

        Map<Integer, Watermark> expectedWatermarks = extractWatermarks.apply(expectedList);
        Map<Integer, Watermark> actualWatermarks = extractWatermarks.apply(actualList);

        assertThat(actualWatermarks).as(message).isEqualTo(expectedWatermarks);
    }
}
