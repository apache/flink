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
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.runtime.operators.TableKeyedAsyncWaitOperator;
import org.apache.flink.table.runtime.operators.TableKeyedAsyncWaitOperatorFactory;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.Epoch;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.EpochManager;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.TableAsyncExecutionController;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorTest.IgnoreTimeoutLazyAsyncFunction;
import static org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorTest.LazyAsyncFunction;
import static org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorTest.MyAsyncFunction;
import static org.apache.flink.table.runtime.util.AsyncKeyOrderedTestUtils.assertKeyOrdered;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Harness tests for {@link TableKeyedAsyncWaitOperator}. */
public class TableKeyedAsyncWaitOperatorTest {

    private static final long TIMEOUT = 1000L;

    private static final KeySelector<Integer, Integer> keySelector = value -> value;

    private final MyAsyncFunction noLockAsyncDouble2Function = new MyAsyncFunction();

    @Test
    void testMultiKeysWithWatermark() throws Exception {
        LazyAsyncFunction lazyAsyncFunction = new LazyAsyncFunction();
        try (final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                createKeyedTestHarness(lazyAsyncFunction, TIMEOUT, 10)) {
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

            assertWatermarkEquals(
                    "Output watermark was not correct.", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    void testOneKeyWithWatermark() throws Exception {
        try (final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                createKeyedTestHarness(noLockAsyncDouble2Function, TIMEOUT, 10)) {
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
                    "output is not correct.", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    void testKeysProcessingWithTheSameKeysPending() throws Exception {
        LazyAsyncFunction lazyAsyncFunction = new LazyAsyncFunction();
        try (final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                createKeyedTestHarness(lazyAsyncFunction, TIMEOUT, 10)) {
            final long initialTime = 0L;
            final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
            testHarness.open();

            testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(1, initialTime + 2));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
            testHarness.processWatermark(new Watermark(initialTime + 2));
            TableAsyncExecutionController<?, ?, ?> aec =
                    ((TableKeyedAsyncWaitOperator<?, ?, ?>) testHarness.getOperator())
                            .getAsyncExecutionController();
            assertThat(aec.getBlockingSize()).isEqualTo(2);
            assertThat(aec.getInFlightSize()).isEqualTo(3);
            lazyAsyncFunction.countDown();

            testHarness.endInput();
            assertThat(aec.getBlockingSize()).isEqualTo(0);
            assertThat(aec.getInFlightSize()).isEqualTo(0);

            expectedOutput.add(new StreamRecord<>(1, initialTime + 1));
            expectedOutput.add(new StreamRecord<>(1, initialTime + 2));
            assertKeyOrdered(testHarness.getOutput(), expectedOutput);
            expectedOutput.clear();
            expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
            expectedOutput.add(new StreamRecord<>(2, initialTime + 2));
            assertKeyOrdered(testHarness.getOutput(), expectedOutput);
        }
    }

    @Test
    void testMultiKeysWithWatermarkWithDifferentCapacity() throws Exception {
        for (int capacity = 1; capacity < 10; capacity++) {
            testMultiKeysMixWatermark(capacity);
        }
    }

    private void testMultiKeysMixWatermark(int capacity) throws Exception {
        try (final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                createKeyedTestHarness(noLockAsyncDouble2Function, TIMEOUT, capacity)) {
            testHarness.open();
            final long initialTime = 0L;
            final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

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
    void testMultiKeysWithoutWatermark() throws Exception {
        try (final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                createKeyedTestHarness(noLockAsyncDouble2Function, TIMEOUT, 10)) {
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
                    "Output is not correct.",
                    expectedOutput,
                    testHarness.getOutput(),
                    new StreamRecordComparator());
        }
    }

    @Test
    public void testSnapshotAndRestore() throws Exception {
        LazyAsyncFunction testLazyAsyncFunction = new LazyAsyncFunction();
        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                createKeyedTestHarness(testLazyAsyncFunction, TIMEOUT, 10);

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
        testHarness = createKeyedTestHarness(testLazyAsyncFunction, TIMEOUT, 10);

        testHarness.initializeState(snapshot);
        testHarness.open();

        Optional<Epoch<Integer>> epochWithMinWatermark = unwrapEpoch(testHarness, Long.MIN_VALUE);
        assertThat(epochWithMinWatermark).isPresent();
        assertThat(epochWithMinWatermark.get().getOngoingRecordCount()).isEqualTo(4);

        testHarness.processWatermark(1000L);
        testHarness.processElement(new StreamRecord<>(5, initialTime + 5));
        testHarness.processElement(new StreamRecord<>(6, initialTime + 6));
        testHarness.processElement(new StreamRecord<>(7, initialTime + 7));
        testHarness.processElement(new StreamRecord<>(8, initialTime + 8));
        Optional<Epoch<Integer>> epochWithMidWatermark = unwrapEpoch(testHarness, 1000L);
        assertThat(epochWithMidWatermark).isPresent();
        assertThat(epochWithMidWatermark.get().getOngoingRecordCount()).isEqualTo(4);

        testHarness.processWatermark(Long.MAX_VALUE);
        Optional<Epoch<Integer>> epochWithMaxWatermark = unwrapEpoch(testHarness, Long.MAX_VALUE);
        assertThat(epochWithMaxWatermark).isPresent();
        assertThat(epochWithMaxWatermark.get().getOngoingRecordCount()).isEqualTo(0);

        expected.add(new Watermark(1000L));
        expected.add(new StreamRecord<>(5, initialTime + 5));
        expected.add(new StreamRecord<>(6, initialTime + 6));
        expected.add(new StreamRecord<>(7, initialTime + 7));
        expected.add(new StreamRecord<>(8, initialTime + 8));
        expected.add(new Watermark(Long.MAX_VALUE));

        testLazyAsyncFunction.countDown();
        testHarness.endInput();

        // TODO FLINK-37981 send Long.MAX watermark downstream
        assertThat(unwrapEpochManager(testHarness).getOutputQueue().size()).isEqualTo(1);
        assertThat(
                        unwrapEpochManager(testHarness)
                                .getOutputQueue()
                                .get(0)
                                .getWatermark()
                                .getTimestamp())
                .isEqualTo(Long.MAX_VALUE);

        TestHarnessUtil.assertOutputEqualsSorted(
                "StateAndRestored Test Output was not correct.",
                expected,
                testHarness.getOutput(),
                new StreamRecordComparator());
        testHarness.close();
    }

    @Test
    public void testKeyedAsyncTimeoutFailure() throws Exception {
        testKeyedAsyncTimeout(
                new LazyAsyncFunction(),
                Optional.of(TimeoutException.class),
                List.of(new StreamRecord<>(2, 5L)));
    }

    @Test
    public void testKeyedAsyncTimeoutIgnore() throws Exception {
        testKeyedAsyncTimeout(
                new IgnoreTimeoutLazyAsyncFunction(),
                Optional.empty(),
                List.of(new StreamRecord<>(3, 0L), new StreamRecord<>(2, 5L)));
    }

    private void testKeyedAsyncTimeout(
            LazyAsyncFunction lazyAsyncFunction,
            Optional<Class<? extends Throwable>> expectedException,
            List<StreamRecord<Integer>> expectedRecords)
            throws Exception {
        final long timeout = 10L;
        try (final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                createKeyedTestHarness(lazyAsyncFunction, timeout, 10)) {
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
                    new ConcurrentLinkedQueue<>(expectedRecords);

            TestHarnessUtil.assertOutputEquals(
                    "Output is not correct.", expectedOutput, testHarness.getOutput());

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

    private static <OUT>
            KeyedOneInputStreamOperatorTestHarness<Integer, Integer, OUT> createKeyedTestHarness(
                    AsyncFunction<Integer, OUT> function, long timeout, int capacity)
                    throws Exception {

        return new KeyedOneInputStreamOperatorTestHarness<>(
                new TableKeyedAsyncWaitOperatorFactory<>(
                        function, TableKeyedAsyncWaitOperatorTest.keySelector, timeout, capacity),
                TableKeyedAsyncWaitOperatorTest.keySelector,
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

    private EpochManager<Integer> unwrapEpochManager(
            KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness) {
        TableKeyedAsyncWaitOperator<Integer, Integer, Integer> operator =
                (TableKeyedAsyncWaitOperator<Integer, Integer, Integer>) testHarness.getOperator();
        TableAsyncExecutionController<Integer, Integer, Integer> asyncExecutionController =
                operator.getAsyncExecutionController();
        return asyncExecutionController.getEpochManager();
    }

    private Optional<Epoch<Integer>> unwrapEpoch(
            KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness,
            long targetEpochWatermark) {
        return unwrapEpochManager(testHarness).getProperEpoch(new Watermark(targetEpochWatermark));
    }
}
