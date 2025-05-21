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

package org.apache.flink.table.runtime.operators.aggregate.async;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.runtime.operators.aggregate.async.queue.KeyedAsyncOutputMode;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava33.com.google.common.collect.Lists;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link KeyedAsyncWaitOperator}. These test that:
 *
 * <ul>
 *   <li>Process StreamRecords and Watermarks in ORDERED mode
 *   <li>Process StreamRecords and Watermarks in ROW_TIME mode
 *   <li>Snapshot state and restore state
 * </ul>
 */
public class KeyedAsyncWaitOperatorTest extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(KeyedAsyncWaitOperatorTest.class);

    private static final long TIMEOUT = 1000L;

    @Rule public Timeout timeoutRule = new Timeout(100, TimeUnit.SECONDS);
    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    private abstract static class MyAbstractKeyedAsyncFunction<IN>
            implements KeyedAsyncFunction<Integer, IN, Integer> {
        private static final long serialVersionUID = 8522411971886428444L;

        private static final long TERMINATION_TIMEOUT = 5000L;
        private static final int THREAD_POOL_SIZE = 10;

        static ExecutorService executorService;
        static int counter = 0;

        @Override
        public void open(OpenContext openContext) throws Exception {

            synchronized (MyAbstractKeyedAsyncFunction.class) {
                if (counter == 0) {
                    executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
                }

                ++counter;
            }
        }

        @Override
        public void close() throws Exception {
            freeExecutor();
        }

        private void freeExecutor() {
            synchronized (MyAbstractKeyedAsyncFunction.class) {
                --counter;

                if (counter == 0) {
                    executorService.shutdown();

                    try {
                        if (!executorService.awaitTermination(
                                TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS)) {
                            executorService.shutdownNow();
                        }
                    } catch (InterruptedException interrupted) {
                        executorService.shutdownNow();

                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private static class MyAsyncFunction extends MyAbstractKeyedAsyncFunction<Integer> {
        private static final long serialVersionUID = -1504699677704123889L;

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture)
                throws Exception {
            executorService.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            resultFuture.complete(Collections.singletonList(input * 2));
                        }
                    });
        }
    }

    /**
     * A special {@link AsyncFunction} without issuing {@link ResultFuture#complete} until the latch
     * counts to zero. {@link ResultFuture#complete} until the latch counts to zero. This function
     * is used in the testStateSnapshotAndRestore, ensuring that {@link StreamElement} can stay in
     * the {@link StreamElementQueue} to be snapshotted while checkpointing.
     */
    private static class LazyAsyncFunction extends MyAsyncFunction {
        private static final long serialVersionUID = 3537791752703154670L;

        private static CountDownLatch latch;

        public LazyAsyncFunction() {
            latch = new CountDownLatch(1);
        }

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture)
                throws Exception {
            executorService.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                latch.await();
                            } catch (InterruptedException e) {
                                // do nothing
                            }

                            resultFuture.complete(Collections.singletonList(input));
                        }
                    });
        }

        public static void countDown() {
            latch.countDown();
        }
    }

    private static class LazyAsyncFunctionWithRunning extends MyAsyncFunction {
        private static final long serialVersionUID = 3537791752703154670L;
        private static final AtomicInteger nextToFinish = new AtomicInteger(-1);
        private static ConcurrentHashMap<Integer, AtomicBoolean> running =
                new ConcurrentHashMap<>();
        private static ConcurrentHashMap<Integer, AtomicBoolean> complete =
                new ConcurrentHashMap<>();
        private static AtomicInteger numActive = new AtomicInteger(0);

        public LazyAsyncFunctionWithRunning() {}

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture)
                throws Exception {
            executorService.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                trigger(input, running);
                                numActive.incrementAndGet();
                                synchronized (nextToFinish) {
                                    while (nextToFinish.get() != input) {
                                        nextToFinish.wait();
                                    }
                                    nextToFinish.set(-1);
                                    nextToFinish.notifyAll();
                                }
                                numActive.decrementAndGet();
                                trigger(input, complete);
                            } catch (InterruptedException e) {
                                LOG.error("Error while running async function.", e);
                            }
                            resultFuture.complete(Collections.singletonList(input));
                        }
                    });
        }

        public static void release(int toRelease) {
            synchronized (nextToFinish) {
                while (nextToFinish.get() != -1) {
                    try {
                        nextToFinish.wait();
                    } catch (InterruptedException e) {
                        LOG.error("Error while waiting to release.", e);
                    }
                }
                nextToFinish.set(toRelease);
                nextToFinish.notifyAll();
            }
        }

        public static boolean complete(Integer input) {
            return value(input, complete);
        }

        public static boolean running(Integer input) {
            return value(input, running);
        }

        private static void trigger(int key, ConcurrentHashMap<Integer, AtomicBoolean> map) {
            map.compute(
                    key,
                    (k, v) -> {
                        if (v != null) {
                            v.set(true);
                            return v;
                        } else {
                            return new AtomicBoolean(true);
                        }
                    });
        }

        private static boolean value(int key, ConcurrentHashMap<Integer, AtomicBoolean> map) {
            return map.compute(
                            key,
                            (k, v) -> {
                                if (v != null) {
                                    return v;
                                } else {
                                    return new AtomicBoolean(false);
                                }
                            })
                    .get();
        }

        public static long numActive() {
            return numActive.get();
        }
    }

    private static class InputReusedAsyncFunction
            extends MyAbstractKeyedAsyncFunction<Tuple1<Integer>> {

        private static final long serialVersionUID = 8627909616410487720L;

        @Override
        public void asyncInvoke(Tuple1<Integer> input, ResultFuture<Integer> resultFuture)
                throws Exception {
            executorService.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            resultFuture.complete(Collections.singletonList(input.f0 * 2));
                        }
                    });
        }
    }

    /**
     * A special {@link LazyAsyncFunction} for timeout handling. Complete the result future with 3
     * times the input when the timeout occurred.
     */
    private static class IgnoreTimeoutLazyAsyncFunction extends LazyAsyncFunction {
        private static final long serialVersionUID = 1428714561365346128L;

        @Override
        public void timeout(Integer input, ResultFuture<Integer> resultFuture) throws Exception {
            resultFuture.complete(Collections.singletonList(input * 3));
        }
    }

    /** Completes input at half the TIMEOUT and registers timeouts. */
    private static class TimeoutAfterCompletionTestFunction
            implements AsyncFunction<Integer, Integer> {
        static final AtomicBoolean TIMED_OUT = new AtomicBoolean(false);
        static final CountDownLatch COMPLETION_TRIGGER = new CountDownLatch(1);

        @Override
        public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) {
            ForkJoinPool.commonPool()
                    .submit(
                            () -> {
                                COMPLETION_TRIGGER.await();
                                resultFuture.complete(Collections.singletonList(input));
                                return null;
                            });
        }

        @Override
        public void timeout(Integer input, ResultFuture<Integer> resultFuture) {
            TIMED_OUT.set(true);
        }
    }

    /** A {@link Comparator} to compare {@link StreamRecord} while sorting them. */
    private class StreamRecordComparator implements Comparator<Object> {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 instanceof Watermark || o2 instanceof Watermark) {
                return 0;
            } else {
                StreamRecord<Integer> sr0 = (StreamRecord<Integer>) o1;
                StreamRecord<Integer> sr1 = (StreamRecord<Integer>) o2;

                return Long.compare(sr0.getTimestamp(), sr1.getTimestamp());
            }
        }
    }

    /** Test the KeyedAsyncWaitOperator with unordered mode and event time. */
    @Test
    public void testOrdered() throws Exception {
        testOutputOrdering(KeyedAsyncOutputMode.ORDERED);
    }

    private void testOutputOrdering(KeyedAsyncOutputMode mode) throws Exception {
        final OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(new MyAsyncFunction(), TIMEOUT, 4, mode);

        final long initialTime = 0L;
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(new StreamRecord<>(1, initialTime + 3));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 2));
            testHarness.processWatermark(new Watermark(initialTime + 3));
            testHarness.processElement(new StreamRecord<>(4, initialTime + 5));
            testHarness.processElement(new StreamRecord<>(5, initialTime + 4));
            testHarness.processWatermark(new Watermark(initialTime + 5));
        }

        // wait until all async collectors in the buffer have been emitted out.
        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        expectedOutput.add(new StreamRecord<>(2, initialTime + 3));
        expectedOutput.add(new StreamRecord<>(4, initialTime + 1));
        expectedOutput.add(new StreamRecord<>(6, initialTime + 2));
        expectedOutput.add(new Watermark(initialTime + 3));
        expectedOutput.add(new StreamRecord<>(8, initialTime + 5));
        expectedOutput.add(new StreamRecord<>(10, initialTime + 4));
        expectedOutput.add(new Watermark(initialTime + 5));

        if (KeyedAsyncOutputMode.ORDERED == mode) {
            TestHarnessUtil.assertOutputEquals(
                    "Output with watermark was not correct.",
                    expectedOutput,
                    testHarness.getOutput());
        } else {
            Object[] jobOutputQueue = testHarness.getOutput().toArray();

            Assert.assertEquals(
                    "Watermark should be at index 3",
                    new Watermark(initialTime + 3),
                    jobOutputQueue[3]);

            TestHarnessUtil.assertOutputEqualsSorted(
                    "Output for StreamRecords does not match",
                    expectedOutput,
                    testHarness.getOutput(),
                    new StreamRecordComparator());
        }
    }

    /** Test the KeyedAsyncWaitOperator with ordered mode. */
    @Test
    public void testLimitedActiveOrdered() throws Exception {
        testLimitedActive(KeyedAsyncOutputMode.ORDERED);
    }

    private void testLimitedActive(KeyedAsyncOutputMode mode) throws Exception {
        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);
        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                builder.setupOutputForSingletonOperatorChain(
                                new KeyedAsyncWaitOperatorFactory<>(
                                        new LazyAsyncFunctionWithRunning(),
                                        // We do a lot of waiting in this test, so give a longer
                                        // timeout.
                                        5 * TIMEOUT,
                                        2,
                                        mode))
                        .modifyStreamConfig(
                                streamConfig -> {
                                    streamConfig.setStateKeySerializer(IntSerializer.INSTANCE);
                                    streamConfig.setStatePartitioner(0, new EvenOddKeySelector());
                                })
                        .build()) {
            final long initialTime = 0L;

            testHarness.processElement(new StreamRecord<>(1, initialTime + 2));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 1));

            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.running(1));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.running(2));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.numActive() == 2);

            LazyAsyncFunctionWithRunning.release(1);

            testHarness.processElement(new Watermark(initialTime + 2));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 4));

            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.complete(1));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.running(2));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.running(3));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.numActive() == 2);

            LazyAsyncFunctionWithRunning.release(2);

            testHarness.processElement(new StreamRecord<>(4, initialTime + 3));

            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.complete(2));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.running(3));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.running(4));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.numActive() == 2);

            LazyAsyncFunctionWithRunning.release(3);
            testHarness.processElement(new Watermark(initialTime + 4));
            testHarness.processElement(new StreamRecord<>(5, initialTime + 5));

            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.complete(3));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.running(4));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.running(5));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.numActive() == 2);

            LazyAsyncFunctionWithRunning.release(4);
            testHarness.processElement(new Watermark(initialTime + 5));

            LazyAsyncFunctionWithRunning.release(5);
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.complete(4));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.complete(5));
            testHarness.processUntil(() -> LazyAsyncFunctionWithRunning.numActive() == 0);

            testHarness.processUntil(() -> testHarness.getOutput().size() == 8);
        }
    }

    static class EvenOddKeySelector implements KeySelector<Integer, Integer> {
        private static final long serialVersionUID = -1927524994684581374L;

        @Override
        public Integer getKey(Integer value) throws Exception {
            return value % 2;
        }
    }

    static class TupleEventOddKeySelector implements KeySelector<Tuple1<Integer>, Integer> {
        private static final long serialVersionUID = -1927524994684581374L;

        @Override
        public Integer getKey(Tuple1<Integer> value) throws Exception {
            return value.f0 % 2;
        }
    }

    @Test
    public void testStateSnapshotAndRestore() throws Exception {
        final OneInputStreamTaskTestHarness<Integer, Integer> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        1,
                        1,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();

        KeyedAsyncWaitOperatorFactory<Integer, Integer, Integer> factory =
                new KeyedAsyncWaitOperatorFactory<>(
                        new LazyAsyncFunction(), TIMEOUT, 4, KeyedAsyncOutputMode.ORDERED);

        final StreamConfig streamConfig = testHarness.getStreamConfig();
        OperatorID operatorID = new OperatorID(42L, 4711L);
        streamConfig.setStreamOperatorFactory(factory);
        streamConfig.setOperatorID(operatorID);
        streamConfig.setStateKeySerializer(IntSerializer.INSTANCE);
        streamConfig.setStatePartitioner(0, new EvenOddKeySelector());

        final TestTaskStateManager taskStateManagerMock = testHarness.getTaskStateManager();

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        final OneInputStreamTask<Integer, Integer> task = testHarness.getTask();

        final long initialTime = 0L;

        testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
        testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
        testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
        testHarness.processElement(new StreamRecord<>(4, initialTime + 4));

        testHarness.waitForInputProcessing();

        final long checkpointId = 1L;
        final long checkpointTimestamp = 1L;

        final CheckpointMetaData checkpointMetaData =
                new CheckpointMetaData(checkpointId, checkpointTimestamp);

        task.triggerCheckpointAsync(
                checkpointMetaData, CheckpointOptions.forCheckpointWithDefaultLocation());

        taskStateManagerMock.getWaitForReportLatch().await();

        assertEquals(checkpointId, taskStateManagerMock.getReportedCheckpointId());

        LazyAsyncFunction.countDown();

        testHarness.endInput();
        testHarness.waitForTaskCompletion();

        // set the keyed state from previous attempt into the restored one
        TaskStateSnapshot subtaskStates = taskStateManagerMock.getLastJobManagerTaskStateSnapshot();

        final OneInputStreamTaskTestHarness<Integer, Integer> restoredTaskHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);

        restoredTaskHarness.setTaskStateSnapshot(checkpointId, subtaskStates);
        restoredTaskHarness.setupOutputForSingletonOperatorChain();

        KeyedAsyncWaitOperatorFactory<Integer, Integer, Integer> restoredOperator =
                new KeyedAsyncWaitOperatorFactory<>(
                        new MyAsyncFunction(), TIMEOUT, 6, KeyedAsyncOutputMode.ORDERED);

        restoredTaskHarness.getStreamConfig().setStreamOperatorFactory(restoredOperator);
        restoredTaskHarness.getStreamConfig().setOperatorID(operatorID);
        restoredTaskHarness.getStreamConfig().setStateKeySerializer(IntSerializer.INSTANCE);
        restoredTaskHarness.getStreamConfig().setStatePartitioner(0, new EvenOddKeySelector());

        restoredTaskHarness.invoke();
        restoredTaskHarness.waitForTaskRunning();

        final OneInputStreamTask<Integer, Integer> restoredTask = restoredTaskHarness.getTask();

        restoredTaskHarness.processElement(new StreamRecord<>(5, initialTime + 5));
        restoredTaskHarness.processElement(new StreamRecord<>(6, initialTime + 6));
        restoredTaskHarness.processElement(new StreamRecord<>(7, initialTime + 7));

        // trigger the checkpoint while processing stream elements
        restoredTask
                .triggerCheckpointAsync(
                        new CheckpointMetaData(checkpointId, checkpointTimestamp),
                        CheckpointOptions.forCheckpointWithDefaultLocation())
                .get();

        restoredTaskHarness.processElement(new StreamRecord<>(8, initialTime + 8));

        restoredTaskHarness.endInput();
        restoredTaskHarness.waitForTaskCompletion();

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        // Note that the restore only keeps order within the same key, so the output may not be
        // identical to the original input order.
        // 1, 3 Restored
        expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
        expectedOutput.add(new StreamRecord<>(6, initialTime + 3));
        // 2, 4 Restored
        expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
        expectedOutput.add(new StreamRecord<>(8, initialTime + 4));
        // New elements
        expectedOutput.add(new StreamRecord<>(10, initialTime + 5));
        expectedOutput.add(new StreamRecord<>(12, initialTime + 6));
        expectedOutput.add(new StreamRecord<>(14, initialTime + 7));
        expectedOutput.add(new StreamRecord<>(16, initialTime + 8));

        // remove CheckpointBarrier which is not expected
        restoredTaskHarness.getOutput().removeIf(record -> record instanceof CheckpointBarrier);

        TestHarnessUtil.assertOutputEquals(
                "StateAndRestored Test Output was not correct.",
                expectedOutput,
                restoredTaskHarness.getOutput());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testObjectReused() throws Exception {
        TypeSerializer[] fieldSerializers = new TypeSerializer[] {IntSerializer.INSTANCE};
        TupleSerializer<Tuple1> inputSerializer =
                new TupleSerializer<>(Tuple1.class, fieldSerializers);
        KeyedAsyncWaitOperatorFactory<Integer, Tuple1<Integer>, Integer> factory =
                new KeyedAsyncWaitOperatorFactory<>(
                        new InputReusedAsyncFunction(), TIMEOUT, 4, KeyedAsyncOutputMode.ORDERED);

        //noinspection unchecked
        final OneInputStreamOperatorTestHarness<Tuple1<Integer>, Integer> testHarness =
                new OneInputStreamOperatorTestHarness(factory, inputSerializer);
        // enable object reuse
        testHarness.getExecutionConfig().enableObjectReuse();
        testHarness.getStreamConfig().setStateKeySerializer(IntSerializer.INSTANCE);
        testHarness.getStreamConfig().setStatePartitioner(0, new TupleEventOddKeySelector());
        testHarness.getStreamConfig().serializeAllConfigs();

        final long initialTime = 0L;
        Tuple1<Integer> reusedTuple = new Tuple1<>();
        StreamRecord<Tuple1<Integer>> reusedRecord = new StreamRecord<>(reusedTuple, -1L);

        testHarness.setup();
        testHarness.open();

        synchronized (testHarness.getCheckpointLock()) {
            reusedTuple.setFields(1);
            reusedRecord.setTimestamp(initialTime + 1);
            testHarness.processElement(reusedRecord);

            reusedTuple.setFields(2);
            reusedRecord.setTimestamp(initialTime + 2);
            testHarness.processElement(reusedRecord);

            reusedTuple.setFields(3);
            reusedRecord.setTimestamp(initialTime + 3);
            testHarness.processElement(reusedRecord);

            reusedTuple.setFields(4);
            reusedRecord.setTimestamp(initialTime + 4);
            testHarness.processElement(reusedRecord);
        }

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
        expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
        expectedOutput.add(new StreamRecord<>(6, initialTime + 3));
        expectedOutput.add(new StreamRecord<>(8, initialTime + 4));

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        TestHarnessUtil.assertOutputEquals(
                "StateAndRestoredWithObjectReuse Test Output was not correct.",
                expectedOutput,
                testHarness.getOutput());
    }

    @Test
    public void testAsyncTimeoutFailure() throws Exception {
        testAsyncTimeout(
                new LazyAsyncFunction(),
                Optional.of(TimeoutException.class),
                new StreamRecord<>(2, 5L));
    }

    @Test
    public void testAsyncTimeoutIgnore() throws Exception {
        testAsyncTimeout(
                new IgnoreTimeoutLazyAsyncFunction(),
                Optional.empty(),
                new StreamRecord<>(3, 0L),
                new StreamRecord<>(2, 5L));
    }

    private void testAsyncTimeout(
            LazyAsyncFunction lazyAsyncFunction,
            Optional<Class<? extends Throwable>> expectedException,
            StreamRecord<Integer>... expectedRecords)
            throws Exception {
        final long timeout = 10L;

        final OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(lazyAsyncFunction, timeout, 2, KeyedAsyncOutputMode.ORDERED);

        final MockEnvironment mockEnvironment = testHarness.getEnvironment();
        mockEnvironment.setExpectedExternalFailureCause(Throwable.class);

        final long initialTime = 0L;
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.setProcessingTime(initialTime);

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(new StreamRecord<>(1, initialTime));
            testHarness.setProcessingTime(initialTime + 5L);
            testHarness.processElement(new StreamRecord<>(2, initialTime + 5L));
        }

        // trigger the timeout of the first stream record
        testHarness.setProcessingTime(initialTime + timeout + 1L);

        // allow the second async stream record to be processed
        lazyAsyncFunction.countDown();

        // wait until all async collectors in the buffer have been emitted out.
        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        expectedOutput.addAll(Arrays.asList(expectedRecords));

        TestHarnessUtil.assertOutputEquals(
                "Output with watermark was not correct.", expectedOutput, testHarness.getOutput());

        if (expectedException.isPresent()) {
            assertTrue(mockEnvironment.getActualExternalFailureCause().isPresent());
            assertTrue(
                    ExceptionUtils.findThrowable(
                                    mockEnvironment.getActualExternalFailureCause().get(),
                                    expectedException.get())
                            .isPresent());
        }
    }

    /**
     * FLINK-5652 Tests that registered timers are properly canceled upon completion of a {@link
     * StreamElement} in order to avoid resource leaks because TriggerTasks hold a reference on the
     * StreamRecordQueueEntry.
     */
    @Test
    public void testTimeoutCleanup() throws Exception {
        OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createTestHarness(new MyAsyncFunction(), TIMEOUT, 1, KeyedAsyncOutputMode.ORDERED);

        harness.open();

        synchronized (harness.getCheckpointLock()) {
            harness.processElement(42, 1L);
        }

        synchronized (harness.getCheckpointLock()) {
            harness.endInput();
            harness.close();
        }

        // check that we actually outputted the result of the single input
        assertEquals(
                Arrays.asList(new StreamRecord(42 * 2, 1L)), new ArrayList<>(harness.getOutput()));

        // check that we have cancelled our registered timeout
        assertEquals(0, harness.getProcessingTimeService().getNumActiveTimers());
    }

    /**
     * Checks if timeout has been called after the element has been completed within the timeout.
     *
     * @see <a href="https://issues.apache.org/jira/browse/FLINK-22573">FLINK-22573</a>
     */
    @Test
    public void testTimeoutAfterComplete() throws Exception {
        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);
        try (StreamTaskMailboxTestHarness<Integer> harness =
                builder.setupOutputForSingletonOperatorChain(
                                new AsyncWaitOperatorFactory<>(
                                        new TimeoutAfterCompletionTestFunction(),
                                        TIMEOUT,
                                        1,
                                        AsyncDataStream.OutputMode.UNORDERED))
                        .build()) {
            harness.processElement(new StreamRecord<>(1));
            // add a timer after AsyncIO added its timer to verify that AsyncIO timer is processed
            ScheduledFuture<?> testTimer =
                    harness.getTimerService()
                            .registerTimer(
                                    harness.getTimerService().getCurrentProcessingTime() + TIMEOUT,
                                    ts -> {});
            // trigger the regular completion in AsyncIO
            TimeoutAfterCompletionTestFunction.COMPLETION_TRIGGER.countDown();
            // wait until all timers have been processed
            testTimer.get();
            // handle normal completion call outputting the element in mailbox thread
            harness.processAll();
            assertEquals(
                    Collections.singleton(new StreamRecord<>(1)),
                    new HashSet<>(harness.getOutput()));
            assertFalse("no timeout expected", TimeoutAfterCompletionTestFunction.TIMED_OUT.get());
        }
    }

    /**
     * FLINK-6435
     *
     * <p>Tests that a user exception triggers the completion of a StreamElementQueueEntry and does
     * not wait to until another StreamElementQueueEntry is properly completed before it is
     * collected.
     */
    @Test
    public void testOrderedWaitUserExceptionHandling() throws Exception {
        testUserExceptionHandling(KeyedAsyncOutputMode.ORDERED);
    }

    private void testUserExceptionHandling(KeyedAsyncOutputMode outputMode) throws Exception {
        OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createTestHarness(new UserExceptionAsyncFunction(), TIMEOUT, 2, outputMode);

        harness.getEnvironment().setExpectedExternalFailureCause(Throwable.class);
        harness.open();

        synchronized (harness.getCheckpointLock()) {
            harness.processElement(1, 1L);
            harness.processWatermark(new Watermark(1));
        }

        synchronized (harness.getCheckpointLock()) {
            harness.endInput();
            harness.close();
        }

        assertTrue(harness.getEnvironment().getActualExternalFailureCause().isPresent());
    }

    /** AsyncFunction which completes the result with an {@link Exception}. */
    private static class UserExceptionAsyncFunction
            implements KeyedAsyncFunction<Integer, Integer, Integer> {

        private static final long serialVersionUID = 6326568632967110990L;

        @Override
        public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture)
                throws Exception {
            resultFuture.completeExceptionally(new Exception("Test exception"));
        }

        @Override
        public void open(OpenContext context) throws Exception {}
    }

    /**
     * FLINK-6435
     *
     * <p>Tests that timeout exceptions are properly handled in ordered output mode. The proper
     * handling means that a StreamElementQueueEntry is completed in case of a timeout exception.
     */
    @Test
    public void testOrderedWaitTimeoutHandling() throws Exception {
        testTimeoutExceptionHandling(KeyedAsyncOutputMode.ORDERED);
    }

    private void testTimeoutExceptionHandling(KeyedAsyncOutputMode outputMode) throws Exception {
        OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createTestHarness(new NoOpAsyncFunction<>(), 10L, 2, outputMode);

        harness.getEnvironment().setExpectedExternalFailureCause(Throwable.class);
        harness.open();

        synchronized (harness.getCheckpointLock()) {
            harness.processElement(1, 1L);
        }

        harness.setProcessingTime(10L);

        synchronized (harness.getCheckpointLock()) {
            harness.close();
        }
    }

    /**
     * Tests that the AsyncWaitOperator can restart if checkpointed queue was full.
     *
     * <p>See FLINK-7949
     */
    @Test(timeout = 10000)
    public void testRestartWithFullQueue() throws Exception {
        final int capacity = 10;

        // 1. create the snapshot which contains capacity + 1 elements
        final CompletableFuture<Void> trigger = new CompletableFuture<>();

        final OneInputStreamOperatorTestHarness<Integer, Integer> snapshotHarness =
                createTestHarness(
                        new ControllableAsyncFunction<>(
                                trigger), // the NoOpAsyncFunction is like a blocking function
                        1000L,
                        capacity,
                        KeyedAsyncOutputMode.ORDERED);

        snapshotHarness.open();

        final OperatorSubtaskState snapshot;

        final ArrayList<Integer> expectedOutput = new ArrayList<>(capacity);
        final ArrayList<Integer> odds = new ArrayList<>(capacity / 2);
        final ArrayList<Integer> evens = new ArrayList<>(capacity / 2);

        try {
            synchronized (snapshotHarness.getCheckpointLock()) {
                for (int i = 0; i < capacity; i++) {
                    snapshotHarness.processElement(i, 0L);
                    if (i % 2 == 0) {
                        evens.add(i);
                    } else {
                        odds.add(i);
                    }
                }
            }

            expectedOutput.addAll(odds);
            expectedOutput.addAll(evens);

            synchronized (snapshotHarness.getCheckpointLock()) {
                // execute the snapshot within the checkpoint lock, because then it is guaranteed
                // that the lastElementWriter has written the exceeding element
                snapshot = snapshotHarness.snapshot(0L, 0L);
            }

            // trigger the computation to make the close call finish
            trigger.complete(null);
        } finally {
            synchronized (snapshotHarness.getCheckpointLock()) {
                snapshotHarness.close();
            }
        }

        // 2. restore the snapshot and check that we complete
        final OneInputStreamOperatorTestHarness<Integer, Integer> recoverHarness =
                createTestHarness(
                        new ControllableAsyncFunction<>(CompletableFuture.completedFuture(null)),
                        1000L,
                        capacity,
                        KeyedAsyncOutputMode.ORDERED);

        recoverHarness.initializeState(snapshot);

        synchronized (recoverHarness.getCheckpointLock()) {
            recoverHarness.open();
        }

        synchronized (recoverHarness.getCheckpointLock()) {
            recoverHarness.endInput();
            recoverHarness.close();
        }

        final ConcurrentLinkedQueue<Object> output = recoverHarness.getOutput();

        final List<Integer> outputElements =
                output.stream()
                        .map(r -> ((StreamRecord<Integer>) r).getValue())
                        .collect(Collectors.toList());

        assertThat(outputElements, Matchers.equalTo(expectedOutput));
    }

    @Test
    public void testIgnoreAsyncOperatorRecordsOnDrain() throws Exception {
        // given: Async wait operator which are able to collect result futures.
        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);
        SharedReference<List<ResultFuture<?>>> resultFutures = sharedObjects.add(new ArrayList<>());
        try (StreamTaskMailboxTestHarness<Integer> harness =
                builder.setupOutputForSingletonOperatorChain(
                                new KeyedAsyncWaitOperatorFactory<>(
                                        new CollectableFuturesAsyncFunction<>(resultFutures),
                                        TIMEOUT,
                                        5,
                                        KeyedAsyncOutputMode.ORDERED))
                        .modifyStreamConfig(
                                streamConfig -> {
                                    streamConfig.setStateKeySerializer(IntSerializer.INSTANCE);
                                    streamConfig.setStatePartitioner(0, new EvenOddKeySelector());
                                })
                        .build()) {

            // when: Processing at least two elements in reverse order to keep completed queue not
            // empty.
            harness.processElement(new StreamRecord<>(1));
            harness.processElement(new StreamRecord<>(2));

            for (ResultFuture<?> resultFuture : Lists.reverse(resultFutures.get())) {
                resultFuture.complete(Collections.emptyList());
            }

            // then: All records from async operator should be ignored during drain since they will
            // be processed on recovery.
            harness.finishProcessing();
            assertTrue(harness.getOutput().isEmpty());
        }
    }

    @Test
    public void testProcessingTimeWithMailboxThreadOrdered() throws Exception {
        testProcessingTimeWithCallThread(KeyedAsyncOutputMode.ORDERED);
    }

    @Test
    public void testProcessingTimeWithMailboxThreadError() throws Exception {
        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);
        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                builder.setupOutputForSingletonOperatorChain(
                                new KeyedAsyncWaitOperatorFactory<>(
                                        new CallThreadAsyncFunctionError(),
                                        TIMEOUT,
                                        4,
                                        KeyedAsyncOutputMode.ORDERED))
                        .modifyStreamConfig(
                                streamConfig -> {
                                    streamConfig.setStateKeySerializer(IntSerializer.INSTANCE);
                                    streamConfig.setStatePartitioner(0, new EvenOddKeySelector());
                                })
                        .build()) {
            final long initialTime = 0L;
            AtomicReference<Throwable> error = new AtomicReference<>();
            testHarness.getStreamMockEnvironment().setExternalExceptionHandler(error::set);

            // Sometimes, processElement invoke the async function immediately, so we should catch
            // any exception.
            try {
                testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
                while (error.get() == null) {
                    testHarness.processAll();
                }
            } catch (Exception e) {
                // This simulates a mailbox failure failing the job
                error.set(e);
            }

            ExceptionUtils.assertThrowable(error.get(), ExpectedTestException.class);

            testHarness.endInput();
        }
    }

    private void testProcessingTimeWithCallThread(KeyedAsyncOutputMode mode) throws Exception {
        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);
        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                builder.setupOutputForSingletonOperatorChain(
                                new KeyedAsyncWaitOperatorFactory<>(
                                        new CallThreadAsyncFunction(), TIMEOUT, 4, mode))
                        .modifyStreamConfig(
                                streamConfig -> {
                                    streamConfig.setStateKeySerializer(IntSerializer.INSTANCE);
                                    streamConfig.setStatePartitioner(0, new EvenOddKeySelector());
                                })
                        .build()) {

            final long initialTime = 0L;
            final Queue<Object> expectedOutput = new ArrayDeque<>();

            testHarness.processElement(new StreamRecord<>(1, initialTime + 3));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 2));
            testHarness.processElement(new Watermark(initialTime + 3));

            expectedOutput.add(new StreamRecord<>(2, initialTime + 3));
            expectedOutput.add(new StreamRecord<>(4, initialTime + 1));
            expectedOutput.add(new StreamRecord<>(6, initialTime + 2));
            expectedOutput.add(new Watermark(initialTime + 3));

            while (testHarness.getOutput().size() < expectedOutput.size()) {
                testHarness.processAll();
            }

            TestHarnessUtil.assertOutputEquals(
                    "ORDERED Output was not correct.", expectedOutput, testHarness.getOutput());

            testHarness.endInput();
        }
    }

    private static class CollectableFuturesAsyncFunction<K, IN>
            implements KeyedAsyncFunction<K, IN, IN> {

        private static final long serialVersionUID = -4214078239227288637L;

        private final SharedReference<List<ResultFuture<?>>> resultFutures;

        private CollectableFuturesAsyncFunction(
                SharedReference<List<ResultFuture<?>>> resultFutures) {
            this.resultFutures = resultFutures;
        }

        @Override
        public void asyncInvoke(IN input, ResultFuture<IN> resultFuture) throws Exception {
            resultFutures.get().add(resultFuture);
        }
    }

    private static class ControllableAsyncFunction<K, IN> implements KeyedAsyncFunction<K, IN, IN> {

        private static final long serialVersionUID = -4214078239267288636L;

        private transient CompletableFuture<Void> trigger;

        private ControllableAsyncFunction(CompletableFuture<Void> trigger) {
            this.trigger = Preconditions.checkNotNull(trigger);
        }

        @Override
        public void asyncInvoke(IN input, ResultFuture<IN> resultFuture) throws Exception {
            trigger.thenAccept(v -> resultFuture.complete(Collections.singleton(input)));
        }
    }

    private static class NoOpAsyncFunction<K, IN, OUT> implements KeyedAsyncFunction<K, IN, OUT> {
        private static final long serialVersionUID = -3060481953330480694L;

        @Override
        public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {
            // no op
        }
    }

    private static <OUT> OneInputStreamOperatorTestHarness<Integer, OUT> createTestHarness(
            KeyedAsyncFunction<Integer, Integer, OUT> function,
            long timeout,
            int capacity,
            KeyedAsyncOutputMode outputMode)
            throws Exception {

        OneInputStreamOperatorTestHarness<Integer, OUT> result =
                new OneInputStreamOperatorTestHarness<>(
                        new KeyedAsyncWaitOperatorFactory<>(
                                function, timeout, capacity, outputMode),
                        IntSerializer.INSTANCE);
        result.getStreamConfig().setStateKeySerializer(IntSerializer.INSTANCE);
        result.getStreamConfig().setStatePartitioner(0, new EvenOddKeySelector());
        result.getStreamConfig().serializeAllConfigs();
        return result;
    }

    private static <OUT> OneInputStreamOperatorTestHarness<Integer, OUT> createTestHarnessWithRetry(
            KeyedAsyncFunction<Integer, Integer, OUT> function,
            long timeout,
            int capacity,
            KeyedAsyncOutputMode outputMode)
            throws Exception {

        return new OneInputStreamOperatorTestHarness<>(
                new KeyedAsyncWaitOperatorFactory<>(function, timeout, capacity, outputMode),
                IntSerializer.INSTANCE);
    }

    private static class CallThreadAsyncFunction extends MyAbstractKeyedAsyncFunction<Integer> {
        private static final long serialVersionUID = -1504699677704123889L;

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture)
                throws Exception {
            final Thread callThread = Thread.currentThread();
            executorService.submit(
                    () ->
                            resultFuture.complete(
                                    () -> {
                                        assertEquals(callThread, Thread.currentThread());
                                        return Collections.singletonList(input * 2);
                                    }));
        }
    }

    private static class CallThreadAsyncFunctionError
            extends MyAbstractKeyedAsyncFunction<Integer> {
        private static final long serialVersionUID = -1504699677704123889L;

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture)
                throws Exception {
            executorService.submit(
                    () ->
                            resultFuture.complete(
                                    () -> {
                                        throw new ExpectedTestException();
                                    }));
        }
    }
}
