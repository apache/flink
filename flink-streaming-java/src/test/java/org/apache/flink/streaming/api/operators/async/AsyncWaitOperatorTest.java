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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link AsyncWaitOperator}. These test that:
 *
 * <ul>
 *   <li>Process StreamRecords and Watermarks in ORDERED mode
 *   <li>Process StreamRecords and Watermarks in UNORDERED mode
 *   <li>AsyncWaitOperator in operator chain
 *   <li>Snapshot state and restore state
 * </ul>
 */
public class AsyncWaitOperatorTest extends TestLogger {
    private static final long TIMEOUT = 1000L;

    @Rule public Timeout timeoutRule = new Timeout(10, TimeUnit.SECONDS);

    private static class MyAsyncFunction extends RichAsyncFunction<Integer, Integer> {
        private static final long serialVersionUID = 8522411971886428444L;

        private static final long TERMINATION_TIMEOUT = 5000L;
        private static final int THREAD_POOL_SIZE = 10;

        static ExecutorService executorService;
        static int counter = 0;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            synchronized (MyAsyncFunction.class) {
                if (counter == 0) {
                    executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
                }

                ++counter;
            }
        }

        @Override
        public void close() throws Exception {
            super.close();

            freeExecutor();
        }

        private void freeExecutor() {
            synchronized (MyAsyncFunction.class) {
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
            this.executorService.submit(
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

    /** A {@link Comparator} to compare {@link StreamRecord} while sorting them. */
    private class StreamRecordComparator implements Comparator<Object> {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 instanceof Watermark || o2 instanceof Watermark) {
                return 0;
            } else {
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
    }

    /** Test the AsyncWaitOperator with ordered mode and event time. */
    @Test
    public void testEventTimeOrdered() throws Exception {
        testEventTime(AsyncDataStream.OutputMode.ORDERED);
    }

    /** Test the AsyncWaitOperator with unordered mode and event time. */
    @Test
    public void testWaterMarkUnordered() throws Exception {
        testEventTime(AsyncDataStream.OutputMode.UNORDERED);
    }

    private void testEventTime(AsyncDataStream.OutputMode mode) throws Exception {
        final OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(new MyAsyncFunction(), TIMEOUT, 2, mode);

        final long initialTime = 0L;
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
            testHarness.processWatermark(new Watermark(initialTime + 2));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
        }

        // wait until all async collectors in the buffer have been emitted out.
        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
        expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
        expectedOutput.add(new Watermark(initialTime + 2));
        expectedOutput.add(new StreamRecord<>(6, initialTime + 3));

        if (AsyncDataStream.OutputMode.ORDERED == mode) {
            TestHarnessUtil.assertOutputEquals(
                    "Output with watermark was not correct.",
                    expectedOutput,
                    testHarness.getOutput());
        } else {
            Object[] jobOutputQueue = testHarness.getOutput().toArray();

            Assert.assertEquals(
                    "Watermark should be at index 2",
                    new Watermark(initialTime + 2),
                    jobOutputQueue[2]);
            Assert.assertEquals(
                    "StreamRecord 3 should be at the end",
                    new StreamRecord<>(6, initialTime + 3),
                    jobOutputQueue[3]);

            TestHarnessUtil.assertOutputEqualsSorted(
                    "Output for StreamRecords does not match",
                    expectedOutput,
                    testHarness.getOutput(),
                    new StreamRecordComparator());
        }
    }

    /** Test the AsyncWaitOperator with ordered mode and processing time. */
    @Test
    public void testProcessingTimeOrdered() throws Exception {
        testProcessingTime(AsyncDataStream.OutputMode.ORDERED);
    }

    /** Test the AsyncWaitOperator with unordered mode and processing time. */
    @Test
    public void testProcessingUnordered() throws Exception {
        testProcessingTime(AsyncDataStream.OutputMode.UNORDERED);
    }

    private void testProcessingTime(AsyncDataStream.OutputMode mode) throws Exception {
        final OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                createTestHarness(new MyAsyncFunction(), TIMEOUT, 6, mode);

        final long initialTime = 0L;
        final Queue<Object> expectedOutput = new ArrayDeque<>();

        testHarness.open();

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
            testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
            testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
            testHarness.processElement(new StreamRecord<>(4, initialTime + 4));
            testHarness.processElement(new StreamRecord<>(5, initialTime + 5));
            testHarness.processElement(new StreamRecord<>(6, initialTime + 6));
            testHarness.processElement(new StreamRecord<>(7, initialTime + 7));
            testHarness.processElement(new StreamRecord<>(8, initialTime + 8));
        }

        expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
        expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
        expectedOutput.add(new StreamRecord<>(6, initialTime + 3));
        expectedOutput.add(new StreamRecord<>(8, initialTime + 4));
        expectedOutput.add(new StreamRecord<>(10, initialTime + 5));
        expectedOutput.add(new StreamRecord<>(12, initialTime + 6));
        expectedOutput.add(new StreamRecord<>(14, initialTime + 7));
        expectedOutput.add(new StreamRecord<>(16, initialTime + 8));

        synchronized (testHarness.getCheckpointLock()) {
            testHarness.endInput();
            testHarness.close();
        }

        if (mode == AsyncDataStream.OutputMode.ORDERED) {
            TestHarnessUtil.assertOutputEquals(
                    "ORDERED Output was not correct.", expectedOutput, testHarness.getOutput());
        } else {
            TestHarnessUtil.assertOutputEqualsSorted(
                    "UNORDERED Output was not correct.",
                    expectedOutput,
                    testHarness.getOutput(),
                    new StreamRecordComparator());
        }
    }

    /** Tests that the AsyncWaitOperator works together with chaining. */
    @Test
    public void testOperatorChainWithProcessingTime() throws Exception {

        JobVertex chainedVertex = createChainedVertex(new MyAsyncFunction(), new MyAsyncFunction());

        final OneInputStreamTaskTestHarness<Integer, Integer> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        1,
                        1,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);
        testHarness.setupOutputForSingletonOperatorChain();

        testHarness.taskConfig = chainedVertex.getConfiguration();

        final StreamConfig streamConfig = testHarness.getStreamConfig();
        final StreamConfig operatorChainStreamConfig =
                new StreamConfig(chainedVertex.getConfiguration());
        streamConfig.setStreamOperatorFactory(
                operatorChainStreamConfig.getStreamOperatorFactory(
                        AsyncWaitOperatorTest.class.getClassLoader()));

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        long initialTimestamp = 0L;

        testHarness.processElement(new StreamRecord<>(5, initialTimestamp));
        testHarness.processElement(new StreamRecord<>(6, initialTimestamp + 1L));
        testHarness.processElement(new StreamRecord<>(7, initialTimestamp + 2L));
        testHarness.processElement(new StreamRecord<>(8, initialTimestamp + 3L));
        testHarness.processElement(new StreamRecord<>(9, initialTimestamp + 4L));

        testHarness.endInput();
        testHarness.waitForTaskCompletion();

        List<Object> expectedOutput = new LinkedList<>();
        expectedOutput.add(new StreamRecord<>(22, initialTimestamp));
        expectedOutput.add(new StreamRecord<>(26, initialTimestamp + 1L));
        expectedOutput.add(new StreamRecord<>(30, initialTimestamp + 2L));
        expectedOutput.add(new StreamRecord<>(34, initialTimestamp + 3L));
        expectedOutput.add(new StreamRecord<>(38, initialTimestamp + 4L));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Test for chained operator with AsyncWaitOperator failed",
                expectedOutput,
                testHarness.getOutput(),
                new StreamRecordComparator());
    }

    private JobVertex createChainedVertex(
            AsyncFunction<Integer, Integer> firstFunction,
            AsyncFunction<Integer, Integer> secondFunction) {

        StreamExecutionEnvironment chainEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // set parallelism to 2 to avoid chaining with source in case when available processors is
        // 1.
        chainEnv.setParallelism(2);

        // the input is only used to construct a chained operator, and they will not be used in the
        // real tests.
        DataStream<Integer> input = chainEnv.fromElements(1, 2, 3);

        input =
                addAsyncOperatorLegacyChained(
                        input, firstFunction, TIMEOUT, 6, AsyncDataStream.OutputMode.ORDERED);

        // the map function is designed to chain after async function. we place an Integer object in
        // it and
        // it is initialized in the open() method.
        // it is used to verify that operators in the operator chain should be opened from the tail
        // to the head,
        // so the result from AsyncWaitOperator can pass down successfully and correctly.
        // if not, the test can not be passed.
        input =
                input.map(
                        new RichMapFunction<Integer, Integer>() {
                            private static final long serialVersionUID = 1L;

                            private Integer initialValue = null;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                initialValue = 1;
                            }

                            @Override
                            public Integer map(Integer value) throws Exception {
                                return initialValue + value;
                            }
                        });

        input =
                addAsyncOperatorLegacyChained(
                        input, secondFunction, TIMEOUT, 3, AsyncDataStream.OutputMode.UNORDERED);

        input.map(
                        new MapFunction<Integer, Integer>() {
                            private static final long serialVersionUID = 5162085254238405527L;

                            @Override
                            public Integer map(Integer value) throws Exception {
                                return value;
                            }
                        })
                .startNewChain()
                .addSink(new DiscardingSink<Integer>());

        // be build our own OperatorChain
        final JobGraph jobGraph = chainEnv.getStreamGraph().getJobGraph();

        Assert.assertEquals(3, jobGraph.getVerticesSortedTopologicallyFromSources().size());

        return jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
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

        AsyncWaitOperatorFactory<Integer, Integer> factory =
                new AsyncWaitOperatorFactory<>(
                        new LazyAsyncFunction(), TIMEOUT, 4, AsyncDataStream.OutputMode.ORDERED);

        final StreamConfig streamConfig = testHarness.getStreamConfig();
        OperatorID operatorID = new OperatorID(42L, 4711L);
        streamConfig.setStreamOperatorFactory(factory);
        streamConfig.setOperatorID(operatorID);

        final TestTaskStateManager taskStateManagerMock = testHarness.getTaskStateManager();
        taskStateManagerMock.setWaitForReportLatch(new OneShotLatch());

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
                checkpointMetaData, CheckpointOptions.forCheckpointWithDefaultLocation(), false);

        taskStateManagerMock.getWaitForReportLatch().await();

        assertEquals(checkpointId, taskStateManagerMock.getReportedCheckpointId());

        LazyAsyncFunction.countDown();

        testHarness.endInput();
        testHarness.waitForTaskCompletion();

        // set the operator state from previous attempt into the restored one
        TaskStateSnapshot subtaskStates = taskStateManagerMock.getLastJobManagerTaskStateSnapshot();

        final OneInputStreamTaskTestHarness<Integer, Integer> restoredTaskHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO);

        restoredTaskHarness.setTaskStateSnapshot(checkpointId, subtaskStates);
        restoredTaskHarness.setupOutputForSingletonOperatorChain();

        AsyncWaitOperatorFactory<Integer, Integer> restoredOperator =
                new AsyncWaitOperatorFactory<>(
                        new MyAsyncFunction(), TIMEOUT, 6, AsyncDataStream.OutputMode.ORDERED);

        restoredTaskHarness.getStreamConfig().setStreamOperatorFactory(restoredOperator);
        restoredTaskHarness.getStreamConfig().setOperatorID(operatorID);

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
                        CheckpointOptions.forCheckpointWithDefaultLocation(),
                        false)
                .get();

        restoredTaskHarness.processElement(new StreamRecord<>(8, initialTime + 8));

        restoredTaskHarness.endInput();
        restoredTaskHarness.waitForTaskCompletion();

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
        expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
        expectedOutput.add(new StreamRecord<>(6, initialTime + 3));
        expectedOutput.add(new StreamRecord<>(8, initialTime + 4));
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
                createTestHarness(
                        lazyAsyncFunction, timeout, 2, AsyncDataStream.OutputMode.ORDERED);

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
                createTestHarness(
                        new MyAsyncFunction(), TIMEOUT, 1, AsyncDataStream.OutputMode.UNORDERED);

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
     * FLINK-6435
     *
     * <p>Tests that a user exception triggers the completion of a StreamElementQueueEntry and does
     * not wait to until another StreamElementQueueEntry is properly completed before it is
     * collected.
     */
    @Test
    public void testOrderedWaitUserExceptionHandling() throws Exception {
        testUserExceptionHandling(AsyncDataStream.OutputMode.ORDERED);
    }

    /**
     * FLINK-6435
     *
     * <p>Tests that a user exception triggers the completion of a StreamElementQueueEntry and does
     * not wait to until another StreamElementQueueEntry is properly completed before it is
     * collected.
     */
    @Test
    public void testUnorderedWaitUserExceptionHandling() throws Exception {
        testUserExceptionHandling(AsyncDataStream.OutputMode.UNORDERED);
    }

    private void testUserExceptionHandling(AsyncDataStream.OutputMode outputMode) throws Exception {
        OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createTestHarness(new UserExceptionAsyncFunction(), TIMEOUT, 2, outputMode);

        harness.getEnvironment().setExpectedExternalFailureCause(Throwable.class);
        harness.open();

        synchronized (harness.getCheckpointLock()) {
            harness.processElement(1, 1L);
        }

        synchronized (harness.getCheckpointLock()) {
            harness.close();
        }

        assertTrue(harness.getEnvironment().getActualExternalFailureCause().isPresent());
    }

    /** AsyncFunction which completes the result with an {@link Exception}. */
    private static class UserExceptionAsyncFunction implements AsyncFunction<Integer, Integer> {

        private static final long serialVersionUID = 6326568632967110990L;

        @Override
        public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture)
                throws Exception {
            resultFuture.completeExceptionally(new Exception("Test exception"));
        }
    }

    /**
     * FLINK-6435
     *
     * <p>Tests that timeout exceptions are properly handled in ordered output mode. The proper
     * handling means that a StreamElementQueueEntry is completed in case of a timeout exception.
     */
    @Test
    public void testOrderedWaitTimeoutHandling() throws Exception {
        testTimeoutExceptionHandling(AsyncDataStream.OutputMode.ORDERED);
    }

    /**
     * FLINK-6435
     *
     * <p>Tests that timeout exceptions are properly handled in ordered output mode. The proper
     * handling means that a StreamElementQueueEntry is completed in case of a timeout exception.
     */
    @Test
    public void testUnorderedWaitTimeoutHandling() throws Exception {
        testTimeoutExceptionHandling(AsyncDataStream.OutputMode.UNORDERED);
    }

    private void testTimeoutExceptionHandling(AsyncDataStream.OutputMode outputMode)
            throws Exception {
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
     * Tests that the AysncWaitOperator can restart if checkpointed queue was full.
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
                        AsyncDataStream.OutputMode.ORDERED);

        snapshotHarness.open();

        final OperatorSubtaskState snapshot;

        final ArrayList<Integer> expectedOutput = new ArrayList<>(capacity);

        try {
            synchronized (snapshotHarness.getCheckpointLock()) {
                for (int i = 0; i < capacity; i++) {
                    snapshotHarness.processElement(i, 0L);
                    expectedOutput.add(i);
                }
            }

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
                        AsyncDataStream.OutputMode.ORDERED);

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

    private static class ControllableAsyncFunction<IN> implements AsyncFunction<IN, IN> {

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

    private static class NoOpAsyncFunction<IN, OUT> implements AsyncFunction<IN, OUT> {
        private static final long serialVersionUID = -3060481953330480694L;

        @Override
        public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {
            // no op
        }
    }

    /**
     * This helper function is needed to check that the temporary fix for FLINK-13063 can be
     * backwards compatible with the old chaining behavior by setting the ChainingStrategy manually.
     * TODO: remove after a proper fix for FLINK-13063 is in place that allows chaining.
     */
    private <IN, OUT> SingleOutputStreamOperator<OUT> addAsyncOperatorLegacyChained(
            DataStream<IN> in,
            AsyncFunction<IN, OUT> func,
            long timeout,
            int bufSize,
            AsyncDataStream.OutputMode mode) {

        TypeInformation<OUT> outTypeInfo =
                TypeExtractor.getUnaryOperatorReturnType(
                        func,
                        AsyncFunction.class,
                        0,
                        1,
                        new int[] {1, 0},
                        in.getType(),
                        Utils.getCallLocationName(),
                        true);

        // create transform
        AsyncWaitOperatorFactory<IN, OUT> factory =
                new AsyncWaitOperatorFactory<>(
                        in.getExecutionEnvironment().clean(func), timeout, bufSize, mode);

        factory.setChainingStrategy(ChainingStrategy.ALWAYS);

        return in.transform("async wait operator", outTypeInfo, factory);
    }

    private static <OUT> OneInputStreamOperatorTestHarness<Integer, OUT> createTestHarness(
            AsyncFunction<Integer, OUT> function,
            long timeout,
            int capacity,
            AsyncDataStream.OutputMode outputMode)
            throws Exception {

        return new OneInputStreamOperatorTestHarness<>(
                new AsyncWaitOperatorFactory<>(function, timeout, capacity, outputMode),
                IntSerializer.INSTANCE);
    }
}
