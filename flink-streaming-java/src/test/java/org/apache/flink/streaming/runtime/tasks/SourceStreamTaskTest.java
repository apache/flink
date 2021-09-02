/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.writer.RecordOrEventCollectingResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.LifeCycleMonitor.LifeCyclePhase;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.CheckedSupplier;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT_SUSPEND;
import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT_TERMINATE;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.apache.flink.streaming.runtime.tasks.StreamTaskFinalCheckpointsTest.triggerCheckpoint;
import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * These tests verify that the RichFunction methods are called (in correct order). And that
 * checkpointing/element emission don't occur concurrently.
 */
public class SourceStreamTaskTest extends SourceStreamTaskTestBase {

    @Test
    public void testInputEndedBeforeStopWithSavepointConfirmed() throws Exception {
        CancelTestSource source =
                new CancelTestSource(
                        STRING_TYPE_INFO.createSerializer(new ExecutionConfig()), "src");
        TestBoundedOneInputStreamOperator chainTail = new TestBoundedOneInputStreamOperator("t");
        StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(SourceStreamTask::new, STRING_TYPE_INFO)
                        .setupOperatorChain(
                                new OperatorID(),
                                new StreamSource<String, CancelTestSource<String>>(source))
                        .chain(
                                new OperatorID(),
                                chainTail,
                                STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
                        .finish()
                        .build();
        Future<Boolean> triggerFuture =
                harness.streamTask.triggerCheckpointAsync(
                        new CheckpointMetaData(1, 1),
                        new CheckpointOptions(SAVEPOINT_SUSPEND, getDefault()));
        while (!triggerFuture.isDone()) {
            harness.streamTask.runMailboxStep();
        }
        // instead of completing stop with savepoint via `notifyCheckpointCompleted`
        // we simulate that source has finished first. As a result, we expect that the endInput
        // should have been issued
        source.cancel();
        harness.streamTask.invoke();
        harness.waitForTaskCompletion();
        assertTrue(TestBoundedOneInputStreamOperator.isInputEnded());
    }

    /** This test verifies that open() and close() are correctly called by the StreamTask. */
    @Test
    public void testOpenClose() throws Exception {
        final StreamTaskTestHarness<String> testHarness =
                new StreamTaskTestHarness<>(SourceStreamTask::new, STRING_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        StreamSource<String, ?> sourceOperator = new StreamSource<>(new OpenCloseTestSource());
        streamConfig.setStreamOperator(sourceOperator);
        streamConfig.setOperatorID(new OperatorID());

        testHarness.invoke();
        testHarness.waitForTaskCompletion();

        assertTrue("RichFunction methods where not called.", OpenCloseTestSource.closeCalled);

        List<String> resultElements =
                TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
        Assert.assertEquals(10, resultElements.size());
    }

    @Test
    public void testMetrics() throws Exception {
        testMetrics(
                SourceStreamTask::new,
                SimpleOperatorFactory.of(
                        new StreamSource<Integer, SourceFunction<Integer>>(
                                new CancelTestSource(
                                        INT_TYPE_INFO.createSerializer(new ExecutionConfig()),
                                        42))),
                is(Double.NaN));
    }

    /**
     * This test ensures that the SourceStreamTask properly serializes checkpointing and element
     * emission. This also verifies that there are no concurrent invocations of the checkpoint
     * method on the source operator.
     *
     * <p>The source emits elements and performs checkpoints. We have several checkpointer threads
     * that fire checkpoint requests at the source task.
     *
     * <p>If element emission and checkpointing are not in series the count of elements at the
     * beginning of a checkpoint and at the end of a checkpoint are not the same because the source
     * kept emitting elements while the checkpoint was ongoing.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testCheckpointing() throws Exception {
        final int numElements = 100;
        final int numCheckpoints = 100;
        final int numCheckpointers = 1;
        final int checkpointInterval = 5; // in ms
        final int sourceCheckpointDelay =
                1000; // how many random values we sum up in storeCheckpoint
        final int sourceReadDelay = 1; // in ms

        ExecutorService executor = Executors.newFixedThreadPool(10);
        try {
            final TupleTypeInfo<Tuple2<Long, Integer>> typeInfo =
                    new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

            final StreamTaskTestHarness<Tuple2<Long, Integer>> testHarness =
                    new StreamTaskTestHarness<>(SourceStreamTask::new, typeInfo);
            testHarness.setupOutputForSingletonOperatorChain();

            StreamConfig streamConfig = testHarness.getStreamConfig();
            StreamSource<Tuple2<Long, Integer>, ?> sourceOperator =
                    new StreamSource<>(
                            new MockSource(numElements, sourceCheckpointDelay, sourceReadDelay));
            streamConfig.setStreamOperator(sourceOperator);
            streamConfig.setOperatorID(new OperatorID());

            // prepare the

            Future<Boolean>[] checkpointerResults = new Future[numCheckpointers];

            // invoke this first, so the tasks are actually running when the checkpoints are
            // scheduled
            testHarness.invoke();
            testHarness.waitForTaskRunning();

            final StreamTask<Tuple2<Long, Integer>, ?> sourceTask = testHarness.getTask();

            for (int i = 0; i < numCheckpointers; i++) {
                checkpointerResults[i] =
                        executor.submit(
                                new Checkpointer(numCheckpoints, checkpointInterval, sourceTask));
            }

            testHarness.waitForTaskCompletion();

            // Get the result from the checkpointers, if these threw an exception it
            // will be rethrown here
            for (int i = 0; i < numCheckpointers; i++) {
                if (!checkpointerResults[i].isDone()) {
                    checkpointerResults[i].cancel(true);
                }
                if (!checkpointerResults[i].isCancelled()) {
                    checkpointerResults[i].get();
                }
            }

            List<Tuple2<Long, Integer>> resultElements =
                    TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
            Assert.assertEquals(numElements, resultElements.size());
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testClosingAllOperatorsOnChainProperly() throws Exception {
        final StreamTaskTestHarness<String> testHarness =
                new StreamTaskTestHarness<>(SourceStreamTask::new, STRING_TYPE_INFO);

        testHarness
                .setupOperatorChain(
                        new OperatorID(),
                        new OutputRecordInCloseTestSource<>(
                                "Source0",
                                new FromElementsFunction<>(StringSerializer.INSTANCE, "Hello")))
                .chain(
                        new OperatorID(),
                        new TestBoundedOneInputStreamOperator("Operator1"),
                        STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
                .finish();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        streamConfig.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        testHarness.invoke();
        testHarness.waitForTaskCompletion();

        ArrayList<Object> expected = new ArrayList<>();
        Collections.addAll(
                expected,
                new StreamRecord<>("Hello"),
                new StreamRecord<>("[Source0]: End of input"),
                Watermark.MAX_WATERMARK,
                new StreamRecord<>("[Source0]: Finish"),
                new StreamRecord<>("[Operator1]: End of input"),
                new StreamRecord<>("[Operator1]: Finish"));

        final Object[] output = testHarness.getOutput().toArray();
        assertArrayEquals("Output was not correct.", expected.toArray(), output);
    }

    @Test
    public void testNotMarkingEndOfInputWhenTaskCancelled() throws Exception {
        final StreamTaskTestHarness<String> testHarness =
                new StreamTaskTestHarness<>(SourceStreamTask::new, STRING_TYPE_INFO);

        testHarness
                .setupOperatorChain(
                        new OperatorID(),
                        new StreamSource<>(
                                new CancelTestSource(
                                        STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
                                        "Hello")))
                .chain(
                        new OperatorID(),
                        new TestBoundedOneInputStreamOperator("Operator1"),
                        STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
                .finish();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        streamConfig.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.invoke();
        CancelTestSource.getDataProcessing().await();
        testHarness.getTask().cancel();

        try {
            testHarness.waitForTaskCompletion();
        } catch (Throwable t) {
            if (!ExceptionUtils.findThrowable(t, CancelTaskException.class).isPresent()) {
                throw t;
            }
        }

        expectedOutput.add(new StreamRecord<>("Hello"));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testCancellationWithSourceBlockedOnLock() throws Exception {
        testCancellationWithSourceBlockedOnLock(false, false);
    }

    @Test
    public void testCancellationWithSourceBlockedOnLockWithPendingMail() throws Exception {
        testCancellationWithSourceBlockedOnLock(true, false);
    }

    @Test
    public void testCancellationWithSourceBlockedOnLockAndThrowingOnError() throws Exception {
        testCancellationWithSourceBlockedOnLock(false, true);
    }

    @Test
    public void testCancellationWithSourceBlockedOnLockWithPendingMailAndThrowingOnError()
            throws Exception {
        testCancellationWithSourceBlockedOnLock(true, true);
    }

    /**
     * Note that this test is testing also for the shared cancellation logic inside {@link
     * StreamTask} which, as of the time this test is being written, is not tested anywhere else
     * (like {@link StreamTaskTest} or {@link OneInputStreamTaskTest}).
     */
    public void testCancellationWithSourceBlockedOnLock(
            boolean withPendingMail, boolean throwInCancel) throws Exception {
        final StreamTaskTestHarness<String> testHarness =
                new StreamTaskTestHarness<>(SourceStreamTask::new, STRING_TYPE_INFO);

        CancelLockingSource.reset();
        testHarness
                .setupOperatorChain(
                        new OperatorID(),
                        new StreamSource<>(new CancelLockingSource(throwInCancel)))
                .chain(
                        new OperatorID(),
                        new TestBoundedOneInputStreamOperator("Operator1"),
                        STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
                .finish();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        streamConfig.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        testHarness.invoke();
        CancelLockingSource.awaitRunning();
        if (withPendingMail) {
            // This pending mail should be blocked on checkpointLock acquisition, blocking the
            // mailbox (task) thread.
            testHarness
                    .getTask()
                    .getMailboxExecutorFactory()
                    .createExecutor(0)
                    .execute(
                            () ->
                                    assertFalse(
                                            "This should never execute before task cancelation",
                                            testHarness.getTask().isRunning()),
                            "Test");
        }

        try {
            testHarness.getTask().cancel();
        } catch (ExpectedTestException e) {
            checkState(throwInCancel);
        }

        try {
            testHarness.waitForTaskCompletion();
        } catch (Throwable t) {
            if (!ExceptionUtils.findThrowable(t, InterruptedException.class).isPresent()
                    && !ExceptionUtils.findThrowable(t, CancelTaskException.class).isPresent()) {
                throw t;
            }
        }
    }

    /** A source that locks if cancellation attempts to cleanly shut down. */
    public static class CancelLockingSource implements SourceFunction<String> {
        private static final long serialVersionUID = 8713065281092996042L;

        private static CompletableFuture<Void> isRunning = new CompletableFuture<>();

        private final boolean throwOnCancel;

        private volatile boolean cancelled = false;

        public CancelLockingSource(boolean throwOnCancel) {
            this.throwOnCancel = throwOnCancel;
        }

        public static void reset() {
            isRunning = new CompletableFuture<>();
        }

        public static void awaitRunning() throws ExecutionException, InterruptedException {
            isRunning.get();
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            synchronized (ctx.getCheckpointLock()) {
                while (!cancelled) {
                    isRunning.complete(null);

                    if (throwOnCancel) {
                        Thread.sleep(1000000000);
                    } else {
                        try {
                            //noinspection SleepWhileHoldingLock
                            Thread.sleep(1000000000);
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
            }
        }

        @Override
        public void cancel() {
            if (throwOnCancel) {
                throw new ExpectedTestException();
            }
            cancelled = true;
        }
    }

    @Test
    public void testInterruptionExceptionNotSwallowed() throws Exception {
        testInterruptionExceptionNotSwallowed(InterruptedException::new);
    }

    @Test
    public void testWrappedInterruptionExceptionNotSwallowed() throws Exception {
        testInterruptionExceptionNotSwallowed(
                () -> new RuntimeException(new FlinkRuntimeException(new InterruptedException())));
    }

    private void testInterruptionExceptionNotSwallowed(
            InterruptedSource.ExceptionGenerator exceptionGenerator) throws Exception {
        final StreamTaskTestHarness<String> testHarness =
                new StreamTaskTestHarness<>(SourceStreamTask::new, STRING_TYPE_INFO);

        CancelLockingSource.reset();
        testHarness
                .setupOperatorChain(
                        new OperatorID(),
                        new StreamSource<>(new InterruptedSource(exceptionGenerator)))
                .chain(
                        new OperatorID(),
                        new TestBoundedOneInputStreamOperator("Operator1"),
                        STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
                .finish();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        streamConfig.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        testHarness.invoke();
        try {
            testHarness.waitForTaskCompletion();
        } catch (Exception e) {
            if (!ExceptionUtils.findThrowable(e, InterruptedException.class).isPresent()) {
                throw e;
            }
        }
    }

    /** A source that locks if cancellation attempts to cleanly shut down. */
    public static class InterruptedSource implements SourceFunction<String> {
        interface ExceptionGenerator extends CheckedSupplier<Exception>, Serializable {}

        private static final long serialVersionUID = 8713065281092996042L;

        private ExceptionGenerator exceptionGenerator;

        public InterruptedSource(final ExceptionGenerator exceptionGenerator) {
            this.exceptionGenerator = exceptionGenerator;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            synchronized (ctx.getCheckpointLock()) {
                Thread.currentThread().interrupt();
                throw exceptionGenerator.get();
            }
        }

        @Override
        public void cancel() {}
    }

    /** If finishing a task doesn't swallow exceptions this test would fail with an exception. */
    @Test
    public void finishingIgnoresExceptions() throws Exception {
        final StreamTaskTestHarness<String> testHarness =
                new StreamTaskTestHarness<>(SourceStreamTask::new, STRING_TYPE_INFO);

        final CompletableFuture<Void> operatorRunningWaitingFuture = new CompletableFuture<>();
        ExceptionThrowingSource.setIsInRunLoopFuture(operatorRunningWaitingFuture);

        testHarness.setupOutputForSingletonOperatorChain();
        StreamConfig streamConfig = testHarness.getStreamConfig();
        streamConfig.setStreamOperator(new StreamSource<>(new ExceptionThrowingSource()));
        streamConfig.setOperatorID(new OperatorID());

        testHarness.invoke();
        operatorRunningWaitingFuture.get();
        testHarness.getTask().finishTask();

        testHarness.waitForTaskCompletion();
    }

    @Test
    public void testWaitsForSourceThreadOnCancel() throws Exception {
        StreamTaskTestHarness<String> harness =
                new StreamTaskTestHarness<>(SourceStreamTask::new, STRING_TYPE_INFO);

        harness.setupOutputForSingletonOperatorChain();
        harness.getStreamConfig().setStreamOperator(new StreamSource<>(new NonStoppingSource()));

        harness.invoke();
        NonStoppingSource.waitForStart();

        harness.getTask().cancel();
        harness.waitForTaskCompletion(500, true); // allow task to exit prematurely
        assertTrue(harness.taskThread.isAlive());

        NonStoppingSource.forceCancel();
        harness.waitForTaskCompletion(Long.MAX_VALUE, true);
    }

    @Test
    public void testStopWithSavepointShouldNotInterruptTheSource() throws Exception {
        long checkpointId = 1;
        WasInterruptedTestingSource interruptedTestingSource = new WasInterruptedTestingSource();
        WasInterruptedTestingSource.reset();
        try (StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(SourceStreamTask::new, STRING_TYPE_INFO)
                        .setupOutputForSingletonOperatorChain(
                                new StreamSource<>(interruptedTestingSource))
                        .build()) {

            harness.processAll();

            Future<Boolean> triggerFuture =
                    harness.streamTask.triggerCheckpointAsync(
                            new CheckpointMetaData(checkpointId, 1),
                            new CheckpointOptions(SAVEPOINT_SUSPEND, getDefault()));
            while (!triggerFuture.isDone()) {
                harness.streamTask.runMailboxStep();
            }
            triggerFuture.get();

            Future<Void> notifyFuture =
                    harness.streamTask.notifyCheckpointCompleteAsync(checkpointId);
            while (!notifyFuture.isDone()) {
                harness.streamTask.runMailboxStep();
            }
            notifyFuture.get();

            WasInterruptedTestingSource.allowExit();

            harness.waitForTaskCompletion();
            harness.finishProcessing();

            assertTrue(notifyFuture.isDone());
            assertFalse(interruptedTestingSource.wasInterrupted());
        }
    }

    @Test
    public void testTriggeringCheckpointAfterSourceThreadFinished() throws Exception {
        ResultPartition[] partitionWriters = new ResultPartition[2];
        try (NettyShuffleEnvironment env =
                new NettyShuffleEnvironmentBuilder()
                        .setNumNetworkBuffers(partitionWriters.length * 2)
                        .build()) {
            for (int i = 0; i < partitionWriters.length; ++i) {
                partitionWriters[i] =
                        PartitionTestUtils.createPartition(
                                env, ResultPartitionType.PIPELINED_BOUNDED, 1);
                partitionWriters[i].setup();
            }

            final CompletableFuture<Long> checkpointCompleted = new CompletableFuture<>();
            try (StreamTaskMailboxTestHarness<String> testHarness =
                    new StreamTaskMailboxTestHarnessBuilder<>(
                                    SourceStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                            .modifyStreamConfig(
                                    config -> {
                                        config.setCheckpointingEnabled(true);
                                        config.getConfiguration()
                                                .set(
                                                        ExecutionCheckpointingOptions
                                                                .ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH,
                                                        true);
                                    })
                            .setCheckpointResponder(
                                    new TestCheckpointResponder() {
                                        @Override
                                        public void acknowledgeCheckpoint(
                                                JobID jobID,
                                                ExecutionAttemptID executionAttemptID,
                                                long checkpointId,
                                                CheckpointMetrics checkpointMetrics,
                                                TaskStateSnapshot subtaskState) {
                                            super.acknowledgeCheckpoint(
                                                    jobID,
                                                    executionAttemptID,
                                                    checkpointId,
                                                    checkpointMetrics,
                                                    subtaskState);
                                            checkpointCompleted.complete(checkpointId);
                                        }
                                    })
                            .addAdditionalOutput(partitionWriters)
                            .setupOperatorChain(new StreamSource<>(new MockSource(0, 0, 1)))
                            .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                            .build()) {

                testHarness.processAll();
                CompletableFuture<Void> taskFinished =
                        testHarness.getStreamTask().getCompletionFuture();
                do {
                    testHarness.processAll();
                } while (!taskFinished.isDone());

                Future<Boolean> checkpointFuture = triggerCheckpoint(testHarness, 2);
                // Notifies the result partition that all records are processed after the
                // last checkpoint is triggered.
                checkState(
                        checkpointFuture instanceof CompletableFuture,
                        "The trigger future should " + " be also CompletableFuture.");
                ((CompletableFuture<?>) checkpointFuture)
                        .thenAccept(
                                (ignored) -> {
                                    for (ResultPartition resultPartition : partitionWriters) {
                                        resultPartition.onSubpartitionAllDataProcessed(0);
                                    }
                                });

                checkpointCompleted.whenComplete(
                        (id, error) ->
                                testHarness.getStreamTask().notifyCheckpointCompleteAsync(2));
                testHarness.finishProcessing();
                assertTrue(checkpointFuture.isDone());

                // Each result partition should have emitted 1 barrier, 1 max watermark and 1
                // EndOfUserRecordEvent.
                for (ResultPartition resultPartition : partitionWriters) {
                    assertEquals(3, resultPartition.getNumberOfQueuedBuffers());
                }
            }
        } finally {
            for (ResultPartitionWriter writer : partitionWriters) {
                if (writer != null) {
                    writer.close();
                }
            }
        }
    }

    @Test
    public void testClosedOnRestoreSourceSkipExecution() throws Exception {
        LifeCycleMonitorSource testSource = new LifeCycleMonitorSource();
        List<Object> output = new ArrayList<>();
        try (StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(SourceStreamTask::new, STRING_TYPE_INFO)
                        .setTaskStateSnapshot(1, TaskStateSnapshot.FINISHED_ON_RESTORE)
                        .addAdditionalOutput(
                                new RecordOrEventCollectingResultPartitionWriter<StreamElement>(
                                        output,
                                        new StreamElementSerializer<>(IntSerializer.INSTANCE)) {
                                    @Override
                                    public void notifyEndOfData() throws IOException {
                                        broadcastEvent(EndOfData.INSTANCE, false);
                                    }
                                })
                        .setupOperatorChain(new StreamSource<>(testSource))
                        .chain(new TestFinishedOnRestoreStreamOperator(), StringSerializer.INSTANCE)
                        .finish()
                        .build()) {
            harness.getStreamTask().invoke();
            harness.processAll();
            harness.streamTask.getCompletionFuture().get();

            assertThat(output, contains(Watermark.MAX_WATERMARK, EndOfData.INSTANCE));

            LifeCycleMonitorSource source =
                    (LifeCycleMonitorSource)
                            ((StreamSource<?, ?>) harness.getStreamTask().getMainOperator())
                                    .getUserFunction();
            source.getLifeCycleMonitor()
                    .assertCallTimes(
                            0,
                            LifeCyclePhase.OPEN,
                            LifeCyclePhase.PROCESS_ELEMENT,
                            LifeCyclePhase.CLOSE);
        }
    }

    @Test
    public void testTriggeringStopWithSavepointWithDrain() throws Exception {
        SourceFunction<String> testSource = new EmptySource();

        CompletableFuture<Boolean> checkpointCompleted = new CompletableFuture<>();
        CheckpointResponder checkpointResponder =
                new TestCheckpointResponder() {
                    @Override
                    public void acknowledgeCheckpoint(
                            JobID jobID,
                            ExecutionAttemptID executionAttemptID,
                            long checkpointId,
                            CheckpointMetrics checkpointMetrics,
                            TaskStateSnapshot subtaskState) {
                        super.acknowledgeCheckpoint(
                                jobID,
                                executionAttemptID,
                                checkpointId,
                                checkpointMetrics,
                                subtaskState);
                        checkpointCompleted.complete(null);
                    }
                };

        try (StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(SourceStreamTask::new, STRING_TYPE_INFO)
                        .setTaskStateSnapshot(1, TaskStateSnapshot.FINISHED_ON_RESTORE)
                        .setCheckpointResponder(checkpointResponder)
                        .setupOutputForSingletonOperatorChain(new StreamSource<>(testSource))
                        .build()) {
            CompletableFuture<Boolean> triggerResult =
                    harness.streamTask.triggerCheckpointAsync(
                            new CheckpointMetaData(2, 2),
                            CheckpointOptions.alignedNoTimeout(
                                    SAVEPOINT_TERMINATE,
                                    CheckpointStorageLocationReference.getDefault()));
            checkpointCompleted.whenComplete(
                    (ignored, exception) -> harness.streamTask.notifyCheckpointCompleteAsync(2));

            // Run mailbox till the source thread finished and suspend the mailbox
            harness.streamTask.runMailboxLoop();
            harness.finishProcessing();

            assertTrue(triggerResult.isDone());
            assertTrue(triggerResult.get());
            assertTrue(checkpointCompleted.isDone());
        }
    }

    private static class LifeCycleMonitorSource extends RichParallelSourceFunction<String> {

        private final LifeCycleMonitor lifeCycleMonitor = new LifeCycleMonitor();

        @Override
        public void open(Configuration parameters) throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.OPEN);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.PROCESS_ELEMENT);
        }

        @Override
        public void close() throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.CLOSE);
        }

        public LifeCycleMonitor getLifeCycleMonitor() {
            return lifeCycleMonitor;
        }

        @Override
        public void cancel() {}
    }

    private static class MockSource
            implements SourceFunction<Tuple2<Long, Integer>>, ListCheckpointed<Serializable> {
        private static final long serialVersionUID = 1;

        private int maxElements;
        private int checkpointDelay;
        private int readDelay;

        private volatile int count;
        private volatile long lastCheckpointId = -1;

        private Semaphore semaphore;

        private volatile boolean isRunning = true;

        public MockSource(int maxElements, int checkpointDelay, int readDelay) {
            this.maxElements = maxElements;
            this.checkpointDelay = checkpointDelay;
            this.readDelay = readDelay;
            this.count = 0;
            semaphore = new Semaphore(1);
        }

        @Override
        public void run(SourceContext<Tuple2<Long, Integer>> ctx) {
            while (isRunning && count < maxElements) {
                // simulate some work
                try {
                    Thread.sleep(readDelay);
                } catch (InterruptedException e) {
                    // ignore and reset interruption state
                    Thread.currentThread().interrupt();
                }

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new Tuple2<>(lastCheckpointId, count));
                    count++;
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public List<Serializable> snapshotState(long checkpointId, long timestamp)
                throws Exception {
            if (!semaphore.tryAcquire()) {
                Assert.fail("Concurrent invocation of snapshotState.");
            }
            int startCount = count;
            lastCheckpointId = checkpointId;

            long sum = 0;
            for (int i = 0; i < checkpointDelay; i++) {
                sum += new Random().nextLong();
            }

            if (startCount != count) {
                semaphore.release();
                // This means that next() was invoked while the snapshot was ongoing
                Assert.fail("Count is different at start end end of snapshot.");
            }
            semaphore.release();
            return Collections.singletonList(sum);
        }

        @Override
        public void restoreState(List<Serializable> state) throws Exception {}
    }

    /** This calls triggerCheckpointAsync on the given task with the given interval. */
    private static class Checkpointer implements Callable<Boolean> {
        private final int numCheckpoints;
        private final int checkpointInterval;
        private final AtomicLong checkpointId;
        private final StreamTask<Tuple2<Long, Integer>, ?> sourceTask;

        Checkpointer(
                int numCheckpoints,
                int checkpointInterval,
                StreamTask<Tuple2<Long, Integer>, ?> task) {
            this.numCheckpoints = numCheckpoints;
            checkpointId = new AtomicLong(0);
            sourceTask = task;
            this.checkpointInterval = checkpointInterval;
        }

        @Override
        public Boolean call() throws Exception {
            for (int i = 0; i < numCheckpoints; i++) {
                long currentCheckpointId = checkpointId.getAndIncrement();
                try {
                    sourceTask.triggerCheckpointAsync(
                            new CheckpointMetaData(currentCheckpointId, 0L),
                            CheckpointOptions.forCheckpointWithDefaultLocation());
                } catch (RejectedExecutionException e) {
                    // We are late with a checkpoint, the mailbox is already closed.
                    return false;
                }
                Thread.sleep(checkpointInterval);
            }
            return true;
        }
    }

    private static class NonStoppingSource implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private static boolean running = true;
        private static CompletableFuture<Void> startFuture = new CompletableFuture<>();

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            startFuture.complete(null);
            while (running) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }

        @Override
        public void cancel() {
            // do nothing - ignore usual cancellation
        }

        static void forceCancel() {
            running = false;
        }

        static void waitForStart() {
            startFuture.join();
        }
    }

    private static class OpenCloseTestSource extends RichSourceFunction<String> {
        private static final long serialVersionUID = 1L;

        public static boolean openCalled = false;
        public static boolean closeCalled = false;

        OpenCloseTestSource() {
            openCalled = false;
            closeCalled = false;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            if (closeCalled) {
                Assert.fail("Close called before open.");
            }
            openCalled = true;
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (!openCalled) {
                Assert.fail("Open was not called before close.");
            }
            closeCalled = true;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            if (!openCalled) {
                Assert.fail("Open was not called before run.");
            }
            for (int i = 0; i < 10; i++) {
                ctx.collect("Hello" + i);
            }
        }

        @Override
        public void cancel() {}
    }

    private static class CancelTestSource<T> extends FromElementsFunction<T> {
        private static final long serialVersionUID = 8713065281092996067L;

        private static MultiShotLatch dataProcessing = new MultiShotLatch();

        private static MultiShotLatch cancellationWaiting = new MultiShotLatch();

        public CancelTestSource(TypeSerializer<T> serializer, T... elements) throws IOException {
            super(serializer, elements);
        }

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            super.run(ctx);

            dataProcessing.trigger();
            cancellationWaiting.await();
        }

        @Override
        public void cancel() {
            super.cancel();
            cancellationWaiting.trigger();
        }

        public static MultiShotLatch getDataProcessing() {
            return dataProcessing;
        }
    }

    /**
     * A {@link SourceFunction} that throws an exception from {@link #run(SourceContext)} when it is
     * cancelled via {@link #cancel()}.
     */
    private static class ExceptionThrowingSource implements SourceFunction<String> {

        private static volatile CompletableFuture<Void> isInRunLoop;

        private volatile boolean running = true;

        public static class TestException extends RuntimeException {
            public TestException(String message) {
                super(message);
            }
        }

        public static void setIsInRunLoopFuture(
                @Nonnull final CompletableFuture<Void> waitingLatch) {
            ExceptionThrowingSource.isInRunLoop = waitingLatch;
        }

        @Override
        public void run(SourceContext<String> ctx) throws TestException {
            checkState(isInRunLoop != null && !isInRunLoop.isDone());

            while (running) {
                if (!isInRunLoop.isDone()) {
                    isInRunLoop.complete(null);
                }
                ctx.collect("hello");
            }

            throw new TestException("Oh no, we're failing.");
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class OutputRecordInCloseTestSource<SRC extends SourceFunction<String>>
            extends StreamSource<String, SRC> implements BoundedOneInput {

        private final String name;

        public OutputRecordInCloseTestSource(String name, SRC sourceFunction) {
            super(sourceFunction);
            this.name = name;
        }

        @Override
        public void endInput() {
            output("[" + name + "]: End of input");
        }

        @Override
        public void finish() throws Exception {
            ProcessingTimeService timeService = getProcessingTimeService();
            timeService.registerTimer(
                    timeService.getCurrentProcessingTime(),
                    t -> output("[" + name + "]: Timer registered in close"));

            output("[" + name + "]: Finish");
            super.finish();
        }

        private void output(String record) {
            output.collect(new StreamRecord<>(record));
        }
    }

    /**
     * This source sleeps a little bit before processing cancellation and records whether it was
     * interrupted by the {@link SourceStreamTask} or not.
     */
    private static class WasInterruptedTestingSource implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;

        private static final OneShotLatch ALLOW_EXIT = new OneShotLatch();
        private static final AtomicBoolean WAS_INTERRUPTED = new AtomicBoolean();

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            try {
                while (running || !ALLOW_EXIT.isTriggered()) {
                    Thread.sleep(1);
                }
            } catch (InterruptedException e) {
                WAS_INTERRUPTED.set(true);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        public static boolean wasInterrupted() {
            return WAS_INTERRUPTED.get();
        }

        public static void reset() {
            ALLOW_EXIT.reset();
            WAS_INTERRUPTED.set(false);
        }

        public static void allowExit() {
            ALLOW_EXIT.trigger();
        }
    }

    private static class EmptySource implements SourceFunction<String> {

        private volatile boolean isCanceled;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (!isCanceled) {
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isCanceled = true;
        }
    }
}
