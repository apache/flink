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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
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

import org.junit.jupiter.api.Test;

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
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.streaming.runtime.tasks.StreamTaskFinalCheckpointsTest.triggerCheckpoint;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These tests verify that the RichFunction methods are called (in correct order). And that
 * checkpointing/element emission don't occur concurrently.
 */
class SourceStreamTaskTest extends SourceStreamTaskTestBase {

    /** This test verifies that open() and close() are correctly called by the StreamTask. */
    @Test
    void testOpenClose() throws Exception {
        final StreamTaskTestHarness<String> testHarness =
                new StreamTaskTestHarness<>(SourceStreamTask::new, STRING_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();

        StreamConfig streamConfig = testHarness.getStreamConfig();
        StreamSource<String, ?> sourceOperator = new StreamSource<>(new OpenCloseTestSource());
        streamConfig.setStreamOperator(sourceOperator);
        streamConfig.setOperatorID(new OperatorID());

        testHarness.invoke();
        testHarness.waitForTaskCompletion();

        assertThat(OpenCloseTestSource.closeCalled)
                .as("RichFunction methods where not called.")
                .isTrue();

        List<String> resultElements =
                TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
        assertThat(resultElements.size()).isEqualTo(10);
    }

    @Test
    void testMetrics() throws Exception {
        testMetrics(
                SourceStreamTask::new,
                SimpleOperatorFactory.of(
                        new StreamSource<Integer, SourceFunction<Integer>>(
                                new CancelTestSource(
                                        INT_TYPE_INFO.createSerializer(new ExecutionConfig()),
                                        42))),
                busyTime -> busyTime.isNaN());
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
    void testCheckpointing() throws Exception {
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
            assertThat(resultElements.size()).isEqualTo(numElements);
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void testClosingAllOperatorsOnChainProperly() throws Exception {
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
        assertThat(output).as("Output was not correct.").isEqualTo(expected.toArray());
    }

    @Test
    void testNotMarkingEndOfInputWhenTaskCancelled() throws Exception {
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
    void testCancellationWithSourceBlockedOnLock() throws Exception {
        testCancellationWithSourceBlockedOnLock(false, false);
    }

    @Test
    void testCancellationWithSourceBlockedOnLockWithPendingMail() throws Exception {
        testCancellationWithSourceBlockedOnLock(true, false);
    }

    @Test
    void testCancellationWithSourceBlockedOnLockAndThrowingOnError() throws Exception {
        testCancellationWithSourceBlockedOnLock(false, true);
    }

    @Test
    void testCancellationWithSourceBlockedOnLockWithPendingMailAndThrowingOnError()
            throws Exception {
        testCancellationWithSourceBlockedOnLock(true, true);
    }

    /**
     * Note that this test is testing also for the shared cancellation logic inside {@link
     * StreamTask} which, as of the time this test is being written, is not tested anywhere else
     * (like {@link StreamTaskTest} or {@link OneInputStreamTaskTest}).
     */
    void testCancellationWithSourceBlockedOnLock(boolean withPendingMail, boolean throwInCancel)
            throws Exception {
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
                                    assertThat(testHarness.getTask().isRunning())
                                            .as("This should never execute before task cancelation")
                                            .isFalse(),
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
    void testInterruptionExceptionNotSwallowed() throws Exception {
        testInterruptionExceptionNotSwallowed(InterruptedException::new);
    }

    @Test
    void testWrappedInterruptionExceptionNotSwallowed() throws Exception {
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

    @Test
    void testWaitsForSourceThreadOnCancel() throws Exception {
        StreamTaskTestHarness<String> harness =
                new StreamTaskTestHarness<>(SourceStreamTask::new, STRING_TYPE_INFO);

        harness.setupOutputForSingletonOperatorChain();
        harness.getStreamConfig().setStreamOperator(new StreamSource<>(new NonStoppingSource()));

        harness.invoke();
        NonStoppingSource.waitForStart();

        // SourceStreamTask should be still waiting for NonStoppingSource after cancellation
        harness.getTask().cancel();
        harness.waitForTaskCompletion(50, true); // allow task to exit prematurely
        assertThat(harness.taskThread.isAlive()).isTrue();

        // SourceStreamTask should be still waiting for NonStoppingSource after interruptions
        for (int i = 0; i < 10; i++) {
            harness.getTask().maybeInterruptOnCancel(harness.getTaskThread(), null, null);
            harness.waitForTaskCompletion(50, true); // allow task to exit prematurely
            assertThat(harness.taskThread.isAlive()).isTrue();
        }

        // It should only exit once NonStoppingSource allows for it
        NonStoppingSource.forceCancel();
        harness.waitForTaskCompletion(Long.MAX_VALUE, true);
    }

    @Test
    void testTriggeringCheckpointAfterSourceThreadFinished() throws Exception {
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
                            .modifyStreamConfig(config -> config.setCheckpointingEnabled(true))
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
                assertThat(checkpointFuture.isDone()).isTrue();

                // Each result partition should have emitted 1 barrier, 1 max watermark and 1
                // EndOfUserRecordEvent.
                for (ResultPartition resultPartition : partitionWriters) {
                    assertThat(resultPartition.getNumberOfQueuedBuffers()).isEqualTo(3);
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
    void testDisableOverdraftBuffer() throws Exception {
        try (NettyShuffleEnvironment env =
                        new NettyShuffleEnvironmentBuilder().setNumNetworkBuffers(2).build();
                ResultPartition partitionWriter =
                        PartitionTestUtils.createPartition(
                                env, ResultPartitionType.PIPELINED_BOUNDED, 1)) {
            partitionWriter.setup();
            assertTrue(partitionWriter.getBufferPool().getMaxOverdraftBuffersPerGate() > 0);

            final CompletableFuture<Long> checkpointCompleted = new CompletableFuture<>();
            try (StreamTaskMailboxTestHarness<String> testHarness =
                    new StreamTaskMailboxTestHarnessBuilder<>(
                                    SourceStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                            .addAdditionalOutput(partitionWriter)
                            .setupOperatorChain(new StreamSource<>(new MockSource(0, 0, 1)))
                            .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                            .build()) {

                assertEquals(0, partitionWriter.getBufferPool().getMaxOverdraftBuffersPerGate());
            }
        }
    }

    @Test
    void testClosedOnRestoreSourceSkipExecution() throws Exception {
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
                                    public void notifyEndOfData(StopMode mode) throws IOException {
                                        broadcastEvent(new EndOfData(mode), false);
                                    }
                                })
                        .setupOperatorChain(new StreamSource<>(testSource))
                        .chain(new TestFinishedOnRestoreStreamOperator(), StringSerializer.INSTANCE)
                        .finish()
                        .build()) {
            harness.getStreamTask().invoke();
            harness.processAll();
            harness.streamTask.getCompletionFuture().get();

            assertThat(output)
                    .containsExactly(Watermark.MAX_WATERMARK, new EndOfData(StopMode.DRAIN));

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
    void testTriggeringStopWithSavepointWithDrain() throws Exception {
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
                                    SavepointType.terminate(SavepointFormatType.CANONICAL),
                                    CheckpointStorageLocationReference.getDefault()));
            checkpointCompleted.whenComplete(
                    (ignored, exception) -> harness.streamTask.notifyCheckpointCompleteAsync(2));

            // Run mailbox till the source thread finished and suspend the mailbox
            harness.streamTask.runMailboxLoop();
            harness.finishProcessing();

            assertThat(triggerResult.isDone()).isTrue();
            assertThat(triggerResult.get()).isTrue();
            assertThat(checkpointCompleted.isDone()).isTrue();
        }
    }

    private static class LifeCycleMonitorSource extends RichParallelSourceFunction<String> {

        private final LifeCycleMonitor lifeCycleMonitor = new LifeCycleMonitor();

        @Override
        public void open(OpenContext openContext) throws Exception {
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
                fail("Concurrent invocation of snapshotState.");
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
                fail("Count is different at start end end of snapshot.");
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
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            if (closeCalled) {
                fail("Close called before open.");
            }
            openCalled = true;
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (!openCalled) {
                fail("Open was not called before close.");
            }
            closeCalled = true;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            if (!openCalled) {
                fail("Open was not called before run.");
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
