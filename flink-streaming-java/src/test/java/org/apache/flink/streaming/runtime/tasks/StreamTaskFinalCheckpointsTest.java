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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.PipelinedResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.LifeCycleMonitor.LifeCyclePhase;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests the behavior of {@link StreamTask} related to final checkpoint. */
public class StreamTaskFinalCheckpointsTest {

    @Test
    public void testCheckpointDoneOnFinishedOperator() throws Exception {
        FinishingOperator finishingOperator = new FinishingOperator();
        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO);
        StreamTaskMailboxTestHarness<Integer> harness =
                builder.setupOutputForSingletonOperatorChain(finishingOperator).build();
        // keeps the mailbox from suspending
        harness.setAutoProcess(false);
        harness.processElement(new StreamRecord<>(1));

        harness.streamTask.operatorChain.finishOperators(harness.streamTask.getActionExecutor());
        assertTrue(FinishingOperator.finished);

        harness.getTaskStateManager().setWaitForReportLatch(new OneShotLatch());
        harness.streamTask.triggerCheckpointOnBarrier(
                new CheckpointMetaData(2, 0),
                CheckpointOptions.forCheckpointWithDefaultLocation(),
                new CheckpointMetricsBuilder()
                        .setBytesProcessedDuringAlignment(0L)
                        .setAlignmentDurationNanos(0L));
        harness.getTaskStateManager().getWaitForReportLatch().await();
        assertEquals(2, harness.getTaskStateManager().getReportedCheckpointId());
    }

    @Test
    public void testNotWaitingForAllRecordsProcessedIfCheckpointNotEnabled() throws Exception {
        ResultPartitionWriter[] partitionWriters = new ResultPartitionWriter[2];
        try {
            for (int i = 0; i < partitionWriters.length; ++i) {
                partitionWriters[i] =
                        PartitionTestUtils.createPartition(ResultPartitionType.PIPELINED_BOUNDED);
                partitionWriters[i].setup();
            }

            try (StreamTaskMailboxTestHarness<String> testHarness =
                    new StreamTaskMailboxTestHarnessBuilder<>(
                                    OneInputStreamTask::new, STRING_TYPE_INFO)
                            .modifyStreamConfig(config -> config.setCheckpointingEnabled(false))
                            .addInput(STRING_TYPE_INFO)
                            .addAdditionalOutput(partitionWriters)
                            .setupOperatorChain(new EmptyOperator())
                            .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                            .build()) {
                testHarness.endInput();

                // In this case the result partition should not emit EndOfUserRecordsEvent.
                for (ResultPartitionWriter writer : partitionWriters) {
                    assertEquals(0, ((PipelinedResultPartition) writer).getNumberOfQueuedBuffers());
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
    public void testTriggeringCheckpointWithFinishedChannels() throws Exception {
        ResultPartition[] partitionWriters = new ResultPartition[2];
        try {
            for (int i = 0; i < partitionWriters.length; ++i) {
                partitionWriters[i] =
                        PartitionTestUtils.createPartition(ResultPartitionType.PIPELINED_BOUNDED);
                partitionWriters[i].setup();
            }

            try (StreamTaskMailboxTestHarness<String> testHarness =
                    new StreamTaskMailboxTestHarnessBuilder<>(
                                    OneInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                            .addInput(BasicTypeInfo.STRING_TYPE_INFO, 3)
                            .addAdditionalOutput(partitionWriters)
                            .modifyStreamConfig(config -> config.setCheckpointingEnabled(true))
                            .setupOperatorChain(new EmptyOperator())
                            .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                            .build()) {
                testHarness
                        .getStreamTask()
                        .getCheckpointCoordinator()
                        .setEnableCheckpointAfterTasksFinished(true);
                testHarness
                        .getStreamTask()
                        .getCheckpointBarrierHandler()
                        .get()
                        .setEnableCheckpointAfterTasksFinished(true);

                // Tests triggering checkpoint when all the inputs are alive.
                Future<Boolean> checkpointFuture = triggerCheckpoint(testHarness, 2);
                processMailTillCheckpointSucceeds(testHarness, checkpointFuture);
                assertEquals(2, testHarness.getTaskStateManager().getReportedCheckpointId());

                // Tests triggering checkpoint after some inputs have received EndOfPartition.
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 0, 0);
                checkpointFuture = triggerCheckpoint(testHarness, 4);
                processMailTillCheckpointSucceeds(testHarness, checkpointFuture);
                assertEquals(4, testHarness.getTaskStateManager().getReportedCheckpointId());

                // Tests triggering checkpoint after received all the inputs have received
                // EndOfPartition.
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 0, 1);
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 0, 2);
                checkpointFuture = triggerCheckpoint(testHarness, 6);

                // Notifies the result partition that all records are processed after the
                // last checkpoint is triggered.
                checkState(
                        checkpointFuture instanceof CompletableFuture,
                        "The trigger future should " + " be also CompletableFuture.");
                ((CompletableFuture<?>) checkpointFuture)
                        .thenAccept(
                                (ignored) -> {
                                    for (ResultPartition resultPartition : partitionWriters) {
                                        resultPartition.onSubpartitionAllRecordsProcessed(0);
                                    }
                                });

                // The checkpoint 6 would be triggered successfully.
                testHarness.finishProcessing();
                assertTrue(checkpointFuture.isDone());
                testHarness.getTaskStateManager().getWaitForReportLatch().await();
                assertEquals(6, testHarness.getTaskStateManager().getReportedCheckpointId());

                // Each result partition should have emitted 3 barriers and 1 EndOfUserRecordsEvent.
                for (ResultPartition resultPartition : partitionWriters) {
                    assertEquals(4, resultPartition.getNumberOfQueuedBuffers());
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

    static Future<Boolean> triggerCheckpoint(
            StreamTaskMailboxTestHarness<String> testHarness, long checkpointId) {
        testHarness.getTaskStateManager().setWaitForReportLatch(new OneShotLatch());
        return testHarness
                .getStreamTask()
                .triggerCheckpointAsync(
                        new CheckpointMetaData(checkpointId, checkpointId * 1000),
                        CheckpointOptions.alignedNoTimeout(
                                CheckpointType.CHECKPOINT,
                                CheckpointStorageLocationReference.getDefault()));
    }

    static void processMailTillCheckpointSucceeds(
            StreamTaskMailboxTestHarness<String> testHarness, Future<Boolean> checkpointFuture)
            throws Exception {
        while (!checkpointFuture.isDone()) {
            testHarness.processSingleStep();
        }
        testHarness.getTaskStateManager().getWaitForReportLatch().await();
    }

    @Test
    public void testWaitingForPendingCheckpointsOnFinished() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            OneShotLatch asyncCheckpointExecuted = new OneShotLatch();
            OneShotLatch canCheckpointBeAcknowledged = new OneShotLatch();
            OneShotLatch invokeCompleted = new OneShotLatch();

            TestCheckpointResponder responder =
                    new TestCheckpointResponder() {
                        @Override
                        public void acknowledgeCheckpoint(
                                JobID jobID,
                                ExecutionAttemptID executionAttemptID,
                                long checkpointId,
                                CheckpointMetrics checkpointMetrics,
                                TaskStateSnapshot subtaskState) {

                            try {
                                asyncCheckpointExecuted.trigger();
                                canCheckpointBeAcknowledged.await();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };

            CompletableFuture<Void> taskClosed =
                    CompletableFuture.runAsync(
                            () -> {
                                try (StreamTaskMailboxTestHarness<String> harness =
                                        new StreamTaskMailboxTestHarnessBuilder<>(
                                                        OneInputStreamTask::new,
                                                        BasicTypeInfo.STRING_TYPE_INFO)
                                                .addInput(BasicTypeInfo.STRING_TYPE_INFO, 3)
                                                .modifyStreamConfig(
                                                        config ->
                                                                config.setCheckpointingEnabled(
                                                                        true))
                                                .setCheckpointResponder(responder)
                                                .setupOperatorChain(new EmptyOperator())
                                                .finishForSingletonOperatorChain(
                                                        StringSerializer.INSTANCE)
                                                .build()) {

                                    harness.streamTask
                                            .getCheckpointCoordinator()
                                            .setEnableCheckpointAfterTasksFinished(true);

                                    harness.streamTask.triggerCheckpointOnBarrier(
                                            new CheckpointMetaData(1, 101),
                                            CheckpointOptions.forCheckpointWithDefaultLocation(),
                                            new CheckpointMetricsBuilder()
                                                    .setBytesProcessedDuringAlignment(0L)
                                                    .setAlignmentDurationNanos(0L));

                                    harness.waitForTaskCompletion();
                                    invokeCompleted.trigger();
                                    harness.finishProcessing();
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            },
                            executor);

            asyncCheckpointExecuted.await();
            invokeCompleted.await();
            // Give some potential time for the task to finish before the
            // checkpoint is acknowledged
            Thread.sleep(500);
            assertFalse(taskClosed.isDone());
            canCheckpointBeAcknowledged.trigger();
            taskClosed.get();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testOperatorSkipLifeCycleIfFinishedOnRestore() throws Exception {
        try (StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO, 3)
                        .setTaskStateSnapshot(1, TaskStateSnapshot.FINISHED)
                        .setupOutputForSingletonOperatorChain(new LifeCycleMonitorOperator<>())
                        .build()) {
            // Finish the restore, including state initialization and open.
            harness.processAll();

            // Try trigger a checkpoint.
            harness.getTaskStateManager().setWaitForReportLatch(new OneShotLatch());
            harness.streamTask.triggerCheckpointOnBarrier(
                    new CheckpointMetaData(2, 2),
                    new CheckpointOptions(CheckpointType.CHECKPOINT, getDefault()),
                    new CheckpointMetricsBuilder()
                            .setBytesProcessedDuringAlignment(0)
                            .setAlignmentDurationNanos(0));
            harness.getTaskStateManager().getWaitForReportLatch().await();
            assertEquals(2, harness.getTaskStateManager().getReportedCheckpointId());

            // Checkpoint notification.
            harness.streamTask.notifyCheckpointCompleteAsync(2);
            harness.streamTask.notifyCheckpointAbortAsync(3);
            harness.processAll();

            // Finish & close operators.
            harness.waitForTaskCompletion();
            harness.finishProcessing();

            LifeCycleMonitorOperator<String> operator =
                    (LifeCycleMonitorOperator<String>) harness.getStreamTask().getMainOperator();
            operator.getLifeCycleMonitor().assertCallTimes(0, LifeCyclePhase.values());
        }
    }

    private static class EmptyOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {}
    }

    private static class FinishingOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {
        static boolean finished = false;

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {}

        @Override
        public void finish() throws Exception {
            finished = true;
        }
    }

    /** A special one-input operator that monitors the lifecycle of the operator. */
    static class LifeCycleMonitorOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T> {

        private final LifeCycleMonitor lifeCycleMonitor = new LifeCycleMonitor();

        @Override
        public void open() throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.OPEN);
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.INITIALIZE_STATE);
        }

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.PROCESS_ELEMENT);
        }

        @Override
        public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.PREPARE_SNAPSHOT_PRE_BARRIER);
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.SNAPSHOT_STATE);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.NOTIFY_CHECKPOINT_COMPLETE);
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.NOTIFY_CHECKPOINT_ABORT);
        }

        @Override
        public void finish() throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.FINISH);
        }

        @Override
        public void close() throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.CLOSE);
        }

        public LifeCycleMonitor getLifeCycleMonitor() {
            return lifeCycleMonitor;
        }
    }
}
