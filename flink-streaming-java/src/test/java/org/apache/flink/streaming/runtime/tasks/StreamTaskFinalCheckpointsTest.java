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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.PipelinedResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.CompletingCheckpointResponder;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests the behavior of {@link StreamTask} related to final checkpoint. */
public class StreamTaskFinalCheckpointsTest {

    private static final long CONCURRENT_EVENT_WAIT_PERIOD_MS = 500L;

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

        harness.streamTask.operatorChain.finishOperators(
                harness.streamTask.getActionExecutor(), StopMode.DRAIN);
        assertTrue(FinishingOperator.finished);

        harness.getTaskStateManager().getWaitForReportLatch().reset();
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
                            .finishForSingletonOperatorChain(
                                    StringSerializer.INSTANCE, new BroadcastPartitioner<>())
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
    public void testWaitingForFinalCheckpoint() throws Exception {
        ResultPartition[] partitionWriters = new ResultPartition[2];
        try {
            for (int i = 0; i < partitionWriters.length; ++i) {
                partitionWriters[i] =
                        PartitionTestUtils.createPartition(ResultPartitionType.PIPELINED_BOUNDED);
                partitionWriters[i].setup();
            }

            int lastCheckpointId = 6;
            CompletingCheckpointResponder checkpointResponder = new CompletingCheckpointResponder();
            try (StreamTaskMailboxTestHarness<String> testHarness =
                    createTestHarness(partitionWriters, checkpointResponder, false)) {
                // Tests triggering checkpoint when all the inputs are alive.
                CompletableFuture<Boolean> checkpointFuture = triggerCheckpoint(testHarness, 2);
                processMailTillCheckpointSucceeds(testHarness, checkpointFuture);
                assertEquals(2, testHarness.getTaskStateManager().getReportedCheckpointId());

                // Tests triggering checkpoint after some inputs have received EndOfPartition.
                testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0, 0);
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 0, 0);
                checkpointFuture = triggerCheckpoint(testHarness, 4);
                processMailTillCheckpointSucceeds(testHarness, checkpointFuture);
                assertEquals(4, testHarness.getTaskStateManager().getReportedCheckpointId());

                // Tests triggering checkpoint after received all the inputs have received
                // EndOfPartition.
                testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0, 1);
                testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0, 2);
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 0, 1);
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 0, 2);
                checkpointFuture = triggerCheckpoint(testHarness, lastCheckpointId);

                // Notifies the result partition that all records are processed after the
                // last checkpoint is triggered.
                checkpointFuture.thenAccept(
                        (ignored) -> {
                            for (ResultPartition resultPartition : partitionWriters) {
                                resultPartition.onSubpartitionAllDataProcessed(0);
                            }
                        });

                // The checkpoint 6 would be triggered successfully.
                testHarness.finishProcessing();
                assertTrue(checkpointFuture.isDone());
                testHarness.getTaskStateManager().getWaitForReportLatch().await();
                assertEquals(6, testHarness.getTaskStateManager().getReportedCheckpointId());
                assertEquals(
                        6, testHarness.getTaskStateManager().getNotifiedCompletedCheckpointId());

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

    private StreamTaskMailboxTestHarness<String> createTestHarness(
            CompletingCheckpointResponder checkpointResponder) throws Exception {
        return createTestHarness(null, checkpointResponder, false);
    }

    private StreamTaskMailboxTestHarness<String> createTestHarness(
            @Nullable ResultPartition[] partitionWriters,
            CompletingCheckpointResponder checkpointResponder,
            boolean enableUnalignedCheckpoint)
            throws Exception {
        StreamTaskMailboxTestHarnessBuilder<String> testHarnessBuilder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                        OneInputStreamTask::new, STRING_TYPE_INFO);
        if (partitionWriters != null) {
            testHarnessBuilder = testHarnessBuilder.addAdditionalOutput(partitionWriters);
        }
        StreamTaskMailboxTestHarness<String> testHarness =
                testHarnessBuilder
                        .addInput(STRING_TYPE_INFO, 3)
                        .modifyStreamConfig(
                                config -> {
                                    config.setCheckpointingEnabled(true);
                                    config.setUnalignedCheckpointsEnabled(
                                            enableUnalignedCheckpoint);
                                })
                        .setCheckpointResponder(checkpointResponder)
                        .setupOperatorChain(new EmptyOperator())
                        .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                        .build();

        checkpointResponder.setHandlers(
                testHarness.streamTask::notifyCheckpointCompleteAsync,
                testHarness.streamTask::notifyCheckpointAbortAsync);
        return testHarness;
    }

    @Test
    public void testWaitingForFinalCheckpointNotTheFirsNotifiedComplete() throws Exception {
        ResultPartition[] partitionWriters = new ResultPartition[2];
        try {
            for (int i = 0; i < partitionWriters.length; ++i) {
                partitionWriters[i] =
                        PartitionTestUtils.createPartition(ResultPartitionType.PIPELINED_BOUNDED);
                partitionWriters[i].setup();
            }

            CompletingCheckpointResponder checkpointResponder = new CompletingCheckpointResponder();
            try (StreamTaskMailboxTestHarness<String> testHarness =
                    createTestHarness(partitionWriters, checkpointResponder, false)) {
                // complete only the third checkpoint
                checkpointResponder.completeCheckpoints(Collections.singletonList(3L));
                // finish data on all channels
                testHarness.waitForTaskCompletion();
                // trigger the first checkpoint
                CompletableFuture<Boolean> firstCheckpoint = triggerCheckpoint(testHarness, 1);

                // Notifies the result partition that all records are processed after the
                // first checkpoint is triggered.
                firstCheckpoint.thenAccept(
                        (ignored) -> {
                            for (ResultPartition resultPartition : partitionWriters) {
                                resultPartition.onSubpartitionAllDataProcessed(0);
                            }
                        });
                testHarness.processAll();
                testHarness.getTaskStateManager().getWaitForReportLatch().await();

                // trigger a second checkpoint
                triggerCheckpoint(testHarness, 2L);
                testHarness.processAll();
                testHarness.getTaskStateManager().getWaitForReportLatch().await();

                // trigger the third checkpoint
                triggerCheckpoint(testHarness, 3L);
                testHarness.processAll();

                testHarness.finishProcessing();
                testHarness.getTaskStateManager().getWaitForReportLatch().await();
                assertEquals(3L, testHarness.getTaskStateManager().getReportedCheckpointId());
                assertEquals(
                        3L, testHarness.getTaskStateManager().getNotifiedCompletedCheckpointId());

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

    @Test
    public void testTriggerStopWithSavepointWhenWaitingForFinalCheckpoint() throws Exception {
        ResultPartition[] partitionWriters = new ResultPartition[2];
        try {
            for (int i = 0; i < partitionWriters.length; ++i) {
                partitionWriters[i] =
                        PartitionTestUtils.createPartition(ResultPartitionType.PIPELINED_BOUNDED);
                partitionWriters[i].setup();
            }

            int finalCheckpointId = 6;
            int syncSavepointId = 7;
            CompletingCheckpointResponder checkpointResponder =
                    new CompletingCheckpointResponder() {
                        @Override
                        public void acknowledgeCheckpoint(
                                JobID jobID,
                                ExecutionAttemptID executionAttemptID,
                                long checkpointId,
                                CheckpointMetrics checkpointMetrics,
                                TaskStateSnapshot subtaskState) {
                            if (syncSavepointId == checkpointId) {
                                // complete the final checkpoint when sync savepoint acknowledged
                                // we should wait for the sync savepoint to complete
                                super.acknowledgeCheckpoint(
                                        jobID,
                                        executionAttemptID,
                                        finalCheckpointId,
                                        checkpointMetrics,
                                        subtaskState);
                                try {
                                    // Give some potential time for the task to finish before the
                                    // savepoint is notified complete
                                    Thread.sleep(CONCURRENT_EVENT_WAIT_PERIOD_MS);
                                } catch (InterruptedException e) {
                                    throw new FlinkRuntimeException(e);
                                }
                                super.acknowledgeCheckpoint(
                                        jobID,
                                        executionAttemptID,
                                        syncSavepointId,
                                        checkpointMetrics,
                                        subtaskState);
                            }
                        }
                    };
            try (StreamTaskMailboxTestHarness<String> testHarness =
                    createTestHarness(partitionWriters, checkpointResponder, false)) {

                // Tests triggering checkpoint after received all the inputs have received
                // EndOfPartition.
                testHarness.waitForTaskCompletion();

                // trigger the final checkpoint
                CompletableFuture<Boolean> checkpointFuture =
                        triggerCheckpoint(testHarness, finalCheckpointId);

                // Notifies the result partition that all records are processed after the
                // last checkpoint is triggered.
                checkpointFuture.thenAccept(
                        (ignored) -> {
                            for (ResultPartition resultPartition : partitionWriters) {
                                resultPartition.onSubpartitionAllDataProcessed(0);
                            }
                        });

                // trigger the synchronous savepoint
                CompletableFuture<Boolean> savepointFuture =
                        triggerStopWithSavepointDrain(testHarness, syncSavepointId);

                // The checkpoint 6 would be triggered successfully.
                testHarness.finishProcessing();
                assertTrue(checkpointFuture.isDone());
                assertTrue(savepointFuture.isDone());
                testHarness.getTaskStateManager().getWaitForReportLatch().await();
                assertEquals(
                        syncSavepointId,
                        testHarness.getTaskStateManager().getReportedCheckpointId());
                assertEquals(
                        syncSavepointId,
                        testHarness.getTaskStateManager().getNotifiedCompletedCheckpointId());

                // Each result partition should have emitted 2 barriers and 1 EndOfUserRecordsEvent.
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
    public void testTriggerStopWithSavepointWhenWaitingForFinalCheckpointOnSourceTask()
            throws Exception {
        doTestTriggerStopWithSavepointWhenWaitingForFinalCheckpointOnSourceTask(true);
    }

    @Test
    public void testTriggerStopWithSavepointNoDrainWhenWaitingForFinalCheckpointOnSourceTask()
            throws Exception {
        doTestTriggerStopWithSavepointWhenWaitingForFinalCheckpointOnSourceTask(false);
    }

    private void doTestTriggerStopWithSavepointWhenWaitingForFinalCheckpointOnSourceTask(
            boolean drain) throws Exception {
        int finalCheckpointId = 6;
        int syncSavepointId = 7;
        CompletingCheckpointResponder checkpointResponder =
                new CompletingCheckpointResponder() {
                    @Override
                    public void acknowledgeCheckpoint(
                            JobID jobID,
                            ExecutionAttemptID executionAttemptID,
                            long checkpointId,
                            CheckpointMetrics checkpointMetrics,
                            TaskStateSnapshot subtaskState) {
                        if (syncSavepointId == checkpointId) {
                            // complete the final checkpoint when sync savepoint acknowledged
                            // we should wait for the sync savepoint to complete
                            super.acknowledgeCheckpoint(
                                    jobID,
                                    executionAttemptID,
                                    finalCheckpointId,
                                    checkpointMetrics,
                                    subtaskState);
                            try {
                                // Give some potential time for the task to finish before the
                                // savepoint is notified complete
                                Thread.sleep(CONCURRENT_EVENT_WAIT_PERIOD_MS);
                            } catch (InterruptedException e) {
                                throw new FlinkRuntimeException(e);
                            }
                            super.acknowledgeCheckpoint(
                                    jobID,
                                    executionAttemptID,
                                    syncSavepointId,
                                    checkpointMetrics,
                                    subtaskState);
                        }
                    }
                };

        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(SourceStreamTask::new, STRING_TYPE_INFO)
                        .modifyStreamConfig(
                                config -> {
                                    config.setCheckpointingEnabled(true);
                                })
                        .setCheckpointResponder(checkpointResponder)
                        .setupOutputForSingletonOperatorChain(
                                new StreamSource<>(new ImmediatelyFinishingSource()))
                        .build()) {
            checkpointResponder.setHandlers(
                    testHarness.streamTask::notifyCheckpointCompleteAsync,
                    testHarness.streamTask::notifyCheckpointAbortAsync);

            // Tests triggering checkpoint after received all the inputs have received
            // EndOfPartition.

            // start task thread
            testHarness.streamTask.runMailboxLoop();

            // trigger the final checkpoint
            CompletableFuture<Boolean> checkpointFuture =
                    triggerCheckpoint(testHarness, finalCheckpointId);

            // trigger the synchronous savepoint
            CompletableFuture<Boolean> savepointFuture =
                    drain
                            ? triggerStopWithSavepointDrain(testHarness, syncSavepointId)
                            : triggerStopWithSavepointNoDrain(testHarness, syncSavepointId);

            // The checkpoint 6 would be triggered successfully.
            testHarness.finishProcessing();
            assertTrue(checkpointFuture.isDone());
            assertTrue(savepointFuture.isDone());
            testHarness.getTaskStateManager().getWaitForReportLatch().await();
            assertEquals(
                    syncSavepointId, testHarness.getTaskStateManager().getReportedCheckpointId());
            assertEquals(
                    syncSavepointId,
                    testHarness.getTaskStateManager().getNotifiedCompletedCheckpointId());
        }
    }

    @Test
    public void testTriggeringAlignedNoTimeoutCheckpointWithFinishedChannels() throws Exception {
        testTriggeringCheckpointWithFinishedChannels(
                CheckpointOptions.alignedNoTimeout(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault()));
    }

    @Test
    public void testTriggeringUnalignedCheckpointWithFinishedChannels() throws Exception {
        testTriggeringCheckpointWithFinishedChannels(
                CheckpointOptions.unaligned(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault()));
    }

    @Test
    public void testTriggeringAlignedWithTimeoutCheckpointWithFinishedChannels() throws Exception {
        testTriggeringCheckpointWithFinishedChannels(
                CheckpointOptions.alignedWithTimeout(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault(),
                        10L));
    }

    private void testTriggeringCheckpointWithFinishedChannels(CheckpointOptions checkpointOptions)
            throws Exception {
        ResultPartition[] partitionWriters = new ResultPartition[2];
        try {
            for (int i = 0; i < partitionWriters.length; ++i) {
                partitionWriters[i] =
                        PartitionTestUtils.createPartition(ResultPartitionType.PIPELINED_BOUNDED);
                partitionWriters[i].setup();
            }

            try (StreamTaskMailboxTestHarness<String> testHarness =
                    createTestHarness(
                            partitionWriters,
                            new CompletingCheckpointResponder(),
                            checkpointOptions.isUnalignedCheckpoint()
                                    || checkpointOptions.isTimeoutable())) {

                int numChannels =
                        testHarness.inputGates[0].getInputGate().getNumberOfInputChannels();
                int[] resumedCount = new int[numChannels];
                for (int i = 0; i < numChannels; ++i) {
                    TestInputChannel inputChannel =
                            (TestInputChannel)
                                    testHarness.inputGates[0].getInputGate().getChannel(i);
                    inputChannel.setActionOnResumed(
                            () -> resumedCount[inputChannel.getChannelIndex()]++);
                }

                // Tests triggering checkpoint when all the inputs are alive.
                CompletableFuture<Boolean> checkpointFuture =
                        triggerCheckpoint(testHarness, 2, checkpointOptions);
                processMailTillCheckpointSucceeds(testHarness, checkpointFuture);
                assertEquals(2, testHarness.getTaskStateManager().getReportedCheckpointId());
                assertArrayEquals(new int[] {0, 0, 0}, resumedCount);

                // Tests triggering checkpoint after some inputs have received EndOfPartition.
                testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0, 0);
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 0, 0);
                checkpointFuture = triggerCheckpoint(testHarness, 4, checkpointOptions);
                processMailTillCheckpointSucceeds(testHarness, checkpointFuture);
                assertEquals(4, testHarness.getTaskStateManager().getReportedCheckpointId());
                assertArrayEquals(new int[] {0, 0, 0}, resumedCount);

                // Tests triggering checkpoint after received all the inputs have received
                // EndOfPartition.
                testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0, 1);
                testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0, 2);
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 0, 1);
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 0, 2);
                checkpointFuture = triggerCheckpoint(testHarness, 6, checkpointOptions);

                // Notifies the result partition that all records are processed after the
                // last checkpoint is triggered.
                checkpointFuture.thenAccept(
                        (ignored) -> {
                            for (ResultPartition resultPartition : partitionWriters) {
                                resultPartition.onSubpartitionAllDataProcessed(0);
                            }
                        });

                // The checkpoint 6 would be triggered successfully.
                testHarness.finishProcessing();
                assertTrue(checkpointFuture.isDone());
                testHarness.getTaskStateManager().getWaitForReportLatch().await();
                assertEquals(6, testHarness.getTaskStateManager().getReportedCheckpointId());
                assertArrayEquals(new int[] {0, 0, 0}, resumedCount);

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

    @Test
    public void testReportOperatorsFinishedInCheckpoint() throws Exception {
        ResultPartition[] partitionWriters = new ResultPartition[2];
        try {
            for (int i = 0; i < partitionWriters.length; ++i) {
                partitionWriters[i] =
                        PartitionTestUtils.createPartition(ResultPartitionType.PIPELINED_BOUNDED);
                partitionWriters[i].setup();
            }

            final CompletingCheckpointResponder checkpointResponder =
                    new CompletingCheckpointResponder();
            try (StreamTaskMailboxTestHarness<String> testHarness =
                    new StreamTaskMailboxTestHarnessBuilder<>(
                                    OneInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                            .addInput(BasicTypeInfo.STRING_TYPE_INFO, 1)
                            .addAdditionalOutput(partitionWriters)
                            .setCheckpointResponder(checkpointResponder)
                            .modifyStreamConfig(config -> config.setCheckpointingEnabled(true))
                            .setupOperatorChain(new StatefulOperator())
                            .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                            .build()) {

                checkpointResponder.setHandlers(
                        testHarness.streamTask::notifyCheckpointCompleteAsync,
                        testHarness.streamTask::notifyCheckpointAbortAsync);

                // Trigger the first checkpoint before we call operators' finish method.
                CompletableFuture<Boolean> checkpointFuture = triggerCheckpoint(testHarness, 2);
                processMailTillCheckpointSucceeds(testHarness, checkpointFuture);
                assertEquals(2, testHarness.getTaskStateManager().getReportedCheckpointId());
                assertFalse(
                        testHarness
                                .getTaskStateManager()
                                .getJobManagerTaskStateSnapshotsByCheckpointId()
                                .get(2L)
                                .isTaskFinished());

                // Trigger the first checkpoint after we call operators' finish method.
                // The checkpoint is added to the mailbox and will be processed in the
                // mailbox loop after call operators' finish method in the afterInvoke()
                // method.
                testHarness.processEvent(new EndOfData(StopMode.DRAIN), 0, 0);
                checkpointFuture = triggerCheckpoint(testHarness, 4);
                checkpointFuture.thenAccept(
                        (ignored) -> {
                            for (ResultPartition resultPartition : partitionWriters) {
                                resultPartition.onSubpartitionAllDataProcessed(0);
                            }
                        });
                testHarness.processAll();
                testHarness.finishProcessing();
                assertTrue(checkpointFuture.isDone());
                testHarness.getTaskStateManager().getWaitForReportLatch().await();
                assertTrue(
                        testHarness
                                .getTaskStateManager()
                                .getJobManagerTaskStateSnapshotsByCheckpointId()
                                .get(4L)
                                .isTaskFinished());
            }

        } finally {
            for (ResultPartitionWriter writer : partitionWriters) {
                if (writer != null) {
                    writer.close();
                }
            }
        }
    }

    static CompletableFuture<Boolean> triggerCheckpoint(
            StreamTaskMailboxTestHarness<String> testHarness, long checkpointId) {
        return triggerCheckpoint(
                testHarness, checkpointId, CheckpointOptions.forCheckpointWithDefaultLocation());
    }

    static CompletableFuture<Boolean> triggerCheckpoint(
            StreamTaskMailboxTestHarness<String> testHarness,
            long checkpointId,
            CheckpointOptions checkpointOptions) {
        testHarness.getTaskStateManager().getWaitForReportLatch().reset();
        return testHarness
                .getStreamTask()
                .triggerCheckpointAsync(
                        new CheckpointMetaData(checkpointId, checkpointId * 1000),
                        checkpointOptions);
    }

    static CompletableFuture<Boolean> triggerStopWithSavepointDrain(
            StreamTaskMailboxTestHarness<String> testHarness, long checkpointId) {
        return triggerStopWithSavepoint(
                testHarness, checkpointId, SavepointType.terminate(SavepointFormatType.CANONICAL));
    }

    static CompletableFuture<Boolean> triggerStopWithSavepointNoDrain(
            StreamTaskMailboxTestHarness<String> testHarness, long checkpointId) {
        return triggerStopWithSavepoint(
                testHarness, checkpointId, SavepointType.suspend(SavepointFormatType.CANONICAL));
    }

    static CompletableFuture<Boolean> triggerStopWithSavepoint(
            StreamTaskMailboxTestHarness<String> testHarness,
            long checkpointId,
            SavepointType checkpointType) {
        testHarness.getTaskStateManager().getWaitForReportLatch().reset();
        return testHarness
                .getStreamTask()
                .triggerCheckpointAsync(
                        new CheckpointMetaData(checkpointId, checkpointId * 1000),
                        CheckpointOptions.alignedNoTimeout(
                                checkpointType, CheckpointStorageLocationReference.getDefault()));
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
        long delayedCheckpointId = 2;
        CompletingCheckpointResponder responder =
                new CompletingCheckpointResponder() {
                    @Override
                    public void acknowledgeCheckpoint(
                            JobID jobID,
                            ExecutionAttemptID executionAttemptID,
                            long checkpointId,
                            CheckpointMetrics checkpointMetrics,
                            TaskStateSnapshot subtaskState) {
                        if (delayedCheckpointId == checkpointId) {
                            try {
                                // Give some potential time for the task to finish before the
                                // checkpoint is acknowledged, also do not notify its completion
                                Thread.sleep(CONCURRENT_EVENT_WAIT_PERIOD_MS);
                            } catch (InterruptedException e) {
                                throw new FlinkRuntimeException(e);
                            }
                        } else {
                            super.acknowledgeCheckpoint(
                                    jobID,
                                    executionAttemptID,
                                    checkpointId,
                                    checkpointMetrics,
                                    subtaskState);
                        }
                    }
                };

        try (StreamTaskMailboxTestHarness<String> harness = createTestHarness(responder)) {

            // finish all data
            harness.waitForTaskCompletion();
            // trigger the final checkpoint
            harness.streamTask.triggerCheckpointOnBarrier(
                    new CheckpointMetaData(1, 101),
                    CheckpointOptions.forCheckpointWithDefaultLocation(),
                    new CheckpointMetricsBuilder()
                            .setBytesProcessedDuringAlignment(0L)
                            .setAlignmentDurationNanos(0L));

            // trigger another checkpoint that we want to complete before finishing the task
            harness.streamTask.triggerCheckpointOnBarrier(
                    new CheckpointMetaData(delayedCheckpointId, 101),
                    CheckpointOptions.forCheckpointWithDefaultLocation(),
                    new CheckpointMetricsBuilder()
                            .setBytesProcessedDuringAlignment(0L)
                            .setAlignmentDurationNanos(0L));

            harness.processAll();
            harness.finishProcessing();
            assertEquals(
                    delayedCheckpointId, harness.getTaskStateManager().getReportedCheckpointId());
        }
    }

    @Test
    public void testOperatorSkipLifeCycleIfFinishedOnRestore() throws Exception {
        try (StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO, 3)
                        .setCollectNetworkEvents()
                        .setTaskStateSnapshot(1, TaskStateSnapshot.FINISHED_ON_RESTORE)
                        .setupOperatorChain(new TestFinishedOnRestoreStreamOperator())
                        .chain(new TestFinishedOnRestoreStreamOperator(), StringSerializer.INSTANCE)
                        .finish()
                        .build()) {
            // Finish the restore, including state initialization and open.
            harness.processAll();

            // Try trigger a checkpoint.
            harness.getTaskStateManager().getWaitForReportLatch().reset();
            CheckpointMetaData checkpointMetaData = new CheckpointMetaData(2, 2);
            CheckpointOptions checkpointOptions =
                    new CheckpointOptions(CheckpointType.CHECKPOINT, getDefault());
            harness.streamTask.triggerCheckpointOnBarrier(
                    checkpointMetaData,
                    checkpointOptions,
                    new CheckpointMetricsBuilder()
                            .setBytesProcessedDuringAlignment(0)
                            .setAlignmentDurationNanos(0));
            harness.getTaskStateManager().getWaitForReportLatch().await();
            assertEquals(2, harness.getTaskStateManager().getReportedCheckpointId());

            // Checkpoint notification.
            harness.streamTask.notifyCheckpointCompleteAsync(2);
            harness.streamTask.notifyCheckpointAbortAsync(3, 2);
            harness.processAll();

            // Finish & close operators.
            harness.processElement(Watermark.MAX_WATERMARK, 0, 0);
            harness.processElement(Watermark.MAX_WATERMARK, 0, 1);
            harness.processElement(Watermark.MAX_WATERMARK, 0, 2);
            harness.waitForTaskCompletion();
            harness.finishProcessing();

            assertThat(
                    harness.getOutput(),
                    contains(
                            new CheckpointBarrier(
                                    checkpointMetaData.getCheckpointId(),
                                    checkpointMetaData.getTimestamp(),
                                    checkpointOptions),
                            Watermark.MAX_WATERMARK,
                            new EndOfData(StopMode.DRAIN)));
        }
    }

    /**
     * This test verifies for tasks that finished on restore, when taking unaligned checkpoint the
     * asynchronous part would wait for the channel states futures get completed, which means the
     * barriers are aligned.
     */
    @Test
    public void testWaitingForUnalignedChannelStatesIfFinishedOnRestore() throws Exception {
        OperatorID operatorId = new OperatorID();
        try (StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .modifyStreamConfig(
                                streamConfig -> streamConfig.setUnalignedCheckpointsEnabled(true))
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO, 3)
                        .setCollectNetworkEvents()
                        .setTaskStateSnapshot(1, TaskStateSnapshot.FINISHED_ON_RESTORE)
                        .setupOperatorChain(new TestFinishedOnRestoreStreamOperator())
                        .chain(
                                operatorId,
                                new TestFinishedOnRestoreStreamOperator(operatorId),
                                StringSerializer.INSTANCE)
                        .finish()
                        .build()) {
            // Finish the restore, including state initialization and open.
            harness.processAll();

            TestCheckpointResponder checkpointResponder = harness.getCheckpointResponder();
            checkpointResponder.setAcknowledgeLatch(new OneShotLatch());
            checkpointResponder.setDeclinedLatch(new OneShotLatch());

            CheckpointBarrier unalignedBarrier =
                    new CheckpointBarrier(
                            2,
                            2,
                            CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, getDefault()));

            // On first unaligned barrier, the task would take snapshot and start the asynchronous
            // part. We slightly extend the process to make the asynchronous part start executing
            // before the other barriers arrived.
            harness.processEvent(unalignedBarrier, 0, 0);
            Thread.sleep(CONCURRENT_EVENT_WAIT_PERIOD_MS);

            // Finish the unaligned checkpoint.
            harness.processEvent(unalignedBarrier, 0, 1);
            harness.processEvent(unalignedBarrier, 0, 2);

            // Wait till the asynchronous part finished either normally or exceptionally.
            CommonTestUtils.waitUntilCondition(
                    () ->
                            checkpointResponder.getAcknowledgeLatch().isTriggered()
                                    || checkpointResponder.getDeclinedLatch().isTriggered());

            assertEquals(
                    Collections.singletonList(2L),
                    checkpointResponder.getAcknowledgeReports().stream()
                            .map(TestCheckpointResponder.AbstractReport::getCheckpointId)
                            .collect(Collectors.toList()));
            assertEquals(
                    Collections.emptyList(),
                    checkpointResponder.getDeclineReports().stream()
                            .map(TestCheckpointResponder.AbstractReport::getCheckpointId)
                            .collect(Collectors.toList()));
        }
    }

    private static class ImmediatelyFinishingSource implements SourceFunction<String> {

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            // just finish
        }

        @Override
        public void cancel() {
            // just finish
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

    private static class StatefulOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {

        private ListState<Integer> state;

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            state =
                    context.getOperatorStateStore()
                            .getUnionListState(new ListStateDescriptor<>("test", Integer.class));
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {}
    }
}
