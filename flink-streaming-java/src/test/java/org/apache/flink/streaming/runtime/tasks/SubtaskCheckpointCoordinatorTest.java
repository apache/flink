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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.writer.NonRecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordOrEventCollectingResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.TestCheckpointStorageWorkerView;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskITCase.NoOpStreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.shaded.guava31.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link SubtaskCheckpointCoordinator}. */
public class SubtaskCheckpointCoordinatorTest {

    @Test
    public void testInitCheckpoint() throws IOException, CheckpointException {
        assertTrue(initCheckpoint(true, CHECKPOINT));
        assertFalse(initCheckpoint(false, CHECKPOINT));
        assertFalse(initCheckpoint(false, SavepointType.savepoint(SavepointFormatType.CANONICAL)));
    }

    private boolean initCheckpoint(boolean unalignedCheckpointEnabled, SnapshotType checkpointType)
            throws IOException, CheckpointException {
        class MockWriter extends ChannelStateWriterImpl.NoOpChannelStateWriter {
            private boolean started;

            @Override
            public void start(long checkpointId, CheckpointOptions checkpointOptions) {
                started = true;
            }
        }

        MockWriter writer = new MockWriter();
        SubtaskCheckpointCoordinator coordinator = coordinator(writer);
        CheckpointStorageLocationReference locationReference =
                CheckpointStorageLocationReference.getDefault();
        coordinator.initInputsCheckpoint(
                1L,
                unalignedCheckpointEnabled
                        ? CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, locationReference)
                        : CheckpointOptions.alignedNoTimeout(checkpointType, locationReference));
        return writer.started;
    }

    @Test
    public void testNotifyCheckpointComplete() throws Exception {
        TestTaskStateManager stateManager = new TestTaskStateManager();
        MockEnvironment mockEnvironment =
                MockEnvironment.builder().setTaskStateManager(stateManager).build();

        try (SubtaskCheckpointCoordinator subtaskCheckpointCoordinator =
                new MockSubtaskCheckpointCoordinatorBuilder()
                        .setEnvironment(mockEnvironment)
                        .build()) {
            final OperatorChain<?, ?> operatorChain = getOperatorChain(mockEnvironment);

            long checkpointId = 42L;
            {
                subtaskCheckpointCoordinator.notifyCheckpointComplete(
                        checkpointId, operatorChain, () -> true);
                assertEquals(checkpointId, stateManager.getNotifiedCompletedCheckpointId());
            }

            long newCheckpointId = checkpointId + 1;
            {
                subtaskCheckpointCoordinator.notifyCheckpointComplete(
                        newCheckpointId, operatorChain, () -> false);
                // even task is not running, state manager could still receive the notification.
                assertEquals(newCheckpointId, stateManager.getNotifiedCompletedCheckpointId());
            }
        }
    }

    @Test
    public void testSavepointNotResultingInPriorityEvents() throws Exception {
        MockEnvironment mockEnvironment = MockEnvironment.builder().build();

        try (SubtaskCheckpointCoordinator coordinator =
                new MockSubtaskCheckpointCoordinatorBuilder()
                        .setUnalignedCheckpointEnabled(true)
                        .setEnvironment(mockEnvironment)
                        .build()) {
            AtomicReference<Boolean> broadcastedPriorityEvent = new AtomicReference<>(null);
            final OperatorChain<?, ?> operatorChain =
                    new RegularOperatorChain(
                            new MockStreamTaskBuilder(mockEnvironment).build(),
                            new NonRecordWriter<>()) {
                        @Override
                        public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent)
                                throws IOException {
                            super.broadcastEvent(event, isPriorityEvent);
                            broadcastedPriorityEvent.set(isPriorityEvent);
                        }
                    };

            coordinator.checkpointState(
                    new CheckpointMetaData(0, 0),
                    new CheckpointOptions(
                            SavepointType.savepoint(SavepointFormatType.CANONICAL),
                            CheckpointStorageLocationReference.getDefault()),
                    new CheckpointMetricsBuilder(),
                    operatorChain,
                    false,
                    () -> true);

            assertEquals(false, broadcastedPriorityEvent.get());
        }
    }

    @Test
    public void testForceAlignedCheckpointResultingInPriorityEvents() throws Exception {
        final long checkpointId = 42L;
        MockEnvironment mockEnvironment = MockEnvironment.builder().build();

        try (SubtaskCheckpointCoordinator coordinator =
                new MockSubtaskCheckpointCoordinatorBuilder()
                        .setUnalignedCheckpointEnabled(true)
                        .setEnvironment(mockEnvironment)
                        .build()) {

            AtomicReference<Boolean> broadcastedPriorityEvent = new AtomicReference<>(null);
            final OperatorChain<?, ?> operatorChain =
                    new RegularOperatorChain(
                            new MockStreamTaskBuilder(mockEnvironment).build(),
                            new NonRecordWriter<>()) {
                        @Override
                        public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent)
                                throws IOException {
                            super.broadcastEvent(event, isPriorityEvent);
                            broadcastedPriorityEvent.set(isPriorityEvent);
                            // test if we can write output data
                            coordinator
                                    .getChannelStateWriter()
                                    .addOutputData(
                                            checkpointId,
                                            new ResultSubpartitionInfo(0, 0),
                                            0,
                                            BufferBuilderTestUtils.buildSomeBuffer(500));
                        }
                    };

            CheckpointOptions forcedAlignedOptions =
                    CheckpointOptions.unaligned(
                                    CheckpointType.CHECKPOINT,
                                    CheckpointStorageLocationReference.getDefault())
                            .withUnalignedUnsupported();
            coordinator.checkpointState(
                    new CheckpointMetaData(checkpointId, 0),
                    forcedAlignedOptions,
                    new CheckpointMetricsBuilder(),
                    operatorChain,
                    false,
                    () -> true);

            assertEquals(true, broadcastedPriorityEvent.get());
        }
    }

    @Test
    public void testSkipChannelStateForSavepoints() throws Exception {
        try (SubtaskCheckpointCoordinator coordinator =
                new MockSubtaskCheckpointCoordinatorBuilder()
                        .setUnalignedCheckpointEnabled(true)
                        .setPrepareInputSnapshot(
                                (u1, u2) -> {
                                    fail("should not prepare input snapshot for savepoint");
                                    return null;
                                })
                        .build()) {
            coordinator.checkpointState(
                    new CheckpointMetaData(0, 0),
                    new CheckpointOptions(
                            SavepointType.savepoint(SavepointFormatType.CANONICAL),
                            CheckpointStorageLocationReference.getDefault()),
                    new CheckpointMetricsBuilder(),
                    new RegularOperatorChain<>(
                            new NoOpStreamTask<>(new DummyEnvironment()), new NonRecordWriter<>()),
                    false,
                    () -> true);
        }
    }

    @Test
    public void testNotifyCheckpointSubsumed() throws Exception {
        TestTaskStateManager stateManager = new TestTaskStateManager();
        MockEnvironment mockEnvironment =
                MockEnvironment.builder().setTaskStateManager(stateManager).build();

        try (SubtaskCheckpointCoordinatorImpl subtaskCheckpointCoordinator =
                (SubtaskCheckpointCoordinatorImpl)
                        new MockSubtaskCheckpointCoordinatorBuilder()
                                .setEnvironment(mockEnvironment)
                                .setUnalignedCheckpointEnabled(true)
                                .build()) {

            StreamMap<String, String> streamMap =
                    new StreamMap<>((MapFunction<String, String>) value -> value);
            streamMap.setProcessingTimeService(new TestProcessingTimeService());
            final OperatorChain<String, AbstractStreamOperator<String>> operatorChain =
                    operatorChain(streamMap);
            StreamTaskStateInitializerImpl stateInitializer =
                    new StreamTaskStateInitializerImpl(mockEnvironment, new TestStateBackend());
            operatorChain.initializeStateAndOpenOperators(stateInitializer);

            long checkpointId = 42L;

            subtaskCheckpointCoordinator
                    .getChannelStateWriter()
                    .start(checkpointId, CheckpointOptions.forCheckpointWithDefaultLocation());
            subtaskCheckpointCoordinator.checkpointState(
                    new CheckpointMetaData(checkpointId, System.currentTimeMillis()),
                    CheckpointOptions.forCheckpointWithDefaultLocation(),
                    new CheckpointMetricsBuilder(),
                    operatorChain,
                    false,
                    () -> false);

            long notifySubsumeCheckpointId = checkpointId + 1L;
            // notify checkpoint aborted before execution.
            subtaskCheckpointCoordinator.notifyCheckpointSubsumed(
                    notifySubsumeCheckpointId, operatorChain, () -> true);
            assertEquals(
                    notifySubsumeCheckpointId,
                    ((TestStateBackend.TestKeyedStateBackend<?>) streamMap.getKeyedStateBackend())
                            .getSubsumeCheckpointId());
        }
    }

    @Test
    public void testNotifyCheckpointAbortedManyTimes() throws Exception {
        MockEnvironment mockEnvironment = MockEnvironment.builder().build();
        int maxRecordAbortedCheckpoints = 256;

        try (SubtaskCheckpointCoordinatorImpl subtaskCheckpointCoordinator =
                (SubtaskCheckpointCoordinatorImpl)
                        new MockSubtaskCheckpointCoordinatorBuilder()
                                .setEnvironment(mockEnvironment)
                                .setMaxRecordAbortedCheckpoints(maxRecordAbortedCheckpoints)
                                .build()) {
            final OperatorChain<?, ?> operatorChain = getOperatorChain(mockEnvironment);

            long notifyAbortedTimes = maxRecordAbortedCheckpoints + 42;
            for (int i = 1; i < notifyAbortedTimes; i++) {
                subtaskCheckpointCoordinator.notifyCheckpointAborted(i, operatorChain, () -> true);
                assertEquals(
                        Math.min(maxRecordAbortedCheckpoints, i),
                        subtaskCheckpointCoordinator.getAbortedCheckpointSize());
            }
        }
    }

    @Test
    public void testNotifyCheckpointAbortedBeforeAsyncPhase() throws Exception {
        TestTaskStateManager stateManager = new TestTaskStateManager();
        MockEnvironment mockEnvironment =
                MockEnvironment.builder().setTaskStateManager(stateManager).build();

        try (SubtaskCheckpointCoordinatorImpl subtaskCheckpointCoordinator =
                (SubtaskCheckpointCoordinatorImpl)
                        new MockSubtaskCheckpointCoordinatorBuilder()
                                .setEnvironment(mockEnvironment)
                                .setUnalignedCheckpointEnabled(true)
                                .build()) {
            CheckpointOperator checkpointOperator =
                    new CheckpointOperator(new OperatorSnapshotFutures());

            final OperatorChain<String, AbstractStreamOperator<String>> operatorChain =
                    operatorChain(checkpointOperator);

            long checkpointId = 42L;
            // notify checkpoint aborted before execution.
            subtaskCheckpointCoordinator.notifyCheckpointAborted(
                    checkpointId, operatorChain, () -> true);
            assertEquals(1, subtaskCheckpointCoordinator.getAbortedCheckpointSize());

            subtaskCheckpointCoordinator
                    .getChannelStateWriter()
                    .start(checkpointId, CheckpointOptions.forCheckpointWithDefaultLocation());
            subtaskCheckpointCoordinator.checkpointState(
                    new CheckpointMetaData(checkpointId, System.currentTimeMillis()),
                    CheckpointOptions.forCheckpointWithDefaultLocation(),
                    new CheckpointMetricsBuilder(),
                    operatorChain,
                    false,
                    () -> false);
            assertFalse(checkpointOperator.isCheckpointed());
            assertEquals(-1, stateManager.getReportedCheckpointId());
            assertEquals(0, subtaskCheckpointCoordinator.getAbortedCheckpointSize());
            assertEquals(0, subtaskCheckpointCoordinator.getAsyncCheckpointRunnableSize());
        }
    }

    @Test
    public void testBroadcastCancelCheckpointMarkerOnAbortingFromCoordinator() throws Exception {
        OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        1,
                        1,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();
        StreamConfig streamConfig = testHarness.getStreamConfig();
        streamConfig.setStreamOperator(new MapOperator());

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        MockEnvironment mockEnvironment = MockEnvironment.builder().build();

        try (SubtaskCheckpointCoordinator subtaskCheckpointCoordinator =
                new MockSubtaskCheckpointCoordinatorBuilder()
                        .setEnvironment(mockEnvironment)
                        .build()) {
            ArrayList<Object> recordOrEvents = new ArrayList<>();
            StreamElementSerializer<String> stringStreamElementSerializer =
                    new StreamElementSerializer<>(StringSerializer.INSTANCE);
            ResultPartitionWriter resultPartitionWriter =
                    new RecordOrEventCollectingResultPartitionWriter<>(
                            recordOrEvents, stringStreamElementSerializer);
            mockEnvironment.addOutputs(Collections.singletonList(resultPartitionWriter));

            OneInputStreamTask<String, String> task = testHarness.getTask();
            OperatorChain<String, OneInputStreamOperator<String, String>> operatorChain =
                    new RegularOperatorChain<>(
                            task,
                            StreamTask.createRecordWriterDelegate(streamConfig, mockEnvironment));
            long checkpointId = 42L;
            // notify checkpoint aborted before execution.
            subtaskCheckpointCoordinator.notifyCheckpointAborted(
                    checkpointId, operatorChain, () -> true);
            subtaskCheckpointCoordinator.checkpointState(
                    new CheckpointMetaData(checkpointId, System.currentTimeMillis()),
                    CheckpointOptions.forCheckpointWithDefaultLocation(),
                    new CheckpointMetricsBuilder(),
                    operatorChain,
                    false,
                    () -> false);

            assertEquals(1, recordOrEvents.size());
            Object recordOrEvent = recordOrEvents.get(0);
            // ensure CancelCheckpointMarker is broadcast downstream.
            assertTrue(recordOrEvent instanceof CancelCheckpointMarker);
            assertEquals(checkpointId, ((CancelCheckpointMarker) recordOrEvent).getCheckpointId());
            testHarness.endInput();
            testHarness.waitForTaskCompletion();
        }
    }

    private static class MapOperator extends StreamMap<String, String> {
        private static final long serialVersionUID = 1L;

        public MapOperator() {
            super((MapFunction<String, String>) value -> value);
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {}
    }

    @Test
    public void testNotifyCheckpointAbortedDuringAsyncPhase() throws Exception {
        MockEnvironment mockEnvironment = MockEnvironment.builder().build();

        try (SubtaskCheckpointCoordinatorImpl subtaskCheckpointCoordinator =
                (SubtaskCheckpointCoordinatorImpl)
                        new MockSubtaskCheckpointCoordinatorBuilder()
                                .setEnvironment(mockEnvironment)
                                .setExecutor(Executors.newFixedThreadPool(2))
                                .setUnalignedCheckpointEnabled(true)
                                .build()) {
            final BlockingRunnableFuture rawKeyedStateHandleFuture = new BlockingRunnableFuture();
            OperatorSnapshotFutures operatorSnapshotResult =
                    new OperatorSnapshotFutures(
                            DoneFuture.of(SnapshotResult.empty()),
                            rawKeyedStateHandleFuture,
                            DoneFuture.of(SnapshotResult.empty()),
                            DoneFuture.of(SnapshotResult.empty()),
                            DoneFuture.of(SnapshotResult.empty()),
                            DoneFuture.of(SnapshotResult.empty()));

            final OperatorChain<String, AbstractStreamOperator<String>> operatorChain =
                    operatorChain(new CheckpointOperator(operatorSnapshotResult));

            long checkpointId = 42L;
            subtaskCheckpointCoordinator
                    .getChannelStateWriter()
                    .start(checkpointId, CheckpointOptions.forCheckpointWithDefaultLocation());
            subtaskCheckpointCoordinator.checkpointState(
                    new CheckpointMetaData(checkpointId, System.currentTimeMillis()),
                    CheckpointOptions.forCheckpointWithDefaultLocation(),
                    new CheckpointMetricsBuilder(),
                    operatorChain,
                    false,
                    () -> false);
            rawKeyedStateHandleFuture.awaitRun();
            assertEquals(1, subtaskCheckpointCoordinator.getAsyncCheckpointRunnableSize());
            assertFalse(rawKeyedStateHandleFuture.isCancelled());

            subtaskCheckpointCoordinator.notifyCheckpointAborted(
                    checkpointId, operatorChain, () -> true);
            while (!rawKeyedStateHandleFuture.isDone()) {
                Thread.sleep(10L);
            }
            assertTrue(rawKeyedStateHandleFuture.isCancelled());
            assertEquals(0, subtaskCheckpointCoordinator.getAsyncCheckpointRunnableSize());
        }
    }

    @Test
    public void testNotifyCheckpointAbortedAfterAsyncPhase() throws Exception {
        TestTaskStateManager stateManager = new TestTaskStateManager();
        MockEnvironment mockEnvironment =
                MockEnvironment.builder().setTaskStateManager(stateManager).build();

        try (SubtaskCheckpointCoordinatorImpl subtaskCheckpointCoordinator =
                (SubtaskCheckpointCoordinatorImpl)
                        new MockSubtaskCheckpointCoordinatorBuilder()
                                .setEnvironment(mockEnvironment)
                                .build()) {
            final OperatorChain<?, ?> operatorChain = getOperatorChain(mockEnvironment);

            long checkpointId = 42L;
            subtaskCheckpointCoordinator.checkpointState(
                    new CheckpointMetaData(checkpointId, System.currentTimeMillis()),
                    CheckpointOptions.forCheckpointWithDefaultLocation(),
                    new CheckpointMetricsBuilder(),
                    operatorChain,
                    false,
                    () -> false);
            subtaskCheckpointCoordinator.notifyCheckpointAborted(
                    checkpointId, operatorChain, () -> true);
            assertEquals(0, subtaskCheckpointCoordinator.getAbortedCheckpointSize());
            assertEquals(checkpointId, stateManager.getNotifiedAbortedCheckpointId());
        }
    }

    @Test
    public void testTimeoutableAlignedBarrierNotPriorityAndChannelStateResult() throws Exception {
        long checkpointId = 66;
        MockEnvironment mockEnvironment = MockEnvironment.builder().build();

        try (SubtaskCheckpointCoordinator coordinator =
                new MockSubtaskCheckpointCoordinatorBuilder()
                        .setUnalignedCheckpointEnabled(true)
                        .setEnvironment(mockEnvironment)
                        .build()) {
            AtomicReference<Boolean> broadcastedPriorityEvent = new AtomicReference<>(null);
            AtomicReference<ChannelStateWriter.ChannelStateWriteResult> channelStateResult =
                    new AtomicReference<>(null);
            final OperatorChain<?, ?> operatorChain =
                    new RegularOperatorChain(
                            new NoOpStreamTask<>(new DummyEnvironment()), new NonRecordWriter<>()) {
                        @Override
                        public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent)
                                throws IOException {
                            super.broadcastEvent(event, isPriorityEvent);
                            broadcastedPriorityEvent.set(isPriorityEvent);
                        }

                        @Override
                        public void snapshotState(
                                Map operatorSnapshotsInProgress,
                                CheckpointMetaData checkpointMetaData,
                                CheckpointOptions checkpointOptions,
                                Supplier isRunning,
                                ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
                                CheckpointStreamFactory storage) {
                            channelStateResult.set(channelStateWriteResult);
                            sendAcknowledgeCheckpointEvent(checkpointMetaData.getCheckpointId());
                        }
                    };

            CheckpointOptions checkpointOptions =
                    new CheckpointOptions(
                            CHECKPOINT,
                            CheckpointStorageLocationReference.getDefault(),
                            CheckpointOptions.AlignmentType.ALIGNED,
                            200);
            coordinator.initInputsCheckpoint(checkpointId, checkpointOptions);
            coordinator.checkpointState(
                    new CheckpointMetaData(checkpointId, System.currentTimeMillis()),
                    checkpointOptions,
                    new CheckpointMetricsBuilder(),
                    operatorChain,
                    false,
                    () -> true);

            assertEquals(false, broadcastedPriorityEvent.get());
            assertNotNull(channelStateResult.get());
        }
    }

    @Test
    public void testChannelStateWriteResultLeakAndNotFailAfterCheckpointAborted() throws Exception {
        String taskName = "test";
        DummyEnvironment env = new DummyEnvironment();
        ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(
                        env.getJobVertexId(),
                        taskName,
                        0,
                        new JobManagerCheckpointStorage(),
                        env.getChannelStateExecutorFactory(),
                        5);
        try (MockEnvironment mockEnvironment = MockEnvironment.builder().build();
                SubtaskCheckpointCoordinator coordinator =
                        new SubtaskCheckpointCoordinatorImpl(
                                new TestCheckpointStorageWorkerView(100),
                                taskName,
                                StreamTaskActionExecutor.IMMEDIATE,
                                newDirectExecutorService(),
                                env,
                                (unused1, unused2) -> {},
                                (unused1, unused2) -> CompletableFuture.completedFuture(null),
                                1,
                                writer,
                                true,
                                (callable, duration) -> () -> {})) {
            final OperatorChain<?, ?> operatorChain = getOperatorChain(mockEnvironment);
            int checkpointId = 1;
            // Abort checkpoint 1
            coordinator.notifyCheckpointAborted(checkpointId, operatorChain, () -> true);

            coordinator.initInputsCheckpoint(
                    checkpointId,
                    CheckpointOptions.unaligned(
                            CheckpointType.CHECKPOINT,
                            CheckpointStorageLocationReference.getDefault()));
            ChannelStateWriter.ChannelStateWriteResult writeResult =
                    writer.getWriteResult(checkpointId);
            assertNotNull(writeResult);

            coordinator.checkpointState(
                    new CheckpointMetaData(checkpointId, System.currentTimeMillis()),
                    CheckpointOptions.forCheckpointWithDefaultLocation(),
                    new CheckpointMetricsBuilder(),
                    operatorChain,
                    false,
                    () -> true);
            assertNull(writer.getWriteResult(checkpointId));
            writeResult.waitForDone();
            assertTrue(writeResult.isDone());
            assertTrue(writeResult.getInputChannelStateHandles().isCompletedExceptionally());
            assertTrue(writeResult.getResultSubpartitionStateHandles().isCompletedExceptionally());
        }
    }

    @Test
    public void testAbortOldAndStartNewCheckpoint() throws Exception {
        String taskName = "test";
        CheckpointOptions unalignedOptions =
                CheckpointOptions.unaligned(
                        CHECKPOINT, CheckpointStorageLocationReference.getDefault());
        DummyEnvironment env = new DummyEnvironment();
        ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(
                        env.getJobVertexId(),
                        taskName,
                        0,
                        new JobManagerCheckpointStorage(),
                        env.getChannelStateExecutorFactory(),
                        5);
        try (MockEnvironment mockEnvironment = MockEnvironment.builder().build();
                SubtaskCheckpointCoordinator coordinator =
                        new SubtaskCheckpointCoordinatorImpl(
                                new TestCheckpointStorageWorkerView(100),
                                taskName,
                                StreamTaskActionExecutor.IMMEDIATE,
                                newDirectExecutorService(),
                                env,
                                (unused1, unused2) -> {},
                                (unused1, unused2) -> CompletableFuture.completedFuture(null),
                                1,
                                writer,
                                true,
                                (callable, duration) -> () -> {})) {
            final OperatorChain<?, ?> operatorChain = getOperatorChain(mockEnvironment);
            int checkpoint42 = 42;
            int checkpoint43 = 43;

            coordinator.initInputsCheckpoint(checkpoint42, unalignedOptions);
            ChannelStateWriter.ChannelStateWriteResult result42 =
                    writer.getWriteResult(checkpoint42);
            assertNotNull(result42);

            coordinator.notifyCheckpointAborted(checkpoint42, operatorChain, () -> true);
            coordinator.initInputsCheckpoint(checkpoint43, unalignedOptions);
            ChannelStateWriter.ChannelStateWriteResult result43 =
                    writer.getWriteResult(checkpoint43);

            result42.waitForDone();
            assertTrue(result42.isDone());
            try {
                result42.getInputChannelStateHandles().get();
                fail("The result should have failed.");
            } catch (Throwable throwable) {
                assertTrue(findThrowable(throwable, CancellationException.class).isPresent());
            }

            // test the new checkpoint can be completed
            coordinator.checkpointState(
                    new CheckpointMetaData(checkpoint43, System.currentTimeMillis()),
                    unalignedOptions,
                    new CheckpointMetricsBuilder(),
                    operatorChain,
                    false,
                    () -> true);
            result43.waitForDone();
            assertNotNull(result43);
            assertTrue(result43.isDone());
            assertFalse(result43.getInputChannelStateHandles().isCompletedExceptionally());
            assertFalse(result43.getResultSubpartitionStateHandles().isCompletedExceptionally());
        }
    }

    private OperatorChain<?, ?> getOperatorChain(MockEnvironment mockEnvironment) throws Exception {
        return new RegularOperatorChain<>(
                new MockStreamTaskBuilder(mockEnvironment).build(), new NonRecordWriter<>());
    }

    private <T> OperatorChain<T, AbstractStreamOperator<T>> operatorChain(
            OneInputStreamOperator<T, T>... streamOperators) throws Exception {
        return OperatorChainTest.setupOperatorChain(streamOperators);
    }

    private static final class BlockingRunnableFuture
            implements RunnableFuture<SnapshotResult<KeyedStateHandle>> {

        private final CompletableFuture<SnapshotResult<KeyedStateHandle>> future =
                new CompletableFuture<>();

        private final OneShotLatch signalRunLatch = new OneShotLatch();

        private final CountDownLatch countDownLatch;

        private final SnapshotResult<KeyedStateHandle> value;

        private BlockingRunnableFuture() {
            // count down twice to wait for notify checkpoint aborted to cancel.
            this.countDownLatch = new CountDownLatch(2);
            this.value = SnapshotResult.empty();
        }

        @Override
        public void run() {
            signalRunLatch.trigger();
            countDownLatch.countDown();

            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                ExceptionUtils.rethrow(e);
            }

            future.complete(value);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            future.cancel(mayInterruptIfRunning);
            return true;
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public SnapshotResult<KeyedStateHandle> get()
                throws InterruptedException, ExecutionException {
            return future.get();
        }

        @Override
        public SnapshotResult<KeyedStateHandle> get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException {
            return future.get();
        }

        void awaitRun() throws InterruptedException {
            signalRunLatch.await();
        }
    }

    private static class CheckpointOperator implements OneInputStreamOperator<String, String> {

        private static final long serialVersionUID = 1L;

        private final OperatorSnapshotFutures operatorSnapshotFutures;

        private boolean checkpointed = false;

        CheckpointOperator(OperatorSnapshotFutures operatorSnapshotFutures) {
            this.operatorSnapshotFutures = operatorSnapshotFutures;
        }

        boolean isCheckpointed() {
            return checkpointed;
        }

        @Override
        public void open() throws Exception {}

        @Override
        public void finish() throws Exception {}

        @Override
        public void close() throws Exception {}

        @Override
        public void prepareSnapshotPreBarrier(long checkpointId) {}

        @Override
        public OperatorSnapshotFutures snapshotState(
                long checkpointId,
                long timestamp,
                CheckpointOptions checkpointOptions,
                CheckpointStreamFactory storageLocation)
                throws Exception {
            this.checkpointed = true;
            return operatorSnapshotFutures;
        }

        @Override
        public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
                throws Exception {}

        @Override
        public void setKeyContextElement1(StreamRecord<?> record) {}

        @Override
        public void setKeyContextElement2(StreamRecord<?> record) {}

        @Override
        public OperatorMetricGroup getMetricGroup() {
            return UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
        }

        @Override
        public OperatorID getOperatorID() {
            return new OperatorID();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}

        @Override
        public void setCurrentKey(Object key) {}

        @Override
        public Object getCurrentKey() {
            return null;
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {}

        @Override
        public void processWatermark(Watermark mark) throws Exception {}

        @Override
        public void processLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {}
    }

    private static SubtaskCheckpointCoordinator coordinator(ChannelStateWriter channelStateWriter)
            throws IOException {
        return new SubtaskCheckpointCoordinatorImpl(
                new TestCheckpointStorageWorkerView(100),
                "test",
                StreamTaskActionExecutor.IMMEDIATE,
                newDirectExecutorService(),
                new DummyEnvironment(),
                (message, unused) -> fail(message),
                (unused1, unused2) -> CompletableFuture.completedFuture(null),
                0,
                channelStateWriter,
                true,
                (callable, duration) -> () -> {});
    }
}
