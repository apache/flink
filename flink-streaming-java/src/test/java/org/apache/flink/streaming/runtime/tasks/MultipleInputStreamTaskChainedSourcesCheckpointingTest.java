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
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.LifeCycleMonitor.LifeCyclePhase;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTaskTest.MapToStringMultipleInputOperatorFactory;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTaskTest.addSourceRecords;
import static org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTaskTest.buildTestHarness;
import static org.apache.flink.streaming.runtime.tasks.StreamTaskTest.triggerCheckpoint;
import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link MultipleInputStreamTask} combined with {@link
 * org.apache.flink.streaming.api.operators.SourceOperator} chaining.
 */
public class MultipleInputStreamTaskChainedSourcesCheckpointingTest {
    private static final int MAX_STEPS = 100;

    private final CheckpointMetaData metaData =
            new CheckpointMetaData(1L, System.currentTimeMillis());

    /**
     * In this scenario: 1. checkpoint is triggered via RPC and source is blocked 2. network inputs
     * are processed until CheckpointBarriers are processed 3. aligned checkpoint is performed
     */
    @Test
    public void testSourceCheckpointFirst() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness = buildTestHarness()) {
            testHarness.setAutoProcess(false);
            ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
            CheckpointBarrier barrier = createBarrier(testHarness);
            addRecordsAndBarriers(testHarness, barrier);

            Future<Boolean> checkpointFuture =
                    testHarness
                            .getStreamTask()
                            .triggerCheckpointAsync(metaData, barrier.getCheckpointOptions());
            processSingleStepUntil(testHarness, checkpointFuture::isDone);

            expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));

            ArrayList<Object> actualOutput = new ArrayList<>(testHarness.getOutput());

            assertThat(
                    actualOutput.subList(0, expectedOutput.size()),
                    containsInAnyOrder(expectedOutput.toArray()));
            assertThat(actualOutput.get(expectedOutput.size()), equalTo(barrier));
        }
    }

    /**
     * In this scenario: 1. checkpoint is triggered via RPC and source is blocked 2. unaligned
     * checkpoint is performed 3. all data from network inputs are processed
     */
    @Test
    public void testSourceCheckpointFirstUnaligned() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness = buildTestHarness(true)) {
            testHarness.setAutoProcess(false);
            ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
            addRecords(testHarness);

            CheckpointBarrier barrier = createBarrier(testHarness);
            Future<Boolean> checkpointFuture =
                    testHarness
                            .getStreamTask()
                            .triggerCheckpointAsync(metaData, barrier.getCheckpointOptions());
            processSingleStepUntil(testHarness, checkpointFuture::isDone);

            assertThat(testHarness.getOutput(), contains(barrier));

            testHarness.processAll();

            expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));

            ArrayList<Object> actualOutput = new ArrayList<>(testHarness.getOutput());
            assertThat(
                    actualOutput.subList(1, expectedOutput.size() + 1),
                    containsInAnyOrder(expectedOutput.toArray()));
        }
    }

    /**
     * In this scenario: 1a. network inputs are processed until CheckpointBarriers are processed 1b.
     * source records are processed at the same time 2. checkpoint is triggered via RPC 3. aligned
     * checkpoint is performed
     */
    @Test
    public void testSourceCheckpointLast() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness = buildTestHarness()) {
            testHarness.setAutoProcess(false);
            ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
            CheckpointBarrier barrier = createBarrier(testHarness);
            addRecordsAndBarriers(testHarness, barrier);

            testHarness.processAll();

            Future<Boolean> checkpointFuture =
                    testHarness
                            .getStreamTask()
                            .triggerCheckpointAsync(metaData, barrier.getCheckpointOptions());
            processSingleStepUntil(testHarness, checkpointFuture::isDone);

            expectedOutput.add(new StreamRecord<>("42", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("42", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("42", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));

            ArrayList<Object> actualOutput = new ArrayList<>(testHarness.getOutput());

            assertThat(
                    actualOutput.subList(0, expectedOutput.size()),
                    containsInAnyOrder(expectedOutput.toArray()));
            assertThat(actualOutput.get(expectedOutput.size()), equalTo(barrier));
        }
    }

    /**
     * In this scenario: 1. network inputs are processed until CheckpointBarriers are processed 2.
     * there are no source records to be processed 3. checkpoint is triggered on first received
     * CheckpointBarrier 4. unaligned checkpoint is performed at some point of time blocking the
     * source 5. more source records are added, that shouldn't be processed
     */
    @Test
    public void testSourceCheckpointLastUnaligned() throws Exception {
        boolean unaligned = true;
        try (StreamTaskMailboxTestHarness<String> testHarness = buildTestHarness(unaligned)) {
            testHarness.setAutoProcess(false);
            ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

            addNetworkRecords(testHarness);
            CheckpointBarrier barrier = createBarrier(testHarness);
            addBarriers(testHarness, barrier);

            testHarness.processAll();
            addSourceRecords(testHarness, 1, 1337, 1337, 1337);
            testHarness.processAll();

            expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));
            expectedOutput.add(barrier);

            assertThat(testHarness.getOutput(), containsInAnyOrder(expectedOutput.toArray()));
        }
    }

    @Test
    public void testOnlyOneSource() throws Exception {
        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .modifyExecutionConfig(config -> config.enableObjectReuse())
                        .addSourceInput(
                                new SourceOperatorFactory<>(
                                        new MockSource(Boundedness.BOUNDED, 1),
                                        WatermarkStrategy.noWatermarks()))
                        .setupOutputForSingletonOperatorChain(
                                new MapToStringMultipleInputOperatorFactory(1))
                        .build()) {
            testHarness.setAutoProcess(false);

            ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

            addSourceRecords(testHarness, 0, 42, 43, 44);
            processSingleStepUntil(testHarness, () -> !testHarness.getOutput().isEmpty());
            expectedOutput.add(new StreamRecord<>("42", TimestampAssigner.NO_TIMESTAMP));

            CheckpointBarrier barrier = createBarrier(testHarness);
            Future<Boolean> checkpointFuture =
                    testHarness
                            .getStreamTask()
                            .triggerCheckpointAsync(metaData, barrier.getCheckpointOptions());
            processSingleStepUntil(testHarness, checkpointFuture::isDone);

            ArrayList<Object> actualOutput = new ArrayList<>(testHarness.getOutput());
            assertThat(
                    actualOutput.subList(0, expectedOutput.size()),
                    containsInAnyOrder(expectedOutput.toArray()));
            assertThat(actualOutput.get(expectedOutput.size()), equalTo(barrier));
        }
    }

    @Test
    public void testRpcTriggerCheckpointWithSourceChain() throws Exception {
        ResultPartition[] partitionWriters = new ResultPartition[2];
        try {
            for (int i = 0; i < partitionWriters.length; ++i) {
                partitionWriters[i] =
                        PartitionTestUtils.createPartition(ResultPartitionType.PIPELINED_BOUNDED);
                partitionWriters[i].setup();
            }

            try (StreamTaskMailboxTestHarness<String> testHarness =
                    new StreamTaskMailboxTestHarnessBuilder<>(
                                    MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                            .modifyStreamConfig(config -> config.setCheckpointingEnabled(true))
                            .modifyExecutionConfig(ExecutionConfig::enableObjectReuse)
                            .addInput(BasicTypeInfo.INT_TYPE_INFO)
                            .addInput(BasicTypeInfo.STRING_TYPE_INFO)
                            .addSourceInput(
                                    new SourceOperatorFactory<>(
                                            new MultipleInputStreamTaskTest
                                                    .LifeCycleTrackingMockSource(
                                                    Boundedness.CONTINUOUS_UNBOUNDED, 1),
                                            WatermarkStrategy.noWatermarks()))
                            .addSourceInput(
                                    new SourceOperatorFactory<>(
                                            new MultipleInputStreamTaskTest
                                                    .LifeCycleTrackingMockSource(
                                                    Boundedness.CONTINUOUS_UNBOUNDED, 1),
                                            WatermarkStrategy.noWatermarks()))
                            .addAdditionalOutput(partitionWriters)
                            .setupOperatorChain(new MapToStringMultipleInputOperatorFactory(4))
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

                Future<Boolean> checkpointFuture = triggerCheckpoint(testHarness, 2);
                testHarness.processAll();

                // The checkpoint 2 would be aligned after received all the EndOfPartitionEvent.
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 0, 0);
                testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 1, 0);
                testHarness.getTaskStateManager().getWaitForReportLatch().await();
                assertEquals(2, testHarness.getTaskStateManager().getReportedCheckpointId());

                // Tests triggering checkpoint after all the inputs have received EndOfPartition.
                checkpointFuture = triggerCheckpoint(testHarness, 4);

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

                // The checkpoint 4 would be triggered successfully.
                testHarness.finishProcessing();
                assertTrue(checkpointFuture.isDone());
                testHarness.getTaskStateManager().getWaitForReportLatch().await();
                assertEquals(4, testHarness.getTaskStateManager().getReportedCheckpointId());

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
    public void testSkipExecutionsIfFinishedOnRestoreWithSourceChained() throws Exception {
        OperatorID firstSourceOperatorId = new OperatorID();
        OperatorID secondSourceOperatorId = new OperatorID();
        OperatorID nonSourceOperatorId = new OperatorID();

        try (StreamTaskMailboxTestHarness<String> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .modifyStreamConfig(config -> config.setCheckpointingEnabled(true))
                        .modifyExecutionConfig(ExecutionConfig::enableObjectReuse)
                        .addInput(BasicTypeInfo.INT_TYPE_INFO)
                        .addSourceInput(
                                firstSourceOperatorId,
                                new SourceOperatorFactory<>(
                                        new SourceOperatorStreamTaskTest.LifeCycleMonitorSource(
                                                Boundedness.CONTINUOUS_UNBOUNDED, 1),
                                        WatermarkStrategy.noWatermarks()))
                        .addSourceInput(
                                secondSourceOperatorId,
                                new SourceOperatorFactory<>(
                                        new SourceOperatorStreamTaskTest.LifeCycleMonitorSource(
                                                Boundedness.CONTINUOUS_UNBOUNDED, 1),
                                        WatermarkStrategy.noWatermarks()))
                        .setTaskStateSnapshot(1, TaskStateSnapshot.FINISHED)
                        .setupOperatorChain(
                                nonSourceOperatorId,
                                new LifeCycleMonitorMultipleInputOperatorFactory())
                        .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                        .build()) {

            testHarness.processEvent(EndOfPartitionEvent.INSTANCE, 0, 0);

            testHarness.processAll();
            testHarness.waitForTaskCompletion();

            for (StreamOperatorWrapper<?, ?> wrapper :
                    testHarness.getStreamTask().operatorChain.getAllOperators()) {
                if (wrapper.getStreamOperator() instanceof SourceOperator<?, ?>) {
                    SourceOperatorStreamTaskTest.LifeCycleMonitorSourceReader sourceReader =
                            (SourceOperatorStreamTaskTest.LifeCycleMonitorSourceReader)
                                    ((SourceOperator<?, ?>) wrapper.getStreamOperator())
                                            .getSourceReader();
                    sourceReader.getLifeCycleMonitor().assertCallTimes(0, LifeCyclePhase.values());
                } else if (wrapper.getStreamOperator()
                        instanceof LifeCycleMonitorMultipleInputOperator) {
                    ((LifeCycleMonitorMultipleInputOperator) wrapper.getStreamOperator())
                            .getLifeCycleMonitor()
                            .assertCallTimes(0, LifeCyclePhase.values());
                } else {
                    fail("Unexpected operator type for " + wrapper.getStreamOperator());
                }
            }
        }
    }

    private void addRecordsAndBarriers(
            StreamTaskMailboxTestHarness<String> testHarness, CheckpointBarrier checkpointBarrier)
            throws Exception {
        addRecords(testHarness);
        addBarriers(testHarness, checkpointBarrier);
    }

    private CheckpointBarrier createBarrier(StreamTaskMailboxTestHarness<String> testHarness) {
        StreamConfig config = testHarness.getStreamTask().getConfiguration();
        CheckpointOptions checkpointOptions =
                CheckpointOptions.forConfig(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault(),
                        config.isExactlyOnceCheckpointMode(),
                        config.isUnalignedCheckpointsEnabled(),
                        config.getAlignedCheckpointTimeout().toMillis());

        return new CheckpointBarrier(
                metaData.getCheckpointId(), metaData.getTimestamp(), checkpointOptions);
    }

    private void addBarriers(
            StreamTaskMailboxTestHarness<String> testHarness, CheckpointBarrier checkpointBarrier)
            throws Exception {
        testHarness.processEvent(checkpointBarrier, 0);
        testHarness.processEvent(checkpointBarrier, 1);
    }

    private void addRecords(StreamTaskMailboxTestHarness<String> testHarness) throws Exception {
        addSourceRecords(testHarness, 1, 42, 42, 42);
        addNetworkRecords(testHarness);
    }

    private void addNetworkRecords(StreamTaskMailboxTestHarness<String> testHarness)
            throws Exception {
        testHarness.processElement(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP), 0);
        testHarness.processElement(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP), 0);
        testHarness.processElement(new StreamRecord<>(47d, TimestampAssigner.NO_TIMESTAMP), 1);
        testHarness.processElement(new StreamRecord<>(47d, TimestampAssigner.NO_TIMESTAMP), 1);
    }

    private void processSingleStepUntil(
            StreamTaskMailboxTestHarness<String> testHarness, Supplier<Boolean> condition)
            throws Exception {
        assertFalse(condition.get());
        for (int i = 0; i < MAX_STEPS && !condition.get(); i++) {
            testHarness.processSingleStep();
        }
        assertTrue(condition.get());
    }

    static class LifeCycleMonitorMultipleInputOperator
            extends MultipleInputStreamTaskTest.MapToStringMultipleInputOperator {

        private final LifeCycleMonitor lifeCycleMonitor = new LifeCycleMonitor();

        public LifeCycleMonitorMultipleInputOperator(StreamOperatorParameters<String> parameters) {
            super(parameters, 3);
        }

        @Override
        public void open() throws Exception {
            super.open();
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.OPEN);
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.INITIALIZE_STATE);
        }

        @Override
        public void finish() throws Exception {
            super.finish();
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.FINISH);
        }

        @Override
        public void close() throws Exception {
            super.close();
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.CLOSE);
        }

        public LifeCycleMonitor getLifeCycleMonitor() {
            return lifeCycleMonitor;
        }
    }

    static class LifeCycleMonitorMultipleInputOperatorFactory
            extends AbstractStreamOperatorFactory<String> {
        @Override
        public <T extends StreamOperator<String>> T createStreamOperator(
                StreamOperatorParameters<String> parameters) {
            return (T) new LifeCycleMonitorMultipleInputOperator(parameters);
        }

        @Override
        public Class<? extends StreamOperator<String>> getStreamOperatorClass(
                ClassLoader classLoader) {
            return LifeCycleMonitorMultipleInputOperator.class;
        }
    }
}
