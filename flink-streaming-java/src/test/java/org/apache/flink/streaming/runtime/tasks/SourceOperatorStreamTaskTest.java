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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.writer.RecordOrEventCollectingResultPartitionWriter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.LifeCycleMonitor.LifeCyclePhase;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT_TERMINATE;
import static org.apache.flink.streaming.util.TestHarnessUtil.assertOutputEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for verifying that the {@link SourceOperator} as a task input can be integrated well with
 * {@link org.apache.flink.streaming.runtime.io.StreamOneInputProcessor}.
 */
public class SourceOperatorStreamTaskTest extends SourceStreamTaskTestBase {
    private static final OperatorID OPERATOR_ID = new OperatorID();
    private static final int NUM_RECORDS = 10;

    @Test
    public void testMetrics() throws Exception {
        testMetrics(
                SourceOperatorStreamTask::new,
                new SourceOperatorFactory<>(
                        new MockSource(Boundedness.BOUNDED, 1), WatermarkStrategy.noWatermarks()),
                lessThanOrEqualTo(1_000_000d));
    }

    /**
     * Tests that the stream operator can snapshot and restore the operator state of chained
     * operators.
     */
    @Test
    public void testSnapshotAndRestore() throws Exception {
        // process NUM_RECORDS records and take a snapshot.
        TaskStateSnapshot taskStateSnapshot =
                executeAndWaitForCheckpoint(1, null, IntStream.range(0, NUM_RECORDS));

        // Resume from the snapshot and continue to process another NUM_RECORDS records.
        executeAndWaitForCheckpoint(
                2, taskStateSnapshot, IntStream.range(NUM_RECORDS, NUM_RECORDS * 2));
    }

    @Test
    public void testSnapshotAndAdvanceToEndOfEventTime() throws Exception {
        final int checkpointId = 1;
        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                createTestHarness(checkpointId, null)) {
            getAndMaybeAssignSplit(testHarness);

            final CheckpointOptions checkpointOptions =
                    new CheckpointOptions(
                            CheckpointType.SAVEPOINT_TERMINATE,
                            CheckpointStorageLocationReference.getDefault());
            triggerCheckpointWaitForFinish(testHarness, checkpointId, checkpointOptions);

            Queue<Object> expectedOutput = new LinkedList<>();
            expectedOutput.add(Watermark.MAX_WATERMARK);
            expectedOutput.add(EndOfData.INSTANCE);
            expectedOutput.add(
                    new CheckpointBarrier(checkpointId, checkpointId, checkpointOptions));

            assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
        }
    }

    @Test
    public void testEmittingMaxWatermarkAfterReadingAllRecords() throws Exception {
        try (StreamTaskMailboxTestHarness<Integer> testHarness = createTestHarness()) {
            testHarness.processAll();
            testHarness.finishProcessing();

            Queue<Object> expectedOutput = new LinkedList<>();
            expectedOutput.add(Watermark.MAX_WATERMARK);
            expectedOutput.add(EndOfData.INSTANCE);
            assertThat(testHarness.getOutput().toArray(), equalTo(expectedOutput.toArray()));
        }
    }

    @Test
    public void testNotEmittingMaxWatermarkAfterCancelling() throws Exception {
        try (StreamTaskMailboxTestHarness<Integer> testHarness = createTestHarness()) {
            testHarness.getStreamTask().cancel();
            testHarness.finishProcessing();

            assertThat(testHarness.getOutput(), hasSize(0));
        }
    }

    @Test
    public void testExternallyInducedSource() throws Exception {
        final int numEventsBeforeCheckpoint = 10;
        final int totalNumEvents = 20;
        TestingExternallyInducedSourceReader testingReader =
                new TestingExternallyInducedSourceReader(numEventsBeforeCheckpoint, totalNumEvents);
        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                createTestHarness(new TestingExternallyInducedSource(testingReader), 0, null)) {
            TestingExternallyInducedSourceReader runtimeTestingReader =
                    (TestingExternallyInducedSourceReader)
                            ((SourceOperator) testHarness.getStreamTask().mainOperator)
                                    .getSourceReader();

            testHarness.processAll();

            assertEquals(totalNumEvents, runtimeTestingReader.numEmittedEvents);
            assertTrue(runtimeTestingReader.checkpointed);
            assertEquals(
                    TestingExternallyInducedSourceReader.CHECKPOINT_ID,
                    runtimeTestingReader.checkpointedId);
            assertEquals(numEventsBeforeCheckpoint, runtimeTestingReader.checkpointedAt);
        }
    }

    @Test
    public void testSkipExecutionIfFinishedOnRestore() throws Exception {
        TaskStateSnapshot taskStateSnapshot = TaskStateSnapshot.FINISHED_ON_RESTORE;

        LifeCycleMonitorSource testingSource =
                new LifeCycleMonitorSource(Boundedness.CONTINUOUS_UNBOUNDED, 10);
        SourceOperatorFactory<Integer> sourceOperatorFactory =
                new SourceOperatorFactory<>(testingSource, WatermarkStrategy.noWatermarks());

        List<Object> output = new ArrayList<>();
        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                SourceOperatorStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .setTaskStateSnapshot(1, taskStateSnapshot)
                        .addAdditionalOutput(
                                new RecordOrEventCollectingResultPartitionWriter<StreamElement>(
                                        output,
                                        new StreamElementSerializer<>(IntSerializer.INSTANCE)) {
                                    @Override
                                    public void notifyEndOfData() throws IOException {
                                        broadcastEvent(EndOfData.INSTANCE, false);
                                    }
                                })
                        .setupOperatorChain(sourceOperatorFactory)
                        .chain(new TestFinishedOnRestoreStreamOperator(), StringSerializer.INSTANCE)
                        .finish()
                        .build()) {

            testHarness.getStreamTask().invoke();
            testHarness.processAll();
            assertThat(output, contains(Watermark.MAX_WATERMARK, EndOfData.INSTANCE));

            LifeCycleMonitorSourceReader sourceReader =
                    (LifeCycleMonitorSourceReader)
                            ((SourceOperator<?, ?>) testHarness.getStreamTask().getMainOperator())
                                    .getSourceReader();
            sourceReader.getLifeCycleMonitor().assertCallTimes(0, LifeCyclePhase.values());
        }
    }

    @Test
    public void testTriggeringStopWithSavepointWithDrain() throws Exception {
        SourceOperatorFactory<Integer> sourceOperatorFactory =
                new SourceOperatorFactory<>(
                        new MockSource(Boundedness.CONTINUOUS_UNBOUNDED, 2),
                        WatermarkStrategy.noWatermarks());

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

        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                SourceOperatorStreamTask::new, BasicTypeInfo.INT_TYPE_INFO)
                        .setupOutputForSingletonOperatorChain(sourceOperatorFactory)
                        .setCheckpointResponder(checkpointResponder)
                        .build()) {

            CompletableFuture<Boolean> triggerResult =
                    testHarness.streamTask.triggerCheckpointAsync(
                            new CheckpointMetaData(2, 2),
                            CheckpointOptions.alignedNoTimeout(
                                    SAVEPOINT_TERMINATE,
                                    CheckpointStorageLocationReference.getDefault()));
            checkpointCompleted.whenComplete(
                    (ignored, exception) ->
                            testHarness.streamTask.notifyCheckpointCompleteAsync(2));
            testHarness.waitForTaskCompletion();
            testHarness.finishProcessing();

            assertTrue(triggerResult.isDone());
            assertTrue(triggerResult.get());
            assertTrue(checkpointCompleted.isDone());
        }
    }

    private TaskStateSnapshot executeAndWaitForCheckpoint(
            long checkpointId, TaskStateSnapshot initialSnapshot, IntStream expectedRecords)
            throws Exception {

        try (StreamTaskMailboxTestHarness<Integer> testHarness =
                createTestHarness(checkpointId, initialSnapshot)) {
            // Add records to the splits.
            MockSourceSplit split = getAndMaybeAssignSplit(testHarness);
            // Add records to the split and update expected output.
            addRecords(split, NUM_RECORDS);
            // Process all the records.
            testHarness.processAll();

            CheckpointOptions checkpointOptions =
                    CheckpointOptions.forCheckpointWithDefaultLocation();
            triggerCheckpointWaitForFinish(testHarness, checkpointId, checkpointOptions);

            // Build expected output to verify the results
            Queue<Object> expectedOutput = new LinkedList<>();
            expectedRecords.forEach(
                    r ->
                            expectedOutput.offer(
                                    new StreamRecord<>(r, TimestampAssigner.NO_TIMESTAMP)));
            // Add barrier to the expected output.
            expectedOutput.add(
                    new CheckpointBarrier(checkpointId, checkpointId, checkpointOptions));

            assertEquals(checkpointId, testHarness.taskStateManager.getReportedCheckpointId());
            assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

            return testHarness.taskStateManager.getLastJobManagerTaskStateSnapshot();
        }
    }

    private void triggerCheckpointWaitForFinish(
            StreamTaskMailboxTestHarness<Integer> testHarness,
            long checkpointId,
            CheckpointOptions checkpointOptions)
            throws Exception {
        // Trigger a checkpoint.
        testHarness.taskStateManager.getWaitForReportLatch().reset();
        CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, checkpointId);
        Future<Boolean> checkpointFuture =
                testHarness
                        .getStreamTask()
                        .triggerCheckpointAsync(checkpointMetaData, checkpointOptions);

        // Wait until the checkpoint finishes.
        // We have to mark the source reader as available here, otherwise the runMailboxStep() call
        // after
        // checkpoint is completed will block.
        getSourceReaderFromTask(testHarness).markAvailable();
        processUntil(testHarness, checkpointFuture::isDone);
        Future<Void> checkpointNotified =
                testHarness.getStreamTask().notifyCheckpointCompleteAsync(checkpointId);
        processUntil(testHarness, checkpointNotified::isDone);
        testHarness.taskStateManager.getWaitForReportLatch().await();
    }

    private void processUntil(StreamTaskMailboxTestHarness testHarness, Supplier<Boolean> condition)
            throws Exception {
        do {
            testHarness.getStreamTask().runMailboxStep();
        } while (!condition.get());
    }

    private StreamTaskMailboxTestHarness<Integer> createTestHarness() throws Exception {
        return createTestHarness(0, null);
    }

    private StreamTaskMailboxTestHarness<Integer> createTestHarness(
            long checkpointId, TaskStateSnapshot snapshot) throws Exception {
        return createTestHarness(new MockSource(Boundedness.BOUNDED, 1), checkpointId, snapshot);
    }

    private StreamTaskMailboxTestHarness<Integer> createTestHarness(
            MockSource source, long checkpointId, TaskStateSnapshot snapshot) throws Exception {
        // get a source operator.
        SourceOperatorFactory<Integer> sourceOperatorFactory =
                new SourceOperatorFactory<>(source, WatermarkStrategy.noWatermarks());

        // build a test harness.
        StreamTaskMailboxTestHarnessBuilder<Integer> builder =
                new StreamTaskMailboxTestHarnessBuilder<>(
                        SourceOperatorStreamTask::new, BasicTypeInfo.INT_TYPE_INFO);
        if (snapshot != null) {
            // Set initial snapshot if needed.
            builder.setTaskStateSnapshot(checkpointId, snapshot);
        }
        return builder.setCollectNetworkEvents()
                .setupOutputForSingletonOperatorChain(sourceOperatorFactory, OPERATOR_ID)
                .build();
    }

    private MockSourceSplit getAndMaybeAssignSplit(
            StreamTaskMailboxTestHarness<Integer> testHarness) throws Exception {
        List<MockSourceSplit> assignedSplits =
                getSourceReaderFromTask(testHarness).getAssignedSplits();
        if (assignedSplits.isEmpty()) {
            // Prepare the source split and assign it to the source reader.
            MockSourceSplit split = new MockSourceSplit(0, 0);
            // Assign the split to the source reader.
            AddSplitEvent<MockSourceSplit> addSplitEvent =
                    new AddSplitEvent<>(
                            Collections.singletonList(split), new MockSourceSplitSerializer());
            testHarness
                    .getStreamTask()
                    .dispatchOperatorEvent(OPERATOR_ID, new SerializedValue<>(addSplitEvent));
            // Run the task until the split assignment is done.
            while (assignedSplits.isEmpty()) {
                testHarness.getStreamTask().runMailboxStep();
            }
            // Need to mark the source reader as available for further processing.
            getSourceReaderFromTask(testHarness).markAvailable();
        }
        // The source reader already has an assigned split, just return it
        return assignedSplits.get(0);
    }

    private void addRecords(MockSourceSplit split, int numRecords) {
        int startingIndex = split.index();
        for (int i = startingIndex; i < startingIndex + numRecords; i++) {
            split.addRecord(i);
        }
    }

    private MockSourceReader getSourceReaderFromTask(
            StreamTaskMailboxTestHarness<Integer> testHarness) {
        return (MockSourceReader)
                ((SourceOperator) testHarness.getStreamTask().mainOperator).getSourceReader();
    }

    // ------------- private testing classes ----------

    private static class TestingExternallyInducedSource extends MockSource {
        private static final long serialVersionUID = 3078454109555893721L;
        private final TestingExternallyInducedSourceReader reader;

        private TestingExternallyInducedSource(TestingExternallyInducedSourceReader reader) {
            super(Boundedness.CONTINUOUS_UNBOUNDED, 1);
            this.reader = reader;
        }

        @Override
        public SourceReader<Integer, MockSourceSplit> createReader(
                SourceReaderContext readerContext) {
            return reader;
        }
    }

    private static class TestingExternallyInducedSourceReader
            implements ExternallyInducedSourceReader<Integer, MockSourceSplit>, Serializable {
        private static final long CHECKPOINT_ID = 1234L;
        private final int numEventsBeforeCheckpoint;
        private final int totalNumEvents;
        private int numEmittedEvents;

        private boolean checkpointed;
        private int checkpointedAt;
        private long checkpointedId;

        TestingExternallyInducedSourceReader(int numEventsBeforeCheckpoint, int totalNumEvents) {
            this.numEventsBeforeCheckpoint = numEventsBeforeCheckpoint;
            this.totalNumEvents = totalNumEvents;
            this.numEmittedEvents = 0;
            this.checkpointed = false;
            this.checkpointedAt = -1;
        }

        @Override
        public Optional<Long> shouldTriggerCheckpoint() {
            if (numEmittedEvents == numEventsBeforeCheckpoint && !checkpointed) {
                return Optional.of(CHECKPOINT_ID);
            } else {
                return Optional.empty();
            }
        }

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<Integer> output) throws Exception {
            if (numEmittedEvents == numEventsBeforeCheckpoint - 1) {
                numEmittedEvents++;
                return InputStatus.NOTHING_AVAILABLE;
            } else if (numEmittedEvents < totalNumEvents) {
                numEmittedEvents++;
                return InputStatus.MORE_AVAILABLE;
            } else {
                return InputStatus.END_OF_INPUT;
            }
        }

        @Override
        public List<MockSourceSplit> snapshotState(long checkpointId) {
            checkpointed = true;
            checkpointedAt = numEmittedEvents;
            checkpointedId = checkpointId;
            return Collections.emptyList();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void addSplits(List<MockSourceSplit> splits) {}

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() throws Exception {}
    }

    static class LifeCycleMonitorSource extends MockSource {

        public LifeCycleMonitorSource(Boundedness boundedness, int numSplits) {
            super(boundedness, numSplits);
        }

        @Override
        public SourceReader<Integer, MockSourceSplit> createReader(
                SourceReaderContext readerContext) {
            return new LifeCycleMonitorSourceReader();
        }
    }

    static class LifeCycleMonitorSourceReader extends MockSourceReader {
        private final LifeCycleMonitor lifeCycleMonitor = new LifeCycleMonitor();

        @Override
        public void start() {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.OPEN);
            super.start();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Integer> sourceOutput) throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.PROCESS_ELEMENT);
            return super.pollNext(sourceOutput);
        }

        @Override
        public void close() throws Exception {
            lifeCycleMonitor.incrementCallTime(LifeCyclePhase.CLOSE);
            super.close();
        }

        public LifeCycleMonitor getLifeCycleMonitor() {
            return lifeCycleMonitor;
        }
    }
}
