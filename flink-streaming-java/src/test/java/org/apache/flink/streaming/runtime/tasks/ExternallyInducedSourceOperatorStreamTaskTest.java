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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.operators.SourceOperator;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT_SUSPEND;
import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT_TERMINATE;
import static org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTaskTest.createTestHarness;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for verifying that the {@link ExternallyInducedSourceReader} can trigger any type of
 * snapshot.
 */
@RunWith(Parameterized.class)
public class ExternallyInducedSourceOperatorStreamTaskTest extends SourceStreamTaskTestBase {
    public static final CheckpointStorageLocationReference SAVEPOINT_LOCATION =
            new CheckpointStorageLocationReference("Savepoint".getBytes());
    public static final CheckpointStorageLocationReference CHECKPOINT_LOCATION =
            new CheckpointStorageLocationReference("Checkpoint".getBytes());

    @Parameter(0)
    public CheckpointOptions checkpointOptions;

    @Parameter(1)
    public boolean rpcFirst;

    @Parameters(name = "checkpoint = {0}, rpcFirst = {1}")
    public static List<Object[]> provideExternallyInducedParameters() {
        return Stream.of(
                        CheckpointOptions.alignedNoTimeout(SAVEPOINT, SAVEPOINT_LOCATION),
                        CheckpointOptions.alignedNoTimeout(SAVEPOINT_TERMINATE, SAVEPOINT_LOCATION),
                        CheckpointOptions.alignedNoTimeout(SAVEPOINT_SUSPEND, SAVEPOINT_LOCATION),
                        CheckpointOptions.alignedNoTimeout(
                                CheckpointType.CHECKPOINT, CHECKPOINT_LOCATION),
                        CheckpointOptions.alignedWithTimeout(CHECKPOINT_LOCATION, 123L),
                        CheckpointOptions.unaligned(CHECKPOINT_LOCATION),
                        CheckpointOptions.notExactlyOnce(
                                CheckpointType.CHECKPOINT, CHECKPOINT_LOCATION))
                .flatMap(
                        options ->
                                Stream.of(
                                        new Object[] {options, true},
                                        new Object[] {options, false}))
                .collect(Collectors.toList());
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

            CheckpointMetaData checkpointMetaData =
                    new CheckpointMetaData(TestingExternallyInducedSourceReader.CHECKPOINT_ID, 2);
            if (rpcFirst) {
                testHarness.streamTask.triggerCheckpointAsync(
                        checkpointMetaData, checkpointOptions);
                testHarness.processAll();
            } else {
                do {
                    testHarness.processSingleStep();
                } while (!runtimeTestingReader.shouldTriggerCheckpoint().isPresent());
                // stream task should block when trigger received but no RPC
                assertFalse(testHarness.streamTask.inputProcessor.isAvailable());
                CompletableFuture<Boolean> triggerCheckpointAsync =
                        testHarness.streamTask.triggerCheckpointAsync(
                                checkpointMetaData, checkpointOptions);
                // process mails until checkpoint has been processed
                while (!triggerCheckpointAsync.isDone()) {
                    testHarness.processSingleStep();
                }
                // stream task should be unblocked now
                assertTrue(testHarness.streamTask.inputProcessor.isAvailable());
                testHarness.processAll();
            }

            int expectedEvents =
                    checkpointOptions.getCheckpointType().isSavepoint()
                                    && checkpointOptions.getCheckpointType().isSynchronous()
                                    && checkpointOptions.getCheckpointType().shouldDrain()
                            ? numEventsBeforeCheckpoint
                            : totalNumEvents;
            assertEquals(expectedEvents, runtimeTestingReader.numEmittedEvents);
            assertTrue(runtimeTestingReader.checkpointed);
            assertEquals(
                    TestingExternallyInducedSourceReader.CHECKPOINT_ID,
                    runtimeTestingReader.checkpointedId);
            assertEquals(numEventsBeforeCheckpoint, runtimeTestingReader.checkpointedAt);
            assertThat(
                    testHarness.getOutput(),
                    Matchers.hasItem(
                            new CheckpointBarrier(
                                    checkpointMetaData.getCheckpointId(),
                                    checkpointMetaData.getTimestamp(),
                                    checkpointOptions)));
        }
    }

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
}
