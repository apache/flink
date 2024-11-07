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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.configuration.SinkOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummaryAssert;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.function.IntSupplier;

import static org.apache.flink.streaming.api.connector.sink2.CommittableMessage.EOI;
import static org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions.committableSummary;
import static org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions.committableWithLineage;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

abstract class CommitterOperatorTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testEmitCommittables(boolean withPostCommitTopology) throws Exception {
        SinkAndCounters sinkAndCounters;
        if (withPostCommitTopology) {
            // Insert global committer to simulate post commit topology
            sinkAndCounters = sinkWithPostCommit();
        } else {
            sinkAndCounters = sinkWithoutPostCommit();
        }
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness =
                        new OneInputStreamOperatorTestHarness<>(
                                new CommitterOperatorFactory<>(sinkAndCounters.sink, false, true));
        testHarness.open();

        final CommittableSummary<String> committableSummary =
                new CommittableSummary<>(1, 1, 1L, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));
        final CommittableWithLineage<String> committableWithLineage =
                new CommittableWithLineage<>("1", 1L, 1);
        testHarness.processElement(new StreamRecord<>(committableWithLineage));

        // Trigger commit
        testHarness.notifyOfCompletedCheckpoint(1);

        assertThat(sinkAndCounters.commitCounter.getAsInt()).isEqualTo(1);
        if (withPostCommitTopology) {
            ListAssert<CommittableMessage<String>> records =
                    assertThat(testHarness.extractOutputValues()).hasSize(2);
            records.element(0, as(committableSummary()))
                    .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
                    .hasOverallCommittables(committableSummary.getNumberOfCommittables());
            records.element(1, as(committableWithLineage()))
                    .isEqualTo(committableWithLineage.withSubtaskId(0));
        } else {
            assertThat(testHarness.getOutput()).isEmpty();
        }
        testHarness.close();
    }

    @Test
    void ensureAllCommittablesArrivedBeforeCommitting() throws Exception {
        SinkAndCounters sinkAndCounters = sinkWithPostCommit();
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness = createTestHarness(sinkAndCounters.sink, false, true);
        testHarness.open();
        testHarness.setProcessingTime(0);

        // Only send first committable
        final CommittableSummary<String> committableSummary =
                new CommittableSummary<>(1, 1, 1L, 2, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));
        final CommittableWithLineage<String> first = new CommittableWithLineage<>("1", 1L, 1);
        testHarness.processElement(new StreamRecord<>(first));

        assertThatCode(() -> testHarness.notifyOfCompletedCheckpoint(1))
                .hasMessageContaining("Trying to commit incomplete batch of committables");

        assertThat(testHarness.getOutput()).isEmpty();
        assertThat(sinkAndCounters.commitCounter.getAsInt()).isZero();

        final CommittableWithLineage<String> second = new CommittableWithLineage<>("2", 1L, 1);
        testHarness.processElement(new StreamRecord<>(second));

        assertThatCode(() -> testHarness.notifyOfCompletedCheckpoint(1)).doesNotThrowAnyException();

        assertThat(sinkAndCounters.commitCounter.getAsInt()).isEqualTo(2);
        ListAssert<CommittableMessage<String>> records =
                assertThat(testHarness.extractOutputValues()).hasSize(3);
        records.element(0, as(committableSummary()))
                .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
                .hasOverallCommittables(committableSummary.getNumberOfCommittables());
        records.element(1, as(committableWithLineage())).isEqualTo(first.withSubtaskId(0));
        records.element(2, as(committableWithLineage())).isEqualTo(second.withSubtaskId(0));
        testHarness.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testEmitAllCommittablesOnEndOfInput(boolean isBatchMode) throws Exception {
        SinkAndCounters sinkAndCounters = sinkWithPostCommit();
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness = createTestHarness(sinkAndCounters.sink, isBatchMode, !isBatchMode);
        testHarness.open();

        final CommittableSummary<String> committableSummary =
                new CommittableSummary<>(1, 2, EOI, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));
        final CommittableSummary<String> committableSummary2 =
                new CommittableSummary<>(2, 2, EOI, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary2));

        final CommittableWithLineage<String> first = new CommittableWithLineage<>("1", EOI, 1);
        testHarness.processElement(new StreamRecord<>(first));
        final CommittableWithLineage<String> second = new CommittableWithLineage<>("1", EOI, 2);
        testHarness.processElement(new StreamRecord<>(second));

        testHarness.endInput();
        if (!isBatchMode) {
            assertThat(testHarness.getOutput()).isEmpty();
            // notify final checkpoint complete
            testHarness.notifyOfCompletedCheckpoint(1);
        }

        ListAssert<CommittableMessage<String>> records =
                assertThat(testHarness.extractOutputValues()).hasSize(3);
        records.element(0, as(committableSummary()))
                .hasFailedCommittables(0)
                .hasOverallCommittables(2);
        records.element(1, as(committableWithLineage())).isEqualTo(first.withSubtaskId(0));
        records.element(2, as(committableWithLineage())).isEqualTo(second.withSubtaskId(0));
        testHarness.close();
    }

    @Test
    void testStateRestore() throws Exception {

        final int originalSubtaskId = 0;
        final int subtaskIdAfterRecovery = 9;

        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness =
                        createTestHarness(
                                sinkWithPostCommitWithRetry().sink,
                                false,
                                true,
                                1,
                                1,
                                originalSubtaskId);
        testHarness.open();

        // We cannot test a different checkpoint thant 0 because when using the OperatorTestHarness
        // for recovery the lastCompleted checkpoint is always reset to 0.
        long checkpointId = 0L;

        final CommittableSummary<String> committableSummary =
                new CommittableSummary<>(originalSubtaskId, 1, checkpointId, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));
        final CommittableWithLineage<String> first =
                new CommittableWithLineage<>("1", checkpointId, originalSubtaskId);
        testHarness.processElement(new StreamRecord<>(first));

        // another committable for the same checkpointId but from different subtask.
        final CommittableSummary<String> committableSummary2 =
                new CommittableSummary<>(originalSubtaskId + 1, 1, checkpointId, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary2));
        final CommittableWithLineage<String> second =
                new CommittableWithLineage<>("2", checkpointId, originalSubtaskId + 1);
        testHarness.processElement(new StreamRecord<>(second));

        final OperatorSubtaskState snapshot = testHarness.snapshot(checkpointId, 2L);
        assertThat(testHarness.getOutput()).isEmpty();
        testHarness.close();

        // create new testHarness but with different parallelism level and subtaskId that original
        // one.
        // we will make sure that new subtaskId was used during committable recovery.
        SinkAndCounters sinkAndCounters = sinkWithPostCommit();
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                restored =
                        createTestHarness(
                                sinkAndCounters.sink, false, true, 10, 10, subtaskIdAfterRecovery);

        restored.initializeState(snapshot);
        restored.open();

        // Previous committables are immediately committed if possible
        assertThat(sinkAndCounters.commitCounter.getAsInt()).isEqualTo(2);
        ListAssert<CommittableMessage<String>> records =
                assertThat(restored.extractOutputValues()).hasSize(3);
        CommittableSummaryAssert<Object> objectCommittableSummaryAssert =
                records.element(0, as(committableSummary()))
                        .hasCheckpointId(checkpointId)
                        .hasFailedCommittables(0)
                        .hasSubtaskId(subtaskIdAfterRecovery);
        objectCommittableSummaryAssert.hasOverallCommittables(2);

        // Expect the same checkpointId that the original snapshot was made with.
        records.element(1, as(committableWithLineage()))
                .hasCheckpointId(checkpointId)
                .hasSubtaskId(subtaskIdAfterRecovery)
                .hasCommittable(first.getCommittable());
        records.element(2, as(committableWithLineage()))
                .hasCheckpointId(checkpointId)
                .hasSubtaskId(subtaskIdAfterRecovery)
                .hasCommittable(second.getCommittable());
        restored.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void testNumberOfRetries(int numRetries) throws Exception {
        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness =
                        createTestHarness(
                                sinkWithPostCommitWithRetry().sink, false, true, 1, 1, 0)) {
            testHarness
                    .getStreamConfig()
                    .getConfiguration()
                    .set(SinkOptions.COMMITTER_RETRIES, numRetries);
            testHarness.open();

            long ckdId = 1L;
            testHarness.processElement(
                    new StreamRecord<>(new CommittableSummary<>(0, 1, ckdId, 1, 0)));
            testHarness.processElement(
                    new StreamRecord<>(new CommittableWithLineage<>("1", ckdId, 0)));
            AbstractThrowableAssert<?, ? extends Throwable> throwableAssert =
                    assertThatCode(() -> testHarness.notifyOfCompletedCheckpoint(ckdId));
            if (numRetries == 0) {
                throwableAssert.hasMessageContaining("Failed to commit 1 committables");
            } else {
                throwableAssert.doesNotThrowAnyException();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHandleEndInputInStreamingMode(boolean isCheckpointingEnabled) throws Exception {
        final SinkAndCounters sinkAndCounters = sinkWithPostCommit();

        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness =
                        new OneInputStreamOperatorTestHarness<>(
                                new CommitterOperatorFactory<>(
                                        sinkAndCounters.sink, false, isCheckpointingEnabled))) {
            testHarness.open();

            final CommittableSummary<String> committableSummary =
                    new CommittableSummary<>(1, 1, 1L, 1, 0);
            testHarness.processElement(new StreamRecord<>(committableSummary));
            final CommittableWithLineage<String> committableWithLineage =
                    new CommittableWithLineage<>("1", 1L, 1);
            testHarness.processElement(new StreamRecord<>(committableWithLineage));

            testHarness.endInput();

            // If checkpointing enabled endInput does not emit anything because a final checkpoint
            // follows
            if (isCheckpointingEnabled) {
                testHarness.notifyOfCompletedCheckpoint(1);
            }

            ListAssert<CommittableMessage<String>> records =
                    assertThat(testHarness.extractOutputValues()).hasSize(2);
            CommittableSummaryAssert<Object> objectCommittableSummaryAssert =
                    records.element(0, as(committableSummary())).hasCheckpointId(1L);
            objectCommittableSummaryAssert.hasOverallCommittables(1);
            records.element(1, as(committableWithLineage()))
                    .isEqualTo(committableWithLineage.withSubtaskId(0));

            // Future emission calls should change the output
            testHarness.notifyOfCompletedCheckpoint(2);
            testHarness.endInput();

            assertThat(testHarness.getOutput()).hasSize(2);
        }
    }

    private OneInputStreamOperatorTestHarness<
                    CommittableMessage<String>, CommittableMessage<String>>
            createTestHarness(
                    SupportsCommitter<String> sink,
                    boolean isBatchMode,
                    boolean isCheckpointingEnabled)
                    throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(sink, isBatchMode, isCheckpointingEnabled));
    }

    private OneInputStreamOperatorTestHarness<
                    CommittableMessage<String>, CommittableMessage<String>>
            createTestHarness(
                    SupportsCommitter<String> sink,
                    boolean isBatchMode,
                    boolean isCheckpointingEnabled,
                    int maxParallelism,
                    int parallelism,
                    int subtaskId)
                    throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(sink, isBatchMode, isCheckpointingEnabled),
                maxParallelism,
                parallelism,
                subtaskId);
    }

    abstract SinkAndCounters sinkWithPostCommit();

    abstract SinkAndCounters sinkWithPostCommitWithRetry();

    abstract SinkAndCounters sinkWithoutPostCommit();

    static class SinkAndCounters {
        SupportsCommitter<String> sink;
        IntSupplier commitCounter;

        public SinkAndCounters(SupportsCommitter<String> sink, IntSupplier commitCounter) {
            this.sink = sink;
            this.commitCounter = commitCounter;
        }
    }
}
