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

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;

import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummaryAssert;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions.committableSummary;
import static org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions.committableWithLineage;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;


import java.util.Collection;

class SinkV2CommitterOperatorTest extends CommitterOperatorTestBase {
    @Override
    SinkAndCounters sinkWithPostCommit() {
        ForwardingCommitter committer = new ForwardingCommitter();
        return new SinkAndCounters(
                (SupportsCommitter<String>)
                        TestSinkV2.newBuilder()
                                .setCommitter(committer)
                                .setWithPostCommitTopology(true)
                                .build(),
                () -> committer.successfulCommits);
    }

    @Override
    SinkAndCounters sinkWithPostCommitWithRetry() {
        return new CommitterOperatorTestBase.SinkAndCounters(
                (SupportsCommitter<String>)
                        TestSinkV2.newBuilder()
                                .setCommitter(new TestSinkV2.RetryOnceCommitter())
                                .setWithPostCommitTopology(true)
                                .build(),
                () -> 0);
    }

    @Override
    SinkAndCounters sinkWithoutPostCommit() {
        ForwardingCommitter committer = new ForwardingCommitter();
        return new SinkAndCounters(
                TestSinkV2.newBuilder()
                        .setCommitter(committer)
                        .setWithPostCommitTopology(false)
                        .build()
                        .asSupportsCommitter(),
                () -> committer.successfulCommits);
    }

    @ParameterizedTest
    @CsvSource({"1, 10, 9", "2, 1, 0", "2, 2, 1"})
    void testStateRestoreWithScaling(
            int parallelismBeforeScaling, int parallelismAfterScaling, int subtaskIdAfterRecovery)
            throws Exception {

        final int originalSubtaskId = 0;

        final OneInputStreamOperatorTestHarness<
                CommittableMessage<String>, CommittableMessage<String>>
                testHarness =
                createTestHarness(
                        sinkWithPostCommitWithRetry().sink,
                        false,
                        true,
                        parallelismBeforeScaling,
                        parallelismBeforeScaling,
                        originalSubtaskId);
        testHarness.open();

        // We cannot test a different checkpoint thant 0 because when using the OperatorTestHarness
        // for recovery the lastCompleted checkpoint is always reset to 0.
        long checkpointId = 0L;

        final CommittableSummary<String> committableSummary =
                new CommittableSummary<>(
                        originalSubtaskId, parallelismBeforeScaling, checkpointId, 1, 0, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));
        final CommittableWithLineage<String> first =
                new CommittableWithLineage<>("1", checkpointId, originalSubtaskId);
        testHarness.processElement(new StreamRecord<>(first));

        // another committable for the same checkpointId but from different subtask.
        final CommittableSummary<String> committableSummary2 =
                new CommittableSummary<>(
                        originalSubtaskId + 1, parallelismBeforeScaling, checkpointId, 1, 0, 0);
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
        SinkAndCounters restored = sinkWithPostCommit();
        final OneInputStreamOperatorTestHarness<
                CommittableMessage<String>, CommittableMessage<String>>
                restoredHarness =
                createTestHarness(
                        restored.sink,
                        false,
                        true,
                        parallelismAfterScaling,
                        parallelismAfterScaling,
                        subtaskIdAfterRecovery);

        restoredHarness.initializeState(snapshot);
        restoredHarness.open();

        // Previous committables are immediately committed if possible
        assertThat(restored.commitCounter.getAsInt()).isEqualTo(2);
        ListAssert<CommittableMessage<String>> records =
                assertThat(restoredHarness.extractOutputValues()).hasSize(3);
        CommittableSummaryAssert<Object> objectCommittableSummaryAssert =
                records.element(0, as(committableSummary()))
                        .hasCheckpointId(checkpointId)
                        .hasFailedCommittables(0)
                        .hasSubtaskId(subtaskIdAfterRecovery)
                        .hasNumberOfSubtasks(
                                Math.min(parallelismBeforeScaling, parallelismAfterScaling));
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
        restoredHarness.close();
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


    private static class ForwardingCommitter extends TestSinkV2.DefaultCommitter {
        private int successfulCommits = 0;

        @Override
        public void commit(Collection<CommitRequest<String>> committables) {
            successfulCommits += committables.size();
        }

        @Override
        public void close() throws Exception {
        }
    }
}
