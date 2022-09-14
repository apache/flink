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

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.runtime.operators.sink.SinkTestUtil.fromOutput;
import static org.apache.flink.streaming.runtime.operators.sink.SinkTestUtil.toCommittableSummary;
import static org.apache.flink.streaming.runtime.operators.sink.SinkTestUtil.toCommittableWithLinage;
import static org.assertj.core.api.Assertions.assertThat;

class CommitterOperatorTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testEmitCommittables(boolean withPostCommitTopology) throws Exception {
        final ForwardingCommitter committer = new ForwardingCommitter();

        Sink<Integer> sink;
        if (withPostCommitTopology) {
            // Insert global committer to simulate post commit topology
            sink =
                    TestSink.newBuilder()
                            .setCommitter(committer)
                            .setDefaultGlobalCommitter()
                            .setCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE)
                            .build()
                            .asV2();
        } else {
            sink =
                    TestSink.newBuilder()
                            .setCommitter(committer)
                            .setCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE)
                            .build()
                            .asV2();
        }
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness =
                        new OneInputStreamOperatorTestHarness<>(
                                new CommitterOperatorFactory<>(
                                        (TwoPhaseCommittingSink<?, String>) sink, false, true));
        testHarness.open();

        final CommittableSummary<String> committableSummary =
                new CommittableSummary<>(1, 1, 1L, 1, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));
        final CommittableWithLineage<String> committableWithLineage =
                new CommittableWithLineage<>("1", 1L, 1);
        testHarness.processElement(new StreamRecord<>(committableWithLineage));

        // Trigger commit
        testHarness.notifyOfCompletedCheckpoint(1);

        assertThat(committer.getSuccessfulCommits()).isEqualTo(1);
        if (withPostCommitTopology) {
            final List<StreamElement> output = fromOutput(testHarness.getOutput());
            SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
                    .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
                    .hasOverallCommittables(committableSummary.getNumberOfCommittables())
                    .hasPendingCommittables(0);
            SinkV2Assertions.assertThat(toCommittableWithLinage(output.get(1)))
                    .isEqualTo(copyCommittableWithDifferentOrigin(committableWithLineage, 0));
        } else {
            assertThat(testHarness.getOutput()).isEmpty();
        }
        testHarness.close();
    }

    @Test
    void testWaitForCommittablesOfLatestCheckpointBeforeCommitting() throws Exception {
        final ForwardingCommitter committer = new ForwardingCommitter();
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness = createTestHarness(committer, false, true);
        testHarness.open();
        testHarness.setProcessingTime(0);

        // Only send first committable
        final CommittableSummary<String> committableSummary =
                new CommittableSummary<>(1, 1, 1L, 2, 2, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));
        final CommittableWithLineage<String> first = new CommittableWithLineage<>("1", 1L, 1);
        testHarness.processElement(new StreamRecord<>(first));

        testHarness.notifyOfCompletedCheckpoint(1);

        assertThat(testHarness.getOutput()).isEmpty();
        assertThat(committer.getSuccessfulCommits()).isEqualTo(0);

        final CommittableWithLineage<String> second = new CommittableWithLineage<>("2", 1L, 1);
        testHarness.processElement(new StreamRecord<>(second));

        // Trigger commit Retry
        testHarness.getProcessingTimeService().setCurrentTime(2000);

        final List<StreamElement> output = fromOutput(testHarness.getOutput());
        assertThat(output).hasSize(3);
        assertThat(committer.getSuccessfulCommits()).isEqualTo(2);
        SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
                .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
                .hasOverallCommittables(committableSummary.getNumberOfCommittables())
                .hasPendingCommittables(0);
        SinkV2Assertions.assertThat(toCommittableWithLinage(output.get(1)))
                .isEqualTo(copyCommittableWithDifferentOrigin(first, 0));
        SinkV2Assertions.assertThat(toCommittableWithLinage(output.get(2)))
                .isEqualTo(copyCommittableWithDifferentOrigin(second, 0));
        testHarness.close();
    }

    @Test
    void testImmediatelyCommitLateCommittables() throws Exception {
        final ForwardingCommitter committer = new ForwardingCommitter();
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness = createTestHarness(committer, false, true);
        testHarness.open();

        final CommittableSummary<String> committableSummary =
                new CommittableSummary<>(1, 1, 1L, 1, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));

        // Receive notify checkpoint completed before the last data. This might happen for unaligned
        // checkpoints.
        testHarness.notifyOfCompletedCheckpoint(1);

        assertThat(testHarness.getOutput()).isEmpty();

        final CommittableWithLineage<String> first = new CommittableWithLineage<>("1", 1L, 1);

        // Commit elements with lower or equal the latest checkpoint id immediately
        testHarness.processElement(new StreamRecord<>(first));

        final List<StreamElement> output = fromOutput(testHarness.getOutput());
        assertThat(output).hasSize(2);
        assertThat(committer.getSuccessfulCommits()).isEqualTo(1);
        SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
                .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
                .hasOverallCommittables(committableSummary.getNumberOfCommittables())
                .hasPendingCommittables(0);
        SinkV2Assertions.assertThat(toCommittableWithLinage(output.get(1)))
                .isEqualTo(copyCommittableWithDifferentOrigin(first, 0));
        testHarness.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testEmitAllCommittablesOnEndOfInput(boolean isBatchMode) throws Exception {
        final ForwardingCommitter committer = new ForwardingCommitter();
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness = createTestHarness(committer, isBatchMode, !isBatchMode);
        testHarness.open();

        final CommittableSummary<String> committableSummary =
                new CommittableSummary<>(1, 2, null, 1, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));
        final CommittableSummary<String> committableSummary2 =
                new CommittableSummary<>(2, 2, null, 1, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary2));

        final CommittableWithLineage<String> first = new CommittableWithLineage<>("1", null, 1);
        testHarness.processElement(new StreamRecord<>(first));
        final CommittableWithLineage<String> second = new CommittableWithLineage<>("1", null, 2);
        testHarness.processElement(new StreamRecord<>(second));

        testHarness.endInput();
        if (!isBatchMode) {
            assertThat(testHarness.getOutput()).hasSize(0);
            // notify final checkpoint complete
            testHarness.notifyOfCompletedCheckpoint(1);
        }

        final List<StreamElement> output = fromOutput(testHarness.getOutput());
        assertThat(output).hasSize(3);
        SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
                .hasFailedCommittables(0)
                .hasOverallCommittables(2)
                .hasPendingCommittables(0);
        SinkV2Assertions.assertThat(toCommittableWithLinage(output.get(1)))
                .isEqualTo(copyCommittableWithDifferentOrigin(first, 0));
        SinkV2Assertions.assertThat(toCommittableWithLinage(output.get(2)))
                .isEqualTo(copyCommittableWithDifferentOrigin(second, 0));
        testHarness.close();
    }

    @Test
    void testStateRestore() throws Exception {
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness = createTestHarness(new TestSink.RetryOnceCommitter(), false, true);
        testHarness.open();

        final CommittableSummary<String> committableSummary =
                new CommittableSummary<>(1, 1, 0L, 1, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));
        final CommittableWithLineage<String> first = new CommittableWithLineage<>("1", 0L, 1);
        testHarness.processElement(new StreamRecord<>(first));

        final OperatorSubtaskState snapshot = testHarness.snapshot(0L, 2L);

        // Trigger first checkpoint but committer needs retry
        testHarness.notifyOfCompletedCheckpoint(0);

        assertThat(testHarness.getOutput()).isEmpty();
        testHarness.close();

        final ForwardingCommitter committer = new ForwardingCommitter();
        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                restored = createTestHarness(committer, false, true);

        restored.initializeState(snapshot);
        restored.open();

        // Previous committables are immediately committed if possible
        final List<StreamElement> output = fromOutput(restored.getOutput());
        assertThat(output).hasSize(2);
        assertThat(committer.getSuccessfulCommits()).isEqualTo(1);
        SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
                .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
                .hasOverallCommittables(committableSummary.getNumberOfCommittables())
                .hasPendingCommittables(0);

        SinkV2Assertions.assertThat(toCommittableWithLinage(output.get(1)))
                .isEqualTo(new CommittableWithLineage<>(first.getCommittable(), 1L, 0));
        restored.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHandleEndInputInStreamingMode(boolean isCheckpointingEnabled) throws Exception {
        final Sink<Integer> sink =
                TestSink.newBuilder()
                        .setDefaultCommitter()
                        .setDefaultGlobalCommitter()
                        .setCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE)
                        .build()
                        .asV2();

        final OneInputStreamOperatorTestHarness<
                        CommittableMessage<String>, CommittableMessage<String>>
                testHarness =
                        new OneInputStreamOperatorTestHarness<>(
                                new CommitterOperatorFactory<>(
                                        (TwoPhaseCommittingSink<?, String>) sink,
                                        false,
                                        isCheckpointingEnabled));
        testHarness.open();

        final CommittableSummary<String> committableSummary =
                new CommittableSummary<>(1, 1, 1L, 1, 1, 0);
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

        final List<StreamElement> output = fromOutput(testHarness.getOutput());
        assertThat(output).hasSize(2);
        SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
                .hasCheckpointId(1L)
                .hasPendingCommittables(0)
                .hasOverallCommittables(1)
                .hasFailedCommittables(0);
        SinkV2Assertions.assertThat(toCommittableWithLinage(output.get(1)))
                .isEqualTo(copyCommittableWithDifferentOrigin(committableWithLineage, 0));

        // Future emission calls should change the output
        testHarness.notifyOfCompletedCheckpoint(2);
        testHarness.endInput();

        assertThat(testHarness.getOutput()).hasSize(2);
    }

    CommittableWithLineage<?> copyCommittableWithDifferentOrigin(
            CommittableWithLineage<?> committable, int subtaskId) {
        return new CommittableWithLineage<>(
                committable.getCommittable(),
                committable.getCheckpointId().isPresent()
                        ? committable.getCheckpointId().getAsLong()
                        : null,
                subtaskId);
    }

    private OneInputStreamOperatorTestHarness<
                    CommittableMessage<String>, CommittableMessage<String>>
            createTestHarness(
                    Committer<String> committer,
                    boolean isBatchMode,
                    boolean isCheckpointingEnabled)
                    throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(
                        (TwoPhaseCommittingSink<?, String>)
                                TestSink.newBuilder()
                                        .setCommitter(committer)
                                        .setDefaultGlobalCommitter()
                                        .setCommittableSerializer(
                                                TestSink.StringCommittableSerializer.INSTANCE)
                                        .build()
                                        .asV2(),
                        isBatchMode,
                        isCheckpointingEnabled));
    }

    private static class ForwardingCommitter extends TestSink.DefaultCommitter {
        private int successfulCommits = 0;

        @Override
        public List<String> commit(List<String> committables) {
            successfulCommits += committables.size();
            return Collections.emptyList();
        }

        @Override
        public void close() throws Exception {}

        public int getSuccessfulCommits() {
            return successfulCommits;
        }
    }
}
