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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.streaming.api.connector.sink2.CommittableMessage.EOI;
import static org.assertj.core.api.Assertions.assertThat;

class GlobalCommitterOperatorTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testWaitForCommittablesOfLatestCheckpointBeforeCommitting(boolean commitOnInput)
            throws Exception {
        final MockCommitter committer = new MockCommitter();
        try (OneInputStreamOperatorTestHarness<CommittableMessage<Integer>, Void> testHarness =
                createTestHarness(committer, commitOnInput)) {
            testHarness.open();

            long cid = 1L;
            testHarness.processElement(
                    new StreamRecord<>(new CommittableSummary<>(1, 1, cid, 2, 0)));

            testHarness.processElement(new StreamRecord<>(new CommittableWithLineage<>(1, cid, 1)));

            testHarness.notifyOfCompletedCheckpoint(cid);

            assertThat(testHarness.getOutput()).isEmpty();
            // Not committed because incomplete
            assertThat(committer.committed).isEmpty();

            // immediately commit on receiving the second committable iff commitOnInput is true
            testHarness.processElement(new StreamRecord<>(new CommittableWithLineage<>(2, cid, 1)));
            if (commitOnInput) {
                assertThat(committer.committed).containsExactly(1, 2);
            } else {
                assertThat(committer.committed).isEmpty();
                testHarness.notifyOfCompletedCheckpoint(cid + 1);
                assertThat(committer.committed).containsExactly(1, 2);
            }

            assertThat(testHarness.getOutput()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testWaitForNotifyCheckpointCompleted(boolean commitOnInput) throws Exception {
        final MockCommitter committer = new MockCommitter();
        try (OneInputStreamOperatorTestHarness<CommittableMessage<Integer>, Void> testHarness =
                createTestHarness(committer, commitOnInput)) {
            testHarness.open();

            long cid = 1L;
            testHarness.processElement(
                    new StreamRecord<>(new CommittableSummary<>(1, 1, cid, 2, 0)));

            testHarness.processElement(new StreamRecord<>(new CommittableWithLineage<>(1, cid, 1)));

            assertThat(testHarness.getOutput()).isEmpty();
            // Not committed because incomplete
            assertThat(committer.committed).isEmpty();

            // immediately commit on receiving the second committable iff commitOnInput is true
            testHarness.processElement(new StreamRecord<>(new CommittableWithLineage<>(2, cid, 1)));
            if (commitOnInput) {
                assertThat(committer.committed).containsExactly(1, 2);
            } else {
                assertThat(committer.committed).isEmpty();
            }

            // for commitOnInput = false, the committer waits for notifyCheckpointComplete
            testHarness.notifyOfCompletedCheckpoint(cid);

            assertThat(committer.committed).containsExactly(1, 2);

            assertThat(testHarness.getOutput()).isEmpty();
        }
    }

    @Test
    void testStateRestore() throws Exception {
        final MockCommitter committer = new MockCommitter();
        try (OneInputStreamOperatorTestHarness<CommittableMessage<Integer>, Void> testHarness =
                createTestHarness(committer, false)) {
            testHarness.open();

            final CommittableSummary<Integer> committableSummary =
                    new CommittableSummary<>(1, 1, 0L, 1, 1);
            testHarness.processElement(new StreamRecord<>(committableSummary));
            final CommittableWithLineage<Integer> first = new CommittableWithLineage<>(1, 0L, 1);
            testHarness.processElement(new StreamRecord<>(first));

            final OperatorSubtaskState snapshot = testHarness.snapshot(0L, 2L);
            assertThat(testHarness.getOutput()).isEmpty();
            testHarness.close();
            assertThat(committer.committed).isEmpty();

            try (OneInputStreamOperatorTestHarness<CommittableMessage<Integer>, Void> restored =
                    createTestHarness(committer, true)) {

                restored.initializeState(snapshot);
                restored.open();

                assertThat(testHarness.getOutput()).isEmpty();
                assertThat(committer.committed).containsExactly(1);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitAllCommittablesOnFinalCheckpoint(boolean commitOnInput) throws Exception {
        final MockCommitter committer = new MockCommitter();
        final OneInputStreamOperatorTestHarness<CommittableMessage<Integer>, Void> testHarness =
                createTestHarness(committer, commitOnInput);
        testHarness.open();

        final CommittableSummary<Integer> committableSummary =
                new CommittableSummary<>(1, 2, EOI, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));
        final CommittableSummary<Integer> committableSummary2 =
                new CommittableSummary<>(2, 2, EOI, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary2));

        final CommittableWithLineage<Integer> first = new CommittableWithLineage<>(1, EOI, 1);
        testHarness.processElement(new StreamRecord<>(first));
        final CommittableWithLineage<Integer> second = new CommittableWithLineage<>(2, EOI, 2);
        testHarness.processElement(new StreamRecord<>(second));

        // commitOnInput implies that the global committer is not using notifyCheckpointComplete
        if (commitOnInput) {
            assertThat(committer.committed).containsExactly(1, 2);
        } else {
            assertThat(committer.committed).isEmpty();
            testHarness.notifyOfCompletedCheckpoint(EOI);
            assertThat(committer.committed).containsExactly(1, 2);
        }

        assertThat(testHarness.getOutput()).isEmpty();
    }

    private OneInputStreamOperatorTestHarness<CommittableMessage<Integer>, Void> createTestHarness(
            Committer<Integer> committer, boolean commitOnInput) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new GlobalCommitterOperator<>(
                        () -> committer, IntegerSerializer::new, commitOnInput));
    }

    private static class MockCommitter implements Committer<Integer> {

        final List<Integer> committed = new ArrayList<>();

        @Override
        public void close() throws Exception {}

        @Override
        public void commit(Collection<CommitRequest<Integer>> committables)
                throws IOException, InterruptedException {
            committables.forEach(c -> committed.add(c.getCommittable()));
        }
    }
}
