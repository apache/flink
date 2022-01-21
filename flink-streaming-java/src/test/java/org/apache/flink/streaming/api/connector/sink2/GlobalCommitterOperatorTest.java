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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalCommitterOperatorTest {

    @Test
    void testWaitForCommittablesOfLatestCheckpointBeforeCommitting() throws Exception {
        final MockCommitter committer = new MockCommitter();
        final OneInputStreamOperatorTestHarness<CommittableMessage<Integer>, Void> testHarness =
                createTestHarness(committer);
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(new CommittableSummary<>(1, 1, 1L, 2, 0, 0)));

        testHarness.processElement(new StreamRecord<>(new CommittableWithLineage<>(1, 1L, 1)));

        testHarness.notifyOfCompletedCheckpoint(1);

        assertThat(testHarness.getOutput()).isEmpty();
        // Not committed because incomplete
        assertThat(committer.committed).isEmpty();

        testHarness.processElement(new StreamRecord<>(new CommittableWithLineage<>(2, 1L, 1)));

        testHarness.notifyOfCompletedCheckpoint(2);

        assertThat(testHarness.getOutput()).isEmpty();
        assertThat(committer.committed).containsExactly(1, 2);
        testHarness.close();
    }

    @Test
    void testStateRestore() throws Exception {
        final MockCommitter committer = new MockCommitter();
        final OneInputStreamOperatorTestHarness<CommittableMessage<Integer>, Void> testHarness =
                createTestHarness(committer);
        testHarness.open();

        final CommittableSummary<Integer> committableSummary =
                new CommittableSummary<>(1, 1, 0L, 1, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));
        final CommittableWithLineage<Integer> first = new CommittableWithLineage<>(1, 0L, 1);
        testHarness.processElement(new StreamRecord<>(first));

        final OperatorSubtaskState snapshot = testHarness.snapshot(0L, 2L);
        assertThat(testHarness.getOutput()).isEmpty();
        testHarness.close();
        assertThat(committer.committed).isEmpty();

        final OneInputStreamOperatorTestHarness<CommittableMessage<Integer>, Void> restored =
                createTestHarness(committer);

        restored.initializeState(snapshot);
        restored.open();

        assertThat(testHarness.getOutput()).isEmpty();
        assertThat(committer.committed).containsExactly(1);
        restored.close();
    }

    @Test
    void testCommitAllCommittablesOnEndOfInput() throws Exception {
        final MockCommitter committer = new MockCommitter();
        final OneInputStreamOperatorTestHarness<CommittableMessage<Integer>, Void> testHarness =
                createTestHarness(committer);
        testHarness.open();

        final CommittableSummary<Integer> committableSummary =
                new CommittableSummary<>(1, 2, null, 1, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary));
        final CommittableSummary<Integer> committableSummary2 =
                new CommittableSummary<>(2, 2, null, 1, 1, 0);
        testHarness.processElement(new StreamRecord<>(committableSummary2));

        final CommittableWithLineage<Integer> first = new CommittableWithLineage<>(1, null, 1);
        testHarness.processElement(new StreamRecord<>(first));
        final CommittableWithLineage<Integer> second = new CommittableWithLineage<>(2, null, 2);
        testHarness.processElement(new StreamRecord<>(second));

        testHarness.endInput();

        assertThat(testHarness.getOutput()).isEmpty();
        assertThat(committer.committed).containsExactly(1, 2);
    }

    private OneInputStreamOperatorTestHarness<CommittableMessage<Integer>, Void> createTestHarness(
            Committer<Integer> committer) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new GlobalCommitterOperator<>(() -> committer, IntegerSerializer::new));
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
