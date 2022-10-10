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

package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CheckpointCommittableManagerImplTest {

    @Test
    void testAddSummary() {
        final CheckpointCommittableManagerImpl<Integer> checkpointCommittables =
                new CheckpointCommittableManagerImpl<>(2, 1, 1L);
        assertThat(checkpointCommittables.getSubtaskCommittableManagers()).isEmpty();

        final CommittableSummary<Integer> first = new CommittableSummary<>(1, 1, 1L, 1, 0, 0);
        checkpointCommittables.upsertSummary(first);
        assertThat(checkpointCommittables.getSubtaskCommittableManagers())
                .hasSize(1)
                .satisfiesExactly(
                        (s) -> {
                            assertThat(s.getSubtaskId()).isEqualTo(2);
                            assertThat(s.getCheckpointId()).isEqualTo(1L);
                            assertThat(s.getNumPending()).isEqualTo(1);
                            assertThat(s.getNumFailed()).isEqualTo(0);
                        });

        // Add different subtask id
        final CommittableSummary<Integer> third = new CommittableSummary<>(2, 1, 2L, 2, 1, 1);
        checkpointCommittables.upsertSummary(third);
        assertThat(checkpointCommittables.getSubtaskCommittableManagers()).hasSize(2);
    }

    @Test
    void testCommit() throws IOException, InterruptedException {
        final CheckpointCommittableManagerImpl<Integer> checkpointCommittables =
                new CheckpointCommittableManagerImpl<>(1, 1, 1L);
        checkpointCommittables.upsertSummary(new CommittableSummary<>(1, 1, 1L, 1, 0, 0));
        checkpointCommittables.upsertSummary(new CommittableSummary<>(2, 1, 1L, 2, 0, 0));
        checkpointCommittables.addCommittable(new CommittableWithLineage<>(3, 1L, 1));
        checkpointCommittables.addCommittable(new CommittableWithLineage<>(4, 1L, 2));

        final Committer<Integer> committer = new NoOpCommitter();
        // Only commit fully received committables
        Collection<CommittableWithLineage<Integer>> commitRequests =
                checkpointCommittables.commit(true, committer);
        assertThat(commitRequests)
                .hasSize(1)
                .satisfiesExactly(c -> assertThat(c.getCommittable()).isEqualTo(3));

        // Commit all committables
        commitRequests = checkpointCommittables.commit(false, committer);
        assertThat(commitRequests)
                .hasSize(1)
                .satisfiesExactly(c -> assertThat(c.getCommittable()).isEqualTo(4));
    }

    @Test
    void testUpdateCommittableSummary() {
        final CheckpointCommittableManagerImpl<Integer> checkpointCommittables =
                new CheckpointCommittableManagerImpl<>(1, 1, 1L);
        checkpointCommittables.upsertSummary(new CommittableSummary<>(1, 1, 1L, 1, 0, 0));
        assertThatThrownBy(
                        () ->
                                checkpointCommittables.upsertSummary(
                                        new CommittableSummary<>(1, 1, 1L, 2, 0, 0)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("FLINK-25920");
    }

    // check different values of subtaskId and numberOfSubtasks to make sure that no value is
    // hardcoded.
    @ParameterizedTest(name = "subtaskId = {0}, numberOfSubtasks = {1}, checkpointId = {2}")
    @CsvSource({"1, 10, 100", "2, 20, 200", "3, 30, 300"})
    public void testCopy(int subtaskId, int numberOfSubtasks, long checkpointId) {

        final CheckpointCommittableManagerImpl<Integer> original =
                new CheckpointCommittableManagerImpl<>(subtaskId, numberOfSubtasks, checkpointId);
        original.upsertSummary(
                new CommittableSummary<>(subtaskId, numberOfSubtasks, checkpointId, 1, 0, 0));

        CheckpointCommittableManagerImpl<Integer> copy = original.copy();

        assertThat(copy.getCheckpointId()).isEqualTo(checkpointId);
        SinkV2Assertions.assertThat(copy.getSummary())
                .hasNumberOfSubtasks(numberOfSubtasks)
                .hasSubtaskId(subtaskId)
                .hasCheckpointId(checkpointId);
    }

    private static class NoOpCommitter implements Committer<Integer> {

        @Override
        public void commit(Collection<CommitRequest<Integer>> committables)
                throws IOException, InterruptedException {}

        @Override
        public void close() throws Exception {}
    }
}
