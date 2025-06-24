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
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.MetricsGroupTestUtils;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CheckpointCommittableManagerImplTest {

    private static final SinkCommitterMetricGroup METRIC_GROUP =
            MetricsGroupTestUtils.mockCommitterMetricGroup();
    private static final int MAX_RETRIES = 1;

    @Test
    void testAddSummary() {
        final CheckpointCommittableManagerImpl<Integer> checkpointCommittables =
                new CheckpointCommittableManagerImpl<>(new HashMap<>(), 1, 1L, METRIC_GROUP);
        assertThat(checkpointCommittables.getSubtaskCommittableManagers()).isEmpty();

        final CommittableSummary<Integer> first = new CommittableSummary<>(1, 1, 1L, 1, 0);
        checkpointCommittables.addSummary(first);
        assertThat(checkpointCommittables.getSubtaskCommittableManagers())
                .singleElement()
                .returns(1, SubtaskCommittableManager::getSubtaskId)
                .returns(1L, SubtaskCommittableManager::getCheckpointId);

        // Add different subtask id
        final CommittableSummary<Integer> third = new CommittableSummary<>(2, 1, 2L, 2, 1);
        checkpointCommittables.addSummary(third);
        assertThat(checkpointCommittables.getSubtaskCommittableManagers()).hasSize(2);
    }

    @Test
    void testCommit() throws IOException, InterruptedException {
        final CheckpointCommittableManagerImpl<Integer> checkpointCommittables =
                new CheckpointCommittableManagerImpl<>(new HashMap<>(), 1, 1L, METRIC_GROUP);
        checkpointCommittables.addSummary(new CommittableSummary<>(1, 1, 1L, 1, 0));
        checkpointCommittables.addSummary(new CommittableSummary<>(2, 1, 1L, 2, 0));
        checkpointCommittables.addCommittable(new CommittableWithLineage<>(3, 1L, 1));
        checkpointCommittables.addCommittable(new CommittableWithLineage<>(4, 1L, 2));

        final Committer<Integer> committer = new NoOpCommitter();
        // Only commit fully received committables
        assertThatCode(() -> checkpointCommittables.commit(committer, MAX_RETRIES))
                .hasMessageContaining("Trying to commit incomplete batch of committables");

        // Even on retry
        assertThatCode(() -> checkpointCommittables.commit(committer, MAX_RETRIES))
                .hasMessageContaining("Trying to commit incomplete batch of committables");

        // Add missing committable
        checkpointCommittables.addCommittable(new CommittableWithLineage<>(5, 1L, 2));
        // Commit all committables
        assertThatCode(() -> checkpointCommittables.commit(committer, MAX_RETRIES))
                .doesNotThrowAnyException();
        assertThat(checkpointCommittables.getSuccessfulCommittables())
                .hasSize(3)
                .containsExactlyInAnyOrder(3, 4, 5);
    }

    @Test
    void testUpdateCommittableSummary() {
        final CheckpointCommittableManagerImpl<Integer> checkpointCommittables =
                new CheckpointCommittableManagerImpl<>(new HashMap<>(), 1, 1L, METRIC_GROUP);
        checkpointCommittables.addSummary(new CommittableSummary<>(1, 1, 1L, 1, 0));
        assertThatThrownBy(
                        () ->
                                checkpointCommittables.addSummary(
                                        new CommittableSummary<>(1, 1, 1L, 2, 0)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("FLINK-25920");
    }

    // check different values of subtaskId and numberOfSubtasks to make sure that no value is
    // hardcoded.
    @ParameterizedTest(name = "subtaskId = {0}, numberOfSubtasks = {1}, checkpointId = {2}")
    @CsvSource({"1, 10, 100", "2, 20, 200", "3, 30, 300"})
    public void testCopy(int subtaskId, int numberOfSubtasks, long checkpointId) {

        final CheckpointCommittableManagerImpl<Integer> original =
                new CheckpointCommittableManagerImpl<>(
                        new HashMap<>(), numberOfSubtasks, checkpointId, METRIC_GROUP);
        original.addSummary(
                new CommittableSummary<>(subtaskId, numberOfSubtasks, checkpointId, 1, 0));

        CheckpointCommittableManagerImpl<Integer> copy = original.copy();

        assertThat(copy.getCheckpointId()).isEqualTo(checkpointId);
        assertThat(copy)
                .returns(numberOfSubtasks, CheckpointCommittableManagerImpl::getNumberOfSubtasks)
                .returns(checkpointId, CheckpointCommittableManagerImpl::getCheckpointId);
    }

    private static class NoOpCommitter implements Committer<Integer> {

        @Override
        public void commit(Collection<CommitRequest<Integer>> committables)
                throws IOException, InterruptedException {}

        @Override
        public void close() throws Exception {}
    }
}
