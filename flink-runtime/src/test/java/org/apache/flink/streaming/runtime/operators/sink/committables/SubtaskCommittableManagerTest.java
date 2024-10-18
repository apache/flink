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

import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.MetricsGroupTestUtils;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions.committableWithLineage;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;

class SubtaskCommittableManagerTest {
    private static final SinkCommitterMetricGroup METRIC_GROUP =
            MetricsGroupTestUtils.mockCommitterMetricGroup();

    @Test
    void testDrainCommittables() {
        final SubtaskCommittableManager<Integer> subtaskCommittableManager =
                new SubtaskCommittableManager<>(3, 1, 1L, METRIC_GROUP);
        final CommittableWithLineage<Integer> first = new CommittableWithLineage<>(1, 1L, 1);
        final CommittableWithLineage<Integer> second = new CommittableWithLineage<>(2, 1L, 1);
        final CommittableWithLineage<Integer> third = new CommittableWithLineage<>(3, 1L, 1);

        assertThat(subtaskCommittableManager.getPendingRequests()).hasSize(0);

        // Add committables to subtask committables
        subtaskCommittableManager.add(first);
        subtaskCommittableManager.add(second);
        subtaskCommittableManager.add(third);
        assertThat(subtaskCommittableManager.getPendingRequests()).hasSize(3);
        assertThat(subtaskCommittableManager.getNumCommittables()).isEqualTo(3);
        assertThat(subtaskCommittableManager.getNumDrained()).isEqualTo(0);
        assertThat(subtaskCommittableManager.isFinished()).isFalse();

        // Trigger commit
        final Iterator<CommitRequestImpl<Integer>> requests =
                subtaskCommittableManager.getRequests().iterator();
        IntStream.range(0, 2).forEach(i -> requests.next().setCommittedIfNoError());
        assertThat(subtaskCommittableManager.getPendingRequests()).hasSize(1);
        assertThat(subtaskCommittableManager.getNumCommittables()).isEqualTo(3);
        assertThat(subtaskCommittableManager.getNumDrained()).isEqualTo(0);

        // Drain committed committables
        ListAssert<CommittableWithLineage<Integer>> committables =
                assertThat(subtaskCommittableManager.drainCommitted()).hasSize(2);
        committables
                .element(0, as(committableWithLineage()))
                .hasSubtaskId(1)
                .hasCommittable(1)
                .hasCheckpointId(1);
        committables
                .element(1, as(committableWithLineage()))
                .hasSubtaskId(1)
                .hasCommittable(2)
                .hasCheckpointId(1);
        assertThat(subtaskCommittableManager.getNumFailed()).isEqualTo(0);

        // Drain again should not yield anything
        assertThat(subtaskCommittableManager.drainCommitted()).hasSize(0);

        // Fail commit
        requests.next().signalFailedWithKnownReason(new RuntimeException("Unused exception"));
        assertThat(subtaskCommittableManager.getNumFailed()).isEqualTo(0);
        assertThat(subtaskCommittableManager.getPendingRequests()).hasSize(0);
        assertThat(subtaskCommittableManager.getNumCommittables()).isEqualTo(3);
        assertThat(subtaskCommittableManager.isFinished()).isFalse();

        // Drain to update fail count
        assertThat(subtaskCommittableManager.drainCommitted()).hasSize(0);
        assertThat(subtaskCommittableManager.getNumFailed()).isEqualTo(1);
        assertThat(subtaskCommittableManager.isFinished()).isTrue();
    }

    @Test
    void testMerge() {
        final SubtaskCommittableManager<Integer> subtaskCommittableManager =
                new SubtaskCommittableManager<>(
                        Collections.singletonList(new CommitRequestImpl<>(1, METRIC_GROUP)),
                        5,
                        1,
                        2,
                        1,
                        2L,
                        METRIC_GROUP);
        SubtaskCommittableManager<Integer> merged =
                subtaskCommittableManager.merge(
                        new SubtaskCommittableManager<>(
                                Arrays.asList(
                                        new CommitRequestImpl<>(2, METRIC_GROUP),
                                        new CommitRequestImpl<>(3, METRIC_GROUP)),
                                10,
                                2,
                                3,
                                1,
                                2L,
                                METRIC_GROUP));
        assertThat(merged.getNumCommittables()).isEqualTo(11);
        assertThat(merged.getNumDrained()).isEqualTo(3);
        assertThat(merged.isFinished()).isFalse();
        assertThat(merged.getNumFailed()).isEqualTo(5);
        assertThat(merged.getPendingRequests()).hasSize(3);
    }
}
