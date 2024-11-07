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

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.MetricsGroupTestUtils;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.IntegerSerializer;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class CommittableCollectorSerializerTest {

    private static final SimpleVersionedSerializer<Integer> COMMITTABLE_SERIALIZER =
            new IntegerSerializer();
    private static final int SUBTASK_ID = 1;
    private static final int NUMBER_OF_SUBTASKS = 1;
    private static final SinkCommitterMetricGroup METRIC_GROUP =
            MetricsGroupTestUtils.mockCommitterMetricGroup();
    private static final CommittableCollectorSerializer<Integer> SERIALIZER =
            new CommittableCollectorSerializer<>(
                    COMMITTABLE_SERIALIZER, SUBTASK_ID, NUMBER_OF_SUBTASKS, METRIC_GROUP);

    @Test
    void testCommittableCollectorV1SerDe() throws IOException {
        final List<Integer> legacyState = Arrays.asList(1, 2, 3);
        final DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(SinkV1CommittableDeserializer.MAGIC_NUMBER);
        SimpleVersionedSerialization.writeVersionAndSerializeList(
                COMMITTABLE_SERIALIZER, legacyState, out);
        final byte[] serialized = out.getCopyOfBuffer();
        final CommittableCollector<Integer> committableCollector =
                SERIALIZER.deserialize(1, serialized);

        assertThat(committableCollector.getCheckpointCommittables())
                .singleElement()
                .extracting(
                        checkpointCommittable ->
                                checkpointCommittable
                                        .getSubtaskCommittableManager(0)
                                        .getPendingRequests()
                                        .map(CommitRequestImpl::getCommittable),
                        InstanceOfAssertFactories.stream(Integer.class))
                .containsExactly(1, 2, 3);
    }

    @Test
    void testCommittableCollectorV2SerDe() throws IOException {
        int subtaskId = 2;
        int numberOfSubtasks = 3;

        final CommittableCollectorSerializer<Integer> ccSerializer =
                new CommittableCollectorSerializer<>(
                        COMMITTABLE_SERIALIZER, subtaskId, numberOfSubtasks, METRIC_GROUP);

        final CommittableCollector<Integer> committableCollector =
                new CommittableCollector<>(METRIC_GROUP);
        committableCollector.addMessage(
                new CommittableSummary<>(subtaskId, numberOfSubtasks, 1L, 1, 0));
        committableCollector.addMessage(
                new CommittableSummary<>(subtaskId, numberOfSubtasks, 2L, 1, 0));
        committableCollector.addMessage(new CommittableWithLineage<>(1, 1L, subtaskId));
        committableCollector.addMessage(new CommittableWithLineage<>(2, 2L, subtaskId));

        final CommittableCollector<Integer> copy =
                ccSerializer.deserialize(2, SERIALIZER.serialize(committableCollector));

        // assert original CommittableCollector
        assertCommittableCollector(
                "Original CommittableCollector",
                subtaskId,
                numberOfSubtasks,
                committableCollector,
                Arrays.asList(Collections.singletonList(1), Collections.singletonList(2)));

        // assert deserialized CommittableCollector
        assertCommittableCollector(
                "Deserialized CommittableCollector",
                subtaskId,
                numberOfSubtasks,
                copy,
                Arrays.asList(Collections.singletonList(1), Collections.singletonList(2)));
    }

    @Test
    void testCommittablesForSameSubtaskIdV2SerDe() throws IOException {

        int subtaskId = 1;
        int numberOfSubtasks = 3;

        final CommittableCollectorSerializer<Integer> ccSerializer =
                new CommittableCollectorSerializer<>(
                        COMMITTABLE_SERIALIZER, subtaskId, numberOfSubtasks, METRIC_GROUP);

        final CommittableCollector<Integer> committableCollector =
                new CommittableCollector<>(METRIC_GROUP);
        committableCollector.addMessage(
                new CommittableSummary<>(subtaskId, numberOfSubtasks, 1L, 1, 1, 0));
        committableCollector.addMessage(
                new CommittableSummary<>(subtaskId + 1, numberOfSubtasks, 1L, 1, 1, 0));
        committableCollector.addMessage(new CommittableWithLineage<>(1, 1L, subtaskId));
        committableCollector.addMessage(new CommittableWithLineage<>(1, 1L, subtaskId + 1));

        final CommittableCollector<Integer> copy =
                ccSerializer.deserialize(2, SERIALIZER.serialize(committableCollector));

        // assert original CommittableCollector
        assertCommittableCollector(
                "Original CommittableCollector",
                subtaskId,
                numberOfSubtasks,
                committableCollector,
                Collections.singletonList(Collections.singletonList(1)));

        // assert deserialized CommittableCollector
        assertThat(copy).isEqualTo(committableCollector);
    }

    @Test
    void testAlignSubtaskCommittableManagerCheckpointWithCheckpointCommittableManagerCheckpointId()
            throws IOException {
        // Create CommittableCollector holding a higher checkpointId than
        // WriterInitContext#INITIAL_CHECKPOINT_ID
        long checkpointId = WriterInitContext.INITIAL_CHECKPOINT_ID + 1;
        final CommittableCollector<Integer> committableCollector =
                new CommittableCollector<>(METRIC_GROUP);
        committableCollector.addMessage(
                new CommittableSummary<>(SUBTASK_ID, NUMBER_OF_SUBTASKS, checkpointId, 1, 1, 0));
        committableCollector.addMessage(new CommittableWithLineage<>(1, checkpointId, SUBTASK_ID));

        final CommittableCollector<Integer> copy =
                SERIALIZER.deserialize(2, SERIALIZER.serialize(committableCollector));

        final Collection<CheckpointCommittableManagerImpl<Integer>> checkpointCommittables =
                copy.getCheckpointCommittables();
        assertThat(checkpointCommittables).hasSize(1);
        final CheckpointCommittableManagerImpl<Integer> committableManager =
                checkpointCommittables.iterator().next();
        assertThat(committableManager.getSubtaskCommittableManager(SUBTASK_ID).getCheckpointId())
                .isEqualTo(committableManager.getCheckpointId());
    }

    /**
     * @param assertMessageHeading prefix used for assertion fail message.
     * @param subtaskId subtaskId to get {@link SubtaskCommittableManager} from {@link
     *     CheckpointCommittableManagerImpl}
     * @param numberOfSubtasks expected number of subtasks for {@link CommittableSummary}
     * @param committableCollector collector to get {@link CheckpointCommittableManager}s from.
     * @param committablesPerSubtaskPerCheckpoint every of the list element represents expected
     *     number of pending request per {@link SubtaskCommittableManager}.
     */
    private void assertCommittableCollector(
            String assertMessageHeading,
            int subtaskId,
            int numberOfSubtasks,
            CommittableCollector<Integer> committableCollector,
            List<List<Integer>> committablesPerSubtaskPerCheckpoint) {

        assertThat(committableCollector.getCheckpointCommittables())
                .describedAs(assertMessageHeading)
                .zipSatisfy(
                        committablesPerSubtaskPerCheckpoint,
                        (checkpointCommittableManager, expectedPendingRequestCount) -> {
                            final SubtaskCommittableManager<Integer> subtaskCommittableManager =
                                    checkpointCommittableManager.getSubtaskCommittableManager(
                                            subtaskId);

                            assertThat(checkpointCommittableManager)
                                    .returns(
                                            numberOfSubtasks,
                                            CheckpointCommittableManagerImpl::getNumberOfSubtasks);

                            assertPendingRequests(
                                    subtaskCommittableManager, expectedPendingRequestCount);

                            assertThat(subtaskCommittableManager.getSubtaskId())
                                    .isEqualTo(subtaskId);
                        });
    }

    private void assertPendingRequests(
            SubtaskCommittableManager<Integer> subtaskCommittableManagerCheckpoint,
            List<Integer> expectedPendingRequestCount) {
        assertThat(
                        subtaskCommittableManagerCheckpoint
                                .getPendingRequests()
                                .map(CommitRequestImpl::getCommittable)
                                .collect(Collectors.toList()))
                .containsExactlyElementsOf(expectedPendingRequestCount);
    }
}
