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

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.IntegerSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class CommittableCollectorSerializerTest {

    private static final SimpleVersionedSerializer<Integer> COMMITTABLE_SERIALIZER =
            new IntegerSerializer();

    private static final CommittableCollectorSerializer<Integer> SERIALIZER =
            new CommittableCollectorSerializer<>(COMMITTABLE_SERIALIZER, 1, 1);

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
        assertThat(committableCollector.getNumberOfSubtasks()).isEqualTo(1);
        assertThat(committableCollector.isFinished()).isFalse();
        assertThat(committableCollector.getSubtaskId()).isEqualTo(0);
        final Collection<CheckpointCommittableManagerImpl<Integer>> checkpointCommittables =
                committableCollector.getCheckpointCommittables();
        assertThat(checkpointCommittables).hasSize(1);
        final SubtaskCommittableManager<Integer> subtaskCommittableManager =
                checkpointCommittables.iterator().next().getSubtaskCommittableManager(0);
        assertThat(
                        subtaskCommittableManager
                                .getPendingRequests()
                                .map(CommitRequestImpl::getCommittable)
                                .collect(Collectors.toList()))
                .containsExactly(1, 2, 3);
    }

    @Test
    void testCommittableCollectorV2SerDe() throws IOException {
        final CommittableCollector<Integer> committableCollector = new CommittableCollector<>(2, 3);
        committableCollector.addMessage(new CommittableSummary<>(2, 3, 1L, 1, 1, 0));
        committableCollector.addMessage(new CommittableSummary<>(2, 3, 2L, 1, 1, 0));
        committableCollector.addMessage(new CommittableWithLineage<>(1, 1L, 2));
        committableCollector.addMessage(new CommittableWithLineage<>(2, 2L, 2));
        final CommittableCollector<Integer> copy =
                SERIALIZER.deserialize(2, SERIALIZER.serialize(committableCollector));

        // Expect the subtask Id equal to the origin of the collector
        assertThat(copy.getSubtaskId()).isEqualTo(1);
        assertThat(copy.isFinished()).isFalse();
        assertThat(copy.getNumberOfSubtasks()).isEqualTo(1);
        final Collection<CheckpointCommittableManagerImpl<Integer>> checkpointCommittables =
                committableCollector.getCheckpointCommittables();
        assertThat(checkpointCommittables).hasSize(2);
        final Iterator<CheckpointCommittableManagerImpl<Integer>> committablesIterator =
                checkpointCommittables.iterator();
        final SubtaskCommittableManager<Integer> subtaskCommittableManagerCheckpoint1 =
                committablesIterator.next().getSubtaskCommittableManager(2);
        assertThat(
                        subtaskCommittableManagerCheckpoint1
                                .getPendingRequests()
                                .map(CommitRequestImpl::getCommittable)
                                .collect(Collectors.toList()))
                .containsExactly(1);
        final SubtaskCommittableManager<Integer> subtaskCommittableManagerCheckpoint2 =
                committablesIterator.next().getSubtaskCommittableManager(2);
        assertThat(
                        subtaskCommittableManagerCheckpoint2
                                .getPendingRequests()
                                .map(CommitRequestImpl::getCommittable)
                                .collect(Collectors.toList()))
                .containsExactly(2);
    }
}
