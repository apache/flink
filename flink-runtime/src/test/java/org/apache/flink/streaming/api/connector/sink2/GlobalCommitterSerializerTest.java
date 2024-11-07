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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.MetricsGroupTestUtils;
import org.apache.flink.streaming.runtime.operators.sink.committables.CheckpointCommittableManager;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollector;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollectorSerializer;
import org.apache.flink.streaming.runtime.operators.sink.committables.SinkV1CommittableDeserializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalCommitterSerializerTest {

    private static final int SUBTASK_ID = 0;
    private static final int NUMBER_OF_SUBTASKS = 1;
    private static final SinkCommitterMetricGroup METRIC_GROUP =
            MetricsGroupTestUtils.mockCommitterMetricGroup();
    private static final CommittableCollectorSerializer<Integer> COMMITTABLE_COLLECTOR_SERIALIZER =
            new CommittableCollectorSerializer<>(
                    new IntegerSerializer(), SUBTASK_ID, NUMBER_OF_SUBTASKS, METRIC_GROUP);
    private static final GlobalCommitterSerializer<Integer, String> SERIALIZER =
            new GlobalCommitterSerializer<>(
                    COMMITTABLE_COLLECTOR_SERIALIZER, new StringSerializer(), METRIC_GROUP);

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSerDe(boolean withSinkV1State) throws IOException {
        final GlobalCommitterSerializer<Integer, String> serializer =
                new GlobalCommitterSerializer<>(
                        COMMITTABLE_COLLECTOR_SERIALIZER,
                        withSinkV1State ? new StringSerializer() : null,
                        METRIC_GROUP);
        final CommittableCollector<Integer> collector = new CommittableCollector<>(METRIC_GROUP);
        collector.addMessage(new CommittableSummary<>(2, 3, 1L, 1, 1, 0));
        collector.addMessage(new CommittableWithLineage<>(1, 1L, 2));
        final List<String> v1State =
                withSinkV1State ? Arrays.asList("first", "second") : Collections.emptyList();
        final GlobalCommittableWrapper<Integer, String> wrapper =
                new GlobalCommittableWrapper<>(collector, v1State);
        final GlobalCommittableWrapper<Integer, String> copy =
                serializer.deserialize(2, serializer.serialize(wrapper));
        assertThat(copy.getGlobalCommittables()).containsExactlyInAnyOrderElementsOf(v1State);
        assertThat(collector.getCheckpointCommittablesUpTo(2))
                .singleElement()
                .returns(1L, CheckpointCommittableManager::getCheckpointId)
                .returns(3, CheckpointCommittableManager::getNumberOfSubtasks);
    }

    @Test
    void testDeserializationV1() throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(256);
        final SimpleVersionedSerializer<String> stringSerializer = new StringSerializer();
        out.writeInt(SinkV1CommittableDeserializer.MAGIC_NUMBER);
        out.writeInt(1);
        out.writeInt(2);
        String state1 = "legacy1";
        out.writeInt(stringSerializer.serialize(state1).length);
        out.writeUTF(state1);
        String state2 = "legacy2";
        out.writeInt(stringSerializer.serialize(state2).length);
        out.writeUTF(state2);
        final byte[] serialized = out.getCopyOfBuffer();
        final GlobalCommittableWrapper<Integer, String> wrapper =
                SERIALIZER.deserialize(1, serialized);

        assertThat(wrapper.getGlobalCommittables()).containsExactlyInAnyOrder(state1, state2);
        final CommittableCollector<Integer> collector = wrapper.getCommittableCollector();
        assertThat(collector.getCheckpointCommittablesUpTo(Long.MAX_VALUE)).isEmpty();
    }

    private static class StringSerializer implements SimpleVersionedSerializer<String> {

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(String obj) throws IOException {
            final DataOutputSerializer out = new DataOutputSerializer(256);
            out.writeUTF(obj);
            return out.getCopyOfBuffer();
        }

        @Override
        public String deserialize(int version, byte[] serialized) throws IOException {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            return in.readUTF();
        }
    }
}
