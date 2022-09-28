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

package org.apache.flink.connector.pulsar.source.enumerator;

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.function.FunctionWithException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

import static org.apache.flink.connector.pulsar.common.utils.PulsarSerdeUtils.deserializeMap;
import static org.apache.flink.connector.pulsar.common.utils.PulsarSerdeUtils.deserializeSet;
import static org.apache.flink.connector.pulsar.common.utils.PulsarSerdeUtils.serializeSet;

/** The {@link SimpleVersionedSerializer Serializer} for the enumerator state of Pulsar source. */
public class PulsarSourceEnumStateSerializer
        implements SimpleVersionedSerializer<PulsarSourceEnumState> {

    // This version should be bumped after modifying the PulsarSourceEnumState.
    public static final int CURRENT_VERSION = 2;

    public static final PulsarSourceEnumStateSerializer INSTANCE =
            new PulsarSourceEnumStateSerializer();

    private static final PulsarPartitionSplitSerializer SPLIT_SERIALIZER =
            PulsarPartitionSplitSerializer.INSTANCE;

    private PulsarSourceEnumStateSerializer() {
        // Singleton instance.
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PulsarSourceEnumState obj) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            serializeSet(
                    out, obj.getAppendedPartitions(), SPLIT_SERIALIZER::serializeTopicPartition);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PulsarSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        // VERSION 2 deserialization, support VERSION 0 and 1 deserialization in the meantime.
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            Set<TopicPartition> partitions = null;
            if (version == 2) {
                partitions = deserializeSet(in, deserializePartition(1));
            } else {
                partitions = deserializeSet(in, deserializePartition(0));
            }

            // Only deserialize these fields for backward compatibility.
            if (version == 0) {
                deserializeSet(in, deserializeSplit(0));
                deserializeMap(in, DataInput::readInt, i -> deserializeSet(i, deserializeSplit(0)));
                deserializeMap(in, DataInput::readInt, i -> deserializeSet(i, DataInput::readUTF));
                in.readBoolean();
            }

            return new PulsarSourceEnumState(partitions);
        }
    }

    // ----------------- private methods -------------------

    private FunctionWithException<DataInputStream, TopicPartition, IOException>
            deserializePartition(int version) {
        return in -> SPLIT_SERIALIZER.deserializeTopicPartition(version, in);
    }

    private FunctionWithException<DataInputStream, PulsarPartitionSplit, IOException>
            deserializeSplit(int version) {
        return in -> SPLIT_SERIALIZER.deserializePulsarPartitionSplit(version, in);
    }
}
