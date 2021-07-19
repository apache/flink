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
import org.apache.flink.connector.pulsar.source.utils.PulsarSerdeUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

/** The {@link SimpleVersionedSerializer Serializer} for the enumerator state of Pulsar source. */
public class PulsarSourceEnumStateSerializer
        implements SimpleVersionedSerializer<PulsarSourceEnumState> {

    private static final PulsarPartitionSplitSerializer SPLIT_SERIALIZER =
            new PulsarPartitionSplitSerializer();

    @Override
    public int getVersion() {
        // We use PulsarPartitionSplitSerializer's version because we use reuse this class.
        return PulsarPartitionSplitSerializer.CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PulsarSourceEnumState obj) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            PulsarSerdeUtils.serializeSet(
                    out, obj.getAppendedPartitions(), SPLIT_SERIALIZER::serializeTopicPartition);
            PulsarSerdeUtils.serializeSet(
                    out, obj.getPendingPartitionSplits(), SPLIT_SERIALIZER::serialize);
            out.writeBoolean(obj.isInitialized());

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PulsarSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        // VERSION 0 deserialization
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            Set<TopicPartition> partitions =
                    PulsarSerdeUtils.deserializeSet(
                            in,
                            bytes -> SPLIT_SERIALIZER.deserializeTopicPartition(version, bytes));
            Set<PulsarPartitionSplit> splits =
                    PulsarSerdeUtils.deserializeSet(
                            in, bytes -> SPLIT_SERIALIZER.deserialize(version, bytes));
            boolean initialized = in.readBoolean();

            return new PulsarSourceEnumState(partitions, splits, initialized);
        }
    }
}
