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

package org.apache.flink.connector.pulsar.source.split;

import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.utils.PulsarSerdeUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.pulsar.client.api.MessageId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** The {@link SimpleVersionedSerializer serializer} for {@link PulsarPartitionSplit}. */
public class PulsarPartitionSplitSerializer
        implements SimpleVersionedSerializer<PulsarPartitionSplit> {

    // This version should be bumped after modifying the PulsarPartitionSplit.
    public static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PulsarPartitionSplit obj) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            // partition
            byte[] partitionBytes = serializeTopicPartition(obj.getPartition());
            PulsarSerdeUtils.serializeObject(out, partitionBytes);

            // startCursor
            PulsarSerdeUtils.serializeObject(out, obj.getStartCursor());

            // stopCursor
            PulsarSerdeUtils.serializeObject(out, obj.getStopCursor());

            // latestConsumedId
            MessageId latestConsumedId = obj.getLatestConsumedId();
            if (latestConsumedId == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                byte[] bytes = latestConsumedId.toByteArray();
                PulsarSerdeUtils.serializeBytes(out, bytes);
            }

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PulsarPartitionSplit deserialize(int version, byte[] serialized) throws IOException {
        // VERSION 0 deserialization
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            // partition
            byte[] partitionBytes = PulsarSerdeUtils.deserializeBytes(in);
            TopicPartition partition = deserializeTopicPartition(version, partitionBytes);

            // startCursor
            StartCursor startCursor = PulsarSerdeUtils.deserializeObject(in);

            // stopCursor
            StopCursor stopCursor = PulsarSerdeUtils.deserializeObject(in);

            // latestConsumedId
            MessageId latestConsumedId = null;
            if (in.readBoolean()) {
                byte[] bytes = PulsarSerdeUtils.deserializeBytes(in);
                latestConsumedId = MessageId.fromByteArray(bytes);
            }

            // Creation
            return new PulsarPartitionSplit(partition, startCursor, stopCursor, latestConsumedId);
        }
    }

    public byte[] serializeTopicPartition(TopicPartition partition) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            TopicRange range = partition.getRange();
            out.writeUTF(partition.getTopic());
            out.writeInt(partition.getPartitionId());
            out.writeBoolean(partition.isPartitioned());
            out.writeInt(range.getStart());
            out.writeInt(range.getEnd());

            out.flush();
            return baos.toByteArray();
        }
    }

    public TopicPartition deserializeTopicPartition(int version, byte[] serialized)
            throws IOException {
        // VERSION 0 deserialization
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String topic = in.readUTF();
            int partitionId = in.readInt();
            boolean partitioned = in.readBoolean();
            int start = in.readInt();
            int end = in.readInt();

            TopicRange range = TopicRange.createTopicRange(start, end);
            return new TopicPartition(topic, partitionId, partitioned, range);
        }
    }
}
