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

import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.flink.connector.pulsar.common.utils.PulsarSerdeUtils.deserializeBytes;
import static org.apache.flink.connector.pulsar.common.utils.PulsarSerdeUtils.deserializeObject;
import static org.apache.flink.connector.pulsar.common.utils.PulsarSerdeUtils.serializeBytes;
import static org.apache.flink.connector.pulsar.common.utils.PulsarSerdeUtils.serializeObject;

/** The {@link SimpleVersionedSerializer serializer} for {@link PulsarPartitionSplit}. */
public class PulsarPartitionSplitSerializer
        implements SimpleVersionedSerializer<PulsarPartitionSplit> {

    public static final PulsarPartitionSplitSerializer INSTANCE =
            new PulsarPartitionSplitSerializer();

    // This version should be bumped after modifying the PulsarPartitionSplit.
    public static final int CURRENT_VERSION = 0;

    private PulsarPartitionSplitSerializer() {
        // Singleton instance.
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PulsarPartitionSplit obj) throws IOException {
        // VERSION 0 serialization
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            serializePulsarPartitionSplit(out, obj);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PulsarPartitionSplit deserialize(int version, byte[] serialized) throws IOException {
        // VERSION 0 deserialization
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            return deserializePulsarPartitionSplit(version, in);
        }
    }

    // ----------------- helper methods --------------

    public void serializePulsarPartitionSplit(DataOutputStream out, PulsarPartitionSplit split)
            throws IOException {
        // partition
        serializeTopicPartition(out, split.getPartition());

        // stopCursor
        serializeObject(out, split.getStopCursor());

        // latestConsumedId
        MessageId latestConsumedId = split.getLatestConsumedId();
        if (latestConsumedId == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            serializeBytes(out, latestConsumedId.toByteArray());
        }

        // uncommittedTransactionId
        TxnID uncommittedTransactionId = split.getUncommittedTransactionId();
        if (uncommittedTransactionId == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(uncommittedTransactionId.getMostSigBits());
            out.writeLong(uncommittedTransactionId.getLeastSigBits());
        }
    }

    public PulsarPartitionSplit deserializePulsarPartitionSplit(int version, DataInputStream in)
            throws IOException {
        // partition
        TopicPartition partition = deserializeTopicPartition(version, in);

        // stopCursor
        StopCursor stopCursor = deserializeObject(in);

        // latestConsumedId
        MessageId latestConsumedId = null;
        if (in.readBoolean()) {
            byte[] messageIdBytes = deserializeBytes(in);
            latestConsumedId = MessageId.fromByteArray(messageIdBytes);
        }

        // uncommittedTransactionId
        TxnID uncommittedTransactionId = null;
        if (in.readBoolean()) {
            long mostSigBits = in.readLong();
            long leastSigBits = in.readLong();
            uncommittedTransactionId = new TxnID(mostSigBits, leastSigBits);
        }

        // Creation
        return new PulsarPartitionSplit(
                partition, stopCursor, latestConsumedId, uncommittedTransactionId);
    }

    public void serializeTopicPartition(DataOutputStream out, TopicPartition partition)
            throws IOException {
        // VERSION 0 serialization
        TopicRange range = partition.getRange();
        out.writeUTF(partition.getTopic());
        out.writeInt(partition.getPartitionId());
        out.writeInt(range.getStart());
        out.writeInt(range.getEnd());
    }

    public TopicPartition deserializeTopicPartition(int version, DataInputStream in)
            throws IOException {
        // VERSION 0 deserialization
        String topic = in.readUTF();
        int partitionId = in.readInt();
        int start = in.readInt();
        int end = in.readInt();

        TopicRange range = new TopicRange(start, end);
        return new TopicPartition(topic, partitionId, range);
    }
}
