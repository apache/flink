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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.reader.source.PulsarOrderedSourceReader;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link SourceSplit} implementation for a Pulsar's partition. */
@Internal
public class PulsarPartitionSplit implements SourceSplit, Serializable {
    private static final long serialVersionUID = -6857317360756062625L;

    private final TopicPartition partition;

    private final StopCursor stopCursor;

    /**
     * Since this field in only used in {@link PulsarOrderedSourceReader#snapshotState(long)}, it's
     * no need to serialize this field into flink checkpoint state.
     */
    @Nullable private final MessageId latestConsumedId;

    @Nullable private final TxnID uncommittedTransactionId;

    public PulsarPartitionSplit(TopicPartition partition, StopCursor stopCursor) {
        this.partition = checkNotNull(partition);
        this.stopCursor = checkNotNull(stopCursor);
        this.latestConsumedId = null;
        this.uncommittedTransactionId = null;
    }

    public PulsarPartitionSplit(
            TopicPartition partition,
            StopCursor stopCursor,
            MessageId latestConsumedId,
            TxnID uncommittedTransactionId) {
        this.partition = checkNotNull(partition);
        this.stopCursor = checkNotNull(stopCursor);
        this.latestConsumedId = latestConsumedId;
        this.uncommittedTransactionId = uncommittedTransactionId;
    }

    @Override
    public String splitId() {
        return partition.toString();
    }

    public TopicPartition getPartition() {
        return partition;
    }

    public StopCursor getStopCursor() {
        return stopCursor;
    }

    @Nullable
    public MessageId getLatestConsumedId() {
        return latestConsumedId;
    }

    @Nullable
    public TxnID getUncommittedTransactionId() {
        return uncommittedTransactionId;
    }

    /** Open stop cursor. */
    public void open(PulsarAdmin admin) {
        stopCursor.open(admin, partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PulsarPartitionSplit that = (PulsarPartitionSplit) o;
        return partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition);
    }

    @Override
    public String toString() {
        return "PulsarPartitionSplit{partition=" + partition + '}';
    }
}
