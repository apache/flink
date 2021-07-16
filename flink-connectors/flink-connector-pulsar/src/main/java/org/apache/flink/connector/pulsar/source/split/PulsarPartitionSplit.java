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
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.util.Preconditions;

import org.apache.pulsar.client.api.MessageId;

import java.util.Objects;

/** A {@link SourceSplit} implementation for a Pulsar's partition. */
@Internal
public class PulsarPartitionSplit implements SourceSplit {

    private final TopicPartition partition;

    private final StartCursor startCursor;

    private final StopCursor stopCursor;

    private MessageId latestConsumedId;

    public PulsarPartitionSplit(
            TopicPartition partition, StartCursor startCursor, StopCursor stopCursor) {
        this.partition = partition;
        this.startCursor = Preconditions.checkNotNull(startCursor);
        this.stopCursor = Preconditions.checkNotNull(stopCursor);
    }

    public PulsarPartitionSplit(
            TopicPartition partition,
            StartCursor startCursor,
            StopCursor stopCursor,
            MessageId latestConsumedId) {
        this.partition = partition;
        this.startCursor = Preconditions.checkNotNull(startCursor);
        this.stopCursor = Preconditions.checkNotNull(stopCursor);
        this.latestConsumedId = latestConsumedId;
    }

    @Override
    public String splitId() {
        // Since the partitions for a specified topic could be changed, we use topic name &
        // consuming range as the split id.
        return partition.getTopic() + "|" + partition.getRange();
    }

    public TopicPartition getPartition() {
        return partition;
    }

    public StartCursor getStartCursor() {
        return startCursor;
    }

    public StopCursor getStopCursor() {
        return stopCursor;
    }

    public MessageId getLatestConsumedId() {
        return latestConsumedId;
    }

    /**
     * Update the latest consumed message. Since the acknowledge action is a async commit, this
     * message id may not be acknowledged by pulsar's manage leader.
     */
    public void setLatestConsumedId(MessageId latestConsumedId) {
        this.latestConsumedId = latestConsumedId;
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
}
