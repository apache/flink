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

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;

import javax.annotation.Nullable;

/** Pulsar partition split state. */
public class PulsarPartitionSplitState {

    private final PulsarPartitionSplit split;

    @Nullable private TxnID uncommittedTransactionId;

    @Nullable private MessageId latestConsumedId;

    public PulsarPartitionSplitState(PulsarPartitionSplit split) {
        this.split = split;
    }

    /**
     * Create a partition split which contains the latest consumed message id as the start position.
     */
    public PulsarPartitionSplit toPulsarPartitionSplit() {
        return new PulsarPartitionSplit(
                split.getPartition(),
                split.getStopCursor(),
                latestConsumedId,
                uncommittedTransactionId);
    }

    public TopicPartition getPartition() {
        return split.getPartition();
    }

    @Nullable
    public TxnID getUncommittedTransactionId() {
        return uncommittedTransactionId;
    }

    public void setUncommittedTransactionId(@Nullable TxnID uncommittedTransactionId) {
        this.uncommittedTransactionId = uncommittedTransactionId;
    }

    @Nullable
    public MessageId getLatestConsumedId() {
        return latestConsumedId;
    }

    public void setLatestConsumedId(@Nullable MessageId latestConsumedId) {
        this.latestConsumedId = latestConsumedId;
    }
}
