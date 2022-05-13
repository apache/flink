/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;

/**
 * The state that the Flink Kafka Consumer holds for each Kafka partition. Includes the Kafka
 * descriptor for partitions.
 *
 * <p>This class describes the most basic state (only the offset), subclasses define more elaborate
 * state, containing current watermarks and timestamp extractors.
 *
 * @param <KPH> The type of the Kafka partition descriptor, which varies across Kafka versions.
 */
@Internal
public class KafkaTopicPartitionState<T, KPH> {

    // ------------------------------------------------------------------------

    /** The Flink description of a Kafka partition. */
    private final KafkaTopicPartition partition;

    /** The Kafka description of a Kafka partition (varies across different Kafka versions). */
    private final KPH kafkaPartitionHandle;

    /** The offset within the Kafka partition that we already processed. */
    private volatile long offset;

    /** The offset of the Kafka partition that has been committed. */
    private volatile long committedOffset;

    // ------------------------------------------------------------------------

    public KafkaTopicPartitionState(KafkaTopicPartition partition, KPH kafkaPartitionHandle) {
        this.partition = partition;
        this.kafkaPartitionHandle = kafkaPartitionHandle;
        this.offset = KafkaTopicPartitionStateSentinel.OFFSET_NOT_SET;
        this.committedOffset = KafkaTopicPartitionStateSentinel.OFFSET_NOT_SET;
    }

    // ------------------------------------------------------------------------

    /**
     * Gets Flink's descriptor for the Kafka Partition.
     *
     * @return The Flink partition descriptor.
     */
    public final KafkaTopicPartition getKafkaTopicPartition() {
        return partition;
    }

    /**
     * Gets Kafka's descriptor for the Kafka Partition.
     *
     * @return The Kafka partition descriptor.
     */
    public final KPH getKafkaPartitionHandle() {
        return kafkaPartitionHandle;
    }

    public final String getTopic() {
        return partition.getTopic();
    }

    public final int getPartition() {
        return partition.getPartition();
    }

    /**
     * The current offset in the partition. This refers to the offset last element that we retrieved
     * and emitted successfully. It is the offset that should be stored in a checkpoint.
     */
    public final long getOffset() {
        return offset;
    }

    public final void setOffset(long offset) {
        this.offset = offset;
    }

    public final boolean isOffsetDefined() {
        return offset != KafkaTopicPartitionStateSentinel.OFFSET_NOT_SET;
    }

    public final void setCommittedOffset(long offset) {
        this.committedOffset = offset;
    }

    public final long getCommittedOffset() {
        return committedOffset;
    }

    public long extractTimestamp(T record, long kafkaEventTimestamp) {
        return kafkaEventTimestamp;
    }

    public void onEvent(T event, long timestamp) {
        // do nothing
    }

    public void onPeriodicEmit() {
        // do nothing
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "Partition: "
                + partition
                + ", KafkaPartitionHandle="
                + kafkaPartitionHandle
                + ", offset="
                + (isOffsetDefined() ? String.valueOf(offset) : "(not set)");
    }
}
