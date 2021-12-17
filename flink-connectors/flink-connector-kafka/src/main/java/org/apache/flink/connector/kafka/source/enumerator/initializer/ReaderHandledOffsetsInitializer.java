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

package org.apache.flink.connector.kafka.source.enumerator.initializer;

import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A initializer that initialize the partitions to the earliest / latest / last-committed offsets.
 * The offsets initialization are taken care of by the {@code KafkaPartitionSplitReader} instead of
 * by the {@code KafkaSourceEnumerator}.
 *
 * <p>Package private and should be instantiated via {@link OffsetsInitializer}.
 */
class ReaderHandledOffsetsInitializer implements OffsetsInitializer, OffsetsInitializerValidator {
    private static final long serialVersionUID = 172938052008787981L;
    private final long startingOffset;
    private final OffsetResetStrategy offsetResetStrategy;

    /**
     * The only valid value for startingOffset is following. {@link
     * KafkaPartitionSplit#EARLIEST_OFFSET EARLIEST_OFFSET}, {@link
     * KafkaPartitionSplit#LATEST_OFFSET LATEST_OFFSET}, {@link KafkaPartitionSplit#COMMITTED_OFFSET
     * COMMITTED_OFFSET}
     */
    ReaderHandledOffsetsInitializer(long startingOffset, OffsetResetStrategy offsetResetStrategy) {
        this.startingOffset = startingOffset;
        this.offsetResetStrategy = offsetResetStrategy;
    }

    @Override
    public Map<TopicPartition, Long> getPartitionOffsets(
            Collection<TopicPartition> partitions,
            PartitionOffsetsRetriever partitionOffsetsRetriever) {
        Map<TopicPartition, Long> initialOffsets = new HashMap<>();
        for (TopicPartition tp : partitions) {
            initialOffsets.put(tp, startingOffset);
        }
        return initialOffsets;
    }

    @Override
    public OffsetResetStrategy getAutoOffsetResetStrategy() {
        return offsetResetStrategy;
    }

    @Override
    public void validate(Properties kafkaSourceProperties) {
        if (startingOffset == KafkaPartitionSplit.COMMITTED_OFFSET) {
            checkState(
                    kafkaSourceProperties.containsKey(ConsumerConfig.GROUP_ID_CONFIG),
                    String.format(
                            "Property %s is required when using committed offset for offsets initializer",
                            ConsumerConfig.GROUP_ID_CONFIG));
        }
    }
}
