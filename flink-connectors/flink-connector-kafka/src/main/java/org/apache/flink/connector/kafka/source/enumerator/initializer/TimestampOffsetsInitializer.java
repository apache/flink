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

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link OffsetsInitializer} to initialize the offsets based on a timestamp.
 *
 * <p>Package private and should be instantiated via {@link OffsetsInitializer}.
 */
class TimestampOffsetsInitializer implements OffsetsInitializer {
    private static final long serialVersionUID = 2932230571773627233L;
    private final long startingTimestamp;

    TimestampOffsetsInitializer(long startingTimestamp) {
        this.startingTimestamp = startingTimestamp;
    }

    @Override
    public Map<TopicPartition, Long> getPartitionOffsets(
            Collection<TopicPartition> partitions,
            PartitionOffsetsRetriever partitionOffsetsRetriever) {
        Map<TopicPartition, Long> startingTimestamps = new HashMap<>();
        Map<TopicPartition, Long> initialOffsets = new HashMap<>();

        // First get the current end offsets of the partitions. This is going to be used
        // in case we cannot find a suitable offsets based on the timestamp, i.e. the message
        // meeting the requirement of the timestamp have not been produced to Kafka yet, in
        // this case, we just use the latest offset.
        // We need to get the latest offsets before querying offsets by time to ensure that
        // no message is going to be missed.
        Map<TopicPartition, Long> endOffsets = partitionOffsetsRetriever.endOffsets(partitions);
        partitions.forEach(tp -> startingTimestamps.put(tp, startingTimestamp));
        partitionOffsetsRetriever
                .offsetsForTimes(startingTimestamps)
                .forEach(
                        (tp, offsetMetadata) -> {
                            if (offsetMetadata != null) {
                                initialOffsets.put(tp, offsetMetadata.offset());
                            } else {
                                // The timestamp does not exist in the partition yet, we will just
                                // consume from the latest.
                                initialOffsets.put(tp, endOffsets.get(tp));
                            }
                        });
        return initialOffsets;
    }

    @Override
    public OffsetResetStrategy getAutoOffsetResetStrategy() {
        return OffsetResetStrategy.NONE;
    }
}
