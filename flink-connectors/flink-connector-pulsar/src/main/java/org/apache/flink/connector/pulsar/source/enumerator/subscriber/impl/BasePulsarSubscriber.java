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

package org.apache.flink.connector.pulsar.source.enumerator.subscriber.impl;

import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicMetadata;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator.KeySharedMode;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

/** PulsarSubscriber abstract class to simplify Pulsar admin related operations. */
public abstract class BasePulsarSubscriber implements PulsarSubscriber {
    private static final long serialVersionUID = 2053021503331058888L;

    protected TopicMetadata queryTopicMetadata(PulsarAdmin pulsarAdmin, String topicName) {
        // Drop the complete topic name for a clean partitioned topic name.
        String completeTopicName = TopicNameUtils.topicName(topicName);
        try {
            PartitionedTopicMetadata metadata =
                    pulsarAdmin.topics().getPartitionedTopicMetadata(completeTopicName);
            return new TopicMetadata(topicName, metadata.partitions);
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 404) {
                // Return null for skipping the topic metadata query.
                return null;
            } else {
                // This method would cause failure for subscribers.
                throw new IllegalStateException(e);
            }
        }
    }

    protected List<TopicPartition> toTopicPartitions(
            TopicMetadata metadata, List<TopicRange> ranges, KeySharedMode mode) {
        if (!metadata.isPartitioned()) {
            // For non-partitioned topic.
            return toTopicPartitions(metadata.getName(), -1, ranges, mode);
        } else {
            // For partitioned topic.
            List<TopicPartition> partitions = new ArrayList<>();
            for (int i = 0; i < metadata.getPartitionSize(); i++) {
                partitions.addAll(toTopicPartitions(metadata.getName(), i, ranges, mode));
            }
            return partitions;
        }
    }

    protected List<TopicPartition> toTopicPartitions(
            String topic, int partitionId, List<TopicRange> ranges, KeySharedMode mode) {
        switch (mode) {
            case JOIN:
                return singletonList(new TopicPartition(topic, partitionId, ranges, mode));
            case SPLIT:
                List<TopicPartition> partitions = new ArrayList<>(ranges.size());
                for (TopicRange range : ranges) {
                    TopicPartition partition =
                            new TopicPartition(topic, partitionId, singletonList(range), mode);
                    partitions.add(partition);
                }
                return partitions;
            default:
                throw new UnsupportedOperationException(mode + " isn't supported.");
        }
    }
}
