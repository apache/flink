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

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

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
                // This method would cause the failure for subscriber.
                throw new IllegalStateException(e);
            }
        }
    }

    protected List<TopicPartition> toTopicPartitions(
            TopicMetadata metadata, List<TopicRange> ranges) {
        if (!metadata.isPartitioned()) {
            // For non-partitioned topic.
            return ranges.stream()
                    .map(range -> new TopicPartition(metadata.getName(), -1, range))
                    .collect(toList());
        } else {
            return IntStream.range(0, metadata.getPartitionSize())
                    .boxed()
                    .flatMap(
                            partitionId ->
                                    ranges.stream()
                                            .map(
                                                    range ->
                                                            new TopicPartition(
                                                                    metadata.getName(),
                                                                    partitionId,
                                                                    range)))
                    .collect(toList());
        }
    }
}
