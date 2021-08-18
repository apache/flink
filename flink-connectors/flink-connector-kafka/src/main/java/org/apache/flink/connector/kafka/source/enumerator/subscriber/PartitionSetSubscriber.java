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

package org.apache.flink.connector.kafka.source.enumerator.subscriber;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriberUtils.getTopicMetadata;

/** A subscriber for a partition set. */
class PartitionSetSubscriber implements KafkaSubscriber {
    private static final long serialVersionUID = 390970375272146036L;
    private static final Logger LOG = LoggerFactory.getLogger(PartitionSetSubscriber.class);
    private final Set<TopicPartition> subscribedPartitions;

    PartitionSetSubscriber(Set<TopicPartition> partitions) {
        this.subscribedPartitions = partitions;
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(AdminClient adminClient) {
        final Set<String> topicNames =
                subscribedPartitions.stream()
                        .map(TopicPartition::topic)
                        .collect(Collectors.toSet());

        LOG.debug("Fetching descriptions for topics: {}", topicNames);
        final Map<String, TopicDescription> topicMetadata =
                getTopicMetadata(adminClient, topicNames);

        Set<TopicPartition> existingSubscribedPartitions = new HashSet<>();

        for (TopicPartition subscribedPartition : this.subscribedPartitions) {
            if (topicMetadata.containsKey(subscribedPartition.topic())
                    && partitionExistsInTopic(
                            subscribedPartition, topicMetadata.get(subscribedPartition.topic()))) {
                existingSubscribedPartitions.add(subscribedPartition);
            } else {
                throw new RuntimeException(
                        String.format(
                                "Partition '%s' does not exist on Kafka brokers",
                                subscribedPartition));
            }
        }

        return existingSubscribedPartitions;
    }

    private boolean partitionExistsInTopic(TopicPartition partition, TopicDescription topic) {
        return topic.partitions().size() > partition.partition();
    }
}
