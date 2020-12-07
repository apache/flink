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
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriberUtils.maybeLog;
import static org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriberUtils.updatePartitionChanges;

/**
 * A subscriber to a fixed list of topics. The subscribed topics must hav existed
 * in the Kafka cluster, otherwise an exception will be thrown.
 */
class TopicListSubscriber implements KafkaSubscriber {
	private static final long serialVersionUID = -6917603843104947866L;
	private static final Logger LOG = LoggerFactory.getLogger(TopicListSubscriber.class);
	private final List<String> topics;

	TopicListSubscriber(List<String> topics) {
		this.topics = topics;
	}

	@Override
	public PartitionChange getPartitionChanges(
			AdminClient adminClient,
			Set<TopicPartition> currentAssignment) {
		Set<TopicPartition> newPartitions = new HashSet<>();
		Set<TopicPartition> removedPartitions = new HashSet<>(currentAssignment);

		Map<String, TopicDescription> topicMetadata;
		try {
			topicMetadata = adminClient.describeTopics(topics).all().get();
		} catch (Exception e) {
			throw new RuntimeException("Failed to get topic metadata.", e);
		}
		topics.forEach(topic -> {
			List<TopicPartitionInfo> partitions = topicMetadata.get(topic).partitions();
			if (partitions != null) {
				updatePartitionChanges(topic, newPartitions, removedPartitions, partitions);
			}
		});
		maybeLog(newPartitions, removedPartitions, LOG);
		return new PartitionChange(newPartitions, removedPartitions);
	}
}
