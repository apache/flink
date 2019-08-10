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

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A partition discoverer that can be used to discover topics and partitions metadata
 * from Kafka brokers via the Kafka high-level consumer API.
 */
@Internal
public class KafkaPartitionDiscoverer extends AbstractPartitionDiscoverer {

	private final Properties kafkaProperties;

	private KafkaConsumer<?, ?> kafkaConsumer;

	public KafkaPartitionDiscoverer(
		KafkaTopicsDescriptor topicsDescriptor,
		int indexOfThisSubtask,
		int numParallelSubtasks,
		Properties kafkaProperties) {

		super(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks);
		this.kafkaProperties = checkNotNull(kafkaProperties);
	}

	@Override
	protected void initializeConnections() {
		this.kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
	}

	@Override
	protected List<String> getAllTopics() throws AbstractPartitionDiscoverer.WakeupException {
		try {
			return new ArrayList<>(kafkaConsumer.listTopics().keySet());
		} catch (org.apache.kafka.common.errors.WakeupException e) {
			// rethrow our own wakeup exception
			throw new AbstractPartitionDiscoverer.WakeupException();
		}
	}

	@Override
	protected List<KafkaTopicPartition> getAllPartitionsForTopics(List<String> topics) throws WakeupException, RuntimeException {
		final List<KafkaTopicPartition> partitions = new LinkedList<>();

		try {
			for (String topic : topics) {
				final List<PartitionInfo> kafkaPartitions = kafkaConsumer.partitionsFor(topic);

				if (kafkaPartitions == null) {
					throw new RuntimeException("Could not fetch partitions for %s. Make sure that the topic exists.".format(topic));
				}

				for (PartitionInfo partitionInfo : kafkaPartitions) {
					partitions.add(new KafkaTopicPartition(partitionInfo.topic(), partitionInfo.partition()));
				}
			}
		} catch (org.apache.kafka.common.errors.WakeupException e) {
			// rethrow our own wakeup exception
			throw new WakeupException();
		}

		return partitions;
	}

	@Override
	protected void wakeupConnections() {
		if (this.kafkaConsumer != null) {
			this.kafkaConsumer.wakeup();
		}
	}

	@Override
	protected void closeConnections() throws Exception {
		if (this.kafkaConsumer != null) {
			this.kafkaConsumer.close();

			// de-reference the consumer to avoid closing multiple times
			this.kafkaConsumer = null;
		}
	}

}
