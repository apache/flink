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

package org.apache.flink.streaming.connectors.kafka.api.simple.iterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import kafka.consumer.ConsumerConfig;
import org.apache.flink.streaming.connectors.kafka.api.simple.KafkaTopicUtils;
import org.apache.flink.streaming.connectors.kafka.api.simple.MessageWithMetadata;
import org.apache.flink.streaming.connectors.kafka.api.simple.PersistentKafkaSource;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.KafkaOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator over multiple Kafka partitions.
 *
 * This is needed when num partitions > num kafka sources.
 */
public class KafkaMultiplePartitionsIterator implements KafkaConsumerIterator {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaMultiplePartitionsIterator.class);

	protected List<KafkaSinglePartitionIterator> partitions;
	protected final ConsumerConfig consumerConfig;

	public KafkaMultiplePartitionsIterator(String topic,
										Map<Integer, KafkaOffset> partitionsWithOffset,
										KafkaTopicUtils kafkaTopicUtils, ConsumerConfig consumerConfig) {
		partitions = new ArrayList<KafkaSinglePartitionIterator>(partitionsWithOffset.size());

		this.consumerConfig = consumerConfig;

		for (Map.Entry<Integer, KafkaOffset> partitionWithOffset : partitionsWithOffset.entrySet()) {
			partitions.add(new KafkaSinglePartitionIterator(
					topic,
					partitionWithOffset.getKey(),
					partitionWithOffset.getValue(),
					kafkaTopicUtils,
					this.consumerConfig));
		}
	}

	@Override
	public void initialize() throws InterruptedException {
		LOG.info("Initializing iterator with {} partitions", partitions.size());
		String partInfo = "";
		for (KafkaSinglePartitionIterator partition : partitions) {
			partition.initialize();
			partInfo += partition.toString() + " ";
		}
		LOG.info("Initialized partitions {}", partInfo);
	}

	@Override
	public boolean hasNext() {
		return true;
	}

	@Override
	public byte[] next() throws InterruptedException {
		return nextWithOffset().getMessage();
	}

	protected int lastCheckedPartitionIndex = -1;
	private boolean gotNewMessage = false;

	@Override
	public MessageWithMetadata nextWithOffset() throws InterruptedException {
		KafkaSinglePartitionIterator partition;

		while (true) {
			for (int i = nextPartition(lastCheckedPartitionIndex); i < partitions.size(); i = nextPartition(i)) {
				partition = partitions.get(i);

				if (partition.fetchHasNext()) {
					gotNewMessage = true;
					lastCheckedPartitionIndex = i;
					return partition.nextWithOffset();
				}
			}

			// do not wait if a new message has been fetched
			if (!gotNewMessage) {
				try {
					Thread.sleep(consumerConfig.props().getInt(PersistentKafkaSource.WAIT_ON_EMPTY_FETCH_KEY), consumerConfig.fetchWaitMaxMs());
				} catch (InterruptedException e) {
					LOG.warn("Interrupted while waiting for new messages", e);
				}
			}

			gotNewMessage = false;
		}
	}

	protected int nextPartition(int currentPartition) {
		return (currentPartition + 1) % partitions.size();
	}
}
