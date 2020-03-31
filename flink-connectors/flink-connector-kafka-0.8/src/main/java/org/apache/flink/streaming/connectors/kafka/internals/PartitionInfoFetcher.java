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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;

import java.util.List;
import java.util.Properties;

@Internal
class PartitionInfoFetcher extends Thread {

	private final List<String> topics;
	private final Kafka08PartitionDiscoverer partitionDiscoverer;

	private volatile List<KafkaTopicPartitionLeader> result;
	private volatile Throwable error;

	PartitionInfoFetcher(List<String> topics, Properties properties) {
		// we're only using partial functionality of the partition discoverer; the subtask id arguments doesn't matter
		this.partitionDiscoverer = new Kafka08PartitionDiscoverer(new KafkaTopicsDescriptor(topics, null), 0, 1, properties);
		this.topics = topics;
	}

	@Override
	public void run() {
		try {
			partitionDiscoverer.open();
			result = partitionDiscoverer.getPartitionLeadersForTopics(topics);
		}
		catch (Throwable t) {
			this.error = t;
		} finally {
			try {
				partitionDiscoverer.close();
			} catch (Exception e) {
				throw new RuntimeException("Error while closing partition discoverer.", e);
			}
		}
	}

	public List<KafkaTopicPartitionLeader> getPartitions() throws Exception {
		try {
			this.join();
		}
		catch (InterruptedException e) {
			throw new Exception("Partition fetching was cancelled before completion");
		}

		if (error != null) {
			throw new Exception("Failed to fetch partitions for topics " + topics.toString(), error);
		}

		if (result != null) {
			return result;
		}

		throw new Exception("Partition fetching failed");
	}
}
