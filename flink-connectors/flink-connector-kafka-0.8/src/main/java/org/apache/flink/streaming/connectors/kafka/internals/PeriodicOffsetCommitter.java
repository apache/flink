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

import kafka.common.TopicAndPartition;

import java.util.HashMap;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A thread that periodically writes the current Kafka partition offsets to Zookeeper.
 */
@Internal
public class PeriodicOffsetCommitter extends Thread {

	/** The ZooKeeper handler. */
	private final ZookeeperOffsetHandler offsetHandler;

	private final List<KafkaTopicPartitionState<TopicAndPartition>> partitionStates;

	/** The proxy to forward exceptions to the main thread. */
	private final ExceptionProxy errorHandler;

	/** Interval in which to commit, in milliseconds. */
	private final long commitInterval;

	/** Flag to mark the periodic committer as running. */
	private volatile boolean running = true;

	PeriodicOffsetCommitter(ZookeeperOffsetHandler offsetHandler,
			List<KafkaTopicPartitionState<TopicAndPartition>> partitionStates,
			ExceptionProxy errorHandler,
			long commitInterval) {
		this.offsetHandler = checkNotNull(offsetHandler);
		this.partitionStates = checkNotNull(partitionStates);
		this.errorHandler = checkNotNull(errorHandler);
		this.commitInterval = commitInterval;

		checkArgument(commitInterval > 0);
	}

	@Override
	public void run() {
		try {
			while (running) {
				Thread.sleep(commitInterval);

				// create copy a deep copy of the current offsets
				HashMap<KafkaTopicPartition, Long> offsetsToCommit = new HashMap<>(partitionStates.size());
				for (KafkaTopicPartitionState<?> partitionState : partitionStates) {
					offsetsToCommit.put(partitionState.getKafkaTopicPartition(), partitionState.getOffset());
				}

				offsetHandler.prepareAndCommitOffsets(offsetsToCommit);
			}
		}
		catch (Throwable t) {
			if (running) {
				errorHandler.reportError(
						new Exception("The periodic offset committer encountered an error: " + t.getMessage(), t));
			}
		}
	}

	public void shutdown() {
		this.running = false;
		this.interrupt();
	}
}
