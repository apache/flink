/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.kafka.common.TopicPartition;

import java.util.List;

/**
 * The interface for KafkaFetcher and KafkaConsumerThread
 * to co-ordinate event time alignment.
 */
public class EventTimeAlignmentHandover {

	private final Object lock = new Object();

	/** The list of partitions to be paused for realignment, null if alignment is inactive. */
	private List<TopicPartition> partitionsToPause;

	public List<TopicPartition> getPartitionsToPause() {
		synchronized (lock) {
			return this.partitionsToPause;
		}
	}

	public void activate(List<TopicPartition> partitionsToPause) {
		synchronized (lock) {
			this.partitionsToPause = partitionsToPause;
		}
	}

	public void deactivate() {
		synchronized (lock) {
			this.partitionsToPause = null;
		}
	}

}
