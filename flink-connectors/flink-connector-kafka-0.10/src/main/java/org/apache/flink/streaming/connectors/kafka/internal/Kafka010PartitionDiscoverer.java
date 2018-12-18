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
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;

import java.util.Properties;

/**
 * A partition discoverer that can be used to discover topics and partitions metadata
 * from Kafka brokers via the Kafka 0.10 high-level consumer API.
 */
@Internal
public class Kafka010PartitionDiscoverer extends Kafka09PartitionDiscoverer {

	public Kafka010PartitionDiscoverer(
		KafkaTopicsDescriptor topicsDescriptor,
		int indexOfThisSubtask,
		int numParallelSubtasks,
		Properties kafkaProperties) {

		super(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks, kafkaProperties);
	}
}
