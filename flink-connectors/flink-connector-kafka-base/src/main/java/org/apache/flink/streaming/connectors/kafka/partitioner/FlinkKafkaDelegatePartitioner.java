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

package org.apache.flink.streaming.connectors.kafka.partitioner;

import org.apache.flink.annotation.Internal;

/**
 * Delegate for the deprecated {@link KafkaPartitioner}.
 * This should only be used for bridging deprecated partitioning API methods.
 *
 * @deprecated Delegate for {@link KafkaPartitioner}, use {@link FlinkKafkaPartitioner} instead
 */
@Deprecated
@Internal
public class FlinkKafkaDelegatePartitioner<T> extends FlinkKafkaPartitioner<T> {
	private final KafkaPartitioner<T> kafkaPartitioner;
	private int[] partitions;

	public FlinkKafkaDelegatePartitioner(KafkaPartitioner<T> kafkaPartitioner) {
		this.kafkaPartitioner = kafkaPartitioner;
	}

	public void setPartitions(int[] partitions) {
		this.partitions = partitions;
	}

	@Override
	public void open(int parallelInstanceId, int parallelInstances) {
		this.kafkaPartitioner.open(parallelInstanceId, parallelInstances, partitions);
	}

	@Override
	public int partition(T next, byte[] serializedKey, byte[] serializedValue, String targetTopic, int[] partitions) {
		return this.kafkaPartitioner.partition(next, serializedKey, serializedValue, this.partitions.length);
	}
}
