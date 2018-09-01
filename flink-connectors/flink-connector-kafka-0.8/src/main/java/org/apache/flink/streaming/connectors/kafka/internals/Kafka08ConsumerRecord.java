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
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import kafka.common.TopicAndPartition;


/**
 * Implements KeyedDeserializationSchema.Record for Kafka 0.08.
 */
@Internal
class Kafka08ConsumerRecord implements KeyedDeserializationSchema.Record {
	private final byte[] key;
	private final byte[] value;
	private final String topic;
	private final int partition;
	private final long offset;

	Kafka08ConsumerRecord(
		byte[] key,
		byte[] value,
		KafkaTopicPartitionState<TopicAndPartition> currentPartition,
		long offset) {
		this.key = key;
		this.value = value;
		this.topic = currentPartition.getTopic();
		this.partition = currentPartition.getPartition();
		this.offset = offset;
	}

	@Override
	public byte[] key() {
		return key;
	}

	@Override
	public byte[] value() {
		return value;
	}

	@Override
	public String topic() {
		return topic;
	}

	@Override
	public int partition() {
		return partition;
	}

	@Override
	public long offset() {
		return offset;
	}

}
