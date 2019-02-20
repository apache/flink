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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Wraps KeyedSerializationSchema and FlinkKafkaPartitioner as KafkaSerializationSchema.
 *
 * @param <T> - element type.
 */
public class KafkaSerializationSchemaAdapter010<T>
	extends KafkaSerializationSchemaAdapterBase<T> implements KafkaSerializationSchema<T>, Serializable {

	private static final long serialVersionUID = 1L;

	public KafkaSerializationSchemaAdapter010(
		String defaultTopic,
		KeyedSerializationSchema<T> keyedSerializationSchema,
		@Nullable FlinkKafkaPartitioner<T> partitioner) {

		super(defaultTopic, keyedSerializationSchema, partitioner);
	}

	@Override
	protected ProducerRecord<byte[], byte[]> createProducerRecord(
		String targetTopic, @Nullable Integer partition, @Nullable Long timestamp,
		byte[] serializeKey, byte[] serializeValue) {
		return new ProducerRecord<>(targetTopic, partition, timestamp, serializeKey, serializeValue);
	}
}
