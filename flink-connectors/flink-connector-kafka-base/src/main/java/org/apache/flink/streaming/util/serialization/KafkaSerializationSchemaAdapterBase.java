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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaDelegatePartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/**
 * Simpple adapter to wrap {@link KeyedSerializationSchema} and {@link FlinkKafkaPartitioner} as
 * KafkaSerializationSchema.
 * @param <T> The type to be serialized.
 */
public class KafkaSerializationSchemaAdapterBase<T> implements KafkaSerializationSchema<T>, Serializable {
	private static final long serialVersionUID = 1L;
	private final FlinkKafkaPartitioner<T> partitioner;
	private final KeyedSerializationSchema<T> keyedSerializationSchema;
	private final String defaultTopic;

	public KafkaSerializationSchemaAdapterBase(
		String defaultTopic, KeyedSerializationSchema<T> keyedSerializationSchema,
		@Nullable FlinkKafkaPartitioner<T> partitioner) {

		this.defaultTopic = requireNonNull(defaultTopic, "defaultTopic");
		this.keyedSerializationSchema = requireNonNull(keyedSerializationSchema, "keyedSerializationSchema");
		this.partitioner = partitioner;
		ClosureCleaner.clean(partitioner, true);
		ClosureCleaner.ensureSerializable(keyedSerializationSchema);
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(T element, Long timestamp, PartitionInfo partitionInfo) {
		String targetTopic = keyedSerializationSchema.getTargetTopic(element);
		if (targetTopic == null) {
			targetTopic = defaultTopic;
		}
		final byte[] serializeKey = keyedSerializationSchema.serializeKey(element);
		final byte[] serializeValue = keyedSerializationSchema.serializeValue(element);
		Integer partition = null;
		if (partitioner != null) {
			partition = partitioner.partition(
				element,
				serializeKey, serializeValue,
				targetTopic, partitionInfo.partitionsFor(targetTopic));
		}
		return createProducerRecord(targetTopic, partition, timestamp, serializeKey, serializeValue);
	}

	protected ProducerRecord<byte[], byte[]> createProducerRecord(
		String targetTopic, Integer partition, Long timestamp, byte[] serializeKey, byte[] serializeValue) {
		return new ProducerRecord<>(targetTopic, partition, serializeKey, serializeValue);
	}

	public void open(RuntimeContext runtimeContext, PartitionInfo partitionInfo) {
		if (null != partitioner) {
			if (partitioner instanceof FlinkKafkaDelegatePartitioner) {
				((FlinkKafkaDelegatePartitioner) partitioner).setPartitions(
					partitionInfo.partitionsFor(defaultTopic));
			}
			partitioner.open(runtimeContext.getIndexOfThisSubtask(), runtimeContext.getNumberOfParallelSubtasks());
		}
	}
}
