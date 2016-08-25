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

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Properties;

/**
 * A fetcher that fetches data from Kafka brokers via the Kafka 0.10 consumer API.
 * 
 * @param <T> The type of elements produced by the fetcher.
 */
public class Kafka010Fetcher<T> extends Kafka09Fetcher<T> {

	public Kafka010Fetcher(
			SourceContext<T> sourceContext,
			List<KafkaTopicPartition> assignedPartitions,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			StreamingRuntimeContext runtimeContext,
			KeyedDeserializationSchema<T> deserializer,
			Properties kafkaProperties,
			long pollTimeout,
			boolean useMetrics) throws Exception
	{
		super(sourceContext, assignedPartitions, watermarksPeriodic, watermarksPunctuated, runtimeContext, deserializer, kafkaProperties, pollTimeout, useMetrics);
	}

	@Override
	protected void assignPartitionsToConsumer(KafkaConsumer<byte[], byte[]> consumer, List<TopicPartition> topicPartitions) {
		consumer.assign(topicPartitions);
	}

	@Override
	protected void emitRecord(T record, KafkaTopicPartitionState<TopicPartition> partition, long offset, ConsumerRecord consumerRecord) throws Exception {
		// pass timestamp
		super.emitRecord(record, partition, offset, consumerRecord.timestamp());
	}

	/**
	 * Emit record Kafka-timestamp aware.
	 */
	@Override
	protected void emitRecord(T record, KafkaTopicPartitionState<TopicPartition> partitionState, long offset, long timestamp) throws Exception {
		if (timestampWatermarkMode == NO_TIMESTAMPS_WATERMARKS) {
			// fast path logic, in case there are no watermarks

			// emit the record, using the checkpoint lock to guarantee
			// atomicity of record emission and offset state update
			synchronized (checkpointLock) {
				sourceContext.collectWithTimestamp(record, timestamp);
				partitionState.setOffset(offset);
			}
		}
		else if (timestampWatermarkMode == PERIODIC_WATERMARKS) {
			emitRecordWithTimestampAndPeriodicWatermark(record, partitionState, offset, timestamp);
		}
		else {
			emitRecordWithTimestampAndPunctuatedWatermark(record, partitionState, offset, timestamp);
		}
	}

	@Override
	protected String getFetcherName() {
		return "Kafka 0.10 Fetcher";
	}
}
