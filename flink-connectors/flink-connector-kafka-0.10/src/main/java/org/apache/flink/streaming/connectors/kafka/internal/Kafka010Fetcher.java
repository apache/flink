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

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;

/**
 * A fetcher that fetches data from Kafka brokers via the Kafka 0.10 consumer API.
 *
 * <p>This fetcher re-uses basically all functionality of the 0.9 fetcher. It only additionally
 * takes the KafkaRecord-attached timestamp and attaches it to the Flink records.
 *
 * @param <T> The type of elements produced by the fetcher.
 */
public class Kafka010Fetcher<T> extends Kafka09Fetcher<T> {

	public Kafka010Fetcher(
			SourceContext<T> sourceContext,
			Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			ProcessingTimeService processingTimeProvider,
			long autoWatermarkInterval,
			ClassLoader userCodeClassLoader,
			String taskNameWithSubtasks,
			KeyedDeserializationSchema<T> deserializer,
			Properties kafkaProperties,
			long pollTimeout,
			MetricGroup subtaskMetricGroup,
			MetricGroup consumerMetricGroup,
			boolean useMetrics) throws Exception {
		super(
				sourceContext,
				assignedPartitionsWithInitialOffsets,
				watermarksPeriodic,
				watermarksPunctuated,
				processingTimeProvider,
				autoWatermarkInterval,
				userCodeClassLoader,
				taskNameWithSubtasks,
				deserializer,
				kafkaProperties,
				pollTimeout,
				subtaskMetricGroup,
				consumerMetricGroup,
				useMetrics);
	}

	@Override
	protected void emitRecord(
			T record,
			KafkaTopicPartitionState<TopicPartition> partition,
			long offset,
			ConsumerRecord<?, ?> consumerRecord) throws Exception {

		// we attach the Kafka 0.10 timestamp here
		emitRecordWithTimestamp(record, partition, offset, consumerRecord.timestamp());
	}

	/**
	 * This method needs to be overridden because Kafka broke binary compatibility between 0.9 and 0.10,
	 * changing binary signatures.
	 */
	@Override
	protected KafkaConsumerCallBridge010 createCallBridge() {
		return new KafkaConsumerCallBridge010();
	}

	@Override
	protected String getFetcherName() {
		return "Kafka 0.10 Fetcher";
	}
}
