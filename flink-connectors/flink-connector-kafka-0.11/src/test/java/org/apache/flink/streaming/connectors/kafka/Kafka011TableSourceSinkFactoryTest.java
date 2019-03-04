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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Test for {@link Kafka011TableSource} and {@link Kafka011TableSink} created
 * by {@link Kafka011TableSourceSinkFactory}.
 */
public class Kafka011TableSourceSinkFactoryTest extends KafkaTableSourceSinkFactoryTestBase {

	@Override
	protected String getKafkaVersion() {
		return KafkaValidator.CONNECTOR_VERSION_VALUE_011;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Class<FlinkKafkaConsumerBase<Row>> getExpectedFlinkKafkaConsumer() {
		return (Class) FlinkKafkaConsumer011.class;
	}

	@Override
	protected Class<?> getExpectedFlinkKafkaProducer() {
		return FlinkKafkaProducer011.class;
	}

	@Override
	protected KafkaTableSourceBase getExpectedKafkaTableSource(
			TableSchema schema,
			Optional<String> proctimeAttribute,
			List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
			Map<String, String> fieldMapping,
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets) {

		return new Kafka011TableSource(
			schema,
			proctimeAttribute,
			rowtimeAttributeDescriptors,
			Optional.of(fieldMapping),
			topic,
			properties,
			deserializationSchema,
			startupMode,
			specificStartupOffsets
		);
	}

	@Override
	protected KafkaTableSinkBase getExpectedKafkaTableSink(
			TableSchema schema,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<Row>> partitioner,
			SerializationSchema<Row> serializationSchema) {

		return new Kafka011TableSink(
			schema,
			topic,
			properties,
			partitioner,
			serializationSchema
		);
	}
}
