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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Factory for creating configured instances of {@link KafkaTableSource}.
 *
 * @deprecated Use {@link KafkaDynamicTableFactory}.
 */
@Deprecated
public class KafkaTableSourceSinkFactory extends KafkaTableSourceSinkFactoryBase {

	@Override
	protected String kafkaVersion() {
		return KafkaValidator.CONNECTOR_VERSION_VALUE_UNIVERSAL;
	}

	@Override
	protected boolean supportsKafkaTimestamps() {
		return true;
	}

	@Override
	protected KafkaTableSourceBase createKafkaTableSource(
		TableSchema schema,
		Optional<String> proctimeAttribute,
		List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
		Map<String, String> fieldMapping,
		String topic,
		Properties properties,
		DeserializationSchema<Row> deserializationSchema,
		StartupMode startupMode,
		Map<KafkaTopicPartition, Long> specificStartupOffsets,
		long startupTimestampMillis) {

		return new KafkaTableSource(
			schema,
			proctimeAttribute,
			rowtimeAttributeDescriptors,
			Optional.of(fieldMapping),
			topic,
			properties,
			deserializationSchema,
			startupMode,
			specificStartupOffsets,
			startupTimestampMillis);
	}

	@Override
	protected KafkaTableSinkBase createKafkaTableSink(
		TableSchema schema,
		String topic,
		Properties properties,
		Optional<FlinkKafkaPartitioner<Row>> partitioner,
		SerializationSchema<Row> serializationSchema) {

		return new KafkaTableSink(
			schema,
			topic,
			properties,
			partitioner,
			serializationSchema);
	}
}
