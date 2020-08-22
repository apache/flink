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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_SEMANTIC;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Factory for creating configured instances of {@link Kafka010DynamicSource}.
 */
public class Kafka010DynamicTableFactory extends KafkaDynamicTableFactoryBase {

	public static final String IDENTIFIER = "kafka-0.10";

	@Override
	protected KafkaDynamicSourceBase createKafkaTableSource(
			DataType producedDataType,
			@Nullable List<String> topics,
			@Nullable Pattern topicPattern,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis) {

		return new Kafka010DynamicSource(
			producedDataType,
			topics,
			topicPattern,
			properties,
			decodingFormat,
			startupMode,
			specificStartupOffsets,
			startupTimestampMillis);
	}

	@Override
	protected KafkaDynamicSinkBase createKafkaTableSink(
			DataType consumedDataType,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<RowData>> partitioner,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			KafkaSinkSemantic semantic) {

		// Kafka 0.10 doesn't support exactly once guarantee, thus it should be the default at-least-once
		checkArgument(semantic.equals(KafkaSinkSemantic.AT_LEAST_ONCE));
		return new Kafka010DynamicSink(
			consumedDataType,
			topic,
			properties,
			partitioner,
			encodingFormat);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = super.optionalOptions();
		// Connector kafka-0.10 only supports "at-least-once"
		options.remove(SINK_SEMANTIC);
		return options;
	}
}
