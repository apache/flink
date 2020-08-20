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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Kafka {@link org.apache.flink.table.connector.source.DynamicTableSource}.
 */
@Internal
public class KafkaDynamicSource extends KafkaDynamicSourceBase {

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 *
	 * @param outputDataType         Source output data type
	 * @param topics                 Kafka topic to consume
	 * @param topicPattern           Kafka topic pattern to consume
	 * @param properties             Properties for the Kafka consumer
	 * @param decodingFormat         Decoding format for decoding records from Kafka
	 * @param startupMode            Startup mode for the contained consumer
	 * @param specificStartupOffsets Specific startup offsets; only relevant when startup
	 *                               mode is {@link StartupMode#SPECIFIC_OFFSETS}
	 */
	public KafkaDynamicSource(
			DataType outputDataType,
			@Nullable List<String> topics,
			@Nullable Pattern topicPattern,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis) {

		super(
			outputDataType,
			topics,
			topicPattern,
			properties,
			decodingFormat,
			startupMode,
			specificStartupOffsets,
			startupTimestampMillis);
	}

	@Override
	protected FlinkKafkaConsumerBase<RowData> createKafkaConsumer(
			List<String> topics,
			Properties properties,
			DeserializationSchema<RowData> deserializationSchema) {
		return new FlinkKafkaConsumer<>(topics, deserializationSchema, properties);
	}

	@Override
	protected FlinkKafkaConsumerBase<RowData> createKafkaConsumer(
			Pattern topicPattern,
			Properties properties,
			DeserializationSchema<RowData> deserializationSchema) {
		return new FlinkKafkaConsumer<>(topicPattern, deserializationSchema, properties);
	}

	@Override
	public DynamicTableSource copy() {
		return new KafkaDynamicSource(
				this.outputDataType,
				this.topics,
				this.topicPattern,
				this.properties,
				this.decodingFormat,
				this.startupMode,
				this.specificStartupOffsets,
				this.startupTimestampMillis);
	}

	@Override
	public String asSummaryString() {
		return "Kafka";
	}
}
