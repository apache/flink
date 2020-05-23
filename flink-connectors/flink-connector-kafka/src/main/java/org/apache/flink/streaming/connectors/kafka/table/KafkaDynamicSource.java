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
import org.apache.flink.table.connector.format.ScanFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;

import java.util.Map;
import java.util.Properties;

/**
 * Kafka {@link org.apache.flink.table.connector.source.DynamicTableSource}.
 */
@Internal
public class KafkaDynamicSource extends KafkaDynamicSourceBase {

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 *
	 * @param outputDataType         Source output data type
	 * @param topic                  Kafka topic to consume
	 * @param properties             Properties for the Kafka consumer
	 * @param scanFormat             Scan format for decoding records from Kafka
	 * @param startupMode            Startup mode for the contained consumer
	 * @param specificStartupOffsets Specific startup offsets; only relevant when startup
	 *                               mode is {@link StartupMode#SPECIFIC_OFFSETS}
	 */
	public KafkaDynamicSource(
			DataType outputDataType,
			String topic,
			Properties properties,
			ScanFormat<DeserializationSchema<RowData>> scanFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis) {

		super(
			outputDataType,
			topic,
			properties,
			scanFormat,
			startupMode,
			specificStartupOffsets,
			startupTimestampMillis);
	}

	@Override
	protected FlinkKafkaConsumerBase<RowData> createKafkaConsumer(
			String topic,
			Properties properties,
			DeserializationSchema<RowData> deserializationSchema) {
		return new FlinkKafkaConsumer<>(topic, deserializationSchema, properties);
	}

	@Override
	public DynamicTableSource copy() {
		return new KafkaDynamicSource(
				this.outputDataType,
				this.topic,
				this.properties,
				this.scanFormat,
				this.startupMode,
				this.specificStartupOffsets,
				this.startupTimestampMillis);
	}

	@Override
	public String asSummaryString() {
		return "Kafka";
	}
}
