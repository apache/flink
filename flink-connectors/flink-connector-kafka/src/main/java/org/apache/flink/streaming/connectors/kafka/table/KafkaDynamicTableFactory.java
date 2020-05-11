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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.connector.format.ScanFormat;
import org.apache.flink.table.connector.format.SinkFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Factory for creating configured instances of {@link KafkaDynamicSource}.
 */
public class KafkaDynamicTableFactory extends KafkaDynamicTableFactoryBase {
	public static final String IDENTIFIER = "kafka";

	@Override
	protected KafkaDynamicSourceBase createKafkaTableSource(
			DataType producedDataType,
			String topic,
			Properties properties,
			ScanFormat<DeserializationSchema<RowData>> scanFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis) {
		return new KafkaDynamicSource(
				producedDataType,
				topic,
				properties,
				scanFormat,
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
			SinkFormat<SerializationSchema<RowData>> sinkFormat) {
		return new KafkaDynamicSink(
				consumedDataType,
				topic,
				properties,
				partitioner,
				sinkFormat);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}
}
