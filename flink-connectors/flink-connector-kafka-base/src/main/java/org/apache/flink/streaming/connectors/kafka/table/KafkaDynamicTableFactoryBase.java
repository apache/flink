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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Factory for creating configured instances of
 * {@link KafkaDynamicSourceBase} and {@link KafkaDynamicSinkBase}.
 */
public abstract class KafkaDynamicTableFactoryBase implements
		DynamicTableSourceFactory,
		DynamicTableSinkFactory {

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		ReadableConfig tableOptions = helper.getOptions();

		String topic = tableOptions.get(KafkaOptions.TOPIC);
		DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
				DeserializationFormatFactory.class,
				FactoryUtil.FORMAT);
		// Validate the option data type.
		helper.validateExcept(KafkaOptions.PROPERTIES_PREFIX);
		// Validate the option values.
		KafkaOptions.validateTableOptions(tableOptions);

		DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
		final KafkaOptions.StartupOptions startupOptions = KafkaOptions.getStartupOptions(tableOptions, topic);
		return createKafkaTableSource(
				producedDataType,
				topic,
				KafkaOptions.getKafkaProperties(context.getCatalogTable().getOptions()),
				decodingFormat,
				startupOptions.startupMode,
				startupOptions.specificOffsets,
				startupOptions.startupTimestampMillis);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		ReadableConfig tableOptions = helper.getOptions();

		String topic = tableOptions.get(KafkaOptions.TOPIC);
		EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
				SerializationFormatFactory.class,
				FactoryUtil.FORMAT);
		String semantic = tableOptions.get(KafkaOptions.SINK_SEMANTIC);
		// Validate the option data type.
		helper.validateExcept(KafkaOptions.PROPERTIES_PREFIX);
		// Validate the option values.
		KafkaOptions.validateTableOptions(tableOptions);

		DataType consumedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
		return createKafkaTableSink(
				consumedDataType,
				topic,
				KafkaOptions.getKafkaProperties(context.getCatalogTable().getOptions()),
				KafkaOptions.getFlinkKafkaPartitioner(tableOptions, context.getClassLoader()),
				encodingFormat,
				KafkaOptions.getSinkSemantic(semantic));
	}

	/**
	 * Constructs the version-specific Kafka table source.
	 *
	 * @param producedDataType       Source produced data type
	 * @param topic                  Kafka topic to consume
	 * @param properties             Properties for the Kafka consumer
	 * @param decodingFormat         Decoding format for decoding records from Kafka
	 * @param startupMode            Startup mode for the contained consumer
	 * @param specificStartupOffsets Specific startup offsets; only relevant when startup
	 *                               mode is {@link StartupMode#SPECIFIC_OFFSETS}
	 */
	protected abstract KafkaDynamicSourceBase createKafkaTableSource(
			DataType producedDataType,
			String topic,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis);

	/**
	 * Constructs the version-specific Kafka table sink.
	 *
	 * @param consumedDataType Sink consumed data type
	 * @param topic            Kafka topic to consume
	 * @param properties       Properties for the Kafka consumer
	 * @param partitioner      Partitioner to select Kafka partition for each item
	 * @param encodingFormat   Encoding format for encoding records to Kafka
	 */
	protected abstract KafkaDynamicSinkBase createKafkaTableSink(
			DataType consumedDataType,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<RowData>> partitioner,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			KafkaSemantic semantic);

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(KafkaOptions.TOPIC);
		options.add(FactoryUtil.FORMAT);
		options.add(KafkaOptions.PROPS_BOOTSTRAP_SERVERS);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(KafkaOptions.PROPS_GROUP_ID);
		options.add(KafkaOptions.SCAN_STARTUP_MODE);
		options.add(KafkaOptions.SCAN_STARTUP_SPECIFIC_OFFSETS);
		options.add(KafkaOptions.SCAN_STARTUP_TIMESTAMP_MILLIS);
		options.add(KafkaOptions.SINK_PARTITIONER);
		options.add(KafkaOptions.SINK_SEMANTIC);
		return options;
	}
}
