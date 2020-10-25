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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
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

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPERTIES_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_GROUP_ID;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_TOPIC_PARTITION_DISCOVERY;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_PARTITIONER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_SEMANTIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.StartupOptions;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC_PATTERN;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getFlinkKafkaPartitioner;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getKafkaProperties;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getSinkSemantic;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getStartupOptions;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.validateTableSinkOptions;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.validateTableSourceOptions;

/**
 * Factory for creating configured instances of {@link KafkaDynamicSource} and {@link KafkaDynamicSink}.
 */
@Internal
public class KafkaDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	public static final String IDENTIFIER = "kafka";

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(FactoryUtil.FORMAT);
		options.add(PROPS_BOOTSTRAP_SERVERS);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(TOPIC);
		options.add(TOPIC_PATTERN);
		options.add(PROPS_GROUP_ID);
		options.add(SCAN_STARTUP_MODE);
		options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
		options.add(SCAN_TOPIC_PARTITION_DISCOVERY);
		options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
		options.add(SINK_PARTITIONER);
		options.add(SINK_SEMANTIC);
		return options;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		ReadableConfig tableOptions = helper.getOptions();
		DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
				DeserializationFormatFactory.class,
				FactoryUtil.FORMAT);
		// Validate the option data type.
		helper.validateExcept(PROPERTIES_PREFIX);
		// Validate the option values.
		validateTableSourceOptions(tableOptions);

		final StartupOptions startupOptions = getStartupOptions(tableOptions);
		final Properties properties = getKafkaProperties(context.getCatalogTable().getOptions());
		// add topic-partition discovery
		properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
			String.valueOf(tableOptions
				.getOptional(SCAN_TOPIC_PARTITION_DISCOVERY)
				.map(Duration::toMillis)
				.orElse(FlinkKafkaConsumerBase.PARTITION_DISCOVERY_DISABLED)));

		final DataType physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

		return createKafkaTableSource(
			physicalDataType,
			KafkaOptions.getSourceTopics(tableOptions),
			KafkaOptions.getSourceTopicPattern(tableOptions),
			properties,
			decodingFormat,
			startupOptions.startupMode,
			startupOptions.specificOffsets,
			startupOptions.startupTimestampMillis);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		ReadableConfig tableOptions = helper.getOptions();

		EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
				SerializationFormatFactory.class,
				FactoryUtil.FORMAT);

		// Validate the option values.
		validateTableSinkOptions(tableOptions);
		// Validate the option data type.
		helper.validateExcept(PROPERTIES_PREFIX);

		final DataType physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

		return createKafkaTableSink(
				physicalDataType,
				tableOptions.get(TOPIC).get(0),
				getKafkaProperties(context.getCatalogTable().getOptions()),
				getFlinkKafkaPartitioner(tableOptions, context.getClassLoader()),
				encodingFormat,
				getSinkSemantic(tableOptions));
	}

	// --------------------------------------------------------------------------------------------

	protected KafkaDynamicSource createKafkaTableSource(
			DataType physicalDataType,
			@Nullable List<String> topics,
			@Nullable Pattern topicPattern,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis) {
		return new KafkaDynamicSource(
				physicalDataType,
				topics,
				topicPattern,
				properties,
				decodingFormat,
				startupMode,
				specificStartupOffsets,
				startupTimestampMillis);
	}

	protected KafkaDynamicSink createKafkaTableSink(
			DataType physicalDataType,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<RowData>> partitioner,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			KafkaSinkSemantic semantic) {
		return new KafkaDynamicSink(
				physicalDataType,
				topic,
				properties,
				partitioner,
				encodingFormat,
				semantic);
	}
}
