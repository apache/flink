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
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES_KEY;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES_VALUE;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_CLASS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_VALUE_FIXED;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS_OFFSET;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS_PARTITION;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_STARTUP_MODE;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TOPIC;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TYPE_VALUE_KAFKA;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_TYPE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/**
 * Factory for creating configured instances of {@link KafkaTableSourceBase}.
 */
public abstract class KafkaTableSourceSinkFactoryBase implements
		StreamTableSourceFactory<Row>,
		StreamTableSinkFactory<Row> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(UPDATE_MODE(), UPDATE_MODE_VALUE_APPEND()); // append mode
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_KAFKA); // kafka
		context.put(CONNECTOR_VERSION, kafkaVersion()); // version
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// kafka
		properties.add(CONNECTOR_TOPIC);
		properties.add(CONNECTOR_PROPERTIES);
		properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_KEY);
		properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_VALUE);
		properties.add(CONNECTOR_STARTUP_MODE);
		properties.add(CONNECTOR_SPECIFIC_OFFSETS + ".#." + CONNECTOR_SPECIFIC_OFFSETS_PARTITION);
		properties.add(CONNECTOR_SPECIFIC_OFFSETS + ".#." + CONNECTOR_SPECIFIC_OFFSETS_OFFSET);
		properties.add(CONNECTOR_SINK_PARTITIONER);
		properties.add(CONNECTOR_SINK_PARTITIONER_CLASS);

		// schema
		properties.add(SCHEMA() + ".#." + SCHEMA_TYPE());
		properties.add(SCHEMA() + ".#." + SCHEMA_NAME());
		properties.add(SCHEMA() + ".#." + SCHEMA_FROM());

		// time attributes
		properties.add(SCHEMA() + ".#." + SCHEMA_PROCTIME());
		properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_TYPE());
		properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_FROM());
		properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_CLASS());
		properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED());
		properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_TYPE());
		properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_CLASS());
		properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_SERIALIZED());
		properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_DELAY());

		// format wildcard
		properties.add(FORMAT + ".*");

		return properties;
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String topic = descriptorProperties.getString(CONNECTOR_TOPIC);
		final DeserializationSchema<Row> deserializationSchema = getDeserializationSchema(properties);
		final StartupOptions startupOptions = getStartupOptions(descriptorProperties, topic);

		return createKafkaTableSource(
			descriptorProperties.getTableSchema(SCHEMA()),
			SchemaValidator.deriveProctimeAttribute(descriptorProperties),
			SchemaValidator.deriveRowtimeAttributes(descriptorProperties),
			SchemaValidator.deriveFieldMapping(
				descriptorProperties,
				Optional.of(deserializationSchema.getProducedType())),
			topic,
			getKafkaProperties(descriptorProperties),
			deserializationSchema,
			startupOptions.startupMode,
			startupOptions.specificOffsets);
	}

	@Override
	public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA());
		final String topic = descriptorProperties.getString(CONNECTOR_TOPIC);
		final Optional<String> proctime = SchemaValidator.deriveProctimeAttribute(descriptorProperties);
		final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors =
			SchemaValidator.deriveRowtimeAttributes(descriptorProperties);

		// see also FLINK-9870
		if (proctime.isPresent() || !rowtimeAttributeDescriptors.isEmpty() ||
				checkForCustomFieldMapping(descriptorProperties, schema)) {
			throw new TableException("Time attributes and custom field mappings are not supported yet.");
		}

		return createKafkaTableSink(
			schema,
			topic,
			getKafkaProperties(descriptorProperties),
			getFlinkKafkaPartitioner(descriptorProperties),
			getSerializationSchema(properties));
	}

	// --------------------------------------------------------------------------------------------
	// For version-specific factories
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the Kafka version.
	 */
	protected abstract String kafkaVersion();

	/**
	 * True if the Kafka source supports Kafka timestamps, false otherwise.
	 *
	 * @return True if the Kafka source supports Kafka timestamps, false otherwise.
	 */
	protected abstract boolean supportsKafkaTimestamps();

	/**
	 * Constructs the version-specific Kafka table source.
	 *
	 * @param schema                      Schema of the produced table.
	 * @param proctimeAttribute           Field name of the processing time attribute.
	 * @param rowtimeAttributeDescriptors Descriptor for a rowtime attribute
	 * @param fieldMapping                Mapping for the fields of the table schema to
	 *                                    fields of the physical returned type.
	 * @param topic                       Kafka topic to consume.
	 * @param properties                  Properties for the Kafka consumer.
	 * @param deserializationSchema       Deserialization schema for decoding records from Kafka.
	 * @param startupMode                 Startup mode for the contained consumer.
	 * @param specificStartupOffsets      Specific startup offsets; only relevant when startup
	 *                                    mode is {@link StartupMode#SPECIFIC_OFFSETS}.
	 */
	protected abstract KafkaTableSourceBase createKafkaTableSource(
		TableSchema schema,
		Optional<String> proctimeAttribute,
		List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
		Map<String, String> fieldMapping,
		String topic,
		Properties properties,
		DeserializationSchema<Row> deserializationSchema,
		StartupMode startupMode,
		Map<KafkaTopicPartition, Long> specificStartupOffsets);

	/**
	 * Constructs the version-specific Kafka table sink.
	 *
	 * @param schema      Schema of the produced table.
	 * @param topic       Kafka topic to consume.
	 * @param properties  Properties for the Kafka consumer.
	 * @param partitioner Partitioner to select Kafka partition for each item.
	 */
	protected abstract KafkaTableSinkBase createKafkaTableSink(
		TableSchema schema,
		String topic,
		Properties properties,
		Optional<FlinkKafkaPartitioner<Row>> partitioner,
		SerializationSchema<Row> serializationSchema);

	// --------------------------------------------------------------------------------------------
	// Helper methods
	// --------------------------------------------------------------------------------------------

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		// allow Kafka timestamps to be used, watermarks can not be received from source
		new SchemaValidator(true, supportsKafkaTimestamps(), false).validate(descriptorProperties);
		new KafkaValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	private DeserializationSchema<Row> getDeserializationSchema(Map<String, String> properties) {
		@SuppressWarnings("unchecked")
		final DeserializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
			DeserializationSchemaFactory.class,
			properties,
			this.getClass().getClassLoader());
		return formatFactory.createDeserializationSchema(properties);
	}

	private SerializationSchema<Row> getSerializationSchema(Map<String, String> properties) {
		@SuppressWarnings("unchecked")
		final SerializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
			SerializationSchemaFactory.class,
			properties,
			this.getClass().getClassLoader());
		return formatFactory.createSerializationSchema(properties);
	}

	private Properties getKafkaProperties(DescriptorProperties descriptorProperties) {
		final Properties kafkaProperties = new Properties();
		final List<Map<String, String>> propsList = descriptorProperties.getFixedIndexedProperties(
			CONNECTOR_PROPERTIES,
			Arrays.asList(CONNECTOR_PROPERTIES_KEY, CONNECTOR_PROPERTIES_VALUE));
		propsList.forEach(kv -> kafkaProperties.put(
			descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_KEY)),
			descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_VALUE))
		));
		return kafkaProperties;
	}

	private StartupOptions getStartupOptions(
			DescriptorProperties descriptorProperties,
			String topic) {
		final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
		final StartupMode startupMode = descriptorProperties
			.getOptionalString(CONNECTOR_STARTUP_MODE)
			.map(modeString -> {
				switch (modeString) {
					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST:
						return StartupMode.EARLIEST;

					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_LATEST:
						return StartupMode.LATEST;

					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_GROUP_OFFSETS:
						return StartupMode.GROUP_OFFSETS;

					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS:
						final List<Map<String, String>> offsetList = descriptorProperties.getFixedIndexedProperties(
							CONNECTOR_SPECIFIC_OFFSETS,
							Arrays.asList(CONNECTOR_SPECIFIC_OFFSETS_PARTITION, CONNECTOR_SPECIFIC_OFFSETS_OFFSET));
						offsetList.forEach(kv -> {
							final int partition = descriptorProperties.getInt(kv.get(CONNECTOR_SPECIFIC_OFFSETS_PARTITION));
							final long offset = descriptorProperties.getLong(kv.get(CONNECTOR_SPECIFIC_OFFSETS_OFFSET));
							final KafkaTopicPartition topicPartition = new KafkaTopicPartition(topic, partition);
							specificOffsets.put(topicPartition, offset);
						});
						return StartupMode.SPECIFIC_OFFSETS;
					default:
						throw new TableException("Unsupported startup mode. Validator should have checked that.");
				}
			}).orElse(StartupMode.GROUP_OFFSETS);
		final StartupOptions options = new StartupOptions();
		options.startupMode = startupMode;
		options.specificOffsets = specificOffsets;
		return options;
	}

	@SuppressWarnings("unchecked")
	private Optional<FlinkKafkaPartitioner<Row>> getFlinkKafkaPartitioner(DescriptorProperties descriptorProperties) {
		return descriptorProperties
			.getOptionalString(CONNECTOR_SINK_PARTITIONER)
			.flatMap((String partitionerString) -> {
				switch (partitionerString) {
					case CONNECTOR_SINK_PARTITIONER_VALUE_FIXED:
						return Optional.of(new FlinkFixedPartitioner<>());
					case CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN:
						return Optional.empty();
					case CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM:
						final Class<? extends FlinkKafkaPartitioner> partitionerClass =
							descriptorProperties.getClass(CONNECTOR_SINK_PARTITIONER_CLASS, FlinkKafkaPartitioner.class);
						return Optional.of(InstantiationUtil.instantiate(partitionerClass));
					default:
						throw new TableException("Unsupported sink partitioner. Validator should have checked that.");
				}
			});
	}

	private boolean checkForCustomFieldMapping(DescriptorProperties descriptorProperties, TableSchema schema) {
		final Map<String, String> fieldMapping = SchemaValidator.deriveFieldMapping(
			descriptorProperties,
			Optional.of(schema.toRowType())); // until FLINK-9870 is fixed we assume that the table schema is the output type
		return fieldMapping.size() != schema.getFieldNames().length ||
			!fieldMapping.entrySet().stream().allMatch(mapping -> mapping.getKey().equals(mapping.getValue()));
	}

	private static class StartupOptions {
		private StartupMode startupMode;
		private Map<KafkaTopicPartition, Long> specificOffsets;
	}
}
