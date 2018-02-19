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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonSchemaConverter;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JsonValidator;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_JSON_SCHEMA;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_SCHEMA;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_TYPE_VALUE;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES_KEY;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES_VALUE;
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
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_DERIVE_FIELDS;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_FIELDS;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_FIELDS_FROM;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_FIELDS_NAME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_FIELDS_PROCTIME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_FIELDS_TYPE;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_PROPERTY_VERSION;

/**
 * Factory for creating configured instances of {@link KafkaJsonTableSource}.
 */
public abstract class KafkaJsonTableSourceFactory implements TableSourceFactory<Row> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE(), CONNECTOR_TYPE_VALUE_KAFKA); // kafka
		context.put(CONNECTOR_VERSION(), kafkaVersion());

		context.put(FORMAT_TYPE(), FORMAT_TYPE_VALUE); // json format

		context.put(CONNECTOR_PROPERTY_VERSION(), "1"); // backwards compatibility
		context.put(FORMAT_PROPERTY_VERSION(), "1");
		context.put(SCHEMA_PROPERTY_VERSION(), "1");

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

		// json format
		properties.add(FORMAT_JSON_SCHEMA);
		properties.add(FORMAT_SCHEMA);
		properties.add(FORMAT_FAIL_ON_MISSING_FIELD);

		// schema
		properties.add(SCHEMA_DERIVE_FIELDS());
		properties.add(SCHEMA_FIELDS() + ".#." + SCHEMA_FIELDS_TYPE());
		properties.add(SCHEMA_FIELDS() + ".#." + SCHEMA_FIELDS_NAME());
		properties.add(SCHEMA_FIELDS() + ".#." + SCHEMA_FIELDS_FROM());

		// time attributes
		properties.add(SCHEMA_FIELDS() + ".#." + SCHEMA_FIELDS_PROCTIME());
		properties.add(SCHEMA_FIELDS() + ".#." + ROWTIME_TIMESTAMPS_TYPE());
		properties.add(SCHEMA_FIELDS() + ".#." + ROWTIME_TIMESTAMPS_FROM());
		properties.add(SCHEMA_FIELDS() + ".#." + ROWTIME_TIMESTAMPS_CLASS());
		properties.add(SCHEMA_FIELDS() + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED());
		properties.add(SCHEMA_FIELDS() + ".#." + ROWTIME_WATERMARKS_TYPE());
		properties.add(SCHEMA_FIELDS() + ".#." + ROWTIME_WATERMARKS_CLASS());
		properties.add(SCHEMA_FIELDS() + ".#." + ROWTIME_WATERMARKS_SERIALIZED());
		properties.add(SCHEMA_FIELDS() + ".#." + ROWTIME_WATERMARKS_DELAY());

		return properties;
	}

	@Override
	public TableSource<Row> create(Map<String, String> properties) {
		final DescriptorProperties params = new DescriptorProperties(true);
		params.putProperties(properties);

		// validate
		new KafkaValidator().validate(params);
		new JsonValidator().validate(params);
		new SchemaValidator(true).validate(params);

		// build
		final KafkaJsonTableSource.Builder builder = createBuilder();

		// topic
		final String topic = params
			.getOptionalString(CONNECTOR_TOPIC)
			.orElseThrow(params.errorSupplier());
		builder.forTopic(topic);

		// properties
		final Properties props = new Properties();
		final int count = params.getIndexedPropertyMap(CONNECTOR_PROPERTIES, CONNECTOR_PROPERTIES_KEY).size();
		for (int i = 0; i < count; i++) {
			final String key = params
				.getOptionalString(CONNECTOR_PROPERTIES + '.' + i + '.' + CONNECTOR_PROPERTIES_KEY)
				.orElseThrow(params.errorSupplier());
			final String value = params
				.getOptionalString(CONNECTOR_PROPERTIES + '.' + i + '.' + CONNECTOR_PROPERTIES_VALUE)
				.orElseThrow(params.errorSupplier());
			props.put(key, value);
		}
		builder.withKafkaProperties(props);

		// startup mode
		params
			.getOptionalString(CONNECTOR_STARTUP_MODE)
			.ifPresent(startupMode -> {
				switch (startupMode) {

				case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST:
					builder.fromEarliest();
					break;

				case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_LATEST:
					builder.fromLatest();
					break;

				case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_GROUP_OFFSETS:
					builder.fromGroupOffsets();
					break;

				case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS:
					final Map<String, String> partitions = params.getIndexedPropertyMap(
						CONNECTOR_SPECIFIC_OFFSETS,
						CONNECTOR_SPECIFIC_OFFSETS_PARTITION);

					final Map<KafkaTopicPartition, Long> offsetMap = new HashMap<>();
					for (int i = 0; i < partitions.size(); i++) {

						final int partition = params
							.getOptionalInt(CONNECTOR_SPECIFIC_OFFSETS + '.' + i + '.' + CONNECTOR_SPECIFIC_OFFSETS_PARTITION)
							.orElseThrow(params.errorSupplier());

						final KafkaTopicPartition topicPartition = new KafkaTopicPartition(topic, partition);

						final long offset = params
							.getOptionalLong(CONNECTOR_SPECIFIC_OFFSETS + "." + i + "." + CONNECTOR_SPECIFIC_OFFSETS_OFFSET)
							.orElseThrow(params.errorSupplier());

						offsetMap.put(topicPartition, offset);
					}
					builder.fromSpecificOffsets(offsetMap);
					break;
				}
			});

		// missing field
		params.getOptionalBoolean(FORMAT_FAIL_ON_MISSING_FIELD).ifPresent(builder::failOnMissingField);

		// json schema
		final TableSchema formatSchema;
		if (params.containsKey(FORMAT_SCHEMA)) {
			final TypeInformation<?> info = params
				.getOptionalType(FORMAT_SCHEMA)
				.orElseThrow(params.errorSupplier());
			formatSchema = TableSchema.fromTypeInfo(info);
		} else {
			final TypeInformation<?> info = JsonSchemaConverter.convert(
				params
					.getOptionalString(FORMAT_JSON_SCHEMA)
					.orElseThrow(params.errorSupplier())
			);
			formatSchema = TableSchema.fromTypeInfo(info);
		}
		builder.forJsonSchema(formatSchema);

		// schema
		final TableSchema schema = SchemaValidator.deriveSchema(params, Optional.of(formatSchema));
		builder.withSchema(schema);

		// proctime
		SchemaValidator.deriveProctimeOptional(params)
			.ifPresent(builder::withProctimeAttribute);

		// rowtime
		final List<RowtimeAttributeDescriptor> descriptors = SchemaValidator.deriveRowtimeAttributes(params);
		if (descriptors.size() > 1) {
			throw new TableException("More than one rowtime attribute is not supported yet.");
		} else if (descriptors.size() == 1) {
			final RowtimeAttributeDescriptor desc = descriptors.get(0);
			builder.withRowtimeAttribute(desc.getAttributeName(), desc.getTimestampExtractor(), desc.getWatermarkStrategy());
		}

		// field mapping
		final Map<String, String> mapping = SchemaValidator.deriveFieldMapping(params, Optional.of(formatSchema));
		builder.withTableToJsonMapping(mapping);

		return builder.build();
	}

	protected abstract KafkaJsonTableSource.Builder createBuilder();

	protected abstract String kafkaVersion();
}
