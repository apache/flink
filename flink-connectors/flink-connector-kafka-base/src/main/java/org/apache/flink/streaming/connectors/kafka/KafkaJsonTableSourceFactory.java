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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA;
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
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_TYPE;

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
		properties.add(FORMAT_DERIVE_SCHEMA());

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

		return properties;
	}

	@Override
	public TableSource<Row> create(Map<String, String> properties) {
		final DescriptorProperties params = new DescriptorProperties(true);
		params.putProperties(properties);

		// validate
		new SchemaValidator(true).validate(params);
		new KafkaValidator().validate(params);
		new JsonValidator().validate(params);

		// build
		final KafkaJsonTableSource.Builder builder = createBuilder();

		// topic
		final String topic = params.getString(CONNECTOR_TOPIC);
		builder.forTopic(topic);

		// properties
		final Properties props = new Properties();
		final List<Map<String, String>> propsList = params.getFixedIndexedProperties(
			CONNECTOR_PROPERTIES,
			Arrays.asList(CONNECTOR_PROPERTIES_KEY, CONNECTOR_PROPERTIES_VALUE));
		propsList.forEach(kv -> props.put(
			params.getString(kv.get(CONNECTOR_PROPERTIES_KEY)),
			params.getString(kv.get(CONNECTOR_PROPERTIES_VALUE))
		));
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
					final Map<KafkaTopicPartition, Long> offsetMap = new HashMap<>();

					final List<Map<String, String>> offsetList = params.getFixedIndexedProperties(
						CONNECTOR_SPECIFIC_OFFSETS,
						Arrays.asList(CONNECTOR_SPECIFIC_OFFSETS_PARTITION, CONNECTOR_SPECIFIC_OFFSETS_OFFSET));
					offsetList.forEach(kv -> {
						final int partition = params.getInt(kv.get(CONNECTOR_SPECIFIC_OFFSETS_PARTITION));
						final long offset = params.getLong(kv.get(CONNECTOR_SPECIFIC_OFFSETS_OFFSET));
						final KafkaTopicPartition topicPartition = new KafkaTopicPartition(topic, partition);
						offsetMap.put(topicPartition, offset);
					});
					builder.fromSpecificOffsets(offsetMap);
					break;
				}
			});

		// missing field
		params.getOptionalBoolean(FORMAT_FAIL_ON_MISSING_FIELD).ifPresent(builder::failOnMissingField);

		// json schema
		final TableSchema formatSchema;
		if (params.containsKey(FORMAT_SCHEMA)) {
			final TypeInformation<?> info = params.getType(FORMAT_SCHEMA);
			formatSchema = TableSchema.fromTypeInfo(info);
		} else if (params.containsKey(FORMAT_JSON_SCHEMA)) {
			final TypeInformation<?> info = JsonSchemaConverter.convert(params.getString(FORMAT_JSON_SCHEMA));
			formatSchema = TableSchema.fromTypeInfo(info);
		} else {
			formatSchema = SchemaValidator.deriveFormatFields(params);
		}
		builder.forJsonSchema(formatSchema);

		// schema
		final TableSchema schema = params.getTableSchema(SCHEMA());
		builder.withSchema(schema);

		// proctime
		SchemaValidator.deriveProctimeAttribute(params).ifPresent(builder::withProctimeAttribute);

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
