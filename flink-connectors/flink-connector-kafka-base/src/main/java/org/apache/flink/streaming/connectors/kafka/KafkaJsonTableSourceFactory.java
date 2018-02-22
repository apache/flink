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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JsonValidator;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_VERSION;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_SCHEMA_STRING;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_TYPE_VALUE;
import static org.apache.flink.table.descriptors.KafkaValidator.BOOTSTRAP_SERVERS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TYPE_VALUE;
import static org.apache.flink.table.descriptors.KafkaValidator.GROUP_ID;
import static org.apache.flink.table.descriptors.KafkaValidator.JSON_FIELD;
import static org.apache.flink.table.descriptors.KafkaValidator.KAFKA_VERSION;
import static org.apache.flink.table.descriptors.KafkaValidator.OFFSET;
import static org.apache.flink.table.descriptors.KafkaValidator.PARTITION;
import static org.apache.flink.table.descriptors.KafkaValidator.SPECIFIC_OFFSETS;
import static org.apache.flink.table.descriptors.KafkaValidator.STARTUP_MODE;
import static org.apache.flink.table.descriptors.KafkaValidator.STARTUP_MODE_VALUE_EARLIEST;
import static org.apache.flink.table.descriptors.KafkaValidator.STARTUP_MODE_VALUE_GROUP_OFFSETS;
import static org.apache.flink.table.descriptors.KafkaValidator.STARTUP_MODE_VALUE_LATEST;
import static org.apache.flink.table.descriptors.KafkaValidator.STARTUP_MODE_VALUE_SPECIFIC_OFFSETS;
import static org.apache.flink.table.descriptors.KafkaValidator.TABLE_FIELD;
import static org.apache.flink.table.descriptors.KafkaValidator.TABLE_JSON_MAPPING;
import static org.apache.flink.table.descriptors.KafkaValidator.TOPIC;
import static org.apache.flink.table.descriptors.KafkaValidator.ZOOKEEPER_CONNECT;
import static org.apache.flink.table.descriptors.SchemaValidator.PROCTIME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_VERSION;

import scala.Option;
import scala.collection.JavaConversions;

/**
 * Factory for creating configured instances of {@link KafkaJsonTableSource}.
 */
public abstract class KafkaJsonTableSourceFactory implements TableSourceFactory<Row> {
	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE(), CONNECTOR_TYPE_VALUE); // kafka connector
		context.put(FORMAT_TYPE(), FORMAT_TYPE_VALUE()); // Json format
		context.put(KAFKA_VERSION, kafkaVersion()); // for different implementations
		context.put(CONNECTOR_VERSION(), "1");
		context.put(FORMAT_VERSION(), "1");
		context.put(SCHEMA_VERSION(), "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// kafka
		properties.add(KAFKA_VERSION);
		properties.add(BOOTSTRAP_SERVERS);
		properties.add(GROUP_ID);
		properties.add(ZOOKEEPER_CONNECT);
		properties.add(TOPIC);
		properties.add(STARTUP_MODE);
		properties.add(SPECIFIC_OFFSETS + ".#." + PARTITION);
		properties.add(SPECIFIC_OFFSETS + ".#." + OFFSET);

		// json format
		properties.add(FORMAT_SCHEMA_STRING());
		properties.add(FORMAT_FAIL_ON_MISSING_FIELD());

		// table json mapping
		properties.add(TABLE_JSON_MAPPING + ".#." + TABLE_FIELD);
		properties.add(TABLE_JSON_MAPPING + ".#." + JSON_FIELD);

		// schema
		properties.add(SCHEMA() + ".#." + DescriptorProperties.TYPE());
		properties.add(SCHEMA() + ".#." + DescriptorProperties.NAME());

		// time attributes
		properties.add(SCHEMA() + ".#." + PROCTIME());
//		properties.add(SCHEMA() + ".#." + ROWTIME() + ".#." + TIMESTAMPS_CLASS());
//		properties.add(SCHEMA() + ".#." + ROWTIME() + ".#." + TIMESTAMPS_TYPE());

		return properties;
	}

	@Override
	public TableSource<Row> create(Map<String, String> properties) {
		DescriptorProperties params = new DescriptorProperties(true);
		params.putProperties(properties);

		// validate
		new KafkaValidator().validate(params);
		new JsonValidator().validate(params);
		new SchemaValidator(true).validate(params);

		// build
		KafkaJsonTableSource.Builder builder = createBuilder();
		Properties kafkaProps = new Properties();

		// Set the required parameters.
		String topic = params.getString(TOPIC).get();
		TableSchema tableSchema = params.getTableSchema(SCHEMA()).get();

		kafkaProps.put(BOOTSTRAP_SERVERS, params.getString(BOOTSTRAP_SERVERS).get());
		kafkaProps.put(GROUP_ID, params.getString(GROUP_ID).get());

		// Set the zookeeper connect for kafka 0.8.
		Option<String> zkConnect = params.getString(ZOOKEEPER_CONNECT);
		if (zkConnect.isDefined()) {
			kafkaProps.put(ZOOKEEPER_CONNECT, zkConnect.get());
		}

		builder.withKafkaProperties(kafkaProps).forTopic(topic).withSchema(tableSchema);

		// Set the startup mode.
		String startupMode = params.getString(STARTUP_MODE).get();
		if (null != startupMode) {
			switch (startupMode) {
				case STARTUP_MODE_VALUE_EARLIEST:
					builder.fromEarliest();
					break;
				case STARTUP_MODE_VALUE_LATEST:
					builder.fromLatest();
					break;
				case STARTUP_MODE_VALUE_GROUP_OFFSETS:
					builder.fromGroupOffsets();
					break;
				case STARTUP_MODE_VALUE_SPECIFIC_OFFSETS:
					Map<String, String> partitions = JavaConversions.
							mapAsJavaMap(params.getIndexedProperty(SPECIFIC_OFFSETS, PARTITION));
					Map<KafkaTopicPartition, Long> offsetMap = new HashMap<>();
					for (int i = 0; i < partitions.size(); i++) {
						offsetMap.put(
								new KafkaTopicPartition(
										topic,
										Integer.valueOf(params.getString(
												SPECIFIC_OFFSETS + "" + "." + i + "." + PARTITION).get())),
								Long.valueOf(params.getString(
										SPECIFIC_OFFSETS + "" + "." + i + "." + OFFSET).get()));
					}
					builder.fromSpecificOffsets(offsetMap);
					break;
			}
		}

		// Set whether fail on missing JSON field.
		Option<String> failOnMissing = params.getString(FORMAT_FAIL_ON_MISSING_FIELD());
		if (failOnMissing.isDefined()) {
			builder.failOnMissingField(Boolean.valueOf(failOnMissing.get()));
		}

		// Set the JSON schema.
		Option<String> jsonSchema = params.getString(FORMAT_SCHEMA_STRING());
		if (jsonSchema.isDefined()) {
			TypeInformation jsonSchemaType = JsonSchemaConverter.convert(jsonSchema.get());
			builder.forJsonSchema(TableSchema.fromTypeInfo(jsonSchemaType));
		}

		// Set the table => JSON fields mapping.
		Map<String, String>  mappingTableFields = JavaConversions.
				mapAsJavaMap(params.getIndexedProperty(TABLE_JSON_MAPPING, TABLE_FIELD));

		if (!mappingTableFields.isEmpty()) {
			Map<String, String> tableJsonMapping = new HashMap<>();
			for (int i = 0; i < mappingTableFields.size(); i++) {
				tableJsonMapping.put(params.getString(TABLE_JSON_MAPPING + "." + i + "." + TABLE_FIELD).get(),
						params.getString(TABLE_JSON_MAPPING + "." + i + "." + JSON_FIELD).get()
				);
			}
			builder.withTableToJsonMapping(tableJsonMapping);
		}

		// Set the time attributes.
		setTimeAttributes(tableSchema, params, builder);

		return builder.build();
	}

	protected abstract KafkaJsonTableSource.Builder createBuilder();

	protected abstract String kafkaVersion();

	private void setTimeAttributes(TableSchema schema, DescriptorProperties params, KafkaJsonTableSource.Builder builder) {
		// TODO to deal with rowtime fields
		Option<String> proctimeField;
		for (int i = 0; i < schema.getColumnNum(); i++) {
			proctimeField = params.getString(SCHEMA() + "." + i + "." + PROCTIME());
			if (proctimeField.isDefined()) {
				builder.withProctimeAttribute(schema.getColumnName(i).get());
			}
		}
	}
}
