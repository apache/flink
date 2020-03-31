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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * A version-agnostic Kafka {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaConsumer(String, Properties, DeserializationSchema)}}.
 */
@Internal
public abstract class KafkaTableSourceBase implements
		StreamTableSource<Row>,
		DefinedProctimeAttribute,
		DefinedRowtimeAttributes,
		DefinedFieldMapping {

	// common table source attributes

	/** The schema of the table. */
	private final TableSchema schema;

	/** Field name of the processing time attribute, null if no processing time field is defined. */
	private final Optional<String> proctimeAttribute;

	/** Descriptor for a rowtime attribute. */
	private final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

	/** Mapping for the fields of the table schema to fields of the physical returned type. */
	private final Optional<Map<String, String>> fieldMapping;

	// Kafka-specific attributes

	/** The Kafka topic to consume. */
	private final String topic;

	/** Properties for the Kafka consumer. */
	private final Properties properties;

	/** Deserialization schema for decoding records from Kafka. */
	private final DeserializationSchema<Row> deserializationSchema;

	/** The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
	private final StartupMode startupMode;

	/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
	private final Map<KafkaTopicPartition, Long> specificStartupOffsets;

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
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
	protected KafkaTableSourceBase(
			TableSchema schema,
			Optional<String> proctimeAttribute,
			List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
			Optional<Map<String, String>> fieldMapping,
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets) {
		this.schema = Preconditions.checkNotNull(schema, "Schema must not be null.");
		this.proctimeAttribute = validateProctimeAttribute(proctimeAttribute);
		this.rowtimeAttributeDescriptors = validateRowtimeAttributeDescriptors(rowtimeAttributeDescriptors);
		this.fieldMapping = fieldMapping;
		this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.deserializationSchema = Preconditions.checkNotNull(
			deserializationSchema, "Deserialization schema must not be null.");
		this.startupMode = Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
		this.specificStartupOffsets = Preconditions.checkNotNull(
			specificStartupOffsets, "Specific offsets must not be null.");
	}

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 *
	 * @param schema                Schema of the produced table.
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema for decoding records from Kafka.
	 */
	protected KafkaTableSourceBase(
			TableSchema schema,
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema) {
		this(
			schema,
			Optional.empty(),
			Collections.emptyList(),
			Optional.empty(),
			topic, properties,
			deserializationSchema,
			StartupMode.GROUP_OFFSETS,
			Collections.emptyMap());
	}

	/**
	 * NOTE: This method is for internal use only for defining a TableSource.
	 *       Do not use it in Table API programs.
	 */
	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {

		DeserializationSchema<Row> deserializationSchema = getDeserializationSchema();
		// Version-specific Kafka consumer
		FlinkKafkaConsumerBase<Row> kafkaConsumer = getKafkaConsumer(topic, properties, deserializationSchema);
		return env.addSource(kafkaConsumer).name(explainSource());
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return deserializationSchema.getProducedType();
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public String getProctimeAttribute() {
		return proctimeAttribute.orElse(null);
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		return rowtimeAttributeDescriptors;
	}

	@Override
	public Map<String, String> getFieldMapping() {
		return fieldMapping.orElse(null);
	}

	@Override
	public String explainSource() {
		return TableConnectorUtils.generateRuntimeName(this.getClass(), schema.getFieldNames());
	}

	/**
	 * Returns the properties for the Kafka consumer.
	 *
	 * @return properties for the Kafka consumer.
	 */
	public Properties getProperties() {
		return properties;
	}

	/**
	 * Returns the deserialization schema.
	 *
	 * @return The deserialization schema
	 */
	public DeserializationSchema<Row> getDeserializationSchema(){
		return deserializationSchema;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final KafkaTableSourceBase that = (KafkaTableSourceBase) o;
		return Objects.equals(schema, that.schema) &&
			Objects.equals(proctimeAttribute, that.proctimeAttribute) &&
			Objects.equals(rowtimeAttributeDescriptors, that.rowtimeAttributeDescriptors) &&
			Objects.equals(fieldMapping, that.fieldMapping) &&
			Objects.equals(topic, that.topic) &&
			Objects.equals(properties, that.properties) &&
			Objects.equals(deserializationSchema, that.deserializationSchema) &&
			startupMode == that.startupMode &&
			Objects.equals(specificStartupOffsets, that.specificStartupOffsets);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			schema,
			proctimeAttribute,
			rowtimeAttributeDescriptors,
			fieldMapping,
			topic,
			properties,
			deserializationSchema,
			startupMode,
			specificStartupOffsets);
	}

	/**
	 * Returns a version-specific Kafka consumer with the start position configured.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected FlinkKafkaConsumerBase<Row> getKafkaConsumer(
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema) {
		FlinkKafkaConsumerBase<Row> kafkaConsumer =
				createKafkaConsumer(topic, properties, deserializationSchema);
		switch (startupMode) {
			case EARLIEST:
				kafkaConsumer.setStartFromEarliest();
				break;
			case LATEST:
				kafkaConsumer.setStartFromLatest();
				break;
			case GROUP_OFFSETS:
				kafkaConsumer.setStartFromGroupOffsets();
				break;
			case SPECIFIC_OFFSETS:
				kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);
				break;
		}
		return kafkaConsumer;
	}

	//////// VALIDATION FOR PARAMETERS

	/**
	 * Validates a field of the schema to be the processing time attribute.
	 *
	 * @param proctimeAttribute The name of the field that becomes the processing time field.
	 */
	private Optional<String> validateProctimeAttribute(Optional<String> proctimeAttribute) {
		return proctimeAttribute.map((attribute) -> {
			// validate that field exists and is of correct type
			Optional<TypeInformation<?>> tpe = schema.getFieldType(attribute);
			if (!tpe.isPresent()) {
				throw new ValidationException("Processing time attribute '" + attribute + "' is not present in TableSchema.");
			} else if (tpe.get() != Types.SQL_TIMESTAMP()) {
				throw new ValidationException("Processing time attribute '" + attribute + "' is not of type SQL_TIMESTAMP.");
			}
			return attribute;
		});
	}

	/**
	 * Validates a list of fields to be rowtime attributes.
	 *
	 * @param rowtimeAttributeDescriptors The descriptors of the rowtime attributes.
	 */
	private List<RowtimeAttributeDescriptor> validateRowtimeAttributeDescriptors(List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
		Preconditions.checkNotNull(rowtimeAttributeDescriptors, "List of rowtime attributes must not be null.");
		// validate that all declared fields exist and are of correct type
		for (RowtimeAttributeDescriptor desc : rowtimeAttributeDescriptors) {
			String rowtimeAttribute = desc.getAttributeName();
			Optional<TypeInformation<?>> tpe = schema.getFieldType(rowtimeAttribute);
			if (!tpe.isPresent()) {
				throw new ValidationException("Rowtime attribute '" + rowtimeAttribute + "' is not present in TableSchema.");
			} else if (tpe.get() != Types.SQL_TIMESTAMP()) {
				throw new ValidationException("Rowtime attribute '" + rowtimeAttribute + "' is not of type SQL_TIMESTAMP.");
			}
		}
		return rowtimeAttributeDescriptors;
	}

	//////// ABSTRACT METHODS FOR SUBCLASSES

	/**
	 * Creates a version-specific Kafka consumer.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected abstract FlinkKafkaConsumerBase<Row> createKafkaConsumer(
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema);

}
