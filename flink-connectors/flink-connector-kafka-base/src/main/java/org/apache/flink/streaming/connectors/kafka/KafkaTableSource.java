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
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.tsextractors.TimestampExtractor;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import scala.Option;

/**
 * A version-agnostic Kafka {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaConsumer(String, Properties, DeserializationSchema)}}.
 */
@Internal
public abstract class KafkaTableSource implements
		StreamTableSource<Row>,
		DefinedProctimeAttribute,
		DefinedRowtimeAttributes,
		DefinedFieldMapping {

	// common table source attributes
	// TODO make all attributes final once we drop support for format-specific table sources

	/** The schema of the table. */
	private final TableSchema schema;

	/** Field name of the processing time attribute, null if no processing time field is defined. */
	private Optional<String> proctimeAttribute;

	/** Descriptor for a rowtime attribute. */
	private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

	/** Mapping for the fields of the table schema to fields of the physical returned type. */
	private Optional<Map<String, String>> fieldMapping;

	// Kafka-specific attributes

	/** The Kafka topic to consume. */
	private final String topic;

	/** Properties for the Kafka consumer. */
	private final Properties properties;

	/** Deserialization schema for decoding records from Kafka. */
	private final DeserializationSchema<Row> deserializationSchema;

	/** The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
	private StartupMode startupMode;

	/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
	private Map<KafkaTopicPartition, Long> specificStartupOffsets;

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
	protected KafkaTableSource(
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
	protected KafkaTableSource(
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
		return TableConnectorUtil.generateRuntimeName(this.getClass(), schema.getColumnNames());
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
		// TODO force classes to be equal once we drop support for format-specific table sources
		// if (o == null || getClass() != o.getClass()) {
		if (o == null || !(o instanceof KafkaTableSource)) {
			return false;
		}
		final KafkaTableSource that = (KafkaTableSource) o;
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
			Option<TypeInformation<?>> tpe = schema.getType(attribute);
			if (tpe.isEmpty()) {
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
			Option<TypeInformation<?>> tpe = schema.getType(rowtimeAttribute);
			if (tpe.isEmpty()) {
				throw new ValidationException("Rowtime attribute '" + rowtimeAttribute + "' is not present in TableSchema.");
			} else if (tpe.get() != Types.SQL_TIMESTAMP()) {
				throw new ValidationException("Rowtime attribute '" + rowtimeAttribute + "' is not of type SQL_TIMESTAMP.");
			}
		}
		return rowtimeAttributeDescriptors;
	}

	//////// SETTERS FOR OPTIONAL PARAMETERS

	/**
	 * Declares a field of the schema to be the processing time attribute.
	 *
	 * @param proctimeAttribute The name of the field that becomes the processing time field.
	 * @deprecated Use table descriptors instead of implementation-specific builders.
	 */
	@Deprecated
	protected void setProctimeAttribute(String proctimeAttribute) {
		this.proctimeAttribute = validateProctimeAttribute(Optional.ofNullable(proctimeAttribute));
	}

	/**
	 * Declares a list of fields to be rowtime attributes.
	 *
	 * @param rowtimeAttributeDescriptors The descriptors of the rowtime attributes.
	 * @deprecated Use table descriptors instead of implementation-specific builders.
	 */
	@Deprecated
	protected void setRowtimeAttributeDescriptors(List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
		this.rowtimeAttributeDescriptors = validateRowtimeAttributeDescriptors(rowtimeAttributeDescriptors);
	}

	/**
	 * Sets the startup mode of the TableSource.
	 *
	 * @param startupMode The startup mode.
	 * @deprecated Use table descriptors instead of implementation-specific builders.
	 */
	@Deprecated
	protected void setStartupMode(StartupMode startupMode) {
		this.startupMode = Preconditions.checkNotNull(startupMode);
	}

	/**
	 * Sets the startup offsets of the TableSource; only relevant when the startup mode is {@link StartupMode#SPECIFIC_OFFSETS}.
	 *
	 * @param specificStartupOffsets The startup offsets for different partitions.
	 * @deprecated Use table descriptors instead of implementation-specific builders.
	 */
	@Deprecated
	protected void setSpecificStartupOffsets(Map<KafkaTopicPartition, Long> specificStartupOffsets) {
		this.specificStartupOffsets = Preconditions.checkNotNull(specificStartupOffsets);
	}

	/**
	 * Mapping for the fields of the table schema to fields of the physical returned type.
	 *
	 * @param fieldMapping The mapping from table schema fields to format schema fields.
	 * @deprecated Use table descriptors instead of implementation-specific builders.
	 */
	@Deprecated
	protected void setFieldMapping(Map<String, String> fieldMapping) {
		this.fieldMapping = Optional.ofNullable(fieldMapping);
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

	/**
	 * Abstract builder for a {@link KafkaTableSource} to be extended by builders of subclasses of
	 * KafkaTableSource.
	 *
	 * @param <T> Type of the KafkaTableSource produced by the builder.
	 * @param <B> Type of the KafkaTableSource.Builder subclass.
	 * @deprecated Use the {@link org.apache.flink.table.descriptors.Kafka} descriptor together
	 *             with descriptors for schema and format instead. Descriptors allow for
	 *             implementation-agnostic definition of tables. See also
	 *             {@link org.apache.flink.table.api.TableEnvironment#connect(ConnectorDescriptor)}.
	 */
	@Deprecated
	protected abstract static class Builder<T extends KafkaTableSource, B extends KafkaTableSource.Builder> {

		private String topic;

		private Properties kafkaProps;

		private TableSchema schema;

		private String proctimeAttribute;

		private RowtimeAttributeDescriptor rowtimeAttributeDescriptor;

		/** The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
		private StartupMode startupMode = StartupMode.GROUP_OFFSETS;

		/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
		private Map<KafkaTopicPartition, Long> specificStartupOffsets = null;

		/**
		 * Sets the topic from which the table is read.
		 *
		 * @param topic The topic from which the table is read.
		 * @return The builder.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		public B forTopic(String topic) {
			Preconditions.checkNotNull(topic, "Topic must not be null.");
			Preconditions.checkArgument(this.topic == null, "Topic has already been set.");
			this.topic = topic;
			return builder();
		}

		/**
		 * Sets the configuration properties for the Kafka consumer.
		 *
		 * @param props The configuration properties for the Kafka consumer.
		 * @return The builder.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		public B withKafkaProperties(Properties props) {
			Preconditions.checkNotNull(props, "Properties must not be null.");
			Preconditions.checkArgument(this.kafkaProps == null, "Properties have already been set.");
			this.kafkaProps = props;
			return builder();
		}

		/**
		 * Sets the schema of the produced table.
		 *
		 * @param schema The schema of the produced table.
		 * @return The builder.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		public B withSchema(TableSchema schema) {
			Preconditions.checkNotNull(schema, "Schema must not be null.");
			Preconditions.checkArgument(this.schema == null, "Schema has already been set.");
			this.schema = schema;
			return builder();
		}

		/**
		 * Configures a field of the table to be a processing time attribute.
		 * The configured field must be present in the table schema and of type {@link Types#SQL_TIMESTAMP()}.
		 *
		 * @param proctimeAttribute The name of the processing time attribute in the table schema.
		 * @return The builder.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		public B withProctimeAttribute(String proctimeAttribute) {
			Preconditions.checkNotNull(proctimeAttribute, "Proctime attribute must not be null.");
			Preconditions.checkArgument(!proctimeAttribute.isEmpty(), "Proctime attribute must not be empty.");
			Preconditions.checkArgument(this.proctimeAttribute == null, "Proctime attribute has already been set.");
			this.proctimeAttribute = proctimeAttribute;
			return builder();
		}

		/**
		 * Configures a field of the table to be a rowtime attribute.
		 * The configured field must be present in the table schema and of type {@link Types#SQL_TIMESTAMP()}.
		 *
		 * @param rowtimeAttribute The name of the rowtime attribute in the table schema.
		 * @param timestampExtractor The {@link TimestampExtractor} to extract the rowtime attribute from the physical type.
		 * @param watermarkStrategy The {@link WatermarkStrategy} to generate watermarks for the rowtime attribute.
		 * @return The builder.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		public B withRowtimeAttribute(
				String rowtimeAttribute,
				TimestampExtractor timestampExtractor,
				WatermarkStrategy watermarkStrategy) {
			Preconditions.checkNotNull(rowtimeAttribute, "Rowtime attribute must not be null.");
			Preconditions.checkArgument(!rowtimeAttribute.isEmpty(), "Rowtime attribute must not be empty.");
			Preconditions.checkNotNull(timestampExtractor, "Timestamp extractor must not be null.");
			Preconditions.checkNotNull(watermarkStrategy, "Watermark assigner must not be null.");
			Preconditions.checkArgument(this.rowtimeAttributeDescriptor == null,
				"Currently, only one rowtime attribute is supported.");

			this.rowtimeAttributeDescriptor = new RowtimeAttributeDescriptor(
				rowtimeAttribute,
				timestampExtractor,
				watermarkStrategy);
			return builder();
		}

		/**
		 * Configures the Kafka timestamp to be a rowtime attribute.
		 *
		 * <p>Note: Kafka supports message timestamps only since version 0.10.</p>
		 *
		 * @param rowtimeAttribute The name of the rowtime attribute in the table schema.
		 * @param watermarkStrategy The {@link WatermarkStrategy} to generate watermarks for the rowtime attribute.
		 * @return The builder.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		public B withKafkaTimestampAsRowtimeAttribute(
				String rowtimeAttribute,
				WatermarkStrategy watermarkStrategy) {

			Preconditions.checkNotNull(rowtimeAttribute, "Rowtime attribute must not be null.");
			Preconditions.checkArgument(!rowtimeAttribute.isEmpty(), "Rowtime attribute must not be empty.");
			Preconditions.checkNotNull(watermarkStrategy, "Watermark assigner must not be null.");
			Preconditions.checkArgument(this.rowtimeAttributeDescriptor == null,
				"Currently, only one rowtime attribute is supported.");
			Preconditions.checkArgument(supportsKafkaTimestamps(), "Kafka timestamps are only supported since Kafka 0.10.");

			this.rowtimeAttributeDescriptor = new RowtimeAttributeDescriptor(
				rowtimeAttribute,
				new StreamRecordTimestamp(),
				watermarkStrategy);
			return builder();
		}

		/**
		 * Configures the TableSource to start reading from the earliest offset for all partitions.
		 *
		 * @see FlinkKafkaConsumerBase#setStartFromEarliest()
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		public B fromEarliest() {
			this.startupMode = StartupMode.EARLIEST;
			this.specificStartupOffsets = null;
			return builder();
		}

		/**
		 * Configures the TableSource to start reading from the latest offset for all partitions.
		 *
		 * @see FlinkKafkaConsumerBase#setStartFromLatest()
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		public B fromLatest() {
			this.startupMode = StartupMode.LATEST;
			this.specificStartupOffsets = null;
			return builder();
		}

		/**
		 * Configures the TableSource to start reading from any committed group offsets found in Zookeeper / Kafka brokers.
		 *
		 * @see FlinkKafkaConsumerBase#setStartFromGroupOffsets()
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		public B fromGroupOffsets() {
			this.startupMode = StartupMode.GROUP_OFFSETS;
			this.specificStartupOffsets = null;
			return builder();
		}

		/**
		 * Configures the TableSource to start reading partitions from specific offsets, set independently for each partition.
		 *
		 * @param specificStartupOffsets the specified offsets for partitions
		 * @see FlinkKafkaConsumerBase#setStartFromSpecificOffsets(Map)
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		public B fromSpecificOffsets(Map<KafkaTopicPartition, Long> specificStartupOffsets) {
			this.startupMode = StartupMode.SPECIFIC_OFFSETS;
			this.specificStartupOffsets = Preconditions.checkNotNull(specificStartupOffsets);
			return builder();
		}

		/**
		 * Returns the configured topic.
		 *
		 * @return the configured topic.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		protected String getTopic() {
			return this.topic;
		}

		/**
		 * Returns the configured Kafka properties.
		 *
		 * @return the configured Kafka properties.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		protected Properties getKafkaProps() {
			return this.kafkaProps;
		}

		/**
		 * Returns the configured table schema.
		 *
		 * @return the configured table schema.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		protected TableSchema getTableSchema() {
			return this.schema;
		}

		/**
		 * True if the KafkaSource supports Kafka timestamps, false otherwise.
		 *
		 * @return True if the KafkaSource supports Kafka timestamps, false otherwise.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		protected abstract boolean supportsKafkaTimestamps();

		/**
		 * Configures a TableSource with optional parameters.
		 *
		 * @param tableSource The TableSource to configure.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		protected void configureTableSource(T tableSource) {
			// configure processing time attributes
			tableSource.setProctimeAttribute(proctimeAttribute);
			// configure rowtime attributes
			if (rowtimeAttributeDescriptor == null) {
				tableSource.setRowtimeAttributeDescriptors(Collections.emptyList());
			} else {
				tableSource.setRowtimeAttributeDescriptors(Collections.singletonList(rowtimeAttributeDescriptor));
			}
			tableSource.setStartupMode(startupMode);
			switch (startupMode) {
				case EARLIEST:
				case LATEST:
				case GROUP_OFFSETS:
					break;
				case SPECIFIC_OFFSETS:
					tableSource.setSpecificStartupOffsets(specificStartupOffsets);
					break;
			}
		}

		/**
		 * Returns the builder.
		 * @return the builder.
		 * @deprecated Use the {@link org.apache.flink.table.descriptors.Kafka} descriptor together
		 *             with descriptors for schema and format instead. Descriptors allow for
		 *             implementation-agnostic definition of tables. See also
		 *             {@link org.apache.flink.table.api.TableEnvironment#connect(ConnectorDescriptor)}.
		 */
		@Deprecated
		protected abstract B builder();

		/**
		 * Builds the configured {@link KafkaTableSource}.
		 * @return The configured {@link KafkaTableSource}.
		 * @deprecated Use table descriptors instead of implementation-specific builders.
		 */
		@Deprecated
		protected abstract KafkaTableSource build();
	}
}
