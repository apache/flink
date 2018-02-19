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
import java.util.Properties;

import scala.Option;

/**
 * A version-agnostic Kafka {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaConsumer(String, Properties, DeserializationSchema)}}.
 */
@Internal
public abstract class KafkaTableSource
	implements StreamTableSource<Row>, DefinedProctimeAttribute, DefinedRowtimeAttributes {

	/** The schema of the table. */
	private final TableSchema schema;

	/** The Kafka topic to consume. */
	private final String topic;

	/** Properties for the Kafka consumer. */
	private final Properties properties;

	/** Type information describing the result type. */
	private TypeInformation<Row> returnType;

	/** Field name of the processing time attribute, null if no processing time field is defined. */
	private String proctimeAttribute;

	/** Descriptor for a rowtime attribute. */
	private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

	/** The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
	private StartupMode startupMode;

	/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
	private Map<KafkaTopicPartition, Long> specificStartupOffsets;

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param schema                Schema of the produced table.
	 * @param returnType            Type information of the produced physical DataStream.
	 */
	protected KafkaTableSource(
			String topic,
			Properties properties,
			TableSchema schema,
			TypeInformation<Row> returnType) {

		this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.schema = Preconditions.checkNotNull(schema, "Schema must not be null.");
		this.returnType = Preconditions.checkNotNull(returnType, "Type information must not be null.");
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
		return returnType;
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public String getProctimeAttribute() {
		return proctimeAttribute;
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		return rowtimeAttributeDescriptors;
	}

	@Override
	public String explainSource() {
		return TableConnectorUtil.generateRuntimeName(this.getClass(), schema.getColumnNames());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof KafkaTableSource)) {
			return false;
		}
		KafkaTableSource that = (KafkaTableSource) o;
		return Objects.equals(schema, that.schema) &&
			Objects.equals(topic, that.topic) &&
			Objects.equals(properties, that.properties) &&
			Objects.equals(returnType, that.returnType) &&
			Objects.equals(proctimeAttribute, that.proctimeAttribute) &&
			Objects.equals(rowtimeAttributeDescriptors, that.rowtimeAttributeDescriptors) &&
			startupMode == that.startupMode &&
			Objects.equals(specificStartupOffsets, that.specificStartupOffsets);
	}

	@Override
	public int hashCode() {
		return Objects.hash(schema, topic, properties, returnType,
			proctimeAttribute, rowtimeAttributeDescriptors, startupMode, specificStartupOffsets);
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

	//////// SETTERS FOR OPTIONAL PARAMETERS

	/**
	 * Declares a field of the schema to be the processing time attribute.
	 *
	 * @param proctimeAttribute The name of the field that becomes the processing time field.
	 */
	protected void setProctimeAttribute(String proctimeAttribute) {
		if (proctimeAttribute != null) {
			// validate that field exists and is of correct type
			Option<TypeInformation<?>> tpe = schema.getType(proctimeAttribute);
			if (tpe.isEmpty()) {
				throw new ValidationException("Processing time attribute '" + proctimeAttribute + "' is not present in TableSchema.");
			} else if (tpe.get() != Types.SQL_TIMESTAMP()) {
				throw new ValidationException("Processing time attribute '" + proctimeAttribute + "' is not of type SQL_TIMESTAMP.");
			}
		}
		this.proctimeAttribute = proctimeAttribute;
	}

	/**
	 * Declares a list of fields to be rowtime attributes.
	 *
	 * @param rowtimeAttributeDescriptors The descriptors of the rowtime attributes.
	 */
	protected void setRowtimeAttributeDescriptors(List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
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
		this.rowtimeAttributeDescriptors = rowtimeAttributeDescriptors;
	}

	/**
	 * Sets the startup mode of the TableSource.
	 *
	 * @param startupMode The startup mode.
	 */
	protected void setStartupMode(StartupMode startupMode) {
		this.startupMode = startupMode;
	}

	/**
	 * Sets the startup offsets of the TableSource; only relevant when the startup mode is {@link StartupMode#SPECIFIC_OFFSETS}.
	 *
	 * @param specificStartupOffsets The startup offsets for different partitions.
	 */
	protected void setSpecificStartupOffsets(Map<KafkaTopicPartition, Long> specificStartupOffsets) {
		this.specificStartupOffsets = specificStartupOffsets;
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
	 * Returns the deserialization schema.
	 *
	 * @return The deserialization schema
	 */
	protected abstract DeserializationSchema<Row> getDeserializationSchema();

	/**
	 * Abstract builder for a {@link KafkaTableSource} to be extended by builders of subclasses of
	 * KafkaTableSource.
	 *
	 * @param <T> Type of the KafkaTableSource produced by the builder.
	 * @param <B> Type of the KafkaTableSource.Builder subclass.
	 */
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
		 */
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
		 */
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
		 */
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
		 */
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
		 */
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
		 */
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
		 */
		public B fromEarliest() {
			this.startupMode = StartupMode.EARLIEST;
			this.specificStartupOffsets = null;
			return builder();
		}

		/**
		 * Configures the TableSource to start reading from the latest offset for all partitions.
		 *
		 * @see FlinkKafkaConsumerBase#setStartFromLatest()
		 */
		public B fromLatest() {
			this.startupMode = StartupMode.LATEST;
			this.specificStartupOffsets = null;
			return builder();
		}

		/**
		 * Configures the TableSource to start reading from any committed group offsets found in Zookeeper / Kafka brokers.
		 *
		 * @see FlinkKafkaConsumerBase#setStartFromGroupOffsets()
		 */
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
		 */
		public B fromSpecificOffsets(Map<KafkaTopicPartition, Long> specificStartupOffsets) {
			this.startupMode = StartupMode.SPECIFIC_OFFSETS;
			this.specificStartupOffsets = Preconditions.checkNotNull(specificStartupOffsets);
			return builder();
		}

		/**
		 * Returns the configured topic.
		 *
		 * @return the configured topic.
		 */
		protected String getTopic() {
			return this.topic;
		}

		/**
		 * Returns the configured Kafka properties.
		 *
		 * @return the configured Kafka properties.
		 */
		protected Properties getKafkaProps() {
			return this.kafkaProps;
		}

		/**
		 * Returns the configured table schema.
		 *
		 * @return the configured table schema.
		 */
		protected TableSchema getTableSchema() {
			return this.schema;
		}

		/**
		 * True if the KafkaSource supports Kafka timestamps, false otherwise.
		 *
		 * @return True if the KafkaSource supports Kafka timestamps, false otherwise.
		 */
		protected abstract boolean supportsKafkaTimestamps();

		/**
		 * Configures a TableSource with optional parameters.
		 *
		 * @param tableSource The TableSource to configure.
		 */
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
		 */
		protected abstract B builder();

		/**
		 * Builds the configured {@link KafkaTableSource}.
		 * @return The configured {@link KafkaTableSource}.
		 */
		protected abstract KafkaTableSource build();
	}
}
