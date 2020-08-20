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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * A version-agnostic Kafka {@link ScanTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaConsumer(List, Properties, DeserializationSchema)}} and
 *  {@link #createKafkaConsumer(Pattern, Properties, DeserializationSchema)}}.
 */
@Internal
public abstract class KafkaDynamicSourceBase implements ScanTableSource {

	// --------------------------------------------------------------------------------------------
	// Common attributes
	// --------------------------------------------------------------------------------------------
	protected final DataType outputDataType;

	// --------------------------------------------------------------------------------------------
	// Scan format attributes
	// --------------------------------------------------------------------------------------------

	/** Scan format for decoding records from Kafka. */
	protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

	// --------------------------------------------------------------------------------------------
	// Kafka-specific attributes
	// --------------------------------------------------------------------------------------------

	/** The Kafka topics to consume. */
	protected final List<String> topics;

	/** The Kafka topic pattern to consume. */
	protected final Pattern topicPattern;

	/** Properties for the Kafka consumer. */
	protected final Properties properties;

	/** The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
	protected final StartupMode startupMode;

	/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
	protected final Map<KafkaTopicPartition, Long> specificStartupOffsets;

	/** The start timestamp to locate partition offsets; only relevant when startup mode is {@link StartupMode#TIMESTAMP}.*/
	protected final long startupTimestampMillis;

	/** The default value when startup timestamp is not used.*/
	private static final long DEFAULT_STARTUP_TIMESTAMP_MILLIS = 0L;

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 * @param outputDataType         Source produced data type
	 * @param topics                 Kafka topics to consume.
	 * @param topicPattern           Kafka topic pattern to consume.
	 * @param properties             Properties for the Kafka consumer.
	 * @param decodingFormat         Decoding format for decoding records from Kafka.
	 * @param startupMode            Startup mode for the contained consumer.
	 * @param specificStartupOffsets Specific startup offsets; only relevant when startup
	 *                               mode is {@link StartupMode#SPECIFIC_OFFSETS}.
	 * @param startupTimestampMillis Startup timestamp for offsets; only relevant when startup
	 *                               mode is {@link StartupMode#TIMESTAMP}.
	 */
	protected KafkaDynamicSourceBase(
			DataType outputDataType,
			@Nullable List<String> topics,
			@Nullable Pattern topicPattern,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis) {
		this.outputDataType = Preconditions.checkNotNull(
				outputDataType, "Produced data type must not be null.");
		Preconditions.checkArgument((topics != null && topicPattern == null) ||
				(topics == null && topicPattern != null),
			"Either Topic or Topic Pattern must be set for source.");
		this.topics = topics;
		this.topicPattern = topicPattern;
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.decodingFormat = Preconditions.checkNotNull(
			decodingFormat, "Decoding format must not be null.");
		this.startupMode = Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
		this.specificStartupOffsets = Preconditions.checkNotNull(
			specificStartupOffsets, "Specific offsets must not be null.");
		this.startupTimestampMillis = startupTimestampMillis;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return this.decodingFormat.getChangelogMode();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		DeserializationSchema<RowData> deserializationSchema =
				this.decodingFormat.createRuntimeDecoder(runtimeProviderContext, this.outputDataType);
		// Version-specific Kafka consumer
		FlinkKafkaConsumerBase<RowData> kafkaConsumer = getKafkaConsumer(deserializationSchema);
		return SourceFunctionProvider.of(kafkaConsumer, false);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final KafkaDynamicSourceBase that = (KafkaDynamicSourceBase) o;
		return Objects.equals(outputDataType, that.outputDataType) &&
			Objects.equals(topics, that.topics) &&
			Objects.equals(topicPattern, topicPattern) &&
			Objects.equals(properties, that.properties) &&
			Objects.equals(decodingFormat, that.decodingFormat) &&
			startupMode == that.startupMode &&
			Objects.equals(specificStartupOffsets, that.specificStartupOffsets) &&
			startupTimestampMillis == that.startupTimestampMillis;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			outputDataType,
			topics,
			topicPattern,
			properties,
			decodingFormat,
			startupMode,
			specificStartupOffsets,
			startupTimestampMillis);
	}

	// --------------------------------------------------------------------------------------------
	// Abstract methods for subclasses
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a version-specific Kafka consumer.
	 *
	 * @param topics                Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected abstract FlinkKafkaConsumerBase<RowData> createKafkaConsumer(
			List<String> topics,
			Properties properties,
			DeserializationSchema<RowData> deserializationSchema);

	/**
	 * Creates a version-specific Kafka consumer.
	 *
	 * @param topicPattern          afka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected abstract FlinkKafkaConsumerBase<RowData> createKafkaConsumer(
			Pattern topicPattern,
			Properties properties,
			DeserializationSchema<RowData> deserializationSchema);

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns a version-specific Kafka consumer with the start position configured.

	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected FlinkKafkaConsumerBase<RowData> getKafkaConsumer(DeserializationSchema<RowData> deserializationSchema) {
		FlinkKafkaConsumerBase<RowData> kafkaConsumer = topics != null ?
			createKafkaConsumer(topics, properties, deserializationSchema) :
			createKafkaConsumer(topicPattern, properties, deserializationSchema);

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
			case TIMESTAMP:
				kafkaConsumer.setStartFromTimestamp(startupTimestampMillis);
				break;
			}
		kafkaConsumer.setCommitOffsetsOnCheckpoints(properties.getProperty("group.id") != null);
		return kafkaConsumer;
	}
}
