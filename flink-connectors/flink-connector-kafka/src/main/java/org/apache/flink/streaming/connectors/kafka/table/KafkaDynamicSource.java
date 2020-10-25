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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaDeserializationSchema.MetadataConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A version-agnostic Kafka {@link ScanTableSource}.
 */
@Internal
public class KafkaDynamicSource implements ScanTableSource, SupportsReadingMetadata {

	// --------------------------------------------------------------------------------------------
	// Mutable attributes
	// --------------------------------------------------------------------------------------------

	/** Data type that describes the final output of the source. */
	protected DataType producedDataType;

	/** Metadata that is appended at the end of a physical source row. */
	protected List<String> metadataKeys;

	// --------------------------------------------------------------------------------------------
	// Format attributes
	// --------------------------------------------------------------------------------------------

	/** Scan format for decoding records from Kafka. */
	protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

	/** Data type to configure the format. */
	protected final DataType physicalDataType;

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

	public KafkaDynamicSource(
			DataType physicalDataType,
			@Nullable List<String> topics,
			@Nullable Pattern topicPattern,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis) {
		this.physicalDataType = Preconditions.checkNotNull(physicalDataType, "Physical data type must not be null.");
		this.producedDataType = physicalDataType;
		this.metadataKeys = Collections.emptyList();
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
		final DeserializationSchema<RowData> valueDeserialization =
				decodingFormat.createRuntimeDecoder(runtimeProviderContext, physicalDataType);

		final TypeInformation<RowData> producedTypeInfo =
				runtimeProviderContext.createTypeInformation(producedDataType);

		final FlinkKafkaConsumer<RowData> kafkaConsumer = createKafkaConsumer(valueDeserialization, producedTypeInfo);

		return SourceFunctionProvider.of(kafkaConsumer, false);
	}

	@Override
	public Map<String, DataType> listReadableMetadata() {
		final Map<String, DataType> metadataMap = new LinkedHashMap<>();
		Stream.of(ReadableMetadata.values()).forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
		return metadataMap;
	}

	@Override
	public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
		this.metadataKeys = metadataKeys;
		this.producedDataType = producedDataType;
	}

	@Override
	public DynamicTableSource copy() {
		final KafkaDynamicSource copy = new KafkaDynamicSource(
				this.physicalDataType,
				this.topics,
				this.topicPattern,
				this.properties,
				this.decodingFormat,
				this.startupMode,
				this.specificStartupOffsets,
				this.startupTimestampMillis);
		copy.producedDataType = producedDataType;
		copy.metadataKeys = metadataKeys;
		return copy;
	}

	@Override
	public String asSummaryString() {
		return "Kafka table source";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final KafkaDynamicSource that = (KafkaDynamicSource) o;
		return Objects.equals(producedDataType, that.producedDataType) &&
			Objects.equals(metadataKeys, that.metadataKeys) &&
			Objects.equals(physicalDataType, that.physicalDataType) &&
			Objects.equals(topics, that.topics) &&
			Objects.equals(String.valueOf(topicPattern), String.valueOf(that.topicPattern)) &&
			Objects.equals(properties, that.properties) &&
			Objects.equals(decodingFormat, that.decodingFormat) &&
			startupMode == that.startupMode &&
			Objects.equals(specificStartupOffsets, that.specificStartupOffsets) &&
			startupTimestampMillis == that.startupTimestampMillis;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			producedDataType,
			metadataKeys,
			physicalDataType,
			topics,
			topicPattern,
			properties,
			decodingFormat,
			startupMode,
			specificStartupOffsets,
			startupTimestampMillis);
	}

	// --------------------------------------------------------------------------------------------

	protected FlinkKafkaConsumer<RowData> createKafkaConsumer(
			DeserializationSchema<RowData> valueDeserialization,
			TypeInformation<RowData> producedTypeInfo) {

		final MetadataConverter[] metadataConverters = metadataKeys.stream()
				.map(k ->
						Stream.of(ReadableMetadata.values())
							.filter(rm -> rm.key.equals(k))
							.findFirst()
							.orElseThrow(IllegalStateException::new))
				.map(m -> m.converter)
				.toArray(MetadataConverter[]::new);

		final KafkaDeserializationSchema<RowData> kafkaDeserializer = new DynamicKafkaDeserializationSchema(
				valueDeserialization,
				metadataKeys.size() > 0,
				metadataConverters,
				producedTypeInfo);

		final FlinkKafkaConsumer<RowData> kafkaConsumer;
		if (topics != null) {
			kafkaConsumer = new FlinkKafkaConsumer<>(topics, kafkaDeserializer, properties);
		} else {
			kafkaConsumer = new FlinkKafkaConsumer<>(topicPattern, kafkaDeserializer, properties);
		}

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

	// --------------------------------------------------------------------------------------------
	// Metadata handling
	// --------------------------------------------------------------------------------------------

	enum ReadableMetadata {
		TOPIC(
			"topic",
			DataTypes.STRING().notNull(),
			record -> StringData.fromString(record.topic())
		),

		PARTITION(
			"partition",
			DataTypes.INT().notNull(),
			ConsumerRecord::partition
		),

		HEADERS(
			"headers",
			// key and value of the map are nullable to make handling easier in queries
			DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable()).notNull(),
			record -> {
				final Map<StringData, byte[]> map = new HashMap<>();
				for (Header header : record.headers()) {
					map.put(StringData.fromString(header.key()), header.value());
				}
				return new GenericMapData(map);
			}
		),

		LEADER_EPOCH(
			"leader-epoch",
			DataTypes.INT().nullable(),
			record -> record.leaderEpoch().orElse(null)
		),

		OFFSET(
			"offset",
			DataTypes.BIGINT().notNull(),
			ConsumerRecord::offset),

		TIMESTAMP(
			"timestamp",
			DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
			record -> TimestampData.fromEpochMillis(record.timestamp())),

		TIMESTAMP_TYPE(
			"timestamp-type",
			DataTypes.STRING().notNull(),
			record -> StringData.fromString(record.timestampType().toString())
		);

		final String key;

		final DataType dataType;

		final MetadataConverter converter;

		ReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
			this.key = key;
			this.dataType = dataType;
			this.converter = converter;
		}
	}
}
