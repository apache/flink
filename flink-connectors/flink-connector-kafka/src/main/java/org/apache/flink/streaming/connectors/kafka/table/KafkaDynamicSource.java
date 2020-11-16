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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A version-agnostic Kafka {@link ScanTableSource}.
 */
@Internal
public class KafkaDynamicSource implements ScanTableSource, SupportsReadingMetadata, SupportsWatermarkPushDown {

	// --------------------------------------------------------------------------------------------
	// Mutable attributes
	// --------------------------------------------------------------------------------------------

	/** Data type that describes the final output of the source. */
	protected DataType producedDataType;

	/** Metadata that is appended at the end of a physical source row. */
	protected List<String> metadataKeys;

	/** Watermark strategy that is used to generate per-partition watermark. */
	protected @Nullable WatermarkStrategy<RowData> watermarkStrategy;

	// --------------------------------------------------------------------------------------------
	// Format attributes
	// --------------------------------------------------------------------------------------------

	private static final String VALUE_METADATA_PREFIX = "value.";

	/** Data type to configure the formats. */
	protected final DataType physicalDataType;

	/** Optional format for decoding keys from Kafka. */
	protected final @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat;

	/** Format for decoding values from Kafka. */
	protected final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

	/** Indices that determine the key fields and the target position in the produced row. */
	protected final int[] keyProjection;

	/** Indices that determine the value fields and the target position in the produced row. */
	protected final int[] valueProjection;

	/** Prefix that needs to be removed from fields when constructing the physical data type. */
	protected final @Nullable String keyPrefix;

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

	/** Flag to determine source mode. In upsert mode, it will keep the tombstone message. **/
	protected final boolean upsertMode;

	public KafkaDynamicSource(
			DataType physicalDataType,
			@Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
			DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
			int[] keyProjection,
			int[] valueProjection,
			@Nullable String keyPrefix,
			@Nullable List<String> topics,
			@Nullable Pattern topicPattern,
			Properties properties,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis,
			boolean upsertMode) {
		// Format attributes
		this.physicalDataType = Preconditions.checkNotNull(physicalDataType, "Physical data type must not be null.");
		this.keyDecodingFormat = keyDecodingFormat;
		this.valueDecodingFormat = Preconditions.checkNotNull(
				valueDecodingFormat, "Value decoding format must not be null.");
		this.keyProjection = Preconditions.checkNotNull(keyProjection, "Key projection must not be null.");
		this.valueProjection = Preconditions.checkNotNull(valueProjection, "Value projection must not be null.");
		this.keyPrefix = keyPrefix;
		// Mutable attributes
		this.producedDataType = physicalDataType;
		this.metadataKeys = Collections.emptyList();
		this.watermarkStrategy = null;
		// Kafka-specific attributes
		Preconditions.checkArgument((topics != null && topicPattern == null) ||
				(topics == null && topicPattern != null),
			"Either Topic or Topic Pattern must be set for source.");
		this.topics = topics;
		this.topicPattern = topicPattern;
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.startupMode = Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
		this.specificStartupOffsets = Preconditions.checkNotNull(
			specificStartupOffsets, "Specific offsets must not be null.");
		this.startupTimestampMillis = startupTimestampMillis;
		this.upsertMode = upsertMode;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return valueDecodingFormat.getChangelogMode();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
		final DeserializationSchema<RowData> keyDeserialization =
				createDeserialization(context, keyDecodingFormat, keyProjection, keyPrefix);

		final DeserializationSchema<RowData> valueDeserialization =
				createDeserialization(context, valueDecodingFormat, valueProjection, null);

		final TypeInformation<RowData> producedTypeInfo =
				context.createTypeInformation(producedDataType);

		final FlinkKafkaConsumer<RowData> kafkaConsumer =
				createKafkaConsumer(keyDeserialization, valueDeserialization, producedTypeInfo);

		return SourceFunctionProvider.of(kafkaConsumer, false);
	}

	@Override
	public Map<String, DataType> listReadableMetadata() {
		final Map<String, DataType> metadataMap = new LinkedHashMap<>();

		// according to convention, the order of the final row must be
		// PHYSICAL + FORMAT METADATA + CONNECTOR METADATA
		// where the format metadata has highest precedence

		// add value format metadata with prefix
		valueDecodingFormat
			.listReadableMetadata()
			.forEach((key, value) -> metadataMap.put(VALUE_METADATA_PREFIX + key, value));

		// add connector metadata
		Stream.of(ReadableMetadata.values())
			.forEachOrdered(m -> metadataMap.putIfAbsent(m.key, m.dataType));

		return metadataMap;
	}

	@Override
	public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
		// separate connector and format metadata
		final List<String> formatMetadataKeys = metadataKeys.stream()
			.filter(k -> k.startsWith(VALUE_METADATA_PREFIX))
			.collect(Collectors.toList());
		final List<String> connectorMetadataKeys = new ArrayList<>(metadataKeys);
		connectorMetadataKeys.removeAll(formatMetadataKeys);

		// push down format metadata
		final Map<String, DataType> formatMetadata = valueDecodingFormat.listReadableMetadata();
		if (formatMetadata.size() > 0) {
			final List<String> requestedFormatMetadataKeys = formatMetadataKeys.stream()
				.map(k -> k.substring(VALUE_METADATA_PREFIX.length()))
				.collect(Collectors.toList());
			valueDecodingFormat.applyReadableMetadata(requestedFormatMetadataKeys);
		}

		this.metadataKeys = connectorMetadataKeys;
		this.producedDataType = producedDataType;
	}

	@Override
	public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
		this.watermarkStrategy = watermarkStrategy;
	}

	@Override
	public DynamicTableSource copy() {
		final KafkaDynamicSource copy = new KafkaDynamicSource(
				physicalDataType,
				keyDecodingFormat,
				valueDecodingFormat,
				keyProjection,
				valueProjection,
				keyPrefix,
				topics,
				topicPattern,
				properties,
				startupMode,
				specificStartupOffsets,
				startupTimestampMillis,
				upsertMode);
		copy.producedDataType = producedDataType;
		copy.metadataKeys = metadataKeys;
		copy.watermarkStrategy = watermarkStrategy;
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
			Objects.equals(keyDecodingFormat, that.keyDecodingFormat) &&
			Objects.equals(valueDecodingFormat, that.valueDecodingFormat) &&
			Arrays.equals(keyProjection, that.keyProjection) &&
			Arrays.equals(valueProjection, that.valueProjection) &&
			Objects.equals(keyPrefix, that.keyPrefix) &&
			Objects.equals(topics, that.topics) &&
			Objects.equals(String.valueOf(topicPattern), String.valueOf(that.topicPattern)) &&
			Objects.equals(properties, that.properties) &&
			startupMode == that.startupMode &&
			Objects.equals(specificStartupOffsets, that.specificStartupOffsets) &&
			startupTimestampMillis == that.startupTimestampMillis &&
			Objects.equals(upsertMode, that.upsertMode) &&
			Objects.equals(watermarkStrategy, that.watermarkStrategy);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			producedDataType,
			metadataKeys,
			physicalDataType,
			keyDecodingFormat,
			valueDecodingFormat,
			keyProjection,
			valueProjection,
			keyPrefix,
			topics,
			topicPattern,
			properties,
			startupMode,
			specificStartupOffsets,
			startupTimestampMillis,
			upsertMode,
			watermarkStrategy);
	}

	// --------------------------------------------------------------------------------------------

	protected FlinkKafkaConsumer<RowData> createKafkaConsumer(
			DeserializationSchema<RowData> keyDeserialization,
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

		// check if connector metadata is used at all
		final boolean hasMetadata = metadataKeys.size() > 0;

		// adjust physical arity with value format's metadata
		final int adjustedPhysicalArity = producedDataType.getChildren().size() - metadataKeys.size();

		// adjust value format projection to include value format's metadata columns at the end
		final int[] adjustedValueProjection = IntStream.concat(
				IntStream.of(valueProjection),
				IntStream.range(keyProjection.length + valueProjection.length, adjustedPhysicalArity))
			.toArray();

		final KafkaDeserializationSchema<RowData> kafkaDeserializer = new DynamicKafkaDeserializationSchema(
				adjustedPhysicalArity,
				keyDeserialization,
				keyProjection,
				valueDeserialization,
				adjustedValueProjection,
				hasMetadata,
				metadataConverters,
				producedTypeInfo,
				upsertMode);

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

		if (watermarkStrategy != null) {
			kafkaConsumer.assignTimestampsAndWatermarks(watermarkStrategy);
		}
		return kafkaConsumer;
	}

	private @Nullable DeserializationSchema<RowData> createDeserialization(
			DynamicTableSource.Context context,
			@Nullable DecodingFormat<DeserializationSchema<RowData>> format,
			int[] projection,
			@Nullable String prefix) {
		if (format == null) {
			return null;
		}
		DataType physicalFormatDataType = DataTypeUtils.projectRow(this.physicalDataType, projection);
		if (prefix != null) {
			physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
		}
		return format.createRuntimeDecoder(context, physicalFormatDataType);
	}

	// --------------------------------------------------------------------------------------------
	// Metadata handling
	// --------------------------------------------------------------------------------------------

	enum ReadableMetadata {
		TOPIC(
			"topic",
			DataTypes.STRING().notNull(),
			new MetadataConverter() {
				private static final long serialVersionUID = 1L;
				@Override
				public Object read(ConsumerRecord<?, ?> record) {
					return StringData.fromString(record.topic());
				}
			}
		),

		PARTITION(
			"partition",
			DataTypes.INT().notNull(),
			new MetadataConverter() {
				private static final long serialVersionUID = 1L;
				@Override
				public Object read(ConsumerRecord<?, ?> record) {
					return record.partition();
				}
			}
		),

		HEADERS(
			"headers",
			// key and value of the map are nullable to make handling easier in queries
			DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable()).notNull(),
			new MetadataConverter() {
				private static final long serialVersionUID = 1L;
				@Override
				public Object read(ConsumerRecord<?, ?> record) {
					final Map<StringData, byte[]> map = new HashMap<>();
					for (Header header : record.headers()) {
						map.put(StringData.fromString(header.key()), header.value());
					}
					return new GenericMapData(map);
				}
			}
		),

		LEADER_EPOCH(
			"leader-epoch",
			DataTypes.INT().nullable(),
			new MetadataConverter() {
				private static final long serialVersionUID = 1L;
				@Override
				public Object read(ConsumerRecord<?, ?> record) {
					return record.leaderEpoch().orElse(null);
				}
			}
		),

		OFFSET(
			"offset",
			DataTypes.BIGINT().notNull(),
			new MetadataConverter() {
				private static final long serialVersionUID = 1L;
				@Override
				public Object read(ConsumerRecord<?, ?> record) {
					return record.offset();
				}
			}
		),

		TIMESTAMP(
			"timestamp",
			DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
			new MetadataConverter() {
				private static final long serialVersionUID = 1L;
				@Override
				public Object read(ConsumerRecord<?, ?> record) {
					return TimestampData.fromEpochMillis(record.timestamp());
				}
			}
		),

		TIMESTAMP_TYPE(
			"timestamp-type",
			DataTypes.STRING().notNull(),
			new MetadataConverter() {
				private static final long serialVersionUID = 1L;
				@Override
				public Object read(ConsumerRecord<?, ?> record) {
					return StringData.fromString(record.timestampType().toString());
				}
			}
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
