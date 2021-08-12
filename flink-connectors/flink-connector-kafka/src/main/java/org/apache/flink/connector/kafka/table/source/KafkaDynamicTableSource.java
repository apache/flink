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

package org.apache.flink.connector.kafka.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.table.serde.DynamicKafkaDeserializationSchema;
import org.apache.flink.connector.kafka.table.serde.DynamicKafkaDeserializationSchema.MetadataConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
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
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A version-agnostic Kafka {@link ScanTableSource}. */
@Internal
public class KafkaDynamicTableSource
        implements ScanTableSource, SupportsReadingMetadata, SupportsWatermarkPushDown {

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

    protected final TopicPartitionSubscription topicPartitionSubscription;

    protected final OffsetsInitializer startingOffsetsInitializer;

    protected final OffsetsInitializer stoppingOffsetsInitializer;

    protected final Boundedness boundedness;

    /** Properties for the Kafka consumer. */
    protected final Properties properties;

    protected final ObjectPath objectPath;

    /** Flag to determine source mode. In upsert mode, it will keep the tombstone message. * */
    protected final boolean upsertMode;

    public KafkaDynamicTableSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            TopicPartitionSubscription topicPartitionSubscription,
            OffsetsInitializer startingOffsetsInitializer,
            @Nullable OffsetsInitializer stoppingOffsetsInitializer,
            Boundedness boundedness,
            Properties properties,
            ObjectPath objectPath,
            boolean upsertMode) {
        // Format attributes
        this.physicalDataType =
                checkNotNull(physicalDataType, "Physical data type must not be null.");
        this.keyDecodingFormat = keyDecodingFormat;
        this.valueDecodingFormat =
                checkNotNull(valueDecodingFormat, "Value decoding format must not be null.");
        this.keyProjection = checkNotNull(keyProjection, "Key projection must not be null.");
        this.valueProjection = checkNotNull(valueProjection, "Value projection must not be null.");
        this.keyPrefix = keyPrefix;
        // Mutable attributes
        this.producedDataType = physicalDataType;
        this.metadataKeys = Collections.emptyList();
        this.watermarkStrategy = WatermarkStrategy.noWatermarks();
        // Kafka-specific attributes
        this.topicPartitionSubscription = topicPartitionSubscription;
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        this.boundedness = boundedness;
        this.properties = checkNotNull(properties, "Properties must not be null.");
        this.objectPath = objectPath;
        this.upsertMode = upsertMode;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return valueDecodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final KafkaRecordDeserializationSchema<RowData> deserializer = createDeserializer(context);
        final KafkaSource<RowData> kafkaSource =
                createKafkaSource(
                        this.topicPartitionSubscription,
                        this.startingOffsetsInitializer,
                        this.stoppingOffsetsInitializer,
                        this.boundedness,
                        deserializer,
                        this.properties);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
                return execEnv.fromSource(
                        kafkaSource, watermarkStrategy, "KafkaSource-" + objectPath.getFullName());
            }

            @Override
            public boolean isBounded() {
                return kafkaSource.getBoundedness() == Boundedness.BOUNDED;
            }
        };
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
        final List<String> formatMetadataKeys =
                metadataKeys.stream()
                        .filter(k -> k.startsWith(VALUE_METADATA_PREFIX))
                        .collect(Collectors.toList());
        final List<String> connectorMetadataKeys = new ArrayList<>(metadataKeys);
        connectorMetadataKeys.removeAll(formatMetadataKeys);

        // push down format metadata
        final Map<String, DataType> formatMetadata = valueDecodingFormat.listReadableMetadata();
        if (formatMetadata.size() > 0) {
            final List<String> requestedFormatMetadataKeys =
                    formatMetadataKeys.stream()
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
        final KafkaDynamicTableSource copy =
                new KafkaDynamicTableSource(
                        physicalDataType,
                        keyDecodingFormat,
                        valueDecodingFormat,
                        keyProjection,
                        valueProjection,
                        keyPrefix,
                        topicPartitionSubscription,
                        startingOffsetsInitializer,
                        stoppingOffsetsInitializer,
                        boundedness,
                        properties,
                        objectPath,
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
        final KafkaDynamicTableSource that = (KafkaDynamicTableSource) o;
        return Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(keyDecodingFormat, that.keyDecodingFormat)
                && Objects.equals(valueDecodingFormat, that.valueDecodingFormat)
                && Arrays.equals(keyProjection, that.keyProjection)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(keyPrefix, that.keyPrefix)
                && Objects.equals(topicPartitionSubscription, that.topicPartitionSubscription)
                && Objects.equals(startingOffsetsInitializer, that.startingOffsetsInitializer)
                && Objects.equals(stoppingOffsetsInitializer, that.stoppingOffsetsInitializer)
                && Objects.equals(boundedness, that.boundedness)
                && Objects.equals(properties, that.properties)
                && Objects.equals(objectPath, that.objectPath)
                && Objects.equals(upsertMode, that.upsertMode)
                && Objects.equals(watermarkStrategy, that.watermarkStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
                metadataKeys,
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                Arrays.hashCode(keyProjection),
                Arrays.hashCode(valueProjection),
                keyPrefix,
                topicPartitionSubscription,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                boundedness,
                properties,
                upsertMode);
    }

    // --------------------------------------------------------------------------------------------

    protected KafkaRecordDeserializationSchema<RowData> createDeserializer(ScanContext context) {
        final DeserializationSchema<RowData> keyDeserialization =
                createDeserialization(context, keyDecodingFormat, keyProjection, keyPrefix);

        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(context, valueDecodingFormat, valueProjection, null);

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        final MetadataConverter[] metadataConverters =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .map(m -> m.converter)
                        .toArray(MetadataConverter[]::new);

        // check if connector metadata is used at all
        final boolean hasMetadata = metadataKeys.size() > 0;

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity =
                producedDataType.getChildren().size() - metadataKeys.size();

        // adjust value format projection to include value format's metadata columns at the end
        final int[] adjustedValueProjection =
                IntStream.concat(
                                IntStream.of(valueProjection),
                                IntStream.range(
                                        keyProjection.length + valueProjection.length,
                                        adjustedPhysicalArity))
                        .toArray();

        final DynamicKafkaDeserializationSchema dynamicKafkaDeserializationSchema =
                new DynamicKafkaDeserializationSchema(
                        adjustedPhysicalArity,
                        keyDeserialization,
                        keyProjection,
                        valueDeserialization,
                        adjustedValueProjection,
                        hasMetadata,
                        metadataConverters,
                        producedTypeInfo,
                        upsertMode);

        return KafkaRecordDeserializationSchema.of(dynamicKafkaDeserializationSchema);
    }

    protected KafkaSource<RowData> createKafkaSource(
            TopicPartitionSubscription topicPartitionSubscription,
            OffsetsInitializer startingOffsetsInitializer,
            @Nullable OffsetsInitializer stoppingOffsetsInitializer,
            Boundedness boundedness,
            KafkaRecordDeserializationSchema<RowData> deserializer,
            Properties properties) {

        // Initialize Kafka source builder
        final KafkaSourceBuilder<RowData> builder =
                KafkaSource.<RowData>builder()
                        .setDeserializer(deserializer)
                        .setStartingOffsets(startingOffsetsInitializer)
                        .setProperties(properties);

        // Subscribe to topic partitions
        switch (topicPartitionSubscription.getSubscriptionType()) {
            case TOPICS:
                builder.setTopics(topicPartitionSubscription.getTopics());
                break;
            case TOPIC_PATTERN:
                builder.setTopicPattern(topicPartitionSubscription.getTopicPattern());
                break;
            case PARTITIONS:
                builder.setPartitions(topicPartitionSubscription.getPartitions());
                break;
            default:
                throw new IllegalStateException("Unknown topic partition subscription type");
        }

        // Boundedness
        if (boundedness == Boundedness.BOUNDED) {
            // Bounded mode
            checkNotNull(
                    stoppingOffsetsInitializer,
                    "Stopping offset must be specified in bounded mode");
            builder.setBounded(stoppingOffsetsInitializer);
        } else {
            if (stoppingOffsetsInitializer != null) {
                // Unbounded mode with stopping offset
                builder.setUnbounded(stoppingOffsetsInitializer);
            }
        }

        // Additional properties
        builder.setProperties(properties);

        // Build Kafka source
        return builder.build();
    }

    private @Nullable DeserializationSchema<RowData> createDeserialization(
            Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType =
                DataTypeUtils.projectRow(this.physicalDataType, projection);
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
                }),

        PARTITION(
                "partition",
                DataTypes.INT().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return record.partition();
                    }
                }),

        HEADERS(
                "headers",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
                        .notNull(),
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
                }),

        LEADER_EPOCH(
                "leader-epoch",
                DataTypes.INT().nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return record.leaderEpoch().orElse(null);
                    }
                }),

        OFFSET(
                "offset",
                DataTypes.BIGINT().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return record.offset();
                    }
                }),

        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return TimestampData.fromEpochMillis(record.timestamp());
                    }
                }),

        TIMESTAMP_TYPE(
                "timestamp-type",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return StringData.fromString(record.timestampType().toString());
                    }
                });

        final String key;

        final DataType dataType;

        final MetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }

    /** Helper class for subscribing to Kafka topics and partitions. */
    public static class TopicPartitionSubscription {

        private final SubscriptionType subscriptionType;

        private final List<String> topics;
        private final Pattern topicPattern;
        private final Set<TopicPartition> partitions;

        private TopicPartitionSubscription(
                SubscriptionType subscriptionType,
                List<String> topics,
                Pattern topicPattern,
                Set<TopicPartition> partitions) {
            this.subscriptionType = subscriptionType;
            this.topics = topics;
            this.topicPattern = topicPattern;
            this.partitions = partitions;
        }

        public static TopicPartitionSubscription topics(List<String> topics) {
            return new TopicPartitionSubscription(SubscriptionType.TOPICS, topics, null, null);
        }

        public static TopicPartitionSubscription topicPattern(Pattern topicPattern) {
            return new TopicPartitionSubscription(
                    SubscriptionType.TOPIC_PATTERN, null, topicPattern, null);
        }

        public static TopicPartitionSubscription partitions(Set<TopicPartition> partitions) {
            return new TopicPartitionSubscription(
                    SubscriptionType.PARTITIONS, null, null, partitions);
        }

        public SubscriptionType getSubscriptionType() {
            return subscriptionType;
        }

        public List<String> getTopics() {
            return topics;
        }

        public Pattern getTopicPattern() {
            return topicPattern;
        }

        public Set<TopicPartition> getPartitions() {
            return partitions;
        }

        /** Topic / Partition subscription type. */
        public enum SubscriptionType {
            TOPICS,
            TOPIC_PATTERN,
            PARTITIONS
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TopicPartitionSubscription that = (TopicPartitionSubscription) o;
            return Objects.equals(subscriptionType, that.subscriptionType)
                    && Objects.equals(topics, that.topics)
                    && Objects.equals(
                            String.valueOf(topicPattern), String.valueOf(that.topicPattern))
                    && Objects.equals(partitions, that.partitions);
        }
    }

    @VisibleForTesting
    public void setProducedDataType(DataType producedDataType) {
        this.producedDataType = producedDataType;
    }

    @VisibleForTesting
    public void setMetadataKeys(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
    }
}
