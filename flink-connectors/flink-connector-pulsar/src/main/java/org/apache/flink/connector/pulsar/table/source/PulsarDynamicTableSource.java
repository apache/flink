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

package org.apache.flink.connector.pulsar.table.source;

import org.apache.flink.connector.pulsar.table.PulsarTableOptions;
import org.apache.flink.util.Preconditions;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/** pulsar dynamic table source. */
@Slf4j
public class PulsarDynamicTableSource
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

    /** Optional format for decoding keys from Pulsar. */
    protected final @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat;

    /** Format for decoding values from Pulsar. */
    protected final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    /** Indices that determine the key fields and the target position in the produced row. */
    protected final int[] keyProjection;

    /** Indices that determine the value fields and the target position in the produced row. */
    protected final int[] valueProjection;

    /** Prefix that needs to be removed from fields when constructing the physical data type. */
    @Nullable protected final String keyPrefix;
    // --------------------------------------------------------------------------------------------
    // Pulsar-specific attributes
    // --------------------------------------------------------------------------------------------

    /** The Pulsar topic to consume. */
    protected final List<String> topics;

    /** The Pulsar topic to consume. */
    protected final String topicPattern;

    /** The Pulsar topic to consume. */
    protected final String serviceUrl;

    /** The Pulsar topic to consume. */
    protected final String adminUrl;

    /** Properties for the Pulsar consumer. */
    protected final Properties properties;

    /** The startup mode for the contained consumer (default is {@link StartupMode#LATEST}). */
    protected final PulsarTableOptions.StartupOptions startupOptions;

    /** The default value when startup timestamp is not used. */
    private static final long DEFAULT_STARTUP_TIMESTAMP_MILLIS = 0L;

    /** Flag to determine source mode. In upsert mode, it will keep the tombstone message. * */
    protected final boolean upsertMode;

    public PulsarDynamicTableSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            List<String> topics,
            String topicPattern,
            String serviceUrl,
            String adminUrl,
            Properties properties,
            PulsarTableOptions.StartupOptions startupOptions,
            boolean upsertMode) {
        this.producedDataType = physicalDataType;
        setTopicInfo(properties, topics, topicPattern);

        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.keyDecodingFormat = keyDecodingFormat;
        this.valueDecodingFormat =
                Preconditions.checkNotNull(
                        valueDecodingFormat, "Value decoding format must not be null.");
        this.keyProjection =
                Preconditions.checkNotNull(keyProjection, "Key projection must not be null.");
        this.valueProjection =
                Preconditions.checkNotNull(valueProjection, "Value projection must not be null.");
        this.keyPrefix = keyPrefix;
        // Mutable attributes
        this.producedDataType = physicalDataType;
        this.metadataKeys = new ArrayList<>();
        this.watermarkStrategy = null;
        // Pulsar-specific attributes
        Preconditions.checkArgument(
                (topics != null && topicPattern == null)
                        || (topics == null && topicPattern != null),
                "Either Topic or Topic Pattern must be set for source.");
        this.topics = topics;
        this.topicPattern = topicPattern;
        this.adminUrl = adminUrl;
        this.serviceUrl = serviceUrl;
        this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.startupOptions = startupOptions;
        this.upsertMode = upsertMode;
    }

    private void setTopicInfo(Properties properties, List<String> topics, String topicPattern) {
        if (StringUtils.isNotBlank(topicPattern)) {
            properties.putIfAbsent("topicspattern", topicPattern);
            properties.remove("topic");
            properties.remove("topics");
        } else if (topics != null && topics.size() > 1) {
            properties.putIfAbsent("topics", StringUtils.join(topics, ","));
            properties.remove("topicspattern");
            properties.remove("topic");
        } else if (topics != null && topics.size() == 1) {
            properties.putIfAbsent("topic", StringUtils.join(topics, ","));
            properties.remove("topicspattern");
            properties.remove("topics");
        } else {
            throw new IllegalStateException("Use `topics` instead of `topic` for multi topic read");
        }
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return valueDecodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {

        // TODO here it creates the key deserialization
        final DeserializationSchema<RowData> keyDeserialization =
                createDeserialization(context, keyDecodingFormat, keyProjection, keyPrefix);

        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(context, valueDecodingFormat, valueProjection, "");
        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);
        PulsarDeserializationSchema<RowData> deserializationSchema =
                createPulsarDeserialization(
                        keyDeserialization, valueDeserialization, producedTypeInfo);
        final ClientConfigurationData clientConfigurationData =
                PulsarClientUtils.newClientConf(serviceUrl, properties);
        FlinkPulsarSource<RowData> source =
                new FlinkPulsarSource<>(
                        adminUrl, clientConfigurationData, deserializationSchema, properties);

        if (watermarkStrategy != null) {
            source.assignTimestampsAndWatermarks(watermarkStrategy);
        }

        switch (startupOptions.startupMode) {
            case EARLIEST:
                source.setStartFromEarliest();
                break;
            case LATEST:
                source.setStartFromLatest();
                break;
            case SPECIFIC_OFFSETS:
                source.setStartFromSpecificOffsets(startupOptions.specificOffsets);
                break;
            case TIMESTAMP:
                source.setStartFromTimestamp(startupOptions.startupTimestampMills);
                break;
            case EXTERNAL_SUBSCRIPTION:
                MessageId subscriptionPosition = MessageId.latest;
                if (CONNECTOR_STARTUP_MODE_VALUE_EARLIEST.equals(
                        startupOptions.externalSubStartOffset)) {
                    subscriptionPosition = MessageId.earliest;
                }
                source.setStartFromSubscription(
                        startupOptions.externalSubscriptionName, subscriptionPosition);
                break;
        }
        return SourceFunctionProvider.of(source, false);
    }

    private PulsarDeserializationSchema<RowData> createPulsarDeserialization(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {
        final DynamicPulsarDeserializationSchema.MetadataConverter[] metadataConverters =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .map(m -> m.converter)
                        .toArray(DynamicPulsarDeserializationSchema.MetadataConverter[]::new);

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

        return new DynamicPulsarDeserializationSchema(
                adjustedPhysicalArity,
                keyDeserialization,
                keyProjection,
                valueDeserialization,
                adjustedValueProjection,
                hasMetadata,
                metadataConverters,
                producedTypeInfo,
                upsertMode);
    }

    @Override
    public DynamicTableSource copy() {
        final PulsarDynamicTableSource copy =
                new PulsarDynamicTableSource(
                        physicalDataType,
                        keyDecodingFormat,
                        valueDecodingFormat,
                        keyProjection,
                        valueProjection,
                        keyPrefix,
                        topics,
                        topicPattern,
                        serviceUrl,
                        adminUrl,
                        properties,
                        startupOptions,
                        false);
        copy.producedDataType = producedDataType;
        copy.metadataKeys = metadataKeys;
        copy.watermarkStrategy = watermarkStrategy;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Pulsar universal table source";
    }

    private static ClientConfigurationData newClientConf(String serviceUrl) {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(serviceUrl);
        return clientConf;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PulsarDynamicTableSource)) {
            return false;
        }
        PulsarDynamicTableSource that = (PulsarDynamicTableSource) o;
        return upsertMode == that.upsertMode
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(watermarkStrategy, that.watermarkStrategy)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(keyDecodingFormat, that.keyDecodingFormat)
                && Objects.equals(valueDecodingFormat, that.valueDecodingFormat)
                && Arrays.equals(keyProjection, that.keyProjection)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(keyPrefix, that.keyPrefix)
                && Objects.equals(topics, that.topics)
                && Objects.equals(topicPattern, that.topicPattern)
                && Objects.equals(serviceUrl, that.serviceUrl)
                && Objects.equals(adminUrl, that.adminUrl)
                && Objects.equals(new HashMap<>(properties), new HashMap<>(that.properties))
                && Objects.equals(startupOptions, that.startupOptions);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        producedDataType,
                        metadataKeys,
                        watermarkStrategy,
                        physicalDataType,
                        keyDecodingFormat,
                        valueDecodingFormat,
                        keyPrefix,
                        topics,
                        topicPattern,
                        serviceUrl,
                        adminUrl,
                        properties,
                        startupOptions,
                        upsertMode);
        result = 31 * result + Arrays.hashCode(keyProjection);
        result = 31 * result + Arrays.hashCode(valueProjection);
        return result;
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

    private @Nullable DeserializationSchema<RowData> createDeserialization(
            DynamicTableSource.Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        // TODO what does this dataType do ? Get the logical SQL dataType
        // TODO equals to the produced data type
        DataType physicalFormatDataType =
                DataTypeUtils.projectRow(this.physicalDataType, projection);
        if (prefix != null) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum ReadableMetadata {
        TOPIC(
                "topic",
                DataTypes.STRING().notNull(),
                message -> StringData.fromString(message.getTopicName())),

        MESSAGE_ID(
                "messageId",
                DataTypes.BYTES().notNull(),
                message -> message.getMessageId().toByteArray()),

        SEQUENCE_ID("sequenceId", DataTypes.BIGINT().notNull(), Message::getSequenceId),

        PUBLISH_TIME(
                "publishTime",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                message -> TimestampData.fromEpochMillis(message.getPublishTime())),

        EVENT_TIME(
                "eventTime",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                message -> TimestampData.fromEpochMillis(message.getEventTime())),

        PROPERTIES(
                "properties",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.STRING().nullable())
                        .notNull(),
                message -> {
                    final Map<StringData, StringData> map = new HashMap<>();
                    for (Map.Entry<String, String> e : message.getProperties().entrySet()) {
                        map.put(
                                StringData.fromString(e.getKey()),
                                StringData.fromString(e.getValue()));
                    }
                    return new GenericMapData(map);
                });

        final String key;

        final DataType dataType;

        final DynamicPulsarDeserializationSchema.MetadataConverter converter;

        ReadableMetadata(
                String key,
                DataType dataType,
                DynamicPulsarDeserializationSchema.MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
