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

package org.apache.flink.connector.kafka.table.factories;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions;
import org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.OffsetMode;
import org.apache.flink.connector.kafka.table.options.KafkaConnectorOptions.SinkSemantic;
import org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil;
import org.apache.flink.connector.kafka.table.sink.KafkaDynamicSink;
import org.apache.flink.connector.kafka.table.source.KafkaDynamicTableSource;
import org.apache.flink.connector.kafka.table.utils.SinkBufferFlushMode;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.debezium.DebeziumAvroSerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.KafkaLegacyDynamicSource;
import org.apache.flink.streaming.connectors.kafka.table.KafkaLegacyDynamicTableFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.factories.TestFormatFactory.DecodingFormatMock;
import org.apache.flink.table.factories.TestFormatFactory.EncodingFormatMock;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.AVRO_CONFLUENT;
import static org.apache.flink.connector.kafka.table.options.KafkaConnectorOptionsUtil.DEBEZIUM_AVRO_CONFLUENT;
import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Abstract test base for {@link KafkaDynamicTableFactory}. */
public class KafkaDynamicTableFactoryTest extends TestLogger {

    @Rule public ExpectedException thrown = ExpectedException.none();

    private static final String TOPIC = "myTopic";
    private static final String TOPICS = "myTopic-1;myTopic-2;myTopic-3";
    private static final String TOPIC_REGEX = "myTopic-\\d+";
    private static final String PARTITIONS =
            String.format("%s:%d;%s:%d", "myTopic-1", 0, "myTopic-2", 1);
    private static final Set<TopicPartition> PARTITION_SET =
            new HashSet<>(
                    Arrays.asList(
                            new TopicPartition("myTopic-1", 0),
                            new TopicPartition("myTopic-2", 1)));
    private static final List<String> TOPIC_LIST =
            Arrays.asList("myTopic-1", "myTopic-2", "myTopic-3");
    private static final String TEST_REGISTRY_URL = "http://localhost:8081";
    private static final String DEFAULT_VALUE_SUBJECT = TOPIC + "-value";
    private static final String DEFAULT_KEY_SUBJECT = TOPIC + "-key";
    private static final int PARTITION_0 = 0;
    private static final long OFFSET_0 = 100L;
    private static final int PARTITION_1 = 1;
    private static final long OFFSET_1 = 123L;
    private static final String NAME = "name";
    private static final String COUNT = "count";
    private static final String TIME = "time";
    private static final String METADATA = "metadata";
    private static final String WATERMARK_EXPRESSION = TIME + " - INTERVAL '5' SECOND";
    private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
    private static final String COMPUTED_COLUMN_NAME = "computed-column";
    private static final String COMPUTED_COLUMN_EXPRESSION = COUNT + " + 1.0";
    private static final DataType COMPUTED_COLUMN_DATATYPE = DataTypes.DECIMAL(10, 3);
    private static final String DISCOVERY_INTERVAL = "1000 ms";

    private static final Properties KAFKA_SOURCE_PROPERTIES = new Properties();
    private static final Properties KAFKA_SINK_PROPERTIES = new Properties();
    private static final Properties KAFKA_FINAL_SINK_PROPERTIES = new Properties();

    private static final ObjectPath TEST_OBJECT_PATH = FactoryMocks.IDENTIFIER.toObjectPath();

    static {
        KAFKA_SOURCE_PROPERTIES.setProperty("group.id", "dummy");
        KAFKA_SOURCE_PROPERTIES.setProperty("bootstrap.servers", "dummy");
        KAFKA_SOURCE_PROPERTIES.setProperty("partition.discovery.interval.ms", "1000");

        KAFKA_SINK_PROPERTIES.setProperty("group.id", "dummy");
        KAFKA_SINK_PROPERTIES.setProperty("bootstrap.servers", "dummy");

        KAFKA_FINAL_SINK_PROPERTIES.putAll(KAFKA_SINK_PROPERTIES);
        KAFKA_FINAL_SINK_PROPERTIES.setProperty(
                "value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        KAFKA_FINAL_SINK_PROPERTIES.setProperty(
                "key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        KAFKA_FINAL_SINK_PROPERTIES.put("transaction.timeout.ms", 3600000);
    }

    // Properties for legacy dynamic table
    private static final Properties KAFKA_LEGACY_SOURCE_PROPERTIES = new Properties();
    private static final Properties KAFKA_LEGACY_FINAL_SOURCE_PROPERTIES = new Properties();

    static {
        KAFKA_LEGACY_SOURCE_PROPERTIES.setProperty("group.id", "dummy");
        KAFKA_LEGACY_SOURCE_PROPERTIES.setProperty("bootstrap.servers", "dummy");
        KAFKA_LEGACY_SOURCE_PROPERTIES.setProperty(
                "flink.partition-discovery.interval-millis", "1000");

        KAFKA_LEGACY_FINAL_SOURCE_PROPERTIES.putAll(KAFKA_LEGACY_SOURCE_PROPERTIES);
        KAFKA_LEGACY_FINAL_SOURCE_PROPERTIES.setProperty(
                "value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KAFKA_LEGACY_FINAL_SOURCE_PROPERTIES.setProperty(
                "key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }

    private static final String PROPS_SCAN_OFFSETS =
            String.format(
                    "partition:%d,offset:%d;partition:%d,offset:%d",
                    PARTITION_0, OFFSET_0, PARTITION_1, OFFSET_1);

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical(NAME, DataTypes.STRING().notNull()),
                            Column.physical(COUNT, DataTypes.DECIMAL(38, 18)),
                            Column.physical(TIME, DataTypes.TIMESTAMP(3)),
                            Column.computed(
                                    COMPUTED_COLUMN_NAME,
                                    ResolvedExpressionMock.of(
                                            COMPUTED_COLUMN_DATATYPE, COMPUTED_COLUMN_EXPRESSION))),
                    Collections.singletonList(
                            WatermarkSpec.of(
                                    TIME,
                                    ResolvedExpressionMock.of(
                                            WATERMARK_DATATYPE, WATERMARK_EXPRESSION))),
                    null);

    private static final ResolvedSchema SCHEMA_WITH_METADATA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical(NAME, DataTypes.STRING()),
                            Column.physical(COUNT, DataTypes.DECIMAL(38, 18)),
                            Column.metadata(TIME, DataTypes.TIMESTAMP(3), "timestamp", false),
                            Column.metadata(
                                    METADATA, DataTypes.STRING(), "value.metadata_2", false)),
                    Collections.emptyList(),
                    null);

    private static final DataType SCHEMA_DATA_TYPE = SCHEMA.toPhysicalRowDataType();

    protected boolean isLegacySource() {
        return false;
    }

    @Test
    public void testTableSource() {
        final DynamicTableSource actualKafkaSource =
                createTableSource(SCHEMA, getBasicSourceOptions());

        final Map<TopicPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new TopicPartition(TOPIC, PARTITION_0), OFFSET_0);
        specificOffsets.put(new TopicPartition(TOPIC, PARTITION_1), OFFSET_1);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);

        final DynamicTableSource expectedKafkaSource;
        // Test scan source equals
        if (isLegacySource()) {
            expectedKafkaSource =
                    createLegacyExpectedScanSource(
                            SCHEMA_DATA_TYPE,
                            null,
                            valueDecodingFormat,
                            new int[0],
                            new int[] {0, 1, 2},
                            null,
                            Collections.singletonList(TOPIC),
                            null,
                            KAFKA_LEGACY_SOURCE_PROPERTIES,
                            StartupMode.SPECIFIC_OFFSETS,
                            specificOffsets,
                            0);
        } else {
            expectedKafkaSource =
                    createExpectedScanSource(
                            SCHEMA_DATA_TYPE,
                            null,
                            valueDecodingFormat,
                            new int[0],
                            new int[] {0, 1, 2},
                            null,
                            KafkaDynamicTableSource.TopicPartitionSubscription.topics(
                                    Collections.singletonList(TOPIC)),
                            OffsetsInitializer.offsets(specificOffsets),
                            null,
                            Boundedness.CONTINUOUS_UNBOUNDED,
                            KAFKA_SOURCE_PROPERTIES,
                            TEST_OBJECT_PATH);
        }
        assertEquals(actualKafkaSource, expectedKafkaSource);
    }

    @Test
    public void testTableSourceWithPattern() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> {
                            options.remove("topic");
                            options.put("topic-pattern", TOPIC_REGEX);
                            options.put("scan.startup.mode", OffsetMode.EARLIEST_OFFSET.toString());
                            options.remove("scan.startup.specific-offsets");
                        });
        final DynamicTableSource actualSource = createTableSource(SCHEMA, modifiedOptions);

        DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);

        // Test scan source equals
        DynamicTableSource expectedKafkaSource;
        if (isLegacySource()) {
            expectedKafkaSource =
                    createLegacyExpectedScanSource(
                            SCHEMA_DATA_TYPE,
                            null,
                            valueDecodingFormat,
                            new int[0],
                            new int[] {0, 1, 2},
                            null,
                            null,
                            Pattern.compile(TOPIC_REGEX),
                            KAFKA_LEGACY_SOURCE_PROPERTIES,
                            StartupMode.EARLIEST,
                            new HashMap<>(),
                            0);
        } else {
            expectedKafkaSource =
                    createExpectedScanSource(
                            SCHEMA_DATA_TYPE,
                            null,
                            valueDecodingFormat,
                            new int[0],
                            new int[] {0, 1, 2},
                            null,
                            KafkaDynamicTableSource.TopicPartitionSubscription.topicPattern(
                                    Pattern.compile(TOPIC_REGEX)),
                            OffsetsInitializer.earliest(),
                            null,
                            Boundedness.CONTINUOUS_UNBOUNDED,
                            KAFKA_SOURCE_PROPERTIES,
                            TEST_OBJECT_PATH);
        }
        assertEquals(actualSource, expectedKafkaSource);
    }

    @Test
    public void testTableSourceWithPartitionList() {
        if (isLegacySource()) {
            return;
        }
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> {
                            options.remove("topic");
                            options.put("partitions", PARTITIONS);
                            options.put("scan.startup.mode", "earliest-offset");
                        });
        final DynamicTableSource actualSource = createTableSource(SCHEMA, modifiedOptions);

        final Map<TopicPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new TopicPartition(TOPIC, PARTITION_0), OFFSET_0);
        specificOffsets.put(new TopicPartition(TOPIC, PARTITION_1), OFFSET_1);
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);
        final DynamicTableSource expectedSource =
                createExpectedScanSource(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueDecodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        KafkaDynamicTableSource.TopicPartitionSubscription.partitions(
                                PARTITION_SET),
                        OffsetsInitializer.earliest(),
                        null,
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        KAFKA_SOURCE_PROPERTIES,
                        TEST_OBJECT_PATH);

        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testTableSourceWithKeyValue() {
        final DynamicTableSource actualSource = createTableSource(SCHEMA, getKeyValueOptions());
        // initialize stateful testing formats
        ((ScanTableSource) actualSource)
                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        final DecodingFormatMock keyDecodingFormat = new DecodingFormatMock("#", false);
        keyDecodingFormat.producedDataType =
                DataTypes.ROW(DataTypes.FIELD(NAME, DataTypes.STRING().notNull())).notNull();

        final DecodingFormatMock valueDecodingFormat = new DecodingFormatMock("|", false);
        valueDecodingFormat.producedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
                                DataTypes.FIELD(TIME, DataTypes.TIMESTAMP(3)))
                        .notNull();

        DynamicTableSource expectedKafkaSource;
        if (isLegacySource()) {
            expectedKafkaSource =
                    createLegacyExpectedScanSource(
                            SCHEMA_DATA_TYPE,
                            keyDecodingFormat,
                            valueDecodingFormat,
                            new int[] {0},
                            new int[] {1, 2},
                            null,
                            Collections.singletonList(TOPIC),
                            null,
                            KAFKA_LEGACY_FINAL_SOURCE_PROPERTIES,
                            StartupMode.GROUP_OFFSETS,
                            Collections.emptyMap(),
                            0);
        } else {
            expectedKafkaSource =
                    createExpectedScanSource(
                            SCHEMA_DATA_TYPE,
                            keyDecodingFormat,
                            valueDecodingFormat,
                            new int[] {0},
                            new int[] {1, 2},
                            null,
                            KafkaDynamicTableSource.TopicPartitionSubscription.topics(
                                    Collections.singletonList(TOPIC)),
                            OffsetsInitializer.committedOffsets(),
                            null,
                            Boundedness.CONTINUOUS_UNBOUNDED,
                            KAFKA_SOURCE_PROPERTIES,
                            TEST_OBJECT_PATH);
        }
        assertEquals(actualSource, expectedKafkaSource);
    }

    @Test
    public void testTableSourceWithKeyValueAndMetadata() {
        final Map<String, String> options = getKeyValueOptions();
        options.put("value.test-format.readable-metadata", "metadata_1:INT, metadata_2:STRING");

        final DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, options);
        // initialize stateful testing formats
        ((SupportsReadingMetadata) actualSource)
                .applyReadableMetadata(
                        Arrays.asList("timestamp", "value.metadata_2"),
                        SCHEMA_WITH_METADATA.toSourceRowDataType());
        ((ScanTableSource) actualSource)
                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        final DecodingFormatMock expectedKeyFormat =
                new DecodingFormatMock(
                        "#", false, ChangelogMode.insertOnly(), Collections.emptyMap());
        expectedKeyFormat.producedDataType =
                DataTypes.ROW(DataTypes.FIELD(NAME, DataTypes.STRING())).notNull();

        final Map<String, DataType> expectedReadableMetadata = new HashMap<>();
        expectedReadableMetadata.put("metadata_1", DataTypes.INT());
        expectedReadableMetadata.put("metadata_2", DataTypes.STRING());

        final DecodingFormatMock expectedValueFormat =
                new DecodingFormatMock(
                        "|", false, ChangelogMode.insertOnly(), expectedReadableMetadata);
        expectedValueFormat.producedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
                                DataTypes.FIELD("metadata_2", DataTypes.STRING()))
                        .notNull();
        expectedValueFormat.metadataKeys = Collections.singletonList("metadata_2");

        DynamicTableSource expectedKafkaSource;
        if (isLegacySource()) {
            expectedKafkaSource =
                    createLegacyExpectedScanSource(
                            SCHEMA_WITH_METADATA.toPhysicalRowDataType(),
                            expectedKeyFormat,
                            expectedValueFormat,
                            new int[] {0},
                            new int[] {1},
                            null,
                            Collections.singletonList(TOPIC),
                            null,
                            KAFKA_LEGACY_FINAL_SOURCE_PROPERTIES,
                            StartupMode.GROUP_OFFSETS,
                            Collections.emptyMap(),
                            0);
            KafkaLegacyDynamicSource castedExpectedKafkaSource =
                    (KafkaLegacyDynamicSource) expectedKafkaSource;
            castedExpectedKafkaSource.setProducedDataType(
                    SCHEMA_WITH_METADATA.toSourceRowDataType());
            castedExpectedKafkaSource.setMetadataKeys(Collections.singletonList("timestamp"));
        } else {
            expectedKafkaSource =
                    createExpectedScanSource(
                            SCHEMA_WITH_METADATA.toPhysicalRowDataType(),
                            expectedKeyFormat,
                            expectedValueFormat,
                            new int[] {0},
                            new int[] {1},
                            null,
                            KafkaDynamicTableSource.TopicPartitionSubscription.topics(
                                    Collections.singletonList(TOPIC)),
                            OffsetsInitializer.committedOffsets(),
                            null,
                            Boundedness.CONTINUOUS_UNBOUNDED,
                            KAFKA_SOURCE_PROPERTIES,
                            TEST_OBJECT_PATH);
            KafkaDynamicTableSource castedExpectedKafkaSource =
                    (KafkaDynamicTableSource) expectedKafkaSource;
            castedExpectedKafkaSource.setProducedDataType(
                    SCHEMA_WITH_METADATA.toSourceRowDataType());
            castedExpectedKafkaSource.setMetadataKeys(Collections.singletonList("timestamp"));
        }

        assertEquals(actualSource, expectedKafkaSource);
    }

    @Test
    public void testBoundedTableSource() {
        if (isLegacySource()) {
            return;
        }
        final DynamicTableSource actualSource =
                createTableSource(
                        SCHEMA,
                        getModifiedOptions(
                                getBasicSourceOptions(),
                                options ->
                                        options.put(
                                                "scan.stop.mode",
                                                OffsetMode.LATEST_OFFSET.toString())));

        final Map<TopicPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new TopicPartition(TOPIC, PARTITION_0), OFFSET_0);
        specificOffsets.put(new TopicPartition(TOPIC, PARTITION_1), OFFSET_1);
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);
        final DynamicTableSource expectedSource =
                createExpectedScanSource(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueDecodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        KafkaDynamicTableSource.TopicPartitionSubscription.topics(
                                Collections.singletonList(TOPIC)),
                        OffsetsInitializer.offsets(specificOffsets),
                        OffsetsInitializer.latest(),
                        Boundedness.BOUNDED,
                        KAFKA_SOURCE_PROPERTIES,
                        TEST_OBJECT_PATH);

        assertEquals(expectedSource, actualSource);

        final ScanTableSource.ScanRuntimeProvider provider =
                ((ScanTableSource) actualSource)
                        .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertTrue(provider.isBounded());
    }

    @Test
    public void testBoundedTableSourceWithStoppingOffsets() {
        if (isLegacySource()) {
            return;
        }
        final DynamicTableSource actualSource =
                createTableSource(
                        SCHEMA,
                        getModifiedOptions(
                                getBasicSourceOptions(),
                                options -> {
                                    options.put(
                                            "scan.stop.mode",
                                            OffsetMode.SPECIFIC_OFFSETS.toString());
                                    options.put(
                                            "scan.stop.specific-offsets",
                                            String.format(
                                                    "partition:%s,offset:%d;partition:%s,offset:%d",
                                                    PARTITION_0, 15213, PARTITION_1, 18213));
                                }));

        final Map<TopicPartition, Long> startingSpecificOffsets = new HashMap<>();
        startingSpecificOffsets.put(new TopicPartition(TOPIC, PARTITION_0), OFFSET_0);
        startingSpecificOffsets.put(new TopicPartition(TOPIC, PARTITION_1), OFFSET_1);

        final Map<TopicPartition, Long> stoppingSpecificOffsets = new HashMap<>();
        stoppingSpecificOffsets.put(new TopicPartition(TOPIC, PARTITION_0), 15213L);
        stoppingSpecificOffsets.put(new TopicPartition(TOPIC, PARTITION_1), 18213L);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);
        final DynamicTableSource expectedSource =
                createExpectedScanSource(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueDecodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        KafkaDynamicTableSource.TopicPartitionSubscription.topics(
                                Collections.singletonList(TOPIC)),
                        OffsetsInitializer.offsets(startingSpecificOffsets),
                        OffsetsInitializer.offsets(stoppingSpecificOffsets),
                        Boundedness.BOUNDED,
                        KAFKA_SOURCE_PROPERTIES,
                        TEST_OBJECT_PATH);

        assertEquals(expectedSource, actualSource);

        final ScanTableSource.ScanRuntimeProvider provider =
                ((ScanTableSource) actualSource)
                        .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertTrue(provider.isBounded());
    }

    @Test
    public void testBoundedTableSourceWithStoppingTimestamp() {
        if (isLegacySource()) {
            return;
        }
        final DynamicTableSource actualSource =
                createTableSource(
                        SCHEMA,
                        getModifiedOptions(
                                getBasicSourceOptions(),
                                options -> {
                                    options.put("scan.stop.mode", OffsetMode.TIMESTAMP.toString());
                                    options.put("scan.stop.timestamp-millis", "15213");
                                }));

        final Map<TopicPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new TopicPartition(TOPIC, PARTITION_0), OFFSET_0);
        specificOffsets.put(new TopicPartition(TOPIC, PARTITION_1), OFFSET_1);
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);
        final DynamicTableSource expectedSource =
                createExpectedScanSource(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueDecodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        KafkaDynamicTableSource.TopicPartitionSubscription.topics(
                                Collections.singletonList(TOPIC)),
                        OffsetsInitializer.offsets(specificOffsets),
                        OffsetsInitializer.timestamp(15213L),
                        Boundedness.BOUNDED,
                        KAFKA_SOURCE_PROPERTIES,
                        TEST_OBJECT_PATH);

        assertEquals(expectedSource, actualSource);

        final ScanTableSource.ScanRuntimeProvider provider =
                ((ScanTableSource) actualSource)
                        .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertTrue(provider.isBounded());
    }

    @Test
    public void testUnboundedTableSourceWithStoppingOffsets() {
        if (isLegacySource()) {
            return;
        }
        final DynamicTableSource actualSource =
                createTableSource(
                        SCHEMA,
                        getModifiedOptions(
                                getBasicSourceOptions(),
                                options -> {
                                    options.put(
                                            "scan.stop.mode", OffsetMode.LATEST_OFFSET.toString());
                                    options.put("boundedness", "CONTINUOUS_UNBOUNDED");
                                }));

        final Map<TopicPartition, Long> specificOffsets = new HashMap<>();
        specificOffsets.put(new TopicPartition(TOPIC, PARTITION_0), OFFSET_0);
        specificOffsets.put(new TopicPartition(TOPIC, PARTITION_1), OFFSET_1);
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new DecodingFormatMock(",", true);
        final DynamicTableSource expectedSource =
                createExpectedScanSource(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueDecodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        KafkaDynamicTableSource.TopicPartitionSubscription.topics(
                                Collections.singletonList(TOPIC)),
                        OffsetsInitializer.offsets(specificOffsets),
                        OffsetsInitializer.latest(),
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        KAFKA_SOURCE_PROPERTIES,
                        TEST_OBJECT_PATH);

        assertEquals(expectedSource, actualSource);

        // Given source should be unbounded even stopping offsets initializer is specified
        final ScanTableSource.ScanRuntimeProvider provider =
                ((ScanTableSource) actualSource)
                        .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertFalse(provider.isBounded());
    }

    @Test
    public void testTableSink() {
        final DynamicTableSink actualSink = createTableSink(SCHEMA, getBasicSinkOptions());

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                new EncodingFormatMock(",");

        final DynamicTableSink expectedSink =
                createExpectedSink(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueEncodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        TOPIC,
                        KAFKA_SINK_PROPERTIES,
                        new FlinkFixedPartitioner<>(),
                        SinkSemantic.EXACTLY_ONCE,
                        null);
        assertEquals(expectedSink, actualSink);

        // Test kafka producer.
        final KafkaDynamicSink actualKafkaSink = (KafkaDynamicSink) actualSink;
        DynamicTableSink.SinkRuntimeProvider provider =
                actualKafkaSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider, instanceOf(SinkFunctionProvider.class));
        final SinkFunctionProvider sinkFunctionProvider = (SinkFunctionProvider) provider;
        final SinkFunction<RowData> sinkFunction = sinkFunctionProvider.createSinkFunction();
        assertThat(sinkFunction, instanceOf(FlinkKafkaProducer.class));
    }

    @Test
    public void testTableSinkWithKeyValue() {
        final DynamicTableSink actualSink = createTableSink(SCHEMA, getKeyValueOptions());
        final KafkaDynamicSink actualKafkaSink = (KafkaDynamicSink) actualSink;
        // initialize stateful testing formats
        actualKafkaSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        final EncodingFormatMock keyEncodingFormat = new EncodingFormatMock("#");
        keyEncodingFormat.consumedDataType =
                DataTypes.ROW(DataTypes.FIELD(NAME, DataTypes.STRING().notNull())).notNull();

        final EncodingFormatMock valueEncodingFormat = new EncodingFormatMock("|");
        valueEncodingFormat.consumedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
                                DataTypes.FIELD(TIME, DataTypes.TIMESTAMP(3)))
                        .notNull();

        final DynamicTableSink expectedSink =
                createExpectedSink(
                        SCHEMA_DATA_TYPE,
                        keyEncodingFormat,
                        valueEncodingFormat,
                        new int[] {0},
                        new int[] {1, 2},
                        null,
                        TOPIC,
                        KAFKA_FINAL_SINK_PROPERTIES,
                        new FlinkFixedPartitioner<>(),
                        SinkSemantic.EXACTLY_ONCE,
                        null);

        assertEquals(expectedSink, actualSink);
    }

    @Test
    public void testTableSinkWithParallelism() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(), options -> options.put("sink.parallelism", "100"));
        KafkaDynamicSink actualSink = (KafkaDynamicSink) createTableSink(SCHEMA, modifiedOptions);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                new EncodingFormatMock(",");

        final DynamicTableSink expectedSink =
                createExpectedSink(
                        SCHEMA_DATA_TYPE,
                        null,
                        valueEncodingFormat,
                        new int[0],
                        new int[] {0, 1, 2},
                        null,
                        TOPIC,
                        KAFKA_SINK_PROPERTIES,
                        new FlinkFixedPartitioner<>(),
                        SinkSemantic.EXACTLY_ONCE,
                        100);
        assertEquals(expectedSink, actualSink);

        final DynamicTableSink.SinkRuntimeProvider provider =
                actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider, instanceOf(SinkFunctionProvider.class));
        final SinkFunctionProvider sinkFunctionProvider = (SinkFunctionProvider) provider;
        assertTrue(sinkFunctionProvider.getParallelism().isPresent());
        assertEquals(100, (long) sinkFunctionProvider.getParallelism().get());
    }

    @Test
    public void testTableSinkAutoCompleteSchemaRegistrySubject() {
        // only format
        verifyEncoderSubject(
                options -> {
                    options.put("format", "debezium-avro-confluent");
                    options.put("debezium-avro-confluent.url", TEST_REGISTRY_URL);
                },
                DEFAULT_VALUE_SUBJECT,
                "N/A");

        // only value.format
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "avro-confluent");
                    options.put("value.avro-confluent.url", TEST_REGISTRY_URL);
                },
                DEFAULT_VALUE_SUBJECT,
                "N/A");

        // value.format + key.format
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "avro-confluent");
                    options.put("value.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.format", "avro-confluent");
                    options.put("key.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.fields", NAME);
                },
                DEFAULT_VALUE_SUBJECT,
                DEFAULT_KEY_SUBJECT);

        // value.format + non-avro key.format
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "avro-confluent");
                    options.put("value.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.format", "csv");
                    options.put("key.fields", NAME);
                },
                DEFAULT_VALUE_SUBJECT,
                "N/A");

        // non-avro value.format + key.format
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "json");
                    options.put("key.format", "avro-confluent");
                    options.put("key.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.fields", NAME);
                },
                "N/A",
                DEFAULT_KEY_SUBJECT);

        // not override for 'format'
        verifyEncoderSubject(
                options -> {
                    options.put("format", "debezium-avro-confluent");
                    options.put("debezium-avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("debezium-avro-confluent.subject", "sub1");
                },
                "sub1",
                "N/A");

        // not override for 'key.format'
        verifyEncoderSubject(
                options -> {
                    options.put("format", "avro-confluent");
                    options.put("avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.format", "avro-confluent");
                    options.put("key.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.avro-confluent.subject", "sub2");
                    options.put("key.fields", NAME);
                },
                DEFAULT_VALUE_SUBJECT,
                "sub2");
    }

    private void verifyEncoderSubject(
            Consumer<Map<String, String>> optionModifier,
            String expectedValueSubject,
            String expectedKeySubject) {
        Map<String, String> options = new HashMap<>();
        // Kafka specific options.
        options.put("connector", KafkaDynamicTableFactory.IDENTIFIER);
        options.put("topic", TOPIC);
        options.put("properties.group.id", "dummy");
        options.put("properties.bootstrap.servers", "dummy");
        optionModifier.accept(options);

        final RowType rowType = (RowType) SCHEMA_DATA_TYPE.getLogicalType();
        final String valueFormat =
                options.getOrDefault(
                        FactoryUtil.FORMAT.key(),
                        options.get(KafkaConnectorOptions.VALUE_FORMAT.key()));
        final String keyFormat = options.get(KafkaConnectorOptions.KEY_FORMAT.key());

        KafkaDynamicSink sink = (KafkaDynamicSink) createTableSink(SCHEMA, options);
        final Set<String> avroFormats = new HashSet<>();
        avroFormats.add(AVRO_CONFLUENT);
        avroFormats.add(DEBEZIUM_AVRO_CONFLUENT);

        if (avroFormats.contains(valueFormat)) {
            SerializationSchema<RowData> actualValueEncoder =
                    sink.getValueEncodingFormat()
                            .createRuntimeEncoder(
                                    new SinkRuntimeProviderContext(false), SCHEMA_DATA_TYPE);
            final SerializationSchema<RowData> expectedValueEncoder;
            if (AVRO_CONFLUENT.equals(valueFormat)) {
                expectedValueEncoder = createConfluentAvroSerSchema(rowType, expectedValueSubject);
            } else {
                expectedValueEncoder = createDebeziumAvroSerSchema(rowType, expectedValueSubject);
            }
            assertEquals(expectedValueEncoder, actualValueEncoder);
        }

        if (avroFormats.contains(keyFormat)) {
            assert sink.getKeyEncodingFormat() != null;
            SerializationSchema<RowData> actualKeyEncoder =
                    sink.getKeyEncodingFormat()
                            .createRuntimeEncoder(
                                    new SinkRuntimeProviderContext(false), SCHEMA_DATA_TYPE);
            final SerializationSchema<RowData> expectedKeyEncoder;
            if (AVRO_CONFLUENT.equals(keyFormat)) {
                expectedKeyEncoder = createConfluentAvroSerSchema(rowType, expectedKeySubject);
            } else {
                expectedKeyEncoder = createDebeziumAvroSerSchema(rowType, expectedKeySubject);
            }
            assertEquals(expectedKeyEncoder, actualKeyEncoder);
        }
    }

    private SerializationSchema<RowData> createConfluentAvroSerSchema(
            RowType rowType, String subject) {
        return new AvroRowDataSerializationSchema(
                rowType,
                ConfluentRegistryAvroSerializationSchema.forGeneric(
                        subject, AvroSchemaConverter.convertToSchema(rowType), TEST_REGISTRY_URL),
                RowDataToAvroConverters.createConverter(rowType));
    }

    private SerializationSchema<RowData> createDebeziumAvroSerSchema(
            RowType rowType, String subject) {
        return new DebeziumAvroSerializationSchema(rowType, TEST_REGISTRY_URL, subject, null);
    }

    // --------------------------------------------------------------------------------------------
    // Negative tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testSourceTableWithTopicAndTopicPattern() {
        thrown.expect(ValidationException.class);
        String errorMessage;
        if (isLegacySource()) {
            errorMessage = "Option 'topic' and 'topic-pattern' shouldn't be set together.";
        } else {
            errorMessage =
                    "Multiple topic partition subscription patterns shouldn't be set together: 'topic', 'topic-pattern'";
        }
        thrown.expect(containsCause(new ValidationException(errorMessage)));

        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> {
                            options.put("topic", TOPICS);
                            options.put("topic-pattern", TOPIC_REGEX);
                        });

        createTableSource(SCHEMA, modifiedOptions);
    }

    @Test
    public void testMissingStartupTimestamp() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "'scan.startup.timestamp-millis' "
                                        + "is required in 'timestamp' startup mode but missing.")));

        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> options.put("scan.startup.mode", "timestamp"));

        createTableSource(SCHEMA, modifiedOptions);
    }

    @Test
    public void testMissingSpecificOffsets() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "'scan.startup.specific-offsets' "
                                        + "is required in 'specific-offsets' startup mode but missing.")));

        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options -> options.remove("scan.startup.specific-offsets"));

        createTableSource(SCHEMA, modifiedOptions);
    }

    @Test
    public void testInvalidSinkPartitioner() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Could not find and instantiate partitioner " + "class 'abc'")));

        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(), options -> options.put("sink.partitioner", "abc"));

        createTableSink(SCHEMA, modifiedOptions);
    }

    @Test
    public void testInvalidRoundRobinPartitionerWithKeyFields() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Currently 'round-robin' partitioner only works "
                                        + "when option 'key.fields' is not specified.")));

        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getKeyValueOptions(),
                        options -> options.put("sink.partitioner", "round-robin"));

        createTableSink(SCHEMA, modifiedOptions);
    }

    @Test
    public void testSinkWithTopicListOrTopicPattern() {
        Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(),
                        options -> {
                            options.put("topic", TOPICS);
                            options.put("scan.startup.mode", "earliest-offset");
                            options.remove("specific-offsets");
                        });
        final String errorMessageTemp =
                "Flink Kafka sink currently only supports single topic, but got %s: %s.";

        try {
            createTableSink(SCHEMA, modifiedOptions);
        } catch (Throwable t) {
            assertEquals(
                    String.format(
                            errorMessageTemp,
                            "'topic'",
                            String.format("[%s]", String.join(", ", TOPIC_LIST))),
                    t.getCause().getMessage());
        }

        modifiedOptions =
                getModifiedOptions(
                        getBasicSinkOptions(),
                        options -> options.put("topic-pattern", TOPIC_REGEX));

        try {
            createTableSink(SCHEMA, modifiedOptions);
        } catch (Throwable t) {
            assertEquals(
                    String.format(errorMessageTemp, "'topic-pattern'", TOPIC_REGEX),
                    t.getCause().getMessage());
        }
    }

    @Test
    public void testPrimaryKeyValidation() {
        final ResolvedSchema pkSchema =
                new ResolvedSchema(
                        SCHEMA.getColumns(),
                        SCHEMA.getWatermarkSpecs(),
                        UniqueConstraint.primaryKey(NAME, Collections.singletonList(NAME)));

        Map<String, String> sinkOptions =
                getModifiedOptions(
                        getBasicSinkOptions(),
                        options ->
                                options.put(
                                        String.format(
                                                "%s.%s",
                                                TestFormatFactory.IDENTIFIER,
                                                TestFormatFactory.CHANGELOG_MODE.key()),
                                        "I;UA;UB;D"));
        // pk can be defined on cdc table, should pass
        createTableSink(pkSchema, sinkOptions);

        try {
            createTableSink(pkSchema, getBasicSinkOptions());
            fail();
        } catch (Throwable t) {
            String error =
                    "The Kafka table 'default.default.t1' with 'test-format' format"
                            + " doesn't support defining PRIMARY KEY constraint on the table, because it can't"
                            + " guarantee the semantic of primary key.";
            assertEquals(error, t.getCause().getMessage());
        }

        Map<String, String> sourceOptions =
                getModifiedOptions(
                        getBasicSourceOptions(),
                        options ->
                                options.put(
                                        String.format(
                                                "%s.%s",
                                                TestFormatFactory.IDENTIFIER,
                                                TestFormatFactory.CHANGELOG_MODE.key()),
                                        "I;UA;UB;D"));
        // pk can be defined on cdc table, should pass
        createTableSource(pkSchema, sourceOptions);

        try {
            createTableSource(pkSchema, getBasicSourceOptions());
            fail();
        } catch (Throwable t) {
            String error =
                    "The Kafka table 'default.default.t1' with 'test-format' format"
                            + " doesn't support defining PRIMARY KEY constraint on the table, because it can't"
                            + " guarantee the semantic of primary key.";
            assertEquals(error, t.getCause().getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("SameParameterValue")
    private KafkaDynamicTableSource createExpectedScanSource(
            DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            KafkaDynamicTableSource.TopicPartitionSubscription topicPartitionSubscription,
            OffsetsInitializer startingOffsetsInitializer,
            @Nullable OffsetsInitializer stoppingOffsetsInitializer,
            Boundedness boundedness,
            Properties properties,
            ObjectPath objectPath) {
        return new KafkaDynamicTableSource(
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
                false);
    }

    private static KafkaLegacyDynamicSource createLegacyExpectedScanSource(
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
            Map<TopicPartition, Long> specificStartupOffsets,
            long startupTimestampMillis) {
        Map<KafkaTopicPartition, Long> convertedSpecifiedOffsets = new HashMap<>();
        specificStartupOffsets.forEach(
                (tp, offset) ->
                        convertedSpecifiedOffsets.put(
                                new KafkaTopicPartition(tp.topic(), tp.partition()), offset));
        return new KafkaLegacyDynamicSource(
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
                convertedSpecifiedOffsets,
                startupTimestampMillis,
                false);
    }

    @SuppressWarnings("SameParameterValue")
    private static KafkaDynamicSink createExpectedSink(
            DataType physicalDataType,
            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            @Nullable String keyPrefix,
            String topic,
            Properties properties,
            @Nullable FlinkKafkaPartitioner<RowData> partitioner,
            SinkSemantic semantic,
            @Nullable Integer parallelism) {
        return new KafkaDynamicSink(
                physicalDataType,
                physicalDataType,
                keyEncodingFormat,
                valueEncodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topic,
                properties,
                partitioner,
                semantic,
                false,
                SinkBufferFlushMode.DISABLED,
                parallelism);
    }

    /**
     * Returns the full options modified by the given consumer {@code optionModifier}.
     *
     * @param optionModifier Consumer to modify the options
     */
    private static Map<String, String> getModifiedOptions(
            Map<String, String> options, Consumer<Map<String, String>> optionModifier) {
        optionModifier.accept(options);
        return options;
    }

    protected Map<String, String> getBasicSourceOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Kafka specific options.
        String identifier;
        if (isLegacySource()) {
            identifier = KafkaLegacyDynamicTableFactory.IDENTIFIER;
        } else {
            identifier = KafkaDynamicTableFactory.IDENTIFIER;
        }
        tableOptions.put("connector", identifier);
        tableOptions.put("topic", TOPIC);
        tableOptions.put("properties.group.id", "dummy");
        tableOptions.put("properties.bootstrap.servers", "dummy");
        tableOptions.put("scan.startup.mode", "specific-offsets");
        tableOptions.put("scan.startup.specific-offsets", PROPS_SCAN_OFFSETS);
        tableOptions.put("scan.topic-partition-discovery.interval", DISCOVERY_INTERVAL);
        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        final String formatDelimiterKey =
                String.format(
                        "%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
        final String failOnMissingKey =
                String.format(
                        "%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key());
        tableOptions.put(formatDelimiterKey, ",");
        tableOptions.put(failOnMissingKey, "true");
        return tableOptions;
    }

    private static Map<String, String> getBasicSinkOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Kafka specific options.
        tableOptions.put("connector", KafkaDynamicTableFactory.IDENTIFIER);
        tableOptions.put("topic", TOPIC);
        tableOptions.put("properties.group.id", "dummy");
        tableOptions.put("properties.bootstrap.servers", "dummy");
        tableOptions.put(
                "sink.partitioner", KafkaConnectorOptionsUtil.SINK_PARTITIONER_VALUE_FIXED);
        tableOptions.put("sink.semantic", SinkSemantic.EXACTLY_ONCE.toString());
        // Format options.
        tableOptions.put("format", TestFormatFactory.IDENTIFIER);
        final String formatDelimiterKey =
                String.format(
                        "%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
        tableOptions.put(formatDelimiterKey, ",");
        return tableOptions;
    }

    private Map<String, String> getKeyValueOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        // Kafka specific options.
        String identifier;
        if (isLegacySource()) {
            identifier = KafkaLegacyDynamicTableFactory.IDENTIFIER;
        } else {
            identifier = KafkaDynamicTableFactory.IDENTIFIER;
        }
        tableOptions.put("connector", identifier);
        tableOptions.put("topic", TOPIC);
        tableOptions.put("properties.group.id", "dummy");
        tableOptions.put("properties.bootstrap.servers", "dummy");
        tableOptions.put("scan.topic-partition-discovery.interval", DISCOVERY_INTERVAL);
        tableOptions.put(
                "sink.partitioner", KafkaConnectorOptionsUtil.SINK_PARTITIONER_VALUE_FIXED);
        tableOptions.put("sink.semantic", SinkSemantic.EXACTLY_ONCE.toString());
        // Format options.
        tableOptions.put("key.format", TestFormatFactory.IDENTIFIER);
        tableOptions.put(
                String.format(
                        "key.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
                "#");
        tableOptions.put("key.fields", NAME);
        tableOptions.put("value.format", TestFormatFactory.IDENTIFIER);
        tableOptions.put(
                String.format(
                        "value.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
                "|");
        tableOptions.put(
                "value.fields-include",
                KafkaConnectorOptions.ValueFieldsStrategy.EXCEPT_KEY.toString());
        return tableOptions;
    }
}
