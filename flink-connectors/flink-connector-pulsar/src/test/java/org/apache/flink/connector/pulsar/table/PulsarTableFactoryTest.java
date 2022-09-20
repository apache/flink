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

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.table.sink.PulsarTableSerializationSchemaFactory;
import org.apache.flink.connector.pulsar.table.sink.PulsarTableSink;
import org.apache.flink.connector.pulsar.table.source.PulsarTableDeserializationSchemaFactory;
import org.apache.flink.connector.pulsar.table.source.PulsarTableSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.api.SubscriptionType;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.table.PulsarTableFactory.UPSERT_DISABLED;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.ADMIN_URL;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FIELDS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FORMAT;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SERVICE_URL;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.TOPICS;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test aims to verify that {@link org.apache.flink.connector.pulsar.table.PulsarTableFactory}
 * can consume proper config options and produce expected {@link PulsarTableSource} and {@link
 * PulsarTableSink}. It guarantees that config options is used internally by the implementation
 * classes.
 */
public class PulsarTableFactoryTest {
    private static final String TEST_TOPIC = "test-topic";
    private static final String TEST_ADMIN_URL = "http://my-broker.example.com:8080";
    private static final String TEST_SERVICE_URL = "pulsar://localhost:6650";
    private static final String TEST_SUBSCRIPTION_NAME = "default-subscription";

    private static final String NAME = "name";
    private static final String COUNT = "count";
    private static final String TIME = "time";
    private static final String METADATA = "metadata";
    private static final String WATERMARK_EXPRESSION = TIME + " - INTERVAL '5' SECOND";
    private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
    private static final String COMPUTED_COLUMN_NAME = "computed-column";
    private static final String COMPUTED_COLUMN_EXPRESSION = COUNT + " + 1.0";
    private static final DataType COMPUTED_COLUMN_DATATYPE = DataTypes.DECIMAL(10, 3);

    private static final Properties EXPECTED_PULSAR_SOURCE_PROPERTIES = new Properties();
    private static final Properties EXPECTED_PULSAR_SINK_PROPERTIES = new Properties();

    private static final String FORMAT_DELIMITER_KEY =
            String.format("%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());

    private static final String FORMAT_FAIL_ON_MISSING_KEY =
            String.format(
                    "%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key());

    static {
        EXPECTED_PULSAR_SOURCE_PROPERTIES.setProperty(PULSAR_ADMIN_URL.key(), TEST_ADMIN_URL);
        EXPECTED_PULSAR_SOURCE_PROPERTIES.setProperty(PULSAR_SERVICE_URL.key(), TEST_SERVICE_URL);
        EXPECTED_PULSAR_SOURCE_PROPERTIES.setProperty(
                PULSAR_SUBSCRIPTION_NAME.key(), TEST_SUBSCRIPTION_NAME);

        EXPECTED_PULSAR_SINK_PROPERTIES.setProperty(PULSAR_ADMIN_URL.key(), TEST_ADMIN_URL);
        EXPECTED_PULSAR_SINK_PROPERTIES.setProperty(PULSAR_SERVICE_URL.key(), TEST_SERVICE_URL);
    }

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
                            Column.metadata(TIME, DataTypes.TIMESTAMP(3), "publish_time", false),
                            Column.metadata(
                                    METADATA, DataTypes.STRING(), "value.metadata_2", false)),
                    Collections.emptyList(),
                    null);

    private static final DataType SCHEMA_DATA_TYPE = SCHEMA.toPhysicalRowDataType();

    @Test
    public void testTableSource() {
        final Map<String, String> configuration = getBasicSourceOptions();
        final PulsarTableSource actualPulsarSource =
                (PulsarTableSource) createTableSource(SCHEMA, configuration);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                new TestFormatFactory.DecodingFormatMock(",", true);

        final PulsarTableDeserializationSchemaFactory deserializationSchemaFactory =
                new PulsarTableDeserializationSchemaFactory(
                        SCHEMA_DATA_TYPE,
                        null,
                        new int[0],
                        valueDecodingFormat,
                        new int[] {0, 1, 2},
                        UPSERT_DISABLED);

        final PulsarTableSource expectedPulsarSource =
                new PulsarTableSource(
                        deserializationSchemaFactory,
                        valueDecodingFormat,
                        valueDecodingFormat.getChangelogMode(),
                        Lists.list(TEST_TOPIC),
                        EXPECTED_PULSAR_SOURCE_PROPERTIES,
                        StartCursor.earliest(),
                        StopCursor.never(),
                        SubscriptionType.Exclusive);
        assertThat(actualPulsarSource).isEqualTo(expectedPulsarSource);

        ScanTableSource.ScanRuntimeProvider provider =
                actualPulsarSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertPulsarSourceIsSameAsExpected(provider);
    }

    @Test
    public void testTableSourceWithKeyValue() {
        final Map<String, String> configuration = getSourceKeyValueOptions();

        final PulsarTableSource actualPulsarSource =
                (PulsarTableSource) createTableSource(SCHEMA, configuration);
        // initialize stateful testing formats
        actualPulsarSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        final TestFormatFactory.DecodingFormatMock keyDecodingFormat =
                new TestFormatFactory.DecodingFormatMock("#", false);
        keyDecodingFormat.producedDataType =
                DataTypes.ROW(DataTypes.FIELD(NAME, DataTypes.STRING().notNull())).notNull();

        final TestFormatFactory.DecodingFormatMock valueDecodingFormat =
                new TestFormatFactory.DecodingFormatMock("|", false);
        valueDecodingFormat.producedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
                                DataTypes.FIELD(TIME, DataTypes.TIMESTAMP(3)))
                        .notNull();

        final PulsarTableDeserializationSchemaFactory deserializationSchemaFactory =
                new PulsarTableDeserializationSchemaFactory(
                        SCHEMA_DATA_TYPE,
                        keyDecodingFormat,
                        new int[] {0},
                        valueDecodingFormat,
                        new int[] {1, 2},
                        UPSERT_DISABLED);

        final PulsarTableSource expectedPulsarSource =
                new PulsarTableSource(
                        deserializationSchemaFactory,
                        valueDecodingFormat,
                        valueDecodingFormat.getChangelogMode(),
                        Lists.list(TEST_TOPIC),
                        EXPECTED_PULSAR_SOURCE_PROPERTIES,
                        StartCursor.earliest(),
                        StopCursor.never(),
                        SubscriptionType.Exclusive);

        assertThat(actualPulsarSource).isEqualTo(expectedPulsarSource);
    }

    @Test
    public void testTableSourceWithKeyValueAndMetadata() {
        final Map<String, String> options = getSourceKeyValueOptions();
        options.put("test-format.readable-metadata", "metadata_1:INT, metadata_2:STRING");

        final PulsarTableSource actualPulsarSource =
                (PulsarTableSource) createTableSource(SCHEMA_WITH_METADATA, options);
        // initialize stateful testing formats
        actualPulsarSource.applyReadableMetadata(
                Arrays.asList("publish_time", "value.metadata_2"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        actualPulsarSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        final TestFormatFactory.DecodingFormatMock expectedKeyFormat =
                new TestFormatFactory.DecodingFormatMock(
                        "#", false, ChangelogMode.insertOnly(), Collections.emptyMap());
        expectedKeyFormat.producedDataType =
                DataTypes.ROW(DataTypes.FIELD(NAME, DataTypes.STRING())).notNull();

        final Map<String, DataType> expectedReadableMetadata = new HashMap<>();
        expectedReadableMetadata.put("metadata_1", DataTypes.INT());
        expectedReadableMetadata.put("metadata_2", DataTypes.STRING());

        final TestFormatFactory.DecodingFormatMock expectedValueFormat =
                new TestFormatFactory.DecodingFormatMock(
                        "|", false, ChangelogMode.insertOnly(), expectedReadableMetadata);
        expectedValueFormat.producedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
                                DataTypes.FIELD("metadata_2", DataTypes.STRING()))
                        .notNull();
        expectedValueFormat.metadataKeys = Collections.singletonList("metadata_2");

        final PulsarTableDeserializationSchemaFactory deserializationSchemaFactory =
                new PulsarTableDeserializationSchemaFactory(
                        SCHEMA_WITH_METADATA.toPhysicalRowDataType(),
                        expectedKeyFormat,
                        new int[] {0},
                        expectedValueFormat,
                        new int[] {1},
                        UPSERT_DISABLED);

        final PulsarTableSource expectedPulsarSource =
                new PulsarTableSource(
                        deserializationSchemaFactory,
                        expectedValueFormat,
                        expectedValueFormat.getChangelogMode(),
                        Lists.list(TEST_TOPIC),
                        EXPECTED_PULSAR_SOURCE_PROPERTIES,
                        StartCursor.earliest(),
                        StopCursor.never(),
                        SubscriptionType.Exclusive);

        deserializationSchemaFactory.setProducedDataType(
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        deserializationSchemaFactory.setConnectorMetadataKeys(
                Collections.singletonList("publish_time"));

        assertThat(actualPulsarSource).isEqualTo(expectedPulsarSource);
    }

    @Test
    public void testTableSink() {
        final Map<String, String> modifiedOptions = getBasicSinkOptions();
        final DynamicTableSink actualPulsarTableSink = createTableSink(SCHEMA, modifiedOptions);

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
                new TestFormatFactory.EncodingFormatMock(",");

        final PulsarTableSerializationSchemaFactory serializationSchemaFactory =
                new PulsarTableSerializationSchemaFactory(
                        SCHEMA_DATA_TYPE,
                        null,
                        new int[0],
                        valueEncodingFormat,
                        new int[] {0, 1, 2},
                        UPSERT_DISABLED);

        final PulsarTableSink expectedPulsarTableSink =
                new PulsarTableSink(
                        serializationSchemaFactory,
                        valueEncodingFormat.getChangelogMode(),
                        Lists.list(TEST_TOPIC),
                        EXPECTED_PULSAR_SINK_PROPERTIES,
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        null,
                        TopicRoutingMode.ROUND_ROBIN,
                        0);
        assertThat(actualPulsarTableSink).isEqualTo(expectedPulsarTableSink);

        DynamicTableSink.SinkRuntimeProvider provider =
                actualPulsarTableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider).isInstanceOf(SinkV2Provider.class);
        final SinkV2Provider sinkProvider = (SinkV2Provider) provider;
        final Sink<RowData> sinkFunction = sinkProvider.createSink();
        assertThat(sinkFunction).isInstanceOf(PulsarSink.class);
    }

    @Test
    public void testTableSinkWithKeyValue() {
        final Map<String, String> modifiedOptions = getSinkKeyValueOptions();
        final PulsarTableSink actualPulsarTableSink =
                (PulsarTableSink) createTableSink(SCHEMA, modifiedOptions);
        // initialize stateful testing formats
        actualPulsarTableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        final TestFormatFactory.EncodingFormatMock keyEncodingFormat =
                new TestFormatFactory.EncodingFormatMock("#");
        keyEncodingFormat.consumedDataType =
                DataTypes.ROW(DataTypes.FIELD(NAME, DataTypes.STRING().notNull())).notNull();

        final TestFormatFactory.EncodingFormatMock valueEncodingFormat =
                new TestFormatFactory.EncodingFormatMock("|");
        valueEncodingFormat.consumedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
                                DataTypes.FIELD(TIME, DataTypes.TIMESTAMP(3)))
                        .notNull();

        final PulsarTableSerializationSchemaFactory serializationSchemaFactory =
                new PulsarTableSerializationSchemaFactory(
                        SCHEMA_DATA_TYPE,
                        keyEncodingFormat,
                        new int[] {0},
                        valueEncodingFormat,
                        new int[] {1, 2},
                        UPSERT_DISABLED);

        final PulsarTableSink expectedPulsarTableSink =
                new PulsarTableSink(
                        serializationSchemaFactory,
                        valueEncodingFormat.getChangelogMode(),
                        Lists.list(TEST_TOPIC),
                        EXPECTED_PULSAR_SINK_PROPERTIES,
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        null,
                        TopicRoutingMode.ROUND_ROBIN,
                        0);
        assertThat(actualPulsarTableSink).isEqualTo(expectedPulsarTableSink);
    }

    private static Map<String, String> getBasicSourceOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CONNECTOR.key(), PulsarTableFactory.IDENTIFIER);
        tableOptions.put(TOPICS.key(), TEST_TOPIC);
        tableOptions.put(ADMIN_URL.key(), TEST_ADMIN_URL);
        tableOptions.put(SERVICE_URL.key(), TEST_SERVICE_URL);
        tableOptions.put(SOURCE_SUBSCRIPTION_NAME.key(), TEST_SUBSCRIPTION_NAME);
        // Format options.
        tableOptions.put(FORMAT.key(), TestFormatFactory.IDENTIFIER);
        tableOptions.put(FORMAT_DELIMITER_KEY, ",");
        tableOptions.put(FORMAT_FAIL_ON_MISSING_KEY, "true");
        return tableOptions;
    }

    private static Map<String, String> getSourceKeyValueOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CONNECTOR.key(), PulsarTableFactory.IDENTIFIER);
        tableOptions.put(TOPICS.key(), TEST_TOPIC);
        tableOptions.put(ADMIN_URL.key(), TEST_ADMIN_URL);
        tableOptions.put(SERVICE_URL.key(), TEST_SERVICE_URL);
        tableOptions.put(SOURCE_SUBSCRIPTION_NAME.key(), TEST_SUBSCRIPTION_NAME);
        // Format options.
        tableOptions.put(FORMAT.key(), TestFormatFactory.IDENTIFIER);
        tableOptions.put(FORMAT_DELIMITER_KEY, "|");
        tableOptions.put(FORMAT_FAIL_ON_MISSING_KEY, "false");

        tableOptions.put(KEY_FORMAT.key(), TestFormatFactory.IDENTIFIER);
        tableOptions.put("key." + FORMAT_DELIMITER_KEY, "#");
        tableOptions.put("key." + FORMAT_FAIL_ON_MISSING_KEY, "false");
        tableOptions.put(KEY_FIELDS.key(), NAME);
        return tableOptions;
    }

    private static Map<String, String> getBasicSinkOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CONNECTOR.key(), PulsarTableFactory.IDENTIFIER);
        tableOptions.put(TOPICS.key(), TEST_TOPIC);
        tableOptions.put(ADMIN_URL.key(), TEST_ADMIN_URL);
        tableOptions.put(SERVICE_URL.key(), TEST_SERVICE_URL);
        // Format options.
        tableOptions.put(FORMAT.key(), TestFormatFactory.IDENTIFIER);
        tableOptions.put(FORMAT_DELIMITER_KEY, ",");
        tableOptions.put(FORMAT_FAIL_ON_MISSING_KEY, "true");
        return tableOptions;
    }

    private static Map<String, String> getSinkKeyValueOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CONNECTOR.key(), PulsarTableFactory.IDENTIFIER);
        tableOptions.put(TOPICS.key(), TEST_TOPIC);
        tableOptions.put(ADMIN_URL.key(), TEST_ADMIN_URL);
        tableOptions.put(SERVICE_URL.key(), TEST_SERVICE_URL);
        // Format options.
        tableOptions.put(FORMAT.key(), TestFormatFactory.IDENTIFIER);
        tableOptions.put(FORMAT_DELIMITER_KEY, "|");
        tableOptions.put(FORMAT_FAIL_ON_MISSING_KEY, "false");

        tableOptions.put(KEY_FORMAT.key(), TestFormatFactory.IDENTIFIER);
        tableOptions.put("key." + FORMAT_DELIMITER_KEY, "#");
        tableOptions.put("key." + FORMAT_FAIL_ON_MISSING_KEY, "false");
        tableOptions.put(KEY_FIELDS.key(), NAME);
        return tableOptions;
    }

    private PulsarSource<RowData> assertPulsarSourceIsSameAsExpected(
            ScanTableSource.ScanRuntimeProvider provider) {
        assertThat(provider).isInstanceOf(SourceProvider.class);
        final SourceProvider sourceProvider = (SourceProvider) provider;
        return (PulsarSource<RowData>) sourceProvider.createSource();
    }
}
