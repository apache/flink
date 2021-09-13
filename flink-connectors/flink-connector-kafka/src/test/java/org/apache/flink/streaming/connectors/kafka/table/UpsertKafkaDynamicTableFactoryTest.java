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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.runtime.operators.sink.SinkOperatorFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.AVRO_CONFLUENT;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link UpsertKafkaDynamicTableFactory}. */
public class UpsertKafkaDynamicTableFactoryTest extends TestLogger {

    private static final String SOURCE_TOPIC = "sourceTopic_1";

    private static final String SINK_TOPIC = "sinkTopic";

    private static final String TEST_REGISTRY_URL = "http://localhost:8081";
    private static final String DEFAULT_VALUE_SUBJECT = SINK_TOPIC + "-value";
    private static final String DEFAULT_KEY_SUBJECT = SINK_TOPIC + "-key";

    private static final ResolvedSchema SOURCE_SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("window_start", DataTypes.STRING().notNull()),
                            Column.physical("region", DataTypes.STRING().notNull()),
                            Column.physical("view_count", DataTypes.BIGINT())),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("name", Arrays.asList("window_start", "region")));

    private static final int[] SOURCE_KEY_FIELDS = new int[] {0, 1};

    private static final int[] SOURCE_VALUE_FIELDS = new int[] {0, 1, 2};

    private static final ResolvedSchema SINK_SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical(
                                    "region", new AtomicDataType(new VarCharType(false, 100))),
                            Column.physical("view_count", DataTypes.BIGINT())),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("name", Collections.singletonList("region")));

    private static final int[] SINK_KEY_FIELDS = new int[] {0};

    private static final int[] SINK_VALUE_FIELDS = new int[] {0, 1};

    private static final Properties UPSERT_KAFKA_SOURCE_PROPERTIES = new Properties();
    private static final Properties UPSERT_KAFKA_SINK_PROPERTIES = new Properties();

    static {
        UPSERT_KAFKA_SOURCE_PROPERTIES.setProperty("bootstrap.servers", "dummy");

        UPSERT_KAFKA_SINK_PROPERTIES.setProperty("bootstrap.servers", "dummy");
    }

    static EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
            new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());
    static EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
            new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());

    static DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat =
            new TestFormatFactory.DecodingFormatMock(
                    ",", true, ChangelogMode.insertOnly(), Collections.emptyMap());
    static DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
            new TestFormatFactory.DecodingFormatMock(
                    ",", true, ChangelogMode.insertOnly(), Collections.emptyMap());

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testTableSource() {
        final DataType producedDataType = SOURCE_SCHEMA.toPhysicalRowDataType();
        // Construct table source using options and table source factory
        final DynamicTableSource actualSource =
                createTableSource(SOURCE_SCHEMA, getFullSourceOptions());

        final KafkaDynamicSource expectedSource =
                createExpectedScanSource(
                        producedDataType,
                        keyDecodingFormat,
                        valueDecodingFormat,
                        SOURCE_KEY_FIELDS,
                        SOURCE_VALUE_FIELDS,
                        null,
                        SOURCE_TOPIC,
                        UPSERT_KAFKA_SOURCE_PROPERTIES);
        assertEquals(actualSource, expectedSource);

        final KafkaDynamicSource actualUpsertKafkaSource = (KafkaDynamicSource) actualSource;
        ScanTableSource.ScanRuntimeProvider provider =
                actualUpsertKafkaSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        assertKafkaSource(provider);
    }

    @Test
    public void testTableSink() {
        // Construct table sink using options and table sink factory.
        final DynamicTableSink actualSink = createTableSink(SINK_SCHEMA, getFullSinkOptions());

        final DynamicTableSink expectedSink =
                createExpectedSink(
                        SINK_SCHEMA.toPhysicalRowDataType(),
                        keyEncodingFormat,
                        valueEncodingFormat,
                        SINK_KEY_FIELDS,
                        SINK_VALUE_FIELDS,
                        null,
                        SINK_TOPIC,
                        UPSERT_KAFKA_SINK_PROPERTIES,
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        SinkBufferFlushMode.DISABLED,
                        null);

        // Test sink format.
        final KafkaDynamicSink actualUpsertKafkaSink = (KafkaDynamicSink) actualSink;
        assertEquals(expectedSink, actualSink);

        // Test kafka producer.
        DynamicTableSink.SinkRuntimeProvider provider =
                actualUpsertKafkaSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider, instanceOf(SinkProvider.class));
        final SinkProvider sinkFunctionProvider = (SinkProvider) provider;
        final Sink<RowData, ?, ?, ?> sink = sinkFunctionProvider.createSink();
        assertThat(sink, instanceOf(KafkaSink.class));
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testBufferedTableSink() {
        // Construct table sink using options and table sink factory.
        final DynamicTableSink actualSink =
                createTableSink(
                        SINK_SCHEMA,
                        getModifiedOptions(
                                getFullSinkOptions(),
                                options -> {
                                    options.put("sink.buffer-flush.max-rows", "100");
                                    options.put("sink.buffer-flush.interval", "1s");
                                }));

        final DynamicTableSink expectedSink =
                createExpectedSink(
                        SINK_SCHEMA.toPhysicalRowDataType(),
                        keyEncodingFormat,
                        valueEncodingFormat,
                        SINK_KEY_FIELDS,
                        SINK_VALUE_FIELDS,
                        null,
                        SINK_TOPIC,
                        UPSERT_KAFKA_SINK_PROPERTIES,
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        new SinkBufferFlushMode(100, 1000L),
                        null);

        // Test sink format.
        final KafkaDynamicSink actualUpsertKafkaSink = (KafkaDynamicSink) actualSink;
        assertEquals(expectedSink, actualSink);

        // Test kafka producer.
        DynamicTableSink.SinkRuntimeProvider provider =
                actualUpsertKafkaSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider, instanceOf(DataStreamSinkProvider.class));
        final DataStreamSinkProvider sinkProvider = (DataStreamSinkProvider) provider;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        sinkProvider.consumeDataStream(env.fromElements(new BinaryRowData(1)));
        final StreamOperatorFactory<?> sinkOperatorFactory =
                env.getStreamGraph().getStreamNodes().stream()
                        .filter(n -> n.getOperatorName().contains("Sink"))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Expected operator with name Sink in stream graph."))
                        .getOperatorFactory();
        assertThat(sinkOperatorFactory, instanceOf(SinkOperatorFactory.class));
        assertThat(
                ((SinkOperatorFactory) sinkOperatorFactory).getSink(),
                instanceOf(ReducingUpsertSink.class));
    }

    @Test
    public void testTableSinkWithParallelism() {
        final Map<String, String> modifiedOptions =
                getModifiedOptions(
                        getFullSinkOptions(), options -> options.put("sink.parallelism", "100"));
        final DynamicTableSink actualSink = createTableSink(SINK_SCHEMA, modifiedOptions);

        final DynamicTableSink expectedSink =
                createExpectedSink(
                        SINK_SCHEMA.toPhysicalRowDataType(),
                        keyEncodingFormat,
                        valueEncodingFormat,
                        SINK_KEY_FIELDS,
                        SINK_VALUE_FIELDS,
                        null,
                        SINK_TOPIC,
                        UPSERT_KAFKA_SINK_PROPERTIES,
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        SinkBufferFlushMode.DISABLED,
                        100);
        assertEquals(expectedSink, actualSink);

        final DynamicTableSink.SinkRuntimeProvider provider =
                actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat(provider, instanceOf(SinkProvider.class));
        final SinkProvider sinkProvider = (SinkProvider) provider;
        assertTrue(sinkProvider.getParallelism().isPresent());
        assertEquals(100, (long) sinkProvider.getParallelism().get());
    }

    @Test
    public void testTableSinkAutoCompleteSchemaRegistrySubject() {
        // value.format + key.format
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "avro-confluent");
                    options.put("value.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.format", "avro-confluent");
                    options.put("key.avro-confluent.url", TEST_REGISTRY_URL);
                },
                DEFAULT_VALUE_SUBJECT,
                DEFAULT_KEY_SUBJECT);

        // value.format + non-avro key.format
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "avro-confluent");
                    options.put("value.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.format", "csv");
                },
                DEFAULT_VALUE_SUBJECT,
                "N/A");

        // non-avro value.format + key.format
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "json");
                    options.put("key.format", "avro-confluent");
                    options.put("key.avro-confluent.url", TEST_REGISTRY_URL);
                },
                "N/A",
                DEFAULT_KEY_SUBJECT);

        // not override for 'key.format'
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "avro-confluent");
                    options.put("value.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.format", "avro-confluent");
                    options.put("key.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("key.avro-confluent.subject", "sub2");
                },
                DEFAULT_VALUE_SUBJECT,
                "sub2");

        // not override for 'value.format'
        verifyEncoderSubject(
                options -> {
                    options.put("value.format", "avro-confluent");
                    options.put("value.avro-confluent.url", TEST_REGISTRY_URL);
                    options.put("value.avro-confluent.subject", "sub1");
                    options.put("key.format", "avro-confluent");
                    options.put("key.avro-confluent.url", TEST_REGISTRY_URL);
                },
                "sub1",
                DEFAULT_KEY_SUBJECT);
    }

    private void verifyEncoderSubject(
            Consumer<Map<String, String>> optionModifier,
            String expectedValueSubject,
            String expectedKeySubject) {
        Map<String, String> options = new HashMap<>();
        // Kafka specific options.
        options.put("connector", UpsertKafkaDynamicTableFactory.IDENTIFIER);
        options.put("topic", SINK_TOPIC);
        options.put("properties.group.id", "dummy");
        options.put("properties.bootstrap.servers", "dummy");
        optionModifier.accept(options);

        final RowType rowType = (RowType) SINK_SCHEMA.toSinkRowDataType().getLogicalType();
        final String valueFormat =
                options.getOrDefault(
                        FactoryUtil.FORMAT.key(),
                        options.get(KafkaConnectorOptions.VALUE_FORMAT.key()));
        final String keyFormat = options.get(KafkaConnectorOptions.KEY_FORMAT.key());

        KafkaDynamicSink sink = (KafkaDynamicSink) createTableSink(SINK_SCHEMA, options);

        if (AVRO_CONFLUENT.equals(valueFormat)) {
            SerializationSchema<RowData> actualValueEncoder =
                    sink.valueEncodingFormat.createRuntimeEncoder(
                            new SinkRuntimeProviderContext(false), SINK_SCHEMA.toSinkRowDataType());
            assertEquals(
                    createConfluentAvroSerSchema(rowType, expectedValueSubject),
                    actualValueEncoder);
        }

        if (AVRO_CONFLUENT.equals(keyFormat)) {
            assert sink.keyEncodingFormat != null;
            SerializationSchema<RowData> actualKeyEncoder =
                    sink.keyEncodingFormat.createRuntimeEncoder(
                            new SinkRuntimeProviderContext(false), SINK_SCHEMA.toSinkRowDataType());
            assertEquals(
                    createConfluentAvroSerSchema(rowType, expectedKeySubject), actualKeyEncoder);
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

    // --------------------------------------------------------------------------------------------
    // Negative tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testCreateSourceTableWithoutPK() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "'upsert-kafka' tables require to define a PRIMARY KEY constraint. "
                                        + "The PRIMARY KEY specifies which columns should be read from or write to the Kafka message key. "
                                        + "The PRIMARY KEY also defines records in the 'upsert-kafka' table should update or delete on which keys.")));

        ResolvedSchema illegalSchema =
                ResolvedSchema.of(
                        Column.physical("window_start", DataTypes.STRING()),
                        Column.physical("region", DataTypes.STRING()),
                        Column.physical("view_count", DataTypes.BIGINT()));
        createTableSource(illegalSchema, getFullSourceOptions());
    }

    @Test
    public void testCreateSinkTableWithoutPK() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "'upsert-kafka' tables require to define a PRIMARY KEY constraint. "
                                        + "The PRIMARY KEY specifies which columns should be read from or write to the Kafka message key. "
                                        + "The PRIMARY KEY also defines records in the 'upsert-kafka' table should update or delete on which keys.")));

        ResolvedSchema illegalSchema =
                ResolvedSchema.of(
                        Column.physical("region", DataTypes.STRING()),
                        Column.physical("view_count", DataTypes.BIGINT()));
        createTableSink(illegalSchema, getFullSinkOptions());
    }

    @Test
    public void testSerWithCDCFormatAsValue() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                String.format(
                                        "'upsert-kafka' connector doesn't support '%s' as value format, "
                                                + "because '%s' is not in insert-only mode.",
                                        TestFormatFactory.IDENTIFIER,
                                        TestFormatFactory.IDENTIFIER))));

        createTableSink(
                SINK_SCHEMA,
                getModifiedOptions(
                        getFullSinkOptions(),
                        options ->
                                options.put(
                                        String.format(
                                                "value.%s.%s",
                                                TestFormatFactory.IDENTIFIER,
                                                TestFormatFactory.CHANGELOG_MODE.key()),
                                        "I;UA;UB;D")));
    }

    @Test
    public void testDeserWithCDCFormatAsValue() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                String.format(
                                        "'upsert-kafka' connector doesn't support '%s' as value format, "
                                                + "because '%s' is not in insert-only mode.",
                                        TestFormatFactory.IDENTIFIER,
                                        TestFormatFactory.IDENTIFIER))));

        createTableSource(
                SOURCE_SCHEMA,
                getModifiedOptions(
                        getFullSourceOptions(),
                        options ->
                                options.put(
                                        String.format(
                                                "value.%s.%s",
                                                TestFormatFactory.IDENTIFIER,
                                                TestFormatFactory.CHANGELOG_MODE.key()),
                                        "I;UA;UB;D")));
    }

    @Test
    public void testInvalidSinkBufferFlush() {
        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "'sink.buffer-flush.max-rows' and 'sink.buffer-flush.interval' "
                                        + "must be set to be greater than zero together to enable"
                                        + " sink buffer flushing.")));
        createTableSink(
                SINK_SCHEMA,
                getModifiedOptions(
                        getFullSinkOptions(),
                        options -> {
                            options.put("sink.buffer-flush.max-rows", "0");
                            options.put("sink.buffer-flush.interval", "1s");
                        }));
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

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

    private static Map<String, String> getFullSourceOptions() {
        // table options
        Map<String, String> options = new HashMap<>();
        options.put("connector", UpsertKafkaDynamicTableFactory.IDENTIFIER);
        options.put("topic", SOURCE_TOPIC);
        options.put("properties.bootstrap.servers", "dummy");
        // key format options
        options.put("key.format", TestFormatFactory.IDENTIFIER);
        options.put(
                String.format(
                        "key.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
                ",");
        options.put(
                String.format(
                        "key.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key()),
                "true");
        options.put(
                String.format(
                        "key.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()),
                "I");
        // value format options
        options.put("value.format", TestFormatFactory.IDENTIFIER);
        options.put(
                String.format(
                        "value.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
                ",");
        options.put(
                String.format(
                        "value.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key()),
                "true");
        options.put(
                String.format(
                        "value.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()),
                "I");
        return options;
    }

    private static Map<String, String> getFullSinkOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", UpsertKafkaDynamicTableFactory.IDENTIFIER);
        options.put("topic", SINK_TOPIC);
        options.put("properties.bootstrap.servers", "dummy");
        // key format options
        options.put("value.format", TestFormatFactory.IDENTIFIER);
        options.put(
                String.format(
                        "key.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
                ",");
        options.put(
                String.format(
                        "key.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()),
                "I");
        // value format options
        options.put("key.format", TestFormatFactory.IDENTIFIER);
        options.put(
                String.format(
                        "value.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
                ",");
        options.put(
                String.format(
                        "value.%s.%s",
                        TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()),
                "I");
        return options;
    }

    private KafkaDynamicSource createExpectedScanSource(
            DataType producedDataType,
            DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] keyFields,
            int[] valueFields,
            String keyPrefix,
            String topic,
            Properties properties) {
        return new KafkaDynamicSource(
                producedDataType,
                keyDecodingFormat,
                new UpsertKafkaDynamicTableFactory.DecodingFormatWrapper(valueDecodingFormat),
                keyFields,
                valueFields,
                keyPrefix,
                Collections.singletonList(topic),
                null,
                properties,
                StartupMode.EARLIEST,
                Collections.emptyMap(),
                0,
                true,
                FactoryMocks.IDENTIFIER.asSummaryString());
    }

    private static KafkaDynamicSink createExpectedSink(
            DataType consumedDataType,
            EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] keyProjection,
            int[] valueProjection,
            String keyPrefix,
            String topic,
            Properties properties,
            DeliveryGuarantee deliveryGuarantee,
            SinkBufferFlushMode flushMode,
            Integer parallelism) {
        return new KafkaDynamicSink(
                consumedDataType,
                consumedDataType,
                keyEncodingFormat,
                new UpsertKafkaDynamicTableFactory.EncodingFormatWrapper(valueEncodingFormat),
                keyProjection,
                valueProjection,
                keyPrefix,
                topic,
                properties,
                null,
                deliveryGuarantee,
                true,
                flushMode,
                parallelism,
                null);
    }

    private void assertKafkaSource(ScanTableSource.ScanRuntimeProvider provider) {
        assertThat(provider, instanceOf(DataStreamScanProvider.class));
        final DataStreamScanProvider dataStreamScanProvider = (DataStreamScanProvider) provider;
        final Transformation<RowData> transformation =
                dataStreamScanProvider
                        .produceDataStream(StreamExecutionEnvironment.createLocalEnvironment())
                        .getTransformation();
        assertThat(transformation, instanceOf(SourceTransformation.class));
        SourceTransformation<RowData, KafkaPartitionSplit, KafkaSourceEnumState>
                sourceTransformation =
                        (SourceTransformation<RowData, KafkaPartitionSplit, KafkaSourceEnumState>)
                                transformation;
        assertThat(sourceTransformation.getSource(), instanceOf(KafkaSource.class));
    }
}
