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

package org.apache.flink.formats.avro.registry.confluent.debezium;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.registry.confluent.ConfluentSchemaRegistryCoder;
import org.apache.flink.formats.avro.registry.confluent.debezium.DebeziumAvroDecodingFormat.ReadableMetadata;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DebeziumAvroDeserializationSchema}. */
class DebeziumAvroSerDeSchemaTest {

    private static final String SUBJECT = "testDebeziumAvro";

    private static final RowType rowType =
            (RowType)
                    ROW(
                                    FIELD("id", BIGINT()),
                                    FIELD("name", STRING()),
                                    FIELD("description", STRING()),
                                    FIELD("weight", DOUBLE()))
                            .getLogicalType();

    private static final Schema DEBEZIUM_SCHEMA_COMPATIBLE_TEST =
            new Schema.Parser().parse(new String(readBytesFromFile("debezium-test-schema.json")));

    private static final DataType EVOLVING_PHYSICAL_TYPE = ROW(FIELD("id", BIGINT()));

    private final SchemaRegistryClient client = new MockSchemaRegistryClient();

    @Test
    void testSerializationDeserialization() throws Exception {

        RowType rowTypeDe =
                DebeziumAvroDeserializationSchema.createDebeziumAvroRowType(
                        fromLogicalToDataType(rowType), Collections.emptyList());
        RowType rowTypeSe =
                DebeziumAvroSerializationSchema.createDebeziumAvroRowType(
                        fromLogicalToDataType(rowType));

        DebeziumAvroSerializationSchema dbzSerializer =
                new DebeziumAvroSerializationSchema(getSerializationSchema(rowTypeSe));
        dbzSerializer.open(new MockInitializationContext());

        byte[] serialize = dbzSerializer.serialize(debeziumRow2RowData());

        client.register(SUBJECT, DEBEZIUM_SCHEMA_COMPATIBLE_TEST);
        DebeziumAvroDeserializationSchema dbzDeserializer =
                new DebeziumAvroDeserializationSchema(
                        InternalTypeInfo.of(rowType), getDeserializationSchema(rowTypeDe));
        dbzDeserializer.open(new MockInitializationContext());

        SimpleCollector collector = new SimpleCollector();
        dbzDeserializer.deserialize(serialize, collector);

        List<String> actual =
                collector.list.stream().map(Object::toString).collect(Collectors.toList());

        List<String> expected =
                Collections.singletonList("+I(107,rocks,box of assorted rocks,5.3)");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testInsertDataDeserialization() throws Exception {
        List<String> actual = testDeserialization("debezium-avro-insert.avro");

        List<String> expected =
                Collections.singletonList("+I(1,lisi,test debezium avro data,21.799999237060547)");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testUpdateDataDeserialization() throws Exception {
        List<String> actual = testDeserialization("debezium-avro-update.avro");

        List<String> expected =
                Arrays.asList(
                        "-U(1,lisi,test debezium avro data,21.799999237060547)",
                        "+U(1,zhangsan,test debezium avro data,21.799999237060547)");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testDeleteDataDeserialization() throws Exception {
        List<String> actual = testDeserialization("debezium-avro-delete.avro");

        List<String> expected =
                Collections.singletonList(
                        "-D(1,zhangsan,test debezium avro data,21.799999237060547)");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testTombstoneMessages() throws Exception {
        RowType rowTypeDe =
                DebeziumAvroDeserializationSchema.createDebeziumAvroRowType(
                        fromLogicalToDataType(rowType), Collections.emptyList());
        client.register(SUBJECT, DEBEZIUM_SCHEMA_COMPATIBLE_TEST, 1, 81);

        DebeziumAvroDeserializationSchema dbzDeserializer =
                new DebeziumAvroDeserializationSchema(
                        InternalTypeInfo.of(rowType), getDeserializationSchema(rowTypeDe));
        dbzDeserializer.open(new MockInitializationContext());

        SimpleCollector collector = new SimpleCollector();
        dbzDeserializer.deserialize(null, collector);
        dbzDeserializer.deserialize(new byte[] {}, collector);
        assertThat(collector.list).isEmpty();
    }

    @Test
    void testDeserializationWithMetadata() throws Exception {
        testDeserializationWithMetadata(
                "debezium-avro-insert.avro",
                row -> {
                    // Physical columns (0-3): id, name, description, weight
                    assertThat(row.getLong(0)).isEqualTo(1L);
                    assertThat(row.getString(1).toString()).isEqualTo("lisi");
                    assertThat(row.getString(2).toString()).isEqualTo("test debezium avro data");
                    assertThat(row.getDouble(3)).isEqualTo(21.799999237060547);

                    // Metadata columns (4-9)
                    // 4: ingestion-timestamp (envelope ts_ms = 1599207472705);
                    assertThat(row.getTimestamp(4, 3).getMillisecond()).isEqualTo(1599207472705L);

                    // 5: source.timestamp (source ts_ms = 1599207472000)
                    assertThat(row.getTimestamp(5, 3).getMillisecond()).isEqualTo(1599207472000L);

                    // 6: source.database
                    assertThat(row.getString(6).toString()).isEqualTo("test1");

                    // 7: source.schema (Mysql doesn't use schema field)
                    assertThat(row.isNullAt(7)).isTrue();

                    // 8. source.table
                    assertThat(row.getString(8).toString()).isEqualTo("person");

                    // 9. source.properties (MAP with all source fields)
                    MapData sourceMap = row.getMap(9);
                    assertThat(sourceMap).isNotNull();

                    // Mysql source has 14 fields
                    assertThat(sourceMap.size()).isEqualTo(14);

                    // Verify common fields (all Debezium connectors) with exact values
                    assertThat(getMapValue(sourceMap, "version")).isEqualTo("1.2.2.Final");
                    assertThat(getMapValue(sourceMap, "connector")).isEqualTo("mysql");
                    assertThat(getMapValue(sourceMap, "name")).isEqualTo("fullfillment");
                    assertThat(getMapValue(sourceMap, "ts_ms")).isEqualTo("1599207472000");
                    assertThat(getMapValue(sourceMap, "snapshot")).isEqualTo("false");
                    assertThat(getMapValue(sourceMap, "db")).isEqualTo("test1");
                    assertThat(getMapValue(sourceMap, "table")).isEqualTo("person");

                    // Verify MySQL-specific fields with exact values (writer schema intact)
                    assertThat(getMapValue(sourceMap, "server_id")).isEqualTo("1");
                    assertThat(getMapValue(sourceMap, "file")).isEqualTo("mysql-bin.000005");
                    assertThat(getMapValue(sourceMap, "pos")).isEqualTo("213795");
                    assertThat(getMapValue(sourceMap, "row")).isEqualTo("0");
                    assertThat(getMapValue(sourceMap, "thread")).isEqualTo("2");

                    // Verify nullable MySQL fields (null values in this test data)
                    assertThat(getMapValue(sourceMap, "gtid")).isNull();
                    assertThat(getMapValue(sourceMap, "query")).isNull();
                });
    }

    @Test
    void testDeserializationWithMetadata_SourceDatabaseOnly() throws Exception {
        testDeserializationWithMetadata(
                "debezium-avro-insert.avro",
                Collections.singletonList(ReadableMetadata.SOURCE_DATABASE),
                row -> {
                    assertThat(row.getLong(0)).isEqualTo(1L);
                    assertThat(row.getString(4).toString()).isEqualTo("test1");
                });
    }

    @Test
    void testDeserializationWithMetadata_SourcePropertiesOnly() throws Exception {
        testDeserializationWithMetadata(
                "debezium-avro-insert.avro",
                Collections.singletonList(ReadableMetadata.SOURCE_PROPERTIES),
                row -> {
                    MapData sourceMap = row.getMap(4);
                    assertThat(sourceMap).isNotNull();
                    assertThat(getMapValue(sourceMap, "db")).isEqualTo("test1");
                    assertThat(getMapValue(sourceMap, "table")).isEqualTo("person");
                });
    }

    @Test
    void testDeserializationWithMetadata_MixedOrder() throws Exception {
        testDeserializationWithMetadata(
                "debezium-avro-insert.avro",
                Arrays.asList(
                        ReadableMetadata.SOURCE_TABLE,
                        ReadableMetadata.INGESTION_TIMESTAMP,
                        ReadableMetadata.SOURCE_DATABASE),
                row -> {
                    assertThat(row.getString(4).toString()).isEqualTo("person");
                    assertThat(row.getTimestamp(5, 3).getMillisecond()).isEqualTo(1599207472705L);
                    assertThat(row.getString(6).toString()).isEqualTo("test1");
                });
    }

    @Test
    void testSourcePropertiesWithLaterSchemaVersion() throws Exception {
        final DebeziumAvroDeserializationSchema dbzDeserializer =
                createDeserializer(
                        EVOLVING_PHYSICAL_TYPE,
                        Collections.singletonList(ReadableMetadata.SOURCE_PROPERTIES));

        final SimpleCollector collector = new SimpleCollector();
        dbzDeserializer.deserialize(
                serializeEnvelope(evolvingEnvelopeSchema(false, false), 1L), collector);
        dbzDeserializer.deserialize(
                serializeEnvelope(evolvingEnvelopeSchema(false, true), 2L), collector);

        assertThat(collector.list).hasSize(2);
        assertThat(collector.list.get(0).getMap(1).size()).isEqualTo(1);
        final MapData v2Source = collector.list.get(1).getMap(1);
        assertThat(v2Source.size()).isEqualTo(2);
        assertThat(getMapValue(v2Source, "lsn")).isEqualTo("1002");
        assertThat(getMapValue(v2Source, "ts_us")).isEqualTo("1700000000000000");
    }

    @Test
    void testDroppedUndeclaredRequiredColumnWithMetadata() throws Exception {
        final DebeziumAvroDeserializationSchema dbzDeserializer =
                createDeserializer(
                        EVOLVING_PHYSICAL_TYPE,
                        Collections.singletonList(ReadableMetadata.SOURCE_PROPERTIES));

        final SimpleCollector collector = new SimpleCollector();
        dbzDeserializer.deserialize(
                serializeEnvelope(evolvingEnvelopeSchema(true, false), 1L), collector);
        dbzDeserializer.deserialize(
                serializeEnvelope(evolvingEnvelopeSchema(false, false), 2L), collector);

        assertThat(collector.list).hasSize(2);
        assertThat(collector.list.get(1).getLong(0)).isEqualTo(2L);
        assertThat(getMapValue(collector.list.get(1).getMap(1), "lsn")).isEqualTo("1002");
    }

    public List<String> testDeserialization(String dataPath) throws Exception {
        RowType rowTypeDe =
                DebeziumAvroDeserializationSchema.createDebeziumAvroRowType(
                        fromLogicalToDataType(rowType), Collections.emptyList());

        client.register(SUBJECT, DEBEZIUM_SCHEMA_COMPATIBLE_TEST, 1, 81);

        DebeziumAvroDeserializationSchema dbzDeserializer =
                new DebeziumAvroDeserializationSchema(
                        InternalTypeInfo.of(rowType), getDeserializationSchema(rowTypeDe));
        dbzDeserializer.open(new MockInitializationContext());

        SimpleCollector collector = new SimpleCollector();
        dbzDeserializer.deserialize(readBytesFromFile(dataPath), collector);

        return collector.list.stream().map(Object::toString).collect(Collectors.toList());
    }

    private void testDeserializationWithMetadata(String dataPath, Consumer<RowData> testConsumer)
            throws Exception {
        testDeserializationWithMetadata(
                dataPath, Arrays.asList(ReadableMetadata.values()), testConsumer);
    }

    private void testDeserializationWithMetadata(
            String dataPath,
            List<ReadableMetadata> requestedMetadata,
            Consumer<RowData> testConsumer)
            throws Exception {
        client.register(SUBJECT, DEBEZIUM_SCHEMA_COMPATIBLE_TEST, 1, 81);

        DebeziumAvroDeserializationSchema dbzDeserializer =
                createDeserializer(fromLogicalToDataType(rowType), requestedMetadata);

        SimpleCollector collector = new SimpleCollector();
        dbzDeserializer.deserialize(readBytesFromFile(dataPath), collector);

        assertThat(collector.list).hasSize(1);
        assertThat(collector.list.get(0)).satisfies(testConsumer);
    }

    private AvroRowDataDeserializationSchema getDeserializationSchema(RowType rowType) {

        final ConfluentSchemaRegistryCoder registryCoder =
                new ConfluentSchemaRegistryCoder(SUBJECT, client);

        return new AvroRowDataDeserializationSchema(
                new RegistryAvroDeserializationSchema<>(
                        GenericRecord.class,
                        AvroSchemaConverter.convertToSchema(rowType),
                        () -> registryCoder),
                AvroToRowDataConverters.createRowConverter(rowType),
                InternalTypeInfo.of(rowType));
    }

    private AvroRowDataSerializationSchema getSerializationSchema(RowType rowType) {

        ConfluentSchemaRegistryCoder registryCoder =
                new ConfluentSchemaRegistryCoder(SUBJECT, client);
        return new AvroRowDataSerializationSchema(
                rowType,
                new RegistryAvroSerializationSchema<>(
                        GenericRecord.class,
                        AvroSchemaConverter.convertToSchema(rowType, false),
                        () -> registryCoder),
                RowDataToAvroConverters.createConverter(rowType, false));
    }

    private static DebeziumAvroDeserializationSchema.MetadataConverter[] createMetadataConverters(
            RowType debeziumAvroRowType, List<ReadableMetadata> requestedMetadata) {
        return requestedMetadata.stream()
                .map(
                        m -> {
                            final int rootPosition =
                                    debeziumAvroRowType
                                            .getFieldNames()
                                            .indexOf(m.requiredAvroField.getName());
                            return (DebeziumAvroDeserializationSchema.MetadataConverter)
                                    (row, pos) -> m.converter.convert(row, rootPosition);
                        })
                .toArray(DebeziumAvroDeserializationSchema.MetadataConverter[]::new);
    }

    private DebeziumAvroDeserializationSchema createDeserializer(
            DataType physicalDataType, List<ReadableMetadata> requestedMetadata) throws Exception {
        final DataType producedDataType =
                DataTypeUtils.appendRowFields(
                        physicalDataType,
                        requestedMetadata.stream()
                                .map(m -> DataTypes.FIELD(m.key, m.dataType))
                                .collect(Collectors.toList()));
        final RowType rowTypeDe =
                DebeziumAvroDeserializationSchema.createDebeziumAvroRowType(
                        physicalDataType, requestedMetadata);
        final ConfluentSchemaRegistryCoder registryCoder =
                new ConfluentSchemaRegistryCoder(SUBJECT, client);

        final DebeziumAvroDeserializationSchema dbzDeserializer =
                new DebeziumAvroDeserializationSchema(
                        InternalTypeInfo.of(producedDataType.getLogicalType()),
                        getDeserializationSchema(rowTypeDe),
                        true,
                        createMetadataConverters(rowTypeDe, requestedMetadata),
                        rowTypeDe.getFieldNames().indexOf("source"),
                        () -> registryCoder);
        dbzDeserializer.open(new MockInitializationContext());
        return dbzDeserializer;
    }

    private static Schema evolvingEnvelopeSchema(boolean withLegacyColumn, boolean withSourceTsUs) {
        SchemaBuilder.FieldAssembler<Schema> value =
                SchemaBuilder.record("Value")
                        .namespace("pg.public.orders")
                        .fields()
                        .requiredLong("id");
        if (withLegacyColumn) {
            value = value.requiredInt("legacy");
        }
        final Schema valueSchema = value.endRecord();

        SchemaBuilder.FieldAssembler<Schema> source =
                SchemaBuilder.record("Source")
                        .namespace("io.debezium.connector.postgresql")
                        .fields()
                        .optionalLong("lsn");
        if (withSourceTsUs) {
            source = source.optionalLong("ts_us");
        }

        return SchemaBuilder.record("Envelope")
                .namespace("pg.public.orders")
                .fields()
                .name("before")
                .type()
                .optional()
                .type(valueSchema)
                .name("after")
                .type()
                .optional()
                .type(valueSchema)
                .name("source")
                .type(source.endRecord())
                .noDefault()
                .requiredString("op")
                .endRecord();
    }

    private byte[] serializeEnvelope(Schema envelopeSchema, long id) throws IOException {
        final Schema valueSchema = envelopeSchema.getField("after").schema().getTypes().get(1);
        final GenericRecord after = new GenericData.Record(valueSchema);
        after.put("id", id);
        if (valueSchema.getField("legacy") != null) {
            after.put("legacy", 0);
        }

        final Schema sourceSchema = envelopeSchema.getField("source").schema();
        final GenericRecord source = new GenericData.Record(sourceSchema);
        source.put("lsn", 1000L + id);
        if (sourceSchema.getField("ts_us") != null) {
            source.put("ts_us", 1700000000000000L);
        }

        final GenericRecord envelope = new GenericData.Record(envelopeSchema);
        envelope.put("after", after);
        envelope.put("source", source);
        envelope.put("op", "c");

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        new ConfluentSchemaRegistryCoder(SUBJECT, client).writeSchema(envelopeSchema, out);
        final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(envelopeSchema).write(envelope, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static RowData debeziumRow2RowData() {
        GenericRowData rowData = new GenericRowData(4);
        rowData.setField(0, 107L);
        rowData.setField(1, StringData.fromString("rocks"));
        rowData.setField(2, StringData.fromString("box of assorted rocks"));
        rowData.setField(3, 5.3D);
        return rowData;
    }

    private static byte[] readBytesFromFile(String filePath) {
        try {
            URL url = DebeziumAvroSerDeSchemaTest.class.getClassLoader().getResource(filePath);
            assertThat(url).isNotNull();
            Path path = new File(url.getFile()).toPath();
            return FileUtils.readAllBytes(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getMapValue(MapData map, String key) {
        if (map == null) {
            return null;
        }
        GenericMapData genericMap = (GenericMapData) map;
        StringData value = (StringData) genericMap.get(StringData.fromString(key));
        return value != null ? value.toString() : null;
    }

    private static class SimpleCollector implements Collector<RowData> {

        private final List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    private static class MockInitializationContext
            implements DeserializationSchema.InitializationContext,
                    SerializationSchema.InitializationContext {
        @Override
        public MetricGroup getMetricGroup() {
            return new UnregisteredMetricsGroup();
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
        }
    }
}
