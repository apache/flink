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
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    void testMapHelperMethods() {
        Map<StringData, StringData> testMap = new HashMap<>();
        testMap.put(StringData.fromString("db"), StringData.fromString("inventory"));
        testMap.put(StringData.fromString("table"), StringData.fromString("customers"));
        MapData mapData = new GenericMapData(testMap);

        // Test getMapValue
        assertThat(getMapValue(mapData, "db")).isEqualTo("inventory");
        assertThat(getMapValue(mapData, "table")).isEqualTo("customers");
        assertThat(getMapValue(mapData, "nonexistent")).isNull();

        // Test null map
        assertThat(getMapValue(null, "db")).isNull();
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
        final DataType producedDataType =
                DataTypeUtils.appendRowFields(
                        fromLogicalToDataType(rowType),
                        requestedMetadata.stream()
                                .map(m -> DataTypes.FIELD(m.key, m.dataType))
                                .collect(Collectors.toList()));

        RowType rowTypeDe =
                DebeziumAvroDeserializationSchema.createDebeziumAvroRowType(
                        fromLogicalToDataType(rowType), requestedMetadata);

        client.register(SUBJECT, DEBEZIUM_SCHEMA_COMPATIBLE_TEST, 1, 81);

        ConfluentSchemaRegistryCoder registryCoder =
                new ConfluentSchemaRegistryCoder(SUBJECT, client);

        DebeziumAvroDeserializationSchema.MetadataConverter[] metadataConverters =
                createMetadataConverters(rowTypeDe, requestedMetadata);

        int sourceFieldPosition = rowTypeDe.getFieldNames().indexOf("source");

        DebeziumAvroDeserializationSchema dbzDeserializer =
                new DebeziumAvroDeserializationSchema(
                        InternalTypeInfo.of(producedDataType.getLogicalType()),
                        getDeserializationSchema(rowTypeDe),
                        true,
                        metadataConverters,
                        sourceFieldPosition,
                        () -> registryCoder);

        dbzDeserializer.open(new MockInitializationContext());

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
