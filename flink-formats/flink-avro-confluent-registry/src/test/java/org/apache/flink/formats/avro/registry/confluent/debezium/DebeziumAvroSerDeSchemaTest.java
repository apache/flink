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
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/** Tests for {@link DebeziumAvroDeserializationSchema}. */
public class DebeziumAvroSerDeSchemaTest {

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

    private SchemaRegistryClient client = new MockSchemaRegistryClient();

    @Test
    public void testSerializationDeserialization() throws Exception {

        RowType rowTypeDe =
                DebeziumAvroDeserializationSchema.createDebeziumAvroRowType(
                        fromLogicalToDataType(rowType));
        RowType rowTypeSe =
                DebeziumAvroSerializationSchema.createDebeziumAvroRowType(
                        fromLogicalToDataType(rowType));

        DebeziumAvroSerializationSchema dbzSerializer =
                new DebeziumAvroSerializationSchema(getSerializationSchema(rowTypeSe));
        dbzSerializer.open(mock(SerializationSchema.InitializationContext.class));

        byte[] serialize = dbzSerializer.serialize(debeziumRow2RowData());

        client.register(SUBJECT, DEBEZIUM_SCHEMA_COMPATIBLE_TEST);
        DebeziumAvroDeserializationSchema dbzDeserializer =
                new DebeziumAvroDeserializationSchema(
                        InternalTypeInfo.of(rowType), getDeserializationSchema(rowTypeDe));
        dbzDeserializer.open(mock(DeserializationSchema.InitializationContext.class));

        SimpleCollector collector = new SimpleCollector();
        dbzDeserializer.deserialize(serialize, collector);

        List<String> actual =
                collector.list.stream().map(Object::toString).collect(Collectors.toList());

        List<String> expected =
                Collections.singletonList("+I(107,rocks,box of assorted rocks,5.3)");
        assertEquals(expected, actual);
    }

    @Test
    public void testInsertDataDeserialization() throws Exception {
        List<String> actual = testDeserialization("debezium-avro-insert.avro");

        List<String> expected =
                Collections.singletonList("+I(1,lisi,test debezium avro data,21.799999237060547)");
        assertEquals(expected, actual);
    }

    @Test
    public void testUpdateDataDeserialization() throws Exception {
        List<String> actual = testDeserialization("debezium-avro-update.avro");

        List<String> expected =
                Arrays.asList(
                        "-U(1,lisi,test debezium avro data,21.799999237060547)",
                        "+U(1,zhangsan,test debezium avro data,21.799999237060547)");
        assertEquals(expected, actual);
    }

    @Test
    public void testDeleteDataDeserialization() throws Exception {
        List<String> actual = testDeserialization("debezium-avro-delete.avro");

        List<String> expected =
                Collections.singletonList(
                        "-D(1,zhangsan,test debezium avro data,21.799999237060547)");
        assertEquals(expected, actual);
    }

    public List<String> testDeserialization(String dataPath) throws Exception {
        RowType rowTypeDe =
                DebeziumAvroDeserializationSchema.createDebeziumAvroRowType(
                        fromLogicalToDataType(rowType));

        client.register(SUBJECT, DEBEZIUM_SCHEMA_COMPATIBLE_TEST, 1, 81);

        DebeziumAvroDeserializationSchema dbzDeserializer =
                new DebeziumAvroDeserializationSchema(
                        InternalTypeInfo.of(rowType), getDeserializationSchema(rowTypeDe));
        dbzDeserializer.open(mock(DeserializationSchema.InitializationContext.class));

        SimpleCollector collector = new SimpleCollector();
        dbzDeserializer.deserialize(readBytesFromFile(dataPath), collector);

        return collector.list.stream().map(Object::toString).collect(Collectors.toList());
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
                        AvroSchemaConverter.convertToSchema(rowType),
                        () -> registryCoder),
                RowDataToAvroConverters.createConverter(rowType));
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
            assert url != null;
            Path path = new File(url.getFile()).toPath();
            return FileUtils.readAllBytes(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class SimpleCollector implements Collector<RowData> {

        private List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
