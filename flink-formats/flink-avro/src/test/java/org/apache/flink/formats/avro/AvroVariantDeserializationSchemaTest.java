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

package org.apache.flink.formats.avro;

import org.apache.flink.connector.testutils.formats.DummyInitializationContext;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.utils.TestDataGenerator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.types.variant.Variant;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.fixedRoundRobinSchemaCoderProvider;
import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecord;
import static org.assertj.core.api.Assertions.assertThat;

class AvroVariantDeserializationSchemaTest {

    private static final Address ADDRESS = TestDataGenerator.generateRandomAddress(new Random());
    private static final Schema ADDRESS_SCHEMA = Address.getClassSchema();
    private static final Schema ADDRESS_SCHEMA_V2 =
            new Schema.Parser()
                    .parse(
                            "{\"namespace\": \"org.apache.flink.formats.avro.generated\","
                                    + " \"type\": \"record\","
                                    + " \"name\": \"Address\","
                                    + " \"fields\": ["
                                    + "     {\"name\": \"num\", \"type\": \"int\"},"
                                    + "     {\"name\": \"street\", \"type\": \"string\"},"
                                    + "     {\"name\": \"city\", \"type\": \"string\"},"
                                    + "     {\"name\": \"state\", \"type\": \"string\"},"
                                    + "     {\"name\": \"zip\", \"type\": \"string\"},"
                                    + "     {\"name\": \"country\", \"type\": \"string\"}"
                                    + " ]}");

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeserialization(boolean includeSchemaMetadata) throws Exception {
        RowType rowType =
                includeSchemaMetadata
                        ? RowType.of(
                                new VariantType(), new VarCharType(true, VarCharType.MAX_LENGTH))
                        : RowType.of(new VariantType());
        AvroVariantDeserializationSchema deserializer =
                new AvroVariantDeserializationSchema(
                        new RegistryWriterAvroDeserializationSchema(
                                fixedRoundRobinSchemaCoderProvider(ADDRESS_SCHEMA)),
                        includeSchemaMetadata,
                        10,
                        InternalTypeInfo.of(rowType));
        deserializer.open(new DummyInitializationContext());

        RowData rowData = deserializer.deserialize(writeRecord(ADDRESS, ADDRESS_SCHEMA));
        assertThat(rowData.getArity()).isEqualTo(includeSchemaMetadata ? 2 : 1);

        Variant variant = rowData.getVariant(0);
        assertThat(variant.getField("num").getInt()).isEqualTo(ADDRESS.getNum());
        assertThat(variant.getField("street").getString()).isEqualTo(ADDRESS.getStreet());
        assertThat(variant.getField("city").getString()).isEqualTo(ADDRESS.getCity());
        assertThat(variant.getField("state").getString()).isEqualTo(ADDRESS.getState());
        assertThat(variant.getField("zip").getString()).isEqualTo(ADDRESS.getZip());

        if (includeSchemaMetadata) {
            assertThat(rowData.getString(1).toString()).isEqualTo(ADDRESS_SCHEMA.toString());
        }
    }

    @Test
    void testConverterCacheEviction() throws Exception {
        GenericRecord v2Record = new GenericData.Record(ADDRESS_SCHEMA_V2);
        v2Record.put("num", ADDRESS.getNum());
        v2Record.put("street", ADDRESS.getStreet());
        v2Record.put("city", ADDRESS.getCity());
        v2Record.put("state", ADDRESS.getState());
        v2Record.put("zip", ADDRESS.getZip());
        v2Record.put("country", "US");

        AvroVariantDeserializationSchema deserializer =
                new AvroVariantDeserializationSchema(
                        new RegistryWriterAvroDeserializationSchema(
                                fixedRoundRobinSchemaCoderProvider(
                                        ADDRESS_SCHEMA, ADDRESS_SCHEMA_V2)),
                        false,
                        1,
                        InternalTypeInfo.of(RowType.of(new VariantType())));
        deserializer.open(new DummyInitializationContext());

        deserializer.deserialize(writeRecord(ADDRESS, ADDRESS_SCHEMA));
        assertThat(deserializer.converterCache).hasSize(1);
        assertThat(deserializer.converterCache).containsKey(ADDRESS_SCHEMA);

        // ADDRESS_SCHEMA_V2 triggers eviction of ADDRESS_SCHEMA (maxCacheSize=1)
        deserializer.deserialize(writeRecord(v2Record, ADDRESS_SCHEMA_V2));
        assertThat(deserializer.converterCache).hasSize(1);
        assertThat(deserializer.converterCache).doesNotContainKey(ADDRESS_SCHEMA);
        assertThat(deserializer.converterCache).containsKey(ADDRESS_SCHEMA_V2);

        // ADDRESS_SCHEMA re-enters the cache, deserialization still works
        deserializer.deserialize(writeRecord(ADDRESS, ADDRESS_SCHEMA));
        assertThat(deserializer.converterCache).hasSize(1);
        assertThat(deserializer.converterCache).containsKey(ADDRESS_SCHEMA);
    }

    @Test
    void testDeserializeNull() throws Exception {
        AvroVariantDeserializationSchema deserializer =
                new AvroVariantDeserializationSchema(
                        new RegistryWriterAvroDeserializationSchema(
                                fixedRoundRobinSchemaCoderProvider(ADDRESS_SCHEMA)),
                        false,
                        10,
                        InternalTypeInfo.of(RowType.of(new VariantType())));
        deserializer.open(new DummyInitializationContext());

        assertThat(deserializer.deserialize(null)).isNull();
    }
}
