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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.avro.AvroFormatOptions.AvroEncoding;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.UnionLogicalType;
import org.apache.flink.formats.avro.utils.TestDataGenerator;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;
import java.util.Random;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AvroDeserializationSchema}. */
class AvroDeserializationSchemaTest {

    private static final Address address = TestDataGenerator.generateRandomAddress(new Random());

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testNullRecord(AvroEncoding encoding) throws Exception {
        DeserializationSchema<Address> deserializer =
                AvroDeserializationSchema.forSpecific(Address.class, encoding);

        Address deserializedAddress = deserializer.deserialize(null);
        assertThat(deserializedAddress).isNull();
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testGenericRecord(AvroEncoding encoding) throws Exception {
        DeserializationSchema<GenericRecord> deserializationSchema =
                AvroDeserializationSchema.forGeneric(address.getSchema(), encoding);

        byte[] encodedAddress = writeRecord(address, Address.getClassSchema(), encoding);
        GenericRecord genericRecord = deserializationSchema.deserialize(encodedAddress);
        assertThat(genericRecord.get("city").toString()).isEqualTo(address.getCity());
        assertThat(genericRecord.get("num")).isEqualTo(address.getNum());
        assertThat(genericRecord.get("state").toString()).isEqualTo(address.getState());
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testSpecificRecord(AvroEncoding encoding) throws Exception {
        DeserializationSchema<Address> deserializer =
                AvroDeserializationSchema.forSpecific(Address.class, encoding);

        byte[] encodedAddress = writeRecord(address, encoding);
        Address deserializedAddress = deserializer.deserialize(encodedAddress);
        assertThat(deserializedAddress).isEqualTo(address);
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testSpecificRecordWithUnionLogicalType(AvroEncoding encoding) throws Exception {
        Random rnd = new Random();
        UnionLogicalType data = new UnionLogicalType(Instant.ofEpochMilli(rnd.nextLong()));
        DeserializationSchema<UnionLogicalType> deserializer =
                AvroDeserializationSchema.forSpecific(UnionLogicalType.class, encoding);

        byte[] encodedData = writeRecord(data, encoding);
        UnionLogicalType deserializedData = deserializer.deserialize(encodedData);
        assertThat(deserializedData).isEqualTo(data);
    }
}
