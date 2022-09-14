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
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.UnionLogicalType;
import org.apache.flink.formats.avro.utils.TestDataGenerator;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Random;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AvroDeserializationSchema}. */
class AvroDeserializationSchemaTest {

    private static final Address address = TestDataGenerator.generateRandomAddress(new Random());

    @Test
    void testNullRecord() throws Exception {
        DeserializationSchema<Address> deserializer =
                AvroDeserializationSchema.forSpecific(Address.class);

        Address deserializedAddress = deserializer.deserialize(null);
        assertThat(deserializedAddress).isNull();
    }

    @Test
    void testGenericRecord() throws Exception {
        DeserializationSchema<GenericRecord> deserializationSchema =
                AvroDeserializationSchema.forGeneric(address.getSchema());

        byte[] encodedAddress = writeRecord(address, Address.getClassSchema());
        GenericRecord genericRecord = deserializationSchema.deserialize(encodedAddress);
        assertThat(genericRecord.get("city").toString()).isEqualTo(address.getCity());
        assertThat(genericRecord.get("num")).isEqualTo(address.getNum());
        assertThat(genericRecord.get("state").toString()).isEqualTo(address.getState());
    }

    @Test
    void testSpecificRecord() throws Exception {
        DeserializationSchema<Address> deserializer =
                AvroDeserializationSchema.forSpecific(Address.class);

        byte[] encodedAddress = writeRecord(address);
        Address deserializedAddress = deserializer.deserialize(encodedAddress);
        assertThat(deserializedAddress).isEqualTo(address);
    }

    @Test
    void testSpecificRecordWithUnionLogicalType() throws Exception {
        Random rnd = new Random();
        UnionLogicalType data = new UnionLogicalType(Instant.ofEpochMilli(rnd.nextLong()));
        DeserializationSchema<UnionLogicalType> deserializer =
                AvroDeserializationSchema.forSpecific(UnionLogicalType.class);

        byte[] encodedData = writeRecord(data);
        UnionLogicalType deserializedData = deserializer.deserialize(encodedData);
        assertThat(deserializedData).isEqualTo(data);
    }
}
