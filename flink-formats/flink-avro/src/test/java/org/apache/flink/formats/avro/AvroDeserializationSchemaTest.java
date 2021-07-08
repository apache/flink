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
import org.junit.Test;

import java.time.Instant;
import java.util.Random;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Tests for {@link AvroDeserializationSchema}. */
public class AvroDeserializationSchemaTest {

    private static final Address address = TestDataGenerator.generateRandomAddress(new Random());

    @Test
    public void testNullRecord() throws Exception {
        DeserializationSchema<Address> deserializer =
                AvroDeserializationSchema.forSpecific(Address.class);

        Address deserializedAddress = deserializer.deserialize(null);
        assertNull(deserializedAddress);
    }

    @Test
    public void testGenericRecord() throws Exception {
        DeserializationSchema<GenericRecord> deserializationSchema =
                AvroDeserializationSchema.forGeneric(address.getSchema());

        byte[] encodedAddress = writeRecord(address, Address.getClassSchema());
        GenericRecord genericRecord = deserializationSchema.deserialize(encodedAddress);
        assertEquals(address.getCity(), genericRecord.get("city").toString());
        assertEquals(address.getNum(), genericRecord.get("num"));
        assertEquals(address.getState(), genericRecord.get("state").toString());
    }

    @Test
    public void testSpecificRecord() throws Exception {
        DeserializationSchema<Address> deserializer =
                AvroDeserializationSchema.forSpecific(Address.class);

        byte[] encodedAddress = writeRecord(address);
        Address deserializedAddress = deserializer.deserialize(encodedAddress);
        assertEquals(address, deserializedAddress);
    }

    @Test
    public void testSpecificRecordWithUnionLogicalType() throws Exception {
        Random rnd = new Random();
        UnionLogicalType data = new UnionLogicalType(Instant.ofEpochMilli(rnd.nextLong()));
        DeserializationSchema<UnionLogicalType> deserializer =
                AvroDeserializationSchema.forSpecific(UnionLogicalType.class);

        byte[] encodedData = writeRecord(data);
        UnionLogicalType deserializedData = deserializer.deserialize(encodedData);
        assertEquals(data, deserializedData);
    }
}
