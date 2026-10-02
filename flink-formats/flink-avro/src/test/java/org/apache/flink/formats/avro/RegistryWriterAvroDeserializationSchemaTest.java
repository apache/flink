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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.fixedRoundRobinSchemaCoderProvider;
import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecord;
import static org.assertj.core.api.Assertions.assertThat;

class RegistryWriterAvroDeserializationSchemaTest {

    private static final Address address = TestDataGenerator.generateRandomAddress(new Random());
    private static final Schema ADDRESS_SCHEMA = Address.getClassSchema();

    @Test
    void testGenericRecordReadWithFullSchema() throws Exception {
        RegistryWriterAvroDeserializationSchema deserializer =
                new RegistryWriterAvroDeserializationSchema(
                        fixedRoundRobinSchemaCoderProvider(ADDRESS_SCHEMA));

        deserializer.open(new DummyInitializationContext());

        GenericRecord record = deserializer.deserialize(writeRecord(address, ADDRESS_SCHEMA));
        assertThat(record.getSchema()).isSameAs(ADDRESS_SCHEMA);
        assertThat(record.get("num")).isEqualTo(address.getNum());
        assertThat(record.get("street").toString()).isEqualTo(address.getStreet());
        assertThat(record.get("city").toString()).isEqualTo(address.getCity());
        assertThat(record.get("state").toString()).isEqualTo(address.getState());
        assertThat(record.get("zip").toString()).isEqualTo(address.getZip());
    }
}
