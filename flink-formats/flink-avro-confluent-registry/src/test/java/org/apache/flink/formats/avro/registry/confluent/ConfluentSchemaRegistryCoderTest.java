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

package org.apache.flink.formats.avro.registry.confluent;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ConfluentSchemaRegistryCoder}. */
class ConfluentSchemaRegistryCoderTest {

    @Test
    void testSpecificRecordWithConfluentSchemaRegistry() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();

        Schema schema =
                SchemaBuilder.record("testRecord").fields().optionalString("testField").endRecord();
        int schemaId = client.register("testTopic", schema);

        ConfluentSchemaRegistryCoder registryCoder = new ConfluentSchemaRegistryCoder(client);
        ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteOutStream);
        dataOutputStream.writeByte(0);
        dataOutputStream.writeInt(schemaId);
        dataOutputStream.flush();

        ByteArrayInputStream byteInStream = new ByteArrayInputStream(byteOutStream.toByteArray());
        Schema readSchema = registryCoder.readSchema(byteInStream);

        assertThat(readSchema).isEqualTo(schema);
        assertThat(byteInStream).isEmpty();
    }

    @Test
    void testMagicByteVerification() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        int schemaId = client.register("testTopic", Schema.create(Schema.Type.BOOLEAN));

        ConfluentSchemaRegistryCoder coder = new ConfluentSchemaRegistryCoder(client);
        ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteOutStream);
        dataOutputStream.writeByte(5);
        dataOutputStream.writeInt(schemaId);
        dataOutputStream.flush();

        try (ByteArrayInputStream byteInStream =
                new ByteArrayInputStream(byteOutStream.toByteArray())) {
            assertThatThrownBy(() -> coder.readSchema(byteInStream))
                    .isInstanceOf(IOException.class);
        }
    }
}
