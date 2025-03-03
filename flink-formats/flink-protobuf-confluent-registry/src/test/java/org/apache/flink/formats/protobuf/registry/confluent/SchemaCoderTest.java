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

package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@ExtendWith(TestLoggerExtension.class)
class SchemaCoderTest {

    private static SchemaCoder defaultCoder;
    private static ParsedSchema schema;
    private static int schemaId;
    private static final String SUBJECT = "test-subject";

    private static final String PROTO_SCHEMA_STRING =
            "syntax = \"proto3\";\n"
                    + "package io.confluent.protobuf.generated;\n"
                    + "\n"
                    + "import \"google/protobuf/wrappers.proto\";"
                    + "\n"
                    + "message Row {\n"
                    + "  google.protobuf.StringValue string = 1;\n"
                    + "  google.protobuf.Int32Value int = 3;\n"
                    + "}";

    @BeforeAll
    static void setupSchemaCoder() throws RestClientException, IOException {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        schema = new ProtobufSchema(PROTO_SCHEMA_STRING);
        schemaId = client.register(SUBJECT, schema);
        defaultCoder =
                SchemaCoderProviders.createDefault(
                        SUBJECT, RowType.of(new VarCharType(100), new IntType()), client);
        defaultCoder.initialize();
    }

    @Test
    void testPreRegisteredSchemaFetch() throws Exception {
        ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteOutStream);
        dataOutputStream.writeByte(0);
        dataOutputStream.writeInt(schemaId);
        dataOutputStream.write(
                SchemaCoderProviders.PreRegisteredSchemaCoder.emptyMessageIndexes().array());

        // we don't care about data yet
        dataOutputStream.flush();
        ByteArrayInputStream byteInStream = new ByteArrayInputStream(byteOutStream.toByteArray());
        ParsedSchema readSchema = defaultCoder.readSchema(byteInStream);
        assertThat(readSchema).isEqualTo(schema);
        assertThat(byteInStream).isEmpty();
    }

    @Test
    void testMagicByteNotPresentShouldThrow() throws Exception {
        ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteOutStream);
        dataOutputStream.writeByte(5);
        dataOutputStream.writeInt(schemaId);
        dataOutputStream.flush();

        try (ByteArrayInputStream byteInStream =
                new ByteArrayInputStream(byteOutStream.toByteArray())) {
            assertThatThrownBy(() -> defaultCoder.readSchema(byteInStream))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void testInvalidSchemaReferenceShouldThrow() throws Exception {
        ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteOutStream);
        dataOutputStream.writeByte(0);
        dataOutputStream.writeInt(100); // schemaId does not exist
        dataOutputStream.write(
                SchemaCoderProviders.PreRegisteredSchemaCoder.emptyMessageIndexes().array());
        dataOutputStream.flush();
        try (ByteArrayInputStream byteInStream =
                new ByteArrayInputStream(byteOutStream.toByteArray())) {
            assertThatThrownBy(() -> defaultCoder.readSchema(byteInStream))
                    .isInstanceOf(IOException.class);
        }
    }
}
