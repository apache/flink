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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static java.lang.String.format;

/** Reads and Writes schema using Confluent Schema Registry protocol. */
public class SchemaRegistryCoder {

    private final SchemaRegistryClient schemaRegistryClient;
    private static final int CONFLUENT_MAGIC_BYTE = 0;
    private final int schemaId;

    public SchemaRegistryCoder(int schemaId, SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.schemaId = schemaId;
    }

    public ParsedSchema readSchema(InputStream in) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(in);

        if (dataInputStream.readByte() != 0) {
            throw new IOException("Unknown data format. Magic number does not match");
        } else {
            int schemaId = dataInputStream.readInt();

            try {
                return schemaRegistryClient.getSchemaById(schemaId);
                // we assume this is avro schema
            } catch (RestClientException e) {
                throw new IOException(
                        format("Could not find schema with id %s in registry", schemaId), e);
            }
        }
    }

    public void writeSchema(OutputStream out) throws IOException {
        // we do not check the schema, but write the id that we were initialised with
        out.write(CONFLUENT_MAGIC_BYTE);
        writeInt(out, schemaId);
    }

    private static void writeInt(OutputStream out, int registeredId) throws IOException {
        out.write(registeredId >>> 24);
        out.write(registeredId >>> 16);
        out.write(registeredId >>> 8);
        out.write(registeredId);
    }
}
