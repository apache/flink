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

package org.apache.flink.formats.avro.registry.apicurio;

import org.apache.flink.formats.avro.SchemaCoder;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.v2.beans.ArtifactMetaData;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/** Reads and Writes schema using Avro Schema Registry protocol. */
public class ApicurioSchemaRegistryCoder implements SchemaCoder {

    private final RegistryClient registryClient;
    private final Map<String, Object> registryConfigs;
    private String subject;
    private static final int CONFLUENT_MAGIC_BYTE = 0;

    /**
     * Creates {@link SchemaCoder} that uses provided {@link RegistryClient} to connect to schema
     * registry.
     *
     * @param registryClient client to connect schema registry
     * @param subject subject of schema registry to produce
     * @param registryConfigs map for registry configs
     */
    public ApicurioSchemaRegistryCoder(
            String subject, RegistryClient registryClient, Map<String, Object> registryConfigs) {
        this.registryClient = registryClient;
        this.subject = subject;
        this.registryConfigs = registryConfigs;
    }

    /**
     * Creates {@link SchemaCoder} that uses provided {@link RegistryClient} to connect to schema
     * registry.
     *
     * @param registryClient client to connect schema registry
     */
    public ApicurioSchemaRegistryCoder(RegistryClient registryClient) {
        this.registryClient = registryClient;
        this.registryConfigs = null;
    }

    @Override
    public Schema readSchema(InputStream in) throws IOException {
        // TODO error not expected. Hard coding for now
        //        long globalId = 2;
        //        return new Schema.Parser().parse(registryClient.getContentByGlobalId(globalId));
        try {
            throw new Exception("hello");
        } catch (Exception e) {
            StackTraceElement[] stack = e.getStackTrace();
            String str = "";
            for (StackTraceElement s : stack) {
                str = str + s.toString() + "\n\t\t";
            }
            throw new IOException(
                    "Incompatible Kafka connector. Use a Kafka connector that passes headers "
                            + str);
        }
        //        throw new IOException(
        //                "Incompatible Kafka connector. Use a Kafka connector that passes
        // headers");
    }

    public long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip(); // need flip
        return buffer.getLong();
    }

    @Override
    public Schema readSchemaWithHeaders(InputStream in, Map<String, Object> headers)
            throws IOException {
        if (in == null) {
            return null;
        }
        boolean enableHeaders =
                (boolean) registryConfigs.get(AvroApicurioFormatOptions.ENABLE_HEADERS.key());

        // get from headers
        Long globalId;
        Schema schema = null;
        if (headers.get("apicurio.value.globalId") != null) {
            //            throw new RuntimeException(
            //                    "headers.get(\"apicurio.value.globalId\") "
            //                            + headers.get("apicurio.value.globalId"));
            byte[] globalIDByteArray = (byte[]) headers.get("apicurio.value.globalId");
            globalId = bytesToLong(globalIDByteArray);
            try {
                schema = new Schema.Parser().parse(registryClient.getContentByGlobalId(globalId));
            } catch (Exception e) {
                throw new RuntimeException(
                        "Exception during parse "
                                + e.getMessage()
                                + " globalId="
                                + headers.get("apicurio.value.globalId"));
            }
        }
        return schema;

        //        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        //
        //        throw new RuntimeException(
        //                "Schema readSchemaWithHeaders(InputStream in, Map<String, Object>
        // headers)) :"
        //                        + headers.keySet().size()
        //                        + ":"
        //                        + Arrays.stream(stackTraceElements)
        //                                .map(Object::toString)
        //                                .collect(Collectors.joining(",")));
        //        if (globalId != null) {
        //            throw new RuntimeException("Global id from headers is " + globalId);
        //        }
        //        return new Schema.Parser().parse(registryClient.getContentByGlobalId(globalId));
        //        throw new RuntimeException(
        //                "globalId="
        //                        + globalId
        //                        + ", headers are "
        //                        + headers.keySet().stream()
        //                                .map(Object::toString)
        //                                .reduce("", String::concat));
    }

    @Override
    public void writeSchema(Schema schema, OutputStream out) throws IOException {
        boolean enableHeaders =
                (boolean) registryConfigs.get(AvroApicurioFormatOptions.ENABLE_HEADERS.key());
        boolean enableConfluentIdHandler =
                (boolean)
                        registryConfigs.get(
                                AvroApicurioFormatOptions.ENABLE_CONFLUENT_ID_HANDLER.key());
        String idHandlerClassName =
                (String) registryConfigs.get(AvroApicurioFormatOptions.ID_HANDLER.key());
        String headersHandler =
                (String) registryConfigs.get(AvroApicurioFormatOptions.HEADERS_HANDLER.key());
        ArtifactMetaData artifactMetaData = null;

        if (enableConfluentIdHandler) {
            // extract the 4 byte magic bytes using the legacy handler
            idHandlerClassName = "io.apicurio.registry.serde.Legacy4ByteIdHandler";
        }
        //                try {
        //                    ArtifactMetaData metaData =
        //                            registryClient.get()
        //
        //                    out.write(metaData.getGlobalId());
        //                    byte[] schemaIdBytes =
        // ByteBuffer.allocate(4).putInt(registeredId).array();
        //                    out.write(schemaIdBytes);
        //                } catch (RestClientException e) {
        //                    throw new IOException("Could not register schema in registry", e);
        //                }
    }
}
