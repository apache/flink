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

import org.apache.flink.formats.avro.AvroFormatOptions;
import org.apache.flink.formats.avro.SchemaCoder;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.v2.beans.ArtifactMetaData;
import io.apicurio.registry.rest.v2.beans.IfExists;
import io.apicurio.registry.types.ArtifactType;
import org.apache.avro.Schema;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/** Reads and Writes schema using Avro Schema Registry protocol. */
public class ApicurioSchemaRegistryCoder implements SchemaCoder {

    public static final String APICURIO_VALUE_GLOBAL_ID = "apicurio.value.globalId";
    public static final String APICURIO_VALUE_ENCODING = "apicurio.value.encoding";
    private final RegistryClient registryClient;

    private final Map<String, Object> registryConfigs;

    private static final int MAGIC_BYTE = 0;

    /**
     * Creates {@link SchemaCoder} that uses provided {@link RegistryClient} to connect to schema
     * registry.
     *
     * @param registryClient client to connect schema registry
     * @param registryConfigs map for registry configs
     */
    public ApicurioSchemaRegistryCoder(
            RegistryClient registryClient, Map<String, Object> registryConfigs) {
        this.registryClient = registryClient;
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
        throw new IOException(
                "Incompatible Kafka connector. Use a Kafka connector that passes headers");
    }

    public long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip(); // need flip
        return buffer.getLong();
    }

    public byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    /**
     * Get the Avro schema using the Apicurio Registry. In order to call the registry, we need to
     * get the globalId of the Avro Schema in the registry. If the format is configured to expect
     * headers, then the globalId will be obtained from the headers map. Otherwise, the globalId is
     * obtained from the message payload, depending on the format configuration. This format is only
     * used with the Kafka connector. The Kafka connector lives in another repository. This
     * repository does not have any Kafka dependancies; it is therefore not possible for us to use
     * the Kafka consumer record here passing a deserialization handler. So we have to get the
     * information we need directly from the headers or the payload itself.
     *
     * @param in input stream
     * @param headers map of Kafka headers
     * @return the Avro Schema
     * @throws IOException if there is an error
     */
    @Override
    public Schema readSchemaWithHeaders(InputStream in, Map<String, Object> headers)
            throws IOException {
        if (in == null) {
            return null;
        }
        Schema schema = null;
        try {
            boolean enableHeaders =
                    (boolean) registryConfigs.get(AvroApicurioFormatOptions.ENABLE_HEADERS.key());
            boolean isLegacy =
                    (boolean) registryConfigs.get(AvroApicurioFormatOptions.LEGACY_SCHEMA_ID.key());
            boolean isConfluent =
                    (boolean)
                            registryConfigs.get(
                                    AvroApicurioFormatOptions.ENABLE_CONFLUENT_ID_HANDLER.key());
            Long globalId;
            if (enableHeaders) {
                // get from headers
                if (headers.get(APICURIO_VALUE_GLOBAL_ID) != null) {
                    byte[] globalIDByteArray = (byte[]) headers.get(APICURIO_VALUE_GLOBAL_ID);
                    globalId = bytesToLong(globalIDByteArray);
                } else {
                    throw new IOException(
                            "The format was configured to enable headers, but "
                                    + APICURIO_VALUE_GLOBAL_ID
                                    + " not present.\n "
                                    + "Ensure that the kafka message was created by an Apicurio client. Check that it is "
                                    + " sending the globalId in the header, if the message is sending the globalId in the payload,\n"
                                    + " amend the format configuration to not set "
                                    + AvroApicurioFormatOptions.ENABLE_HEADERS);
                }
            } else {
                // the id is in the payload
                DataInputStream dataInputStream = new DataInputStream(in);
                if (isLegacy) {
                    int legacySchemaId = dataInputStream.readInt();
                    globalId = Long.valueOf(new Integer(legacySchemaId));
                } else if (isConfluent) {
                    if (dataInputStream.readByte() != 0) {
                        throw new IOException("Unknown data format. Magic number does not match");
                    } else {
                        int schemaId = dataInputStream.readInt();
                        globalId = Long.valueOf(new Integer(schemaId));
                    }
                } else {
                    globalId = dataInputStream.readLong();
                }
            }
            try {
                // get the schema in canonical form with references dereferenced
                schema =
                        new Schema.Parser()
                                .parse(registryClient.getContentByGlobalId(globalId, true, true));
            } catch (IOException e) {
                throw new IOException(
                        "Attempting to get an Apicurio artifact with globalId "
                                + globalId
                                + " and to parse it into an Avro Schema, but got error "
                                + e.getMessage());
            }
        } catch (Exception e) {
            throw new IOException(
                    "registryConfigs "
                            + registryConfigs
                            + ",AvroApicurioFormatOptions.ENABLE_HEADERS.key()="
                            + AvroApicurioFormatOptions.ENABLE_HEADERS.key(),
                    e);
        }
        return schema;
    }

    @Override
    public void writeSchema(Schema schema, OutputStream out) throws IOException {
        throw new IOException(
                "Incompatible Kafka connector is being used, upgrade the Kafka connector.");
    }

    /**
     * The Avro schema will be written depending on the configuration.
     *
     * @param schema Avro Schema to write into the output stream,
     * @param out the output stream where the schema is serialized.
     * @throws IOException Exception has occurred
     */
    @Override
    public void writeSchema(Schema schema, OutputStream out, Map<String, Object> headers)
            throws IOException {
        boolean enableHeaders =
                (boolean) registryConfigs.get(AvroApicurioFormatOptions.ENABLE_HEADERS.key());
        boolean isLegacy =
                (boolean) registryConfigs.get(AvroApicurioFormatOptions.LEGACY_SCHEMA_ID.key());
        boolean isConfluent =
                (boolean)
                        registryConfigs.get(
                                AvroApicurioFormatOptions.ENABLE_CONFLUENT_ID_HANDLER.key());
        String groupId = (String) registryConfigs.get(AvroApicurioFormatOptions.GROUP_ID.key());
        String artifactId =
                (String)
                        registryConfigs.get(AvroApicurioFormatOptions.REGISTERED_ARTIFACT_ID.key());
        String artifactName =
                (String)
                        registryConfigs.get(
                                AvroApicurioFormatOptions.REGISTERED_ARTIFACT_NAME.key());
        if (artifactId == null) {
            artifactId = new Integer(schema.hashCode()).toString();
        }
        String artifactDescription =
                (String)
                        registryConfigs.get(
                                AvroApicurioFormatOptions.REGISTERED_ARTIFACT_DESCRIPTION.key());
        String artifactVersion =
                (String)
                        registryConfigs.get(
                                AvroApicurioFormatOptions.REGISTERED_ARTIFACT_VERSION.key());
        //            String encoding = getStringValueFromConfig(registryConfigs,
        // AvroFormatOptions.AVRO_ENCODING);
        AvroFormatOptions.AvroEncoding avroEncoding;
        Object configOption = registryConfigs.get(AvroFormatOptions.AVRO_ENCODING.key());
        if (configOption == null) {
            if (AvroFormatOptions.AVRO_ENCODING.hasDefaultValue()) {
                avroEncoding = AvroFormatOptions.AVRO_ENCODING.defaultValue();
            } else {
                avroEncoding = null;
            }
        } else {
            avroEncoding = (AvroFormatOptions.AvroEncoding) configOption;
        }
        // register the schema
        InputStream in = new ByteArrayInputStream(schema.toString().getBytes());

        //        throw new RuntimeException(
        //                "registryConfigs"
        //                        + registryConfigs
        //                        + "artifactName:"
        //                        + artifactName
        //                        + registryConfigs.get(
        //                                AvroApicurioFormatOptions.REGISTERED_ARTIFACT_NAME.key())
        //                        + ",artifactDescription:"
        //                        + artifactDescription
        //                        + "-"
        //                        + registryConfigs.get(
        //
        // AvroApicurioFormatOptions.REGISTERED_ARTIFACT_DESCRIPTION.key())
        //                        + ",artifactId:"
        //                        + artifactId
        //                        + "-"
        //                        + registryConfigs.get(
        //                                AvroApicurioFormatOptions.REGISTERED_ARTIFACT_ID.key())
        //                        + ",artifactVersion "
        //                        + artifactVersion
        //                        + "-"
        //                        + AvroApicurioFormatOptions.REGISTERED_ARTIFACT_VERSION);
        ArtifactMetaData artifactMetaData =
                registryClient.createArtifact(
                        groupId,
                        artifactId,
                        artifactVersion,
                        ArtifactType.AVRO,
                        IfExists.RETURN_OR_UPDATE,
                        true,
                        artifactName,
                        artifactDescription,
                        in);
        Long registeredGlobalId = artifactMetaData.getGlobalId();
        if (enableHeaders) {
            // convert values to byte array
            headers.put(APICURIO_VALUE_GLOBAL_ID, longToBytes(registeredGlobalId));
            headers.put(APICURIO_VALUE_ENCODING, avroEncoding.toString().getBytes());
        } else {
            out.write(MAGIC_BYTE);
            byte[] schemaIdBytes;
            if (isConfluent) {
                schemaIdBytes =
                        ByteBuffer.allocate(4).putInt(registeredGlobalId.intValue()).array();
            } else {
                schemaIdBytes = ByteBuffer.allocate(8).putLong(registeredGlobalId).array();
            }
            out.write(schemaIdBytes);
        }
    }
}
