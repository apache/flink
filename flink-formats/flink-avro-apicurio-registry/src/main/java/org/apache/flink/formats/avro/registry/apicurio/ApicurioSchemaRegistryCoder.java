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
import io.apicurio.registry.rest.v2.beans.ArtifactReference;
import io.apicurio.registry.rest.v2.beans.IfExists;
import io.apicurio.registry.rest.v2.beans.VersionMetaData;
import io.apicurio.registry.types.ArtifactType;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.USE_GLOBALID;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.USE_HEADERS;

/** Reads and Writes schema using Avro Schema Registry protocol. */
public class ApicurioSchemaRegistryCoder implements SchemaCoder {

    public static final String APICURIO_KEY_GLOBAL_ID_HEADER = "apicurio.key.globalId";

    public static final String APICURIO_KEY_CONTENT_ID_HEADER = "apicurio.key.contentId";

    public static final String APICURIO_VALUE_GLOBAL_ID_HEADER = "apicurio.value.globalId";

    public static final String APICURIO_VALUE_CONTENT_ID_HEADER = "apicurio.value.contentId";

    public static final String APICURIO_VALUE_ENCODING = "apicurio.value.encoding";

    private final RegistryClient registryClient;

    private final Map<String, Object> configs;

    private static final int MAGIC_BYTE = 0;

    public static final String TOPIC_NAME = "TOPIC_NAME";

    public static final String HEADERS = "HEADERS";

    public static final String IS_KEY = "IS_KEY";

    private static final Logger LOG = LoggerFactory.getLogger(ApicurioSchemaRegistryCoder.class);

    /**
     * Creates {@link SchemaCoder} that uses provided {@link RegistryClient} to connect to schema
     * registry.
     *
     * @param registryClient client to connect schema registry
     * @param configs map for registry configs
     */
    public ApicurioSchemaRegistryCoder(RegistryClient registryClient, Map<String, Object> configs) {
        this.registryClient = registryClient;
        this.configs = configs;
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

    public static byte[] longToBytes(long x) {
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
     * repository does not have any Kafka dependencies; it is therefore not possible for us to use
     * the Kafka consumer record here passing a deserialization handler. So we have to get the
     * information we need directly from the headers or the payload itself.
     *
     * @param in input stream
     * @param additionalParameters map of additional parameters - including the headers an whether
     *     this is a key or value
     * @return the Avro Schema
     * @throws IOException if there is an error
     */
    @Override
    public Schema readSchemaWithAdditionalParameters(
            InputStream in, Map<String, Object> additionalParameters) throws IOException {
        String methodName = "readSchemaWithAdditionalParameters";
        if (LOG.isDebugEnabled()) {
            LOG.debug(methodName + " entered");
        }
        if (in == null) {
            return null;
        }
        boolean useGlobalId = (boolean) configs.get(USE_GLOBALID.key());

        long schemaId = getSchemaId(in, additionalParameters, useGlobalId);

        if (!useGlobalId) {
            // if we are  using the content ID there is not get that will dereference.
            // so we need to get the artifact references, find the latest version,
            // using its groupId and artifactId we can get the globalId, from which we can get
            // a de-referenced schema
            if (LOG.isDebugEnabled()) {
                LOG.debug(methodName + " got using contentId " + schemaId);
            }
            List<ArtifactReference> artifactReferenceList =
                    registryClient.getArtifactReferencesByContentId(schemaId);
            if (artifactReferenceList == null || artifactReferenceList.isEmpty()) {
                throw new IOException(
                        "Could not find any schemas in the Apicurio Registry with content ID "
                                + schemaId);
            }
            long latestVersion = 0L;
            String artifactId = "";
            String groupId = "";
            for (ArtifactReference artifactReference : artifactReferenceList) {
                long version = Long.valueOf(artifactReference.getVersion());
                // TODO check Apicurio code to see how it checks for the latest version
                if (version > latestVersion) {
                    latestVersion = version;
                    artifactId = artifactReference.getArtifactId();
                    groupId = artifactReference.getGroupId();
                }
            }
            VersionMetaData artifactVersion =
                    registryClient.getArtifactVersionMetaData(
                            groupId, artifactId, latestVersion + "");
            schemaId = artifactVersion.getGlobalId();
        }
        // get the schema in canonical form with references dereferenced
        InputStream schemaInputStream = registryClient.getContentByGlobalId(schemaId, true, true);
        Schema schema = new Schema.Parser().parse(schemaInputStream);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    methodName
                            + " got schema "
                            + schema
                            + " using "
                            + (useGlobalId ? "global" : "content")
                            + " ID "
                            + schemaId);
        }
        return schema;
    }

    /**
     * Get the schema id.
     *
     * @param in the Kafka body as an input stream
     * @param additionalParameters additional Pararamters containing the headers and whetehr we are
     *     processing a key or value
     * @param useGlobalID hen true globalIDs are used, otherwise contentIDS
     * @return the id of the schema
     * @throws IOException error occurred
     */
    protected long getSchemaId(
            InputStream in, Map<String, Object> additionalParameters, boolean useGlobalID)
            throws IOException {
        long schemaId = 0L;
        Map<String, Object> headers = (Map<String, Object>) additionalParameters.get(HEADERS);
        boolean isKey = (boolean) additionalParameters.get(IS_KEY);
        boolean useHeaders = (boolean) configs.get(USE_HEADERS.key());

        if (useHeaders) {
            String globalIDHeaderName =
                    isKey ? APICURIO_KEY_GLOBAL_ID_HEADER : APICURIO_VALUE_GLOBAL_ID_HEADER;
            String contentIDHeaderName =
                    isKey ? APICURIO_KEY_CONTENT_ID_HEADER : APICURIO_VALUE_CONTENT_ID_HEADER;
            // get from headers
            if (headers.get(globalIDHeaderName) != null) {
                if (!useGlobalID) {
                    throw new IOException(
                            "The message has a global ID header "
                                    + globalIDHeaderName
                                    + ", but the configuration is for content ID.\n"
                                    + "Header names received: \n"
                                    + headers.keySet().stream().collect(Collectors.joining(",")));
                }
                byte[] globalIDByteArray = (byte[]) headers.get(globalIDHeaderName);
                schemaId = bytesToLong(globalIDByteArray);
            } else if (headers.get(contentIDHeaderName) != null) {
                if (useGlobalID) {
                    throw new IOException(
                            "The message has a content ID header "
                                    + contentIDHeaderName
                                    + "\n"
                                    + ", but the configuration is for global ID.\n"
                                    + "Header names received: "
                                    + headers.keySet().stream().collect(Collectors.joining(",")));
                }
                byte[] contentIDByteArray = (byte[]) headers.get(contentIDHeaderName);
                schemaId = bytesToLong(contentIDByteArray);
            } else {
                String headerName = useGlobalID ? globalIDHeaderName : contentIDHeaderName;
                throw new IOException(
                        "The format was configured to enable headers, but the expected Kafka header "
                                + headerName
                                + " is not present.\n "
                                + "Header names received: "
                                + headers.keySet().stream().collect(Collectors.joining(","))
                                + "\n "
                                + "Ensure that the kafka message was created by an Apicurio client. Check that it is "
                                + "sending the appropriate ID in the header, if the message is sending the ID in the payload,\n"
                                + " review and amend the format configuration options "
                                + USE_HEADERS
                                + " and "
                                + USE_GLOBALID);
            }
        } else {
            // the id is in the payload
            DataInputStream dataInputStream = new DataInputStream(in);
            if (dataInputStream.readByte() != 0) {
                throw new IOException(
                        "Unknown data format. Magic number was not found. "
                                + USE_HEADERS
                                + "= false"
                                + " configs="
                                + configs);
            }
            schemaId = dataInputStream.readLong();
        }
        return schemaId;
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
     * @param inputProperties properties containing the topic name
     * @param outputProperties headers to populate
     * @throws IOException Exception has occurred
     */
    @Override
    public void writeSchema(
            Schema schema,
            OutputStream out,
            Map<String, Object> inputProperties,
            Map<String, Object> outputProperties)
            throws IOException {
        String methodName = "writeSchema with additional properties";
        if (LOG.isDebugEnabled()) {
            LOG.debug(methodName + " entered");
        }

        boolean useHeaders = (boolean) configs.get(USE_HEADERS.key());
        boolean useGlobalId = (boolean) configs.get(USE_GLOBALID.key());
        boolean isKey = (boolean) inputProperties.get(IS_KEY);

        String groupId = (String) configs.get(AvroApicurioFormatOptions.GROUP_ID.key());
        Object artifactId = configs.get(AvroApicurioFormatOptions.REGISTERED_ARTIFACT_ID.key());
        Object artifactName = configs.get(AvroApicurioFormatOptions.REGISTERED_ARTIFACT_NAME.key());
        if (inputProperties.get(TOPIC_NAME) == null) {
            String configOptionName = null;
            if (artifactId == null) {
                configOptionName = "artifactId";
            }
            if (artifactName == null) {
                configOptionName = "artifactName";
            }
            if (configOptionName != null) {
                throw new IOException(
                        "No topic name was configured, and it is required to provide a default for "
                                + configOptionName);
            }
        }
        String defaultArtifactName =
                (String) inputProperties.get(TOPIC_NAME + (isKey ? "-key" : "-value"));
        artifactId = (artifactId == null) ? defaultArtifactName : artifactId;
        artifactName = (artifactName == null) ? defaultArtifactName : artifactName;

        // config option has a default so no need to check for null
        String artifactDescription =
                (String)
                        configs.get(
                                AvroApicurioFormatOptions.REGISTERED_ARTIFACT_DESCRIPTION.key());
        // config option has a default so no need to check for null
        String artifactVersion =
                (String) configs.get(AvroApicurioFormatOptions.REGISTERED_ARTIFACT_VERSION.key());
        AvroFormatOptions.AvroEncoding avroEncoding =
                (AvroFormatOptions.AvroEncoding) configs.get(AvroFormatOptions.AVRO_ENCODING.key());

        // register the schema
        InputStream in = new ByteArrayInputStream(schema.toString().getBytes());

        ArtifactMetaData artifactMetaData =
                registryClient.createArtifact(
                        groupId,
                        (String) artifactId,
                        artifactVersion,
                        ArtifactType.AVRO,
                        IfExists.RETURN_OR_UPDATE,
                        true,
                        (String) artifactName,
                        artifactDescription,
                        in);
        Long registeredGlobalId = artifactMetaData.getGlobalId();
        Long registeredContentId = artifactMetaData.getContentId();
        Map<String, Object> headers = new HashMap<>();
        if (useHeaders) {
            // always add the global ID as this is a globally unique identifier

            String globalHeaderName =
                    isKey ? APICURIO_KEY_GLOBAL_ID_HEADER : APICURIO_VALUE_GLOBAL_ID_HEADER;
            // optionally also add in the content id if the configuration says to
            headers.put(globalHeaderName, longToBytes(registeredGlobalId));
            if (!useGlobalId) {
                String contentHeaderName =
                        isKey ? APICURIO_KEY_CONTENT_ID_HEADER : APICURIO_VALUE_CONTENT_ID_HEADER;
                headers.put(contentHeaderName, longToBytes(registeredContentId));
            }

        } else {
            out.write(MAGIC_BYTE);
            byte[] schemaIdBytes;
            Long schemaId = (useGlobalId) ? registeredGlobalId : registeredContentId;
            // There could be a case where a table is created as a Kafka source and sink and the
            // user wants the content ID in, but the global ID out. This combination is currently
            // not catered for.

            schemaIdBytes = ByteBuffer.allocate(8).putLong(schemaId).array();
            out.write(schemaIdBytes);
        }
        // always add the encoding header
        AvroFormatOptions.AvroEncoding encoding;
        if (avroEncoding == null) {
            encoding = AvroFormatOptions.AvroEncoding.BINARY;
        } else {
            encoding = avroEncoding;
        }
        headers.put(APICURIO_VALUE_ENCODING, encoding.name().getBytes());

        outputProperties.put(HEADERS, headers);
    }
}
