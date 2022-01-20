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

package org.apache.flink.formats.avro.glue.schema.registry;

import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;
import org.apache.flink.util.TestLogger;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryCompressionHandler;
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDefaultCompression;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.COMPRESSION;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.COMPRESSION.NONE;
import static com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GlueSchemaRegistryInputStreamDeserializer}. */
public class GlueSchemaRegistryInputStreamDeserializerTest extends TestLogger {
    private static final String testTopic = "Test-Topic";
    private static final UUID USER_SCHEMA_VERSION_ID = UUID.randomUUID();
    private static final String AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc";
    private static byte compressionByte;
    private static Schema userSchema;
    private static com.amazonaws.services.schemaregistry.common.Schema glueSchema;
    private static User userDefinedPojo;
    private static final Map<String, Object> configs = new HashMap<>();
    private static final Map<String, String> metadata = new HashMap<>();
    private static GlueSchemaRegistryCompressionHandler compressionHandler;
    private static final AwsCredentialsProvider credentialsProvider =
            DefaultCredentialsProvider.builder().build();
    @Rule public ExpectedException thrown = ExpectedException.none();
    private GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade;

    @Before
    public void setup() throws IOException {
        metadata.put("test-key", "test-value");
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, testTopic);

        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);

        Schema.Parser parser = new Schema.Parser();
        userSchema = parser.parse(new File(AVRO_USER_SCHEMA_FILE));
        glueSchema =
                new com.amazonaws.services.schemaregistry.common.Schema(
                        userSchema.toString(), DataFormat.AVRO.name(), testTopic);
        userDefinedPojo =
                User.newBuilder()
                        .setName("test_avro_schema")
                        .setFavoriteColor("violet")
                        .setFavoriteNumber(10)
                        .build();
    }

    /** Test whether constructor works with configuration map. */
    @Test
    public void testConstructor_withConfigs_succeeds() {
        GlueSchemaRegistryInputStreamDeserializer glueSchemaRegistryInputStreamDeserializer =
                new GlueSchemaRegistryInputStreamDeserializer(configs);
        assertThat(glueSchemaRegistryInputStreamDeserializer)
                .isInstanceOf(GlueSchemaRegistryInputStreamDeserializer.class);
    }

    @Test
    public void testDefaultAwsCredentialsProvider() throws Exception {
        GlueSchemaRegistryInputStreamDeserializer glueSchemaRegistryInputStreamDeserializer =
                new GlueSchemaRegistryInputStreamDeserializer(configs);

        GlueSchemaRegistryDeserializationFacade facade =
                getField(
                        "glueSchemaRegistryDeserializationFacade",
                        glueSchemaRegistryInputStreamDeserializer);

        AwsCredentialsProvider credentialsProvider = facade.getCredentialsProvider();
        assertThat(credentialsProvider).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    public void testAwsCredentialsProviderFromConfig() throws Exception {
        Map<String, Object> config = new HashMap<>(configs);
        config.put(AWS_ACCESS_KEY_ID, "ak");
        config.put(AWS_SECRET_ACCESS_KEY, "sk");

        GlueSchemaRegistryInputStreamDeserializer glueSchemaRegistryInputStreamDeserializer =
                new GlueSchemaRegistryInputStreamDeserializer(config);

        GlueSchemaRegistryDeserializationFacade facade =
                getField(
                        "glueSchemaRegistryDeserializationFacade",
                        glueSchemaRegistryInputStreamDeserializer);

        AwsCredentialsProvider credentialsProvider = facade.getCredentialsProvider();
        assertThat(credentialsProvider.resolveCredentials().accessKeyId()).isEqualTo("ak");
        assertThat(credentialsProvider.resolveCredentials().secretAccessKey()).isEqualTo("sk");
    }

    /** Test whether constructor works with AWS de-serializer input. */
    @Test
    public void testConstructor_withDeserializer_succeeds() {
        GlueSchemaRegistryInputStreamDeserializer glueSchemaRegistryInputStreamDeserializer =
                new GlueSchemaRegistryInputStreamDeserializer(
                        glueSchemaRegistryDeserializationFacade);
        assertThat(glueSchemaRegistryInputStreamDeserializer)
                .isInstanceOf(GlueSchemaRegistryInputStreamDeserializer.class);
    }

    /** Test whether getSchemaAndDeserializedStream method when compression is not enabled works. */
    @Test
    public void testGetSchemaAndDeserializedStream_withoutCompression_succeeds()
            throws IOException {
        compressionByte = COMPRESSION_DEFAULT_BYTE;
        compressionHandler = new GlueSchemaRegistryDefaultCompression();

        ByteArrayOutputStream byteArrayOutputStream =
                buildByteArrayOutputStream(
                        AWSSchemaRegistryConstants.HEADER_VERSION_BYTE, compressionByte);
        byte[] bytes =
                writeToExistingStream(
                        byteArrayOutputStream,
                        encodeData(userDefinedPojo, new SpecificDatumWriter<>(userSchema)));

        MutableByteArrayInputStream mutableByteArrayInputStream = new MutableByteArrayInputStream();
        mutableByteArrayInputStream.setBuffer(bytes);
        glueSchemaRegistryDeserializationFacade =
                new MockGlueSchemaRegistryDeserializationFacade(bytes, glueSchema, NONE);

        GlueSchemaRegistryInputStreamDeserializer glueSchemaRegistryInputStreamDeserializer =
                new GlueSchemaRegistryInputStreamDeserializer(
                        glueSchemaRegistryDeserializationFacade);
        Schema resultSchema =
                glueSchemaRegistryInputStreamDeserializer.getSchemaAndDeserializedStream(
                        mutableByteArrayInputStream);

        assertThat(resultSchema.toString()).isEqualTo(glueSchema.getSchemaDefinition());
    }

    /** Test whether getSchemaAndDeserializedStream method when compression is enabled works. */
    @Test
    public void testGetSchemaAndDeserializedStream_withCompression_succeeds() throws IOException {
        COMPRESSION compressionType = COMPRESSION.ZLIB;
        compressionByte = AWSSchemaRegistryConstants.COMPRESSION_BYTE;
        compressionHandler = new GlueSchemaRegistryDefaultCompression();

        ByteArrayOutputStream byteArrayOutputStream =
                buildByteArrayOutputStream(
                        AWSSchemaRegistryConstants.HEADER_VERSION_BYTE, compressionByte);
        byte[] bytes =
                writeToExistingStream(
                        byteArrayOutputStream,
                        compressData(
                                encodeData(
                                        userDefinedPojo, new SpecificDatumWriter<>(userSchema))));

        MutableByteArrayInputStream mutableByteArrayInputStream = new MutableByteArrayInputStream();
        mutableByteArrayInputStream.setBuffer(bytes);
        glueSchemaRegistryDeserializationFacade =
                new MockGlueSchemaRegistryDeserializationFacade(bytes, glueSchema, compressionType);

        GlueSchemaRegistryInputStreamDeserializer glueSchemaRegistryInputStreamDeserializer =
                new GlueSchemaRegistryInputStreamDeserializer(
                        glueSchemaRegistryDeserializationFacade);
        Schema resultSchema =
                glueSchemaRegistryInputStreamDeserializer.getSchemaAndDeserializedStream(
                        mutableByteArrayInputStream);

        assertThat(resultSchema.toString()).isEqualTo(glueSchema.getSchemaDefinition());
    }

    /** Test whether getSchemaAndDeserializedStream method throws exception with invalid schema. */
    @Test
    public void testGetSchemaAndDeserializedStream_withWrongSchema_throwsException()
            throws IOException {
        String schemaDefinition =
                "{"
                        + "\"type\":\"record\","
                        + "\"name\":\"User\","
                        + "\"namespace\":\"org.apache.flink.formats.avro.glue.schema.registry\","
                        + "\"fields\":"
                        + "["
                        + "{\"name\":\"name\",\"type\":\"string\"},"
                        + "{\"name\":\"favorite_number\",\"name\":[\"int\",\"null\"]},"
                        + "{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}"
                        + "]"
                        + "}";
        MutableByteArrayInputStream mutableByteArrayInputStream = new MutableByteArrayInputStream();
        glueSchema =
                new com.amazonaws.services.schemaregistry.common.Schema(
                        schemaDefinition, DataFormat.AVRO.name(), testTopic);
        glueSchemaRegistryDeserializationFacade =
                new MockGlueSchemaRegistryDeserializationFacade(new byte[20], glueSchema, NONE);
        GlueSchemaRegistryInputStreamDeserializer awsSchemaRegistryInputStreamDeserializer =
                new GlueSchemaRegistryInputStreamDeserializer(
                        glueSchemaRegistryDeserializationFacade);

        thrown.expect(AWSSchemaRegistryException.class);
        thrown.expectMessage(
                "Error occurred while parsing schema, see inner exception for details.");
        awsSchemaRegistryInputStreamDeserializer.getSchemaAndDeserializedStream(
                mutableByteArrayInputStream);
    }

    private ByteArrayOutputStream buildByteArrayOutputStream(byte headerByte, byte compressionByte)
            throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.write(headerByte);
        byteArrayOutputStream.write(compressionByte);
        writeSchemaVersionId(byteArrayOutputStream);

        return byteArrayOutputStream;
    }

    private void writeSchemaVersionId(ByteArrayOutputStream out) throws IOException {
        ByteBuffer buffer =
                ByteBuffer.wrap(new byte[AWSSchemaRegistryConstants.SCHEMA_VERSION_ID_SIZE]);
        buffer.putLong(USER_SCHEMA_VERSION_ID.getMostSignificantBits());
        buffer.putLong(USER_SCHEMA_VERSION_ID.getLeastSignificantBits());
        out.write(buffer.array());
    }

    private byte[] writeToExistingStream(ByteArrayOutputStream toStream, byte[] fromStream)
            throws IOException {
        toStream.write(fromStream);
        return toStream.toByteArray();
    }

    private byte[] encodeData(Object object, DatumWriter<Object> writer) throws IOException {
        ByteArrayOutputStream actualDataBytes = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(actualDataBytes, null);
        writer.write(object, encoder);
        encoder.flush();
        return actualDataBytes.toByteArray();
    }

    private byte[] compressData(byte[] actualDataBytes) throws IOException {
        return compressionHandler.compress(actualDataBytes);
    }

    private static class MockGlueSchemaRegistryDeserializationFacade
            extends GlueSchemaRegistryDeserializationFacade {
        private final byte[] bytes;
        private final com.amazonaws.services.schemaregistry.common.Schema schema;
        private final COMPRESSION compressionType;

        public MockGlueSchemaRegistryDeserializationFacade(
                byte[] bytes,
                com.amazonaws.services.schemaregistry.common.Schema schema,
                COMPRESSION compressionType) {
            super(configs, null, credentialsProvider, null);
            this.bytes = bytes;
            this.schema = schema;
            this.compressionType = compressionType;
        }

        @Override
        public String getSchemaDefinition(@NonNull byte[] data) {
            return schema.getSchemaDefinition();
        }

        @Override
        public byte[] getActualData(byte[] data) {
            return bytes;
        }
    }

    private <T> T getField(final String fieldName, final Object instance) throws Exception {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(instance);
    }
}
