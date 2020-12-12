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

import com.amazonaws.services.schemaregistry.common.AWSCompressionHandler;
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryDefaultCompression;
import com.amazonaws.services.schemaregistry.deserializers.AWSDeserializer;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/** Tests for {@link GlueSchemaRegistryInputStreamDeserializer}. */
@ExtendWith(MockitoExtension.class)
public class GlueSchemaRegistryInputStreamDeserializerTest {
    @Mock private AWSDeserializer mockDeserializer;

    private static byte compressionByte;
    private static Schema userSchema;
    private static com.amazonaws.services.schemaregistry.common.Schema glueSchema;
    private static User userDefinedPojo;
    private static Map<String, Object> configs = new HashMap<>();
    private static Map<String, String> metadata = new HashMap<>();
    private static AWSCompressionHandler awsCompressionHandler;

    private static final String testTopic = "Test-Topic";
    private static final UUID USER_SCHEMA_VERSION_ID = UUID.randomUUID();
    private static final String AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc";

    @BeforeEach
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
                        userSchema.toString(), "Avro", testTopic);
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
        assertThat(
                glueSchemaRegistryInputStreamDeserializer,
                instanceOf(GlueSchemaRegistryInputStreamDeserializer.class));
    }

    /** Test whether constructor works with AWS de-serializer input. */
    @Test
    public void testConstructor_withDeserializer_succeeds() {
        GlueSchemaRegistryInputStreamDeserializer glueSchemaRegistryInputStreamDeserializer =
                new GlueSchemaRegistryInputStreamDeserializer(mockDeserializer);
        assertThat(
                glueSchemaRegistryInputStreamDeserializer,
                instanceOf(GlueSchemaRegistryInputStreamDeserializer.class));
    }

    /** Test whether getSchemaAndDeserializedStream method works. */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testGetSchemaAndDeserializedStream_withValidParams_succeeds(
            AWSSchemaRegistryConstants.COMPRESSION compressionType) throws IOException {
        compressionByte =
                compressionType.name().equals("NONE")
                        ? AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE
                        : AWSSchemaRegistryConstants.COMPRESSION_BYTE;
        awsCompressionHandler = new AWSSchemaRegistryDefaultCompression();

        ByteArrayOutputStream byteArrayOutputStream =
                buildByteArrayOutputStream(
                        AWSSchemaRegistryConstants.HEADER_VERSION_BYTE, compressionByte);
        byte[] bytes =
                writeToExistingStream(
                        byteArrayOutputStream,
                        compressionType.name().equals("NONE")
                                ? encodeData(userDefinedPojo, new SpecificDatumWriter<>(userSchema))
                                : compressData(
                                        encodeData(
                                                userDefinedPojo,
                                                new SpecificDatumWriter<>(userSchema))));

        MutableByteArrayInputStream mutableByteArrayInputStream = new MutableByteArrayInputStream();
        mutableByteArrayInputStream.setBuffer(bytes);

        when(mockDeserializer.getSchema(any())).thenReturn(glueSchema);
        when(mockDeserializer.getActualData(any())).thenReturn(bytes);
        GlueSchemaRegistryInputStreamDeserializer glueSchemaRegistryInputStreamDeserializer =
                new GlueSchemaRegistryInputStreamDeserializer(mockDeserializer);
        Schema resultSchema =
                glueSchemaRegistryInputStreamDeserializer.getSchemaAndDeserializedStream(
                        mutableByteArrayInputStream);

        assertThat(resultSchema.toString(), equalTo(glueSchema.getSchemaDefinition()));
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
                        schemaDefinition, "Avro", testTopic);

        when(mockDeserializer.getSchema(any())).thenReturn(glueSchema);
        when(mockDeserializer.getActualData(any())).thenReturn(new byte[0]);
        GlueSchemaRegistryInputStreamDeserializer awsSchemaRegistryInputStreamDeserializer =
                new GlueSchemaRegistryInputStreamDeserializer(mockDeserializer);

        Exception exception =
                assertThrows(
                        AWSSchemaRegistryException.class,
                        () ->
                                awsSchemaRegistryInputStreamDeserializer
                                        .getSchemaAndDeserializedStream(
                                                mutableByteArrayInputStream));
        assertThat(
                exception.getMessage(),
                equalTo("Error occurred while parsing schema, see inner exception for details."));
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
        return awsCompressionHandler.compress(actualDataBytes);
    }
}
