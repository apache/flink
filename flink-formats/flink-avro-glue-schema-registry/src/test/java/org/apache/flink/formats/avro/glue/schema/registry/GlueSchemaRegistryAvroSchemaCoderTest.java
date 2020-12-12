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

import com.amazonaws.services.schemaregistry.caching.AWSSchemaRegistrySerializerCache;
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/** Tests for {@link GlueSchemaRegistryAvroSchemaCoder}. */
@ExtendWith(MockitoExtension.class)
public class GlueSchemaRegistryAvroSchemaCoderTest {
    @Mock private AWSSchemaRegistryClient mockClient;
    @Mock private AwsCredentialsProvider mockCred;
    @Mock private GlueSchemaRegistryConfiguration mockConfigs;
    @Mock private GlueSchemaRegistryInputStreamDeserializer mockInputStreamDeserializer;

    private static Schema userSchema;
    private static User userDefinedPojo;
    private static Map<String, Object> configs = new HashMap<>();
    private static Map<String, String> metadata = new HashMap<>();

    private static final String testTopic = "Test-Topic";
    private static final String schemaName = "User-Topic";
    private static final UUID USER_SCHEMA_VERSION_ID = UUID.randomUUID();
    private static final String AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc";
    private static final byte[] actualBytes =
            new byte[] {12, 99, 8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116};
    private static final byte[] specificBytes =
            new byte[] {
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99,
                8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116
            };

    @BeforeEach
    public void setup() throws IOException {
        metadata.put("test-key", "test-value");
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, testTopic);

        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, schemaName);
        configs.put(AWSSchemaRegistryConstants.METADATA, metadata);

        Schema.Parser parser = new Schema.Parser();
        userSchema = parser.parse(new File(AVRO_USER_SCHEMA_FILE));
        userDefinedPojo =
                User.newBuilder()
                        .setName("test_avro_schema")
                        .setFavoriteColor("violet")
                        .setFavoriteNumber(10)
                        .build();
    }

    /** Test whether constructor works. */
    @Test
    public void testConstructor_withConfigs_succeeds() {
        assertThat(new GlueSchemaRegistryAvroSchemaCoder(testTopic, configs), notNullValue());
    }

    /**
     * Test whether readSchema method works.
     *
     * @throws IOException
     */
    @Test
    public void testReadSchema_withValidParams_succeeds() throws IOException {
        when(mockInputStreamDeserializer.getSchemaAndDeserializedStream(any(InputStream.class)))
                .thenReturn(userSchema);
        GlueSchemaRegistryAvroSchemaCoder glueSchemaRegistryAvroSchemaCoder =
                new GlueSchemaRegistryAvroSchemaCoder(mockInputStreamDeserializer);
        Schema resultSchema =
                glueSchemaRegistryAvroSchemaCoder.readSchema(buildByteArrayInputStream());

        assertThat(resultSchema, equalTo(userSchema));
    }

    /**
     * Test whether writeSchema method works.
     *
     * @param compressionType compression type
     * @throws IOException
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testWriteSchema_withValidParams_succeeds(
            AWSSchemaRegistryConstants.COMPRESSION compressionType) throws IOException {
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        when(mockClient.getORRegisterSchemaVersionId(
                        anyString(), anyString(), anyString(), anyMap()))
                .thenReturn(USER_SCHEMA_VERSION_ID);
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .schemaRegistryClient(mockClient)
                        .credentialProvider(mockCred)
                        .glueSchemaRegistryConfiguration(
                                new GlueSchemaRegistryConfiguration(configs))
                        .build();
        glueSchemaRegistrySerializationFacade.setCache(invalidateAndGetCache());
        GlueSchemaRegistrySerializationFacade spySerializationFacade =
                spy(glueSchemaRegistrySerializationFacade);
        doCallRealMethod()
                .when(spySerializationFacade)
                .encode(
                        anyString(),
                        any(com.amazonaws.services.schemaregistry.common.Schema.class),
                        any());
        GlueSchemaRegistryOutputStreamSerializer glueSchemaRegistryOutputStreamSerializer =
                new GlueSchemaRegistryOutputStreamSerializer(
                        testTopic, configs, spySerializationFacade);
        GlueSchemaRegistryOutputStreamSerializer spyOutputStreamSerializer =
                spy(glueSchemaRegistryOutputStreamSerializer);
        doCallRealMethod()
                .when(spyOutputStreamSerializer)
                .registerSchemaAndSerializeStream(any(), any(), any());

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(actualBytes);
        GlueSchemaRegistryAvroSchemaCoder glueSchemaRegistryAvroSchemaCoder =
                new GlueSchemaRegistryAvroSchemaCoder(spyOutputStreamSerializer);
        glueSchemaRegistryAvroSchemaCoder.writeSchema(userSchema, outputStream);

        testForSerializedData(outputStream.toByteArray(), USER_SCHEMA_VERSION_ID, compressionType);
    }

    /**
     * Test whether writeSchema method throws exception if auto registration un-enabled.
     *
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    @Test
    public void testWriteSchema_withoutAutoRegistration_throwsException()
            throws NoSuchFieldException, IllegalAccessException {
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, false);

        EntityNotFoundException entityNotFoundException =
                EntityNotFoundException.builder()
                        .message(AWSSchemaRegistryConstants.SCHEMA_NOT_FOUND_MSG)
                        .build();
        AWSSchemaRegistryException awsSchemaRegistryException =
                new AWSSchemaRegistryException(entityNotFoundException);
        when(mockClient.getORRegisterSchemaVersionId(
                        anyString(), anyString(), anyString(), anyMap()))
                .thenCallRealMethod();
        when(mockClient.getSchemaVersionIdByDefinition(anyString(), anyString(), anyString()))
                .thenThrow(awsSchemaRegistryException);
        configureAWSSchemaRegistryClientWithSerdeConfig(
                mockClient, new GlueSchemaRegistryConfiguration(configs));

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .schemaRegistryClient(mockClient)
                        .credentialProvider(mockCred)
                        .glueSchemaRegistryConfiguration(
                                new GlueSchemaRegistryConfiguration(configs))
                        .build();
        glueSchemaRegistrySerializationFacade.setCache(invalidateAndGetCache());

        GlueSchemaRegistryOutputStreamSerializer glueSchemaRegistryOutputStreamSerializer =
                new GlueSchemaRegistryOutputStreamSerializer(
                        testTopic, configs, glueSchemaRegistrySerializationFacade);
        GlueSchemaRegistryAvroSchemaCoder glueSchemaRegistryAvroSchemaCoder =
                new GlueSchemaRegistryAvroSchemaCoder(glueSchemaRegistryOutputStreamSerializer);

        Exception exception =
                assertThrows(
                        AWSSchemaRegistryException.class,
                        () ->
                                glueSchemaRegistryAvroSchemaCoder.writeSchema(
                                        userSchema, new ByteArrayOutputStream()));
        assertThat(
                exception.getMessage(),
                equalTo(AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG));
    }

    private void testForSerializedData(
            byte[] serializedData,
            UUID testGenericSchemaVersionId,
            AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        assertThat(serializedData, Matchers.notNullValue());

        ByteBuffer buffer = getByteBuffer(serializedData);

        byte headerVersionByte = getByte(buffer);
        byte compressionByte = getByte(buffer);
        UUID schemaVersionId = getSchemaVersionId(buffer);

        assertThat(headerVersionByte, equalTo(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE));
        assertThat(schemaVersionId, equalTo(testGenericSchemaVersionId));

        if (AWSSchemaRegistryConstants.COMPRESSION.NONE.name().equals(compressionType.name())) {
            assertThat(
                    compressionByte, equalTo(AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE));
        } else {
            assertThat(compressionByte, equalTo(AWSSchemaRegistryConstants.COMPRESSION_BYTE));
        }
    }

    private ByteArrayInputStream buildByteArrayInputStream() {
        return new ByteArrayInputStream(specificBytes);
    }

    private void configureAWSSchemaRegistryClientWithSerdeConfig(
            AWSSchemaRegistryClient awsSchemaRegistryClient,
            GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration)
            throws NoSuchFieldException, IllegalAccessException {
        Field serdeConfigField =
                AWSSchemaRegistryClient.class.getDeclaredField("glueSchemaRegistryConfiguration");
        serdeConfigField.setAccessible(true);
        serdeConfigField.set(awsSchemaRegistryClient, glueSchemaRegistryConfiguration);
    }

    private ByteBuffer getByteBuffer(byte[] bytes) {
        return ByteBuffer.wrap(bytes);
    }

    private Byte getByte(ByteBuffer buffer) {
        return buffer.get();
    }

    private UUID getSchemaVersionId(ByteBuffer buffer) {
        long mostSigBits = buffer.getLong();
        long leastSigBits = buffer.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    private AWSSchemaRegistrySerializerCache invalidateAndGetCache() {
        AWSSchemaRegistrySerializerCache serializerCache =
                AWSSchemaRegistrySerializerCache.getInstance(mockConfigs);
        serializerCache.flushCache();
        return serializerCache;
    }
}
