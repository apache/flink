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

import org.apache.flink.util.TestLogger;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

/** Tests for {@link GlueSchemaRegistryOutputStreamSerializer}. */
public class GlueSchemaRegistryOutputStreamSerializerTest extends TestLogger {
    private static final String testTopic = "Test-Topic";
    private static final String AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc";
    private static final byte[] actualBytes =
            new byte[] {12, 99, 8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116};
    private static final byte[] specificBytes =
            new byte[] {
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99,
                8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116
            };
    private static Schema userSchema;
    private static User userDefinedPojo;
    private static Map<String, Object> configs = new HashMap<>();
    private static Map<String, String> metadata = new HashMap<>();
    private static AwsCredentialsProvider credentialsProvider =
            DefaultCredentialsProvider.builder().build();
    private static GlueSchemaRegistrySerializationFacade mockSerializationFacade;

    @BeforeClass
    public static void setup() throws IOException {
        metadata.put("test-key", "test-value");
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, testTopic);

        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);

        Schema.Parser parser = new Schema.Parser();
        userSchema = parser.parse(new File(AVRO_USER_SCHEMA_FILE));
        userDefinedPojo =
                User.newBuilder()
                        .setName("test_avro_schema")
                        .setFavoriteColor("violet")
                        .setFavoriteNumber(10)
                        .build();

        mockSerializationFacade = new MockGlueSchemaRegistrySerializationFacade();
    }

    /**
     * Test whether constructor works with topic name and AWS Glue Schema Registry configuration
     * map.
     */
    @Test
    public void testConstructor_withConfigsAndCredential_succeeds() {
        GlueSchemaRegistryOutputStreamSerializer glueSchemaRegistryOutputStreamSerializer =
                new GlueSchemaRegistryOutputStreamSerializer(testTopic, configs);
        assertThat(
                glueSchemaRegistryOutputStreamSerializer,
                instanceOf(GlueSchemaRegistryOutputStreamSerializer.class));
    }

    /** Test whether constructor works with Glue Schema Registry SerializationFacade. */
    @Test
    public void testConstructor_withDeserializer_succeeds() {
        GlueSchemaRegistryOutputStreamSerializer glueSchemaRegistryOutputStreamSerializer =
                new GlueSchemaRegistryOutputStreamSerializer(
                        testTopic, configs, mockSerializationFacade);
        assertThat(
                glueSchemaRegistryOutputStreamSerializer,
                instanceOf(GlueSchemaRegistryOutputStreamSerializer.class));
    }

    /** Test whether registerSchemaAndSerializeStream method works. */
    @Test
    public void testRegisterSchemaAndSerializeStream_withValidParams_succeeds() throws IOException {
        GlueSchemaRegistryOutputStreamSerializer glueSchemaRegistryOutputStreamSerializer =
                new GlueSchemaRegistryOutputStreamSerializer(
                        testTopic, configs, mockSerializationFacade);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        glueSchemaRegistryOutputStreamSerializer.registerSchemaAndSerializeStream(
                userSchema, outputStream, actualBytes);

        assertThat(outputStream.toByteArray(), equalTo(specificBytes));
    }

    private static class MockGlueSchemaRegistrySerializationFacade
            extends GlueSchemaRegistrySerializationFacade {

        public MockGlueSchemaRegistrySerializationFacade() {
            super(credentialsProvider, null, new GlueSchemaRegistryConfiguration(configs));
        }

        @Override
        public byte[] encode(
                String transportName,
                com.amazonaws.services.schemaregistry.common.Schema schema,
                byte[] data) {
            return specificBytes;
        }
    }
}
