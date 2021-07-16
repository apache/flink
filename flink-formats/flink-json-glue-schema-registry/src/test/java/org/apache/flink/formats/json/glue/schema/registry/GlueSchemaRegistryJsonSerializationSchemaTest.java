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

package org.apache.flink.formats.json.glue.schema.registry;

import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import lombok.NonNull;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.time.Instant;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for {@link GlueSchemaRegistryJsonSerializationSchema}. */
public class GlueSchemaRegistryJsonSerializationSchemaTest {
    private static final String testTopic = "Test-Topic";
    private static final String schemaName = "User-Topic";
    private static final String JSON_SCHEMA =
            "{\n"
                    + "    \"$id\": \"https://example.com/address.schema.json\",\n"
                    + "    \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                    + "    \"type\": \"object\",\n"
                    + "    \"properties\": {\n"
                    + "      \"f1\": { \"type\": \"string\" },\n"
                    + "      \"f2\": { \"type\": \"integer\", \"maximum\": 1000 }\n"
                    + "    }\n"
                    + "  }";
    private static final byte[] serializedBytes =
            new byte[] {
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99,
                8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116
            };
    private static final UUID schemaVersionId = UUID.randomUUID();
    private static JsonDataWithSchema userSchema;
    private static Car userDefinedPojo;
    private static Map<String, Object> configs = new HashMap<>();
    private static Map<String, String> metadata = new HashMap<>();
    private static GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration;
    private static AwsCredentialsProvider credentialsProvider =
            DefaultCredentialsProvider.builder().build();
    private static GlueSchemaRegistrySerializationFacade mockSerializationFacade;

    @BeforeClass
    public static void setup() {
        metadata.put("test-key", "test-value");
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, testTopic);

        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, schemaName);
        configs.put(AWSSchemaRegistryConstants.METADATA, metadata);

        userSchema =
                JsonDataWithSchema.builder(JSON_SCHEMA, "{\"f1\":\"iphone\",\"f2\":12}").build();
        userDefinedPojo =
                Car.builder()
                        .make("Tesla")
                        .model("3")
                        .used(false)
                        .miles(6000)
                        .year(2021)
                        .listedDate(new GregorianCalendar(2020, Calendar.FEBRUARY, 20).getTime())
                        .purchaseDate(Date.from(Instant.parse("2020-01-01T00:00:00.000Z")))
                        .owners(new String[] {"Harry", "Megan"})
                        .serviceChecks(Arrays.asList(5000.0f, 10780.30f))
                        .build();

        glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        mockSerializationFacade = new MockGlueSchemaRegistrySerializationFacade();
    }

    /** Test initialization works. */
    @Test
    public void testForGeneric_withValidParams_succeeds() {
        assertThat(
                new GlueSchemaRegistryJsonSerializationSchema<>(testTopic, configs),
                notNullValue());
        assertThat(
                new GlueSchemaRegistryJsonSerializationSchema<>(testTopic, configs),
                instanceOf(GlueSchemaRegistryJsonSerializationSchema.class));
    }

    /**
     * Test whether serialize method for specific type JSON Schema data when compression is not
     * enabled works.
     */
    @Test
    public void testSerializePOJO_withValidParams_withoutCompression_succeeds() {
        AWSSchemaRegistryConstants.COMPRESSION compressionType =
                AWSSchemaRegistryConstants.COMPRESSION.NONE;
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        GlueSchemaRegistryJsonSchemaCoder glueSchemaRegistryJsonSchemaCoder =
                new GlueSchemaRegistryJsonSchemaCoder(
                        testTopic, configs, mockSerializationFacade, null);

        GlueSchemaRegistryJsonSerializationSchema glueSchemaRegistryJsonSerializationSchema =
                new GlueSchemaRegistryJsonSerializationSchema<>(glueSchemaRegistryJsonSchemaCoder);

        byte[] serializedData =
                glueSchemaRegistryJsonSerializationSchema.serialize(userDefinedPojo);
        assertThat(serializedData, equalTo(serializedBytes));
    }

    /**
     * Test whether serialize method for generic type JSON Schema data when compression is not
     * enabled works.
     */
    @Test
    public void testSerializeGenericData_withValidParams_withoutCompression_succeeds() {
        AWSSchemaRegistryConstants.COMPRESSION compressionType =
                AWSSchemaRegistryConstants.COMPRESSION.NONE;
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        GlueSchemaRegistryJsonSchemaCoder glueSchemaRegistryJsonSchemaCoder =
                new GlueSchemaRegistryJsonSchemaCoder(
                        testTopic, configs, mockSerializationFacade, null);

        GlueSchemaRegistryJsonSerializationSchema glueSchemaRegistryJsonSerializationSchema =
                new GlueSchemaRegistryJsonSerializationSchema<>(glueSchemaRegistryJsonSchemaCoder);

        byte[] serializedData = glueSchemaRegistryJsonSerializationSchema.serialize(userSchema);
        assertThat(serializedData, equalTo(serializedBytes));
    }

    /**
     * Test whether serialize method for specific type JSON Schema data when compression is enabled
     * works.
     */
    @Test
    public void testSerializePOJO_withValidParams_withCompression_succeeds() {
        AWSSchemaRegistryConstants.COMPRESSION compressionType =
                AWSSchemaRegistryConstants.COMPRESSION.ZLIB;
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        GlueSchemaRegistryJsonSchemaCoder glueSchemaRegistryJsonSchemaCoder =
                new GlueSchemaRegistryJsonSchemaCoder(
                        testTopic, configs, mockSerializationFacade, null);

        GlueSchemaRegistryJsonSerializationSchema glueSchemaRegistryJsonSerializationSchema =
                new GlueSchemaRegistryJsonSerializationSchema<>(glueSchemaRegistryJsonSchemaCoder);

        byte[] serializedData =
                glueSchemaRegistryJsonSerializationSchema.serialize(userDefinedPojo);
        assertThat(serializedData, equalTo(serializedBytes));
    }

    /**
     * Test whether serialize method for generic type JSON Schema data when compression is enabled
     * works.
     */
    @Test
    public void testSerializeGenericData_withValidParams_withCompression_succeeds() {
        AWSSchemaRegistryConstants.COMPRESSION compressionType =
                AWSSchemaRegistryConstants.COMPRESSION.ZLIB;
        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());

        GlueSchemaRegistryJsonSchemaCoder glueSchemaRegistryJsonSchemaCoder =
                new GlueSchemaRegistryJsonSchemaCoder(
                        testTopic, configs, mockSerializationFacade, null);

        GlueSchemaRegistryJsonSerializationSchema glueSchemaRegistryJsonSerializationSchema =
                new GlueSchemaRegistryJsonSerializationSchema<>(glueSchemaRegistryJsonSchemaCoder);

        byte[] serializedData = glueSchemaRegistryJsonSerializationSchema.serialize(userSchema);
        assertThat(serializedData, equalTo(serializedBytes));
    }

    /** Test whether serialize method returns null when input object is null. */
    @Test
    public void testSerialize_withNullObject_returnNull() {
        GlueSchemaRegistryJsonSerializationSchema glueSchemaRegistryJsonSerializationSchema =
                new GlueSchemaRegistryJsonSerializationSchema<>(testTopic, configs);
        assertThat(glueSchemaRegistryJsonSerializationSchema.serialize(null), nullValue());
    }

    private static class MockGlueSchemaRegistrySerializationFacade
            extends GlueSchemaRegistrySerializationFacade {

        public MockGlueSchemaRegistrySerializationFacade() {
            super(credentialsProvider, null, glueSchemaRegistryConfiguration, configs, null);
        }

        @Override
        public UUID getOrRegisterSchemaVersion(@NonNull AWSSerializerInput serializerInput) {
            return schemaVersionId;
        }

        @Override
        public byte[] serialize(
                DataFormat dataFormat, @NonNull Object data, @NonNull UUID schemaVersionId) {
            return serializedBytes;
        }
    }
}
