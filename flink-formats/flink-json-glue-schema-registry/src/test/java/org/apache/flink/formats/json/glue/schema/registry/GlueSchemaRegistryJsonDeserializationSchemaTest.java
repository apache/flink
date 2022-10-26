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

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import lombok.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.time.Instant;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GlueSchemaRegistryJsonDeserializationSchema}. */
class GlueSchemaRegistryJsonDeserializationSchemaTest {
    private static final String testTopic = "Test-Topic";
    private static final Map<String, Object> configs = new HashMap<>();
    private static final AwsCredentialsProvider credentialsProvider =
            DefaultCredentialsProvider.builder().build();
    private static final byte[] serializedBytes =
            new byte[] {
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99,
                8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116
            };
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
    private static JsonDataWithSchema userSchema;
    private static Car userDefinedPojo;
    private static GlueSchemaRegistryDeserializationFacade mockDeserializationFacadeForSpecific;
    private static GlueSchemaRegistryDeserializationFacade mockDeserializationFacadeForGeneric;

    @BeforeAll
    static void setup() {
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);

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

        mockDeserializationFacadeForSpecific =
                new MockGlueSchemaRegistrySerializationFacadeForSpecific();
        mockDeserializationFacadeForGeneric =
                new MockGlueSchemaRegistrySerializationFacadeForGeneric();
    }

    /** Test initialization for generic type JSON Schema works. */
    @Test
    void testForGeneric_withValidParams_succeeds() {
        assertThat(
                        new GlueSchemaRegistryJsonDeserializationSchema<>(
                                JsonDataWithSchema.class, testTopic, configs))
                .isNotNull();
    }

    /** Test initialization for specific type JSON Schema works. */
    @Test
    void testForSpecific_withValidParams_succeeds() {
        assertThat(new GlueSchemaRegistryJsonDeserializationSchema<>(Car.class, testTopic, configs))
                .isNotNull();
    }

    /** Test whether deserialize method for specific type JSON Schema data works. */
    @Test
    void testDeserializePOJO_withValidParams_succeeds() {
        GlueSchemaRegistryJsonSchemaCoder glueSchemaRegistryJsonSchemaCoder =
                new GlueSchemaRegistryJsonSchemaCoder(
                        testTopic, configs, null, mockDeserializationFacadeForSpecific);

        GlueSchemaRegistryJsonDeserializationSchema<Car>
                glueSchemaRegistryJsonDeserializationSchema =
                        new GlueSchemaRegistryJsonDeserializationSchema<>(
                                Car.class, glueSchemaRegistryJsonSchemaCoder);

        Object deserializedObject =
                glueSchemaRegistryJsonDeserializationSchema.deserialize(serializedBytes);
        assertThat(deserializedObject).isInstanceOf(Car.class).isEqualTo(userDefinedPojo);
    }

    /** Test whether deserialize method for generic type JSON Schema data works. */
    @Test
    void testDeserializeGenericData_withValidParams_succeeds() {
        GlueSchemaRegistryJsonSchemaCoder glueSchemaRegistryJsonSchemaCoder =
                new GlueSchemaRegistryJsonSchemaCoder(
                        testTopic, configs, null, mockDeserializationFacadeForGeneric);

        GlueSchemaRegistryJsonDeserializationSchema<Car>
                glueSchemaRegistryJsonDeserializationSchema =
                        new GlueSchemaRegistryJsonDeserializationSchema<>(
                                Car.class, glueSchemaRegistryJsonSchemaCoder);

        Object deserializedObject =
                glueSchemaRegistryJsonDeserializationSchema.deserialize(serializedBytes);
        assertThat(deserializedObject).isInstanceOf(JsonDataWithSchema.class).isEqualTo(userSchema);
    }

    /** Test whether deserialize method returns null when input byte array is null. */
    @Test
    void testDeserialize_withNullObject_returnNull() {
        GlueSchemaRegistryJsonDeserializationSchema<Car>
                glueSchemaRegistryJsonDeserializationSchema =
                        new GlueSchemaRegistryJsonDeserializationSchema<>(
                                Car.class, testTopic, configs);
        assertThat(glueSchemaRegistryJsonDeserializationSchema.deserialize(null)).isNull();
    }

    private static class MockGlueSchemaRegistrySerializationFacadeForSpecific
            extends GlueSchemaRegistryDeserializationFacade {

        public MockGlueSchemaRegistrySerializationFacadeForSpecific() {
            super(configs, null, credentialsProvider, null);
        }

        @Override
        public Object deserialize(@NonNull AWSDeserializerInput deserializerInput)
                throws AWSSchemaRegistryException {
            return userDefinedPojo;
        }
    }

    private static class MockGlueSchemaRegistrySerializationFacadeForGeneric
            extends GlueSchemaRegistryDeserializationFacade {

        public MockGlueSchemaRegistrySerializationFacadeForGeneric() {
            super(configs, null, credentialsProvider, null);
        }

        @Override
        public Object deserialize(@NonNull AWSDeserializerInput deserializerInput)
                throws AWSSchemaRegistryException {
            return userSchema;
        }
    }
}
