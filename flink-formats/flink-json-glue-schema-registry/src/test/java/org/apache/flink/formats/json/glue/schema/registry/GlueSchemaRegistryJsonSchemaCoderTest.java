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

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GlueSchemaRegistryJsonSchemaCoder}. */
class GlueSchemaRegistryJsonSchemaCoderTest {

    @Test
    void testDefaultAwsCredentialsProvider() throws Exception {
        GlueSchemaRegistryJsonSchemaCoder coder =
                new GlueSchemaRegistryJsonSchemaCoder("test", getBaseConfig());

        GlueSchemaRegistryDeserializationFacade facade =
                getField("glueSchemaRegistryDeserializationFacade", coder);

        AwsCredentialsProvider credentialsProvider = facade.getCredentialsProvider();
        assertThat(credentialsProvider).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void testAwsCredentialsProviderFromConfig() throws Exception {
        Map<String, Object> config = new HashMap<>(getBaseConfig());
        config.put(AWS_ACCESS_KEY_ID, "ak");
        config.put(AWS_SECRET_ACCESS_KEY, "sk");

        GlueSchemaRegistryJsonSchemaCoder coder =
                new GlueSchemaRegistryJsonSchemaCoder("test", config);

        GlueSchemaRegistryDeserializationFacade facade =
                getField("glueSchemaRegistryDeserializationFacade", coder);

        AwsCredentialsProvider credentialsProvider = facade.getCredentialsProvider();
        assertThat(credentialsProvider.resolveCredentials().accessKeyId()).isEqualTo("ak");
        assertThat(credentialsProvider.resolveCredentials().secretAccessKey()).isEqualTo("sk");
    }

    private <T> T getField(final String fieldName, final Object instance) throws Exception {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(instance);
    }

    private Map<String, Object> getBaseConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        return configs;
    }
}
