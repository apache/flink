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

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.mock.Whitebox;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSocketFactory;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for properties set by {@link RegistryAvroFormatFactory} in {@link
 * CachedSchemaCoderProvider}.
 */
class CachedSchemaCoderProviderTest {

    @Test
    void testThatSslIsNotInitializedForNoSslProperties() {
        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(new HashMap<>());
        SSLSocketFactory sslSocketFactory = getSslSocketFactoryFromProvider(provider);

        assertThat(sslSocketFactory).isNull();
    }

    @Test
    void testThatSslIsInitializedForSslProperties() throws URISyntaxException {
        String keystoreFile = getAbsolutePath("/test-keystore.jks");
        String keystorePassword = "123456";
        Map<String, String> configs = new HashMap<>();
        configs.put("schema.registry.ssl.keystore.location", keystoreFile);
        configs.put("schema.registry.ssl.keystore.password", keystorePassword);
        configs.put("schema.registry.ssl.truststore.location", keystoreFile);
        configs.put("schema.registry.ssl.truststore.password", keystorePassword);

        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(configs);
        SSLSocketFactory sslSocketFactory = getSslSocketFactoryFromProvider(provider);

        assertThat(sslSocketFactory).isNotNull();
    }

    @Test
    void testThatBasicAuthIsNotInitializedForNoBasicAuthProperties() {
        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(new HashMap<>());
        BasicAuthCredentialProvider basicAuthCredentialProvider =
                getBasicAuthFromProvider(provider);

        assertThat(basicAuthCredentialProvider).isNull();
    }

    @Test
    void testThatBasicAuthIsInitializedForBasicAuthProperties() {
        String userPassword = "user:pwd";
        Map<String, String> configs = new HashMap<>();
        configs.put("basic.auth.credentials.source", "USER_INFO");
        configs.put("basic.auth.user.info", userPassword);

        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(configs);
        BasicAuthCredentialProvider basicAuthCredentialProvider =
                getBasicAuthFromProvider(provider);

        assertThat(basicAuthCredentialProvider).isNotNull();
        assertThat(basicAuthCredentialProvider.getUserInfo(null)).isEqualTo(userPassword);
    }

    @Test
    void testThatBearerAuthIsNotInitializedForNoBearerAuthProperties() {
        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(new HashMap<>());
        BearerAuthCredentialProvider bearerAuthCredentialProvider =
                getBearerAuthFromProvider(provider);

        assertThat(bearerAuthCredentialProvider).isNull();
    }

    @Test
    void testThatBearerAuthIsInitializedForBearerAuthProperties() {
        String token = "123456";
        Map<String, String> configs = new HashMap<>();
        configs.put("bearer.auth.credentials.source", "STATIC_TOKEN");
        configs.put("bearer.auth.token", token);

        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(configs);
        BearerAuthCredentialProvider bearerAuthCredentialProvider =
                getBearerAuthFromProvider(provider);

        assertThat(bearerAuthCredentialProvider).isNotNull();
        assertThat(bearerAuthCredentialProvider.getBearerToken(null)).isEqualTo(token);
    }

    private String getAbsolutePath(String path) throws URISyntaxException {
        return CachedSchemaCoderProviderTest.class.getResource(path).toURI().getPath();
    }

    private CachedSchemaCoderProvider initCachedSchemaCoderProvider(Map<String, String> config) {
        return new CachedSchemaCoderProvider("test", "someUrl", 1000, config);
    }

    private SSLSocketFactory getSslSocketFactoryFromProvider(CachedSchemaCoderProvider provider) {
        return getInternalStateFromRestService("sslSocketFactory", provider);
    }

    private BasicAuthCredentialProvider getBasicAuthFromProvider(
            CachedSchemaCoderProvider provider) {
        return getInternalStateFromRestService("basicAuthCredentialProvider", provider);
    }

    private BearerAuthCredentialProvider getBearerAuthFromProvider(
            CachedSchemaCoderProvider provider) {
        return getInternalStateFromRestService("bearerAuthCredentialProvider", provider);
    }

    private <T> T getInternalStateFromRestService(String name, CachedSchemaCoderProvider provider) {
        CachedSchemaRegistryClient cachedSchemaRegistryClient =
                Whitebox.getInternalState(provider.get(), "schemaRegistryClient");
        RestService restService =
                Whitebox.getInternalState(cachedSchemaRegistryClient, "restService");
        return Whitebox.getInternalState(restService, name);
    }
}
