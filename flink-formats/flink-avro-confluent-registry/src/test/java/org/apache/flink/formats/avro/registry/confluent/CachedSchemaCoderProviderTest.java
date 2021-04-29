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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import javax.net.ssl.SSLSocketFactory;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests for properties set by {@link RegistryAvroFormatFactory} in {@link
 * CachedSchemaCoderProvider}.
 */
public class CachedSchemaCoderProviderTest {

    @Test
    public void testThatSslIsNotInitializedForNoSslProperties() {
        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(new HashMap<>());
        SSLSocketFactory sslSocketFactory = getSslSocketFactoryFromProvider(provider);

        assertNull(sslSocketFactory);
    }

    @Test
    public void testThatSslIsInitializedForSslProperties() throws URISyntaxException {
        String keystoreFile = getAbsolutePath("/test-keystore.jks");
        String keystorePassword = "123456";
        Map<String, String> configs = new HashMap<>();
        configs.put("schema.registry.ssl.keystore.location", keystoreFile);
        configs.put("schema.registry.ssl.keystore.password", keystorePassword);
        configs.put("schema.registry.ssl.truststore.location", keystoreFile);
        configs.put("schema.registry.ssl.truststore.password", keystorePassword);

        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(configs);
        SSLSocketFactory sslSocketFactory = getSslSocketFactoryFromProvider(provider);

        assertNotNull(sslSocketFactory);
    }

    @Test
    public void testThatBasicAuthIsNotInitializedForNoBasicAuthProperties() {
        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(new HashMap<>());
        BasicAuthCredentialProvider basicAuthCredentialProvider =
                getBasicAuthFromProvider(provider);

        assertNull(basicAuthCredentialProvider);
    }

    @Test
    public void testThatBasicAuthIsInitializedForBasicAuthProperties() {
        String userPassword = "user:pwd";
        Map<String, String> configs = new HashMap<>();
        configs.put("basic.auth.credentials.source", "USER_INFO");
        configs.put("basic.auth.user.info", userPassword);

        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(configs);
        BasicAuthCredentialProvider basicAuthCredentialProvider =
                getBasicAuthFromProvider(provider);

        assertNotNull(basicAuthCredentialProvider);
        assertEquals(basicAuthCredentialProvider.getUserInfo(null), userPassword);
    }

    @Test
    public void testThatBearerAuthIsNotInitializedForNoBearerAuthProperties() {
        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(new HashMap<>());
        BearerAuthCredentialProvider bearerAuthCredentialProvider =
                getBearerAuthFromProvider(provider);

        assertNull(bearerAuthCredentialProvider);
    }

    @Test
    public void testThatBearerAuthIsInitializedForBearerAuthProperties() {
        String token = "123456";
        Map<String, String> configs = new HashMap<>();
        configs.put("bearer.auth.credentials.source", "STATIC_TOKEN");
        configs.put("bearer.auth.token", token);

        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(configs);
        BearerAuthCredentialProvider bearerAuthCredentialProvider =
                getBearerAuthFromProvider(provider);

        assertNotNull(bearerAuthCredentialProvider);
        assertEquals(bearerAuthCredentialProvider.getBearerToken(null), token);
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
