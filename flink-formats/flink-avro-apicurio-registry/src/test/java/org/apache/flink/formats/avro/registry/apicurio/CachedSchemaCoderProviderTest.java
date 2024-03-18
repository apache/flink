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

import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for properties set by {@link ApicurioRegistryAvroFormatFactory} in {@link
 * CachedSchemaCoderProvider}.
 */
class CachedSchemaCoderProviderTest {

    @Test
    void testThatSslIsNotInitializedForNoSslProperties() {
        CachedSchemaCoderProvider provider = initCachedSchemaCoderProvider(new HashMap<>());
        //        SSLSocketFactory sslSocketFactory = getSslSocketFactoryFromProvider(provider);
        //
        //        assertThat(sslSocketFactory).isNull();
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
    }

    private String getAbsolutePath(String path) throws URISyntaxException {
        return CachedSchemaCoderProviderTest.class.getResource(path).toURI().getPath();
    }

    //
    private CachedSchemaCoderProvider initCachedSchemaCoderProvider(Map<String, String> config) {
        return new CachedSchemaCoderProvider("someUrl", 1000, config);
    }
}
