/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.apicurio;

import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.avro.SchemaCoder;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.rest.client.JdkHttpClient;
import io.apicurio.rest.client.auth.Auth;
import io.apicurio.rest.client.auth.BasicAuth;
import io.apicurio.rest.client.auth.OidcAuth;
import io.apicurio.rest.client.auth.exception.AuthErrorHandler;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_PASSWORD;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.BASIC_AUTH_CREDENTIALS_USERID;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.OIDC_AUTH_CLIENT_ID;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.OIDC_AUTH_CLIENT_SECRET;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.OIDC_AUTH_URL;

/**
 * A {@link SchemaCoder.SchemaCoderProvider} that uses a cached schema registry client underneath.
 */
@Internal
class CachedSchemaCoderProvider implements SchemaCoder.SchemaCoderProvider {

    private static final long serialVersionUID = 8610401613495438381L;

    private final String url;
    private final int identityMapCapacity;
    private final @Nullable Map<String, ?> configs;

    CachedSchemaCoderProvider(String url, int identityMapCapacity) {
        this(url, identityMapCapacity, null);
    }

    CachedSchemaCoderProvider(
            String url, int identityMapCapacity, @Nullable Map<String, ?> configs) {
        this.url = Objects.requireNonNull(url);
        // TODO this does not seem to be used for Apicurio
        this.identityMapCapacity = identityMapCapacity;
        // Apicurio null pointers if this is left null.
        this.configs = configs == null ? new HashMap<>() : configs;
    }

    @Override
    public SchemaCoder get() {
        Map<String, Object> registryClientConfigs = getRegistryConfigs(this.configs);

        // This way of calling the client was taken from:
        // https://github.com/Apicurio/apicurio-registry-examples/blob/main/rest-client/src/
        // main/java/io/apicurio/registry/examples/SimpleRegistryDemo.java
        RegistryClient registryClient =
                RegistryClientFactory.create(url, registryClientConfigs, getAuth());
        return new ApicurioSchemaRegistryCoder(registryClient, (Map<String, Object>) this.configs);
    }

    private Map<String, Object> getRegistryConfigs(Map<String, ?> configs) {
        Map<String, Object> registryConfigs = new HashMap<>();
        for (String configKey : configs.keySet()) {
            if (configKey.startsWith("apicurio.registry.")) {
                registryConfigs.put(configKey, configs.get(configKey));
            }
        }
        return registryConfigs;
    }

    private Auth getAuth() {
        Auth auth = null;
        Object basicAuthUserid = this.configs.get(BASIC_AUTH_CREDENTIALS_USERID);
        Object oidcAuthURL = this.configs.get(OIDC_AUTH_URL);
        if (!Objects.isNull(oidcAuthURL)) {
            auth =
                    new OidcAuth(
                            new JdkHttpClient(
                                    (String) oidcAuthURL,
                                    Collections.emptyMap(),
                                    null,
                                    new AuthErrorHandler()),
                            (String) this.configs.get(OIDC_AUTH_CLIENT_ID),
                            (String) this.configs.get(OIDC_AUTH_CLIENT_SECRET));
        } else if (!Objects.isNull(basicAuthUserid)) {
            Object basicAuthPassword = this.configs.get(BASIC_AUTH_CREDENTIALS_PASSWORD);
            if (!Objects.isNull(basicAuthPassword)) {
                auth = new BasicAuth((String) basicAuthUserid, (String) basicAuthPassword);
            }
        }
        return auth;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CachedSchemaCoderProvider that = (CachedSchemaCoderProvider) o;
        return identityMapCapacity == that.identityMapCapacity
                && url.equals(that.url)
                && Objects.equals(configs, that.configs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, identityMapCapacity, configs);
    }
}
