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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.ConnectionFactory;
import org.apache.flink.table.secret.ReadableSecretStore;
import org.apache.flink.table.secret.WritableSecretStore;
import org.apache.flink.table.secret.exceptions.SecretException;
import org.apache.flink.table.secret.exceptions.SecretNotFoundException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default implementation of {@link ConnectionFactory} that identifies sensitive fields by a
 * predefined whitelist of field names.
 *
 * <p>During {@link #createConnection}, sensitive fields are extracted from the connection options
 * and stored as a single secret in the {@link WritableSecretStore}. A reference key ({@value
 * #SECRET_REFERENCE_KEY}) pointing to the stored secret is added to the returned {@link
 * CatalogConnection}.
 *
 * <p>During {@link #resolveConnection}, the secret reference is used to retrieve the sensitive
 * fields from the {@link ReadableSecretStore} and merge them back into the options.
 *
 * <p>See {@link #SENSITIVE_FIELD_NAMES} for the default whitelist of sensitive field names.
 */
@Internal
public class DefaultConnectionFactory implements ConnectionFactory {

    /**
     * Factory identifier used to discover this factory via SPI. Also used as the fallback when a
     * connection does not specify a {@link ConnectionFactory#CONNECTION_TYPE_KEY} option.
     */
    public static final String IDENTIFIER = "default";

    /**
     * Reserved option key used to store the reference to secrets in the secret store. The
     * surrounding double underscores make collision with user-supplied option names unlikely; user
     * options containing this key will be rejected at create-time.
     */
    static final String SECRET_REFERENCE_KEY = "__flink.encrypted-secret-key__";

    /**
     * Default whitelist of option keys treated as sensitive. Seeded from {@link
     * org.apache.flink.configuration.GlobalConfiguration#SENSITIVE_KEYS}; the list is intentionally
     * small to start and can be expanded over time as new sensitive options are introduced.
     */
    private static final Set<String> SENSITIVE_FIELD_NAMES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    "password",
                                    "secret",
                                    "fs.azure.account.key",
                                    "apikey",
                                    "api-key",
                                    "auth-params",
                                    "service-key",
                                    "token",
                                    "basic-auth",
                                    "jaas.config",
                                    "http-headers")));

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public CatalogConnection createConnection(
            SensitiveConnection connection, WritableSecretStore secretStore) {
        Map<String, String> allOptions = connection.getOptions();

        if (allOptions.containsKey(SECRET_REFERENCE_KEY)) {
            throw new ValidationException(
                    String.format(
                            "Connection option '%s' is reserved and cannot be set by users.",
                            SECRET_REFERENCE_KEY));
        }

        Map<String, String> sensitiveOptions =
                allOptions.entrySet().stream()
                        .filter(e -> SENSITIVE_FIELD_NAMES.contains(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, String> nonSensitiveOptions =
                allOptions.entrySet().stream()
                        .filter(e -> !SENSITIVE_FIELD_NAMES.contains(e.getKey()))
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (a, b) -> a,
                                        HashMap::new));

        if (!sensitiveOptions.isEmpty()) {
            final String secretId;
            try {
                secretId = secretStore.storeSecret(sensitiveOptions);
            } catch (SecretException e) {
                throw e;
            } catch (RuntimeException e) {
                throw new SecretException("Failed to store connection secret.", e);
            }
            nonSensitiveOptions.put(SECRET_REFERENCE_KEY, secretId);
        }

        return CatalogConnection.of(nonSensitiveOptions, connection.getComment());
    }

    @Override
    public SensitiveConnection resolveConnection(
            CatalogConnection connection, ReadableSecretStore secretStore) {
        Map<String, String> options = new HashMap<>(connection.getOptions());

        String secretId = options.remove(SECRET_REFERENCE_KEY);
        if (secretId != null) {
            try {
                Map<String, String> secrets = secretStore.getSecret(secretId);
                options.putAll(secrets);
            } catch (SecretNotFoundException e) {
                throw new ValidationException(
                        String.format(
                                "Failed to resolve connection secrets. Secret with ID '%s' not found.",
                                secretId),
                        e);
            } catch (SecretException e) {
                throw e;
            } catch (RuntimeException e) {
                throw new SecretException(
                        String.format(
                                "Failed to retrieve connection secret with ID '%s'.", secretId),
                        e);
            }
        }

        return SensitiveConnection.of(options, connection.getComment());
    }

    @Override
    public void deleteSecrets(CatalogConnection connection, WritableSecretStore secretStore) {
        String secretId = connection.getOptions().get(SECRET_REFERENCE_KEY);
        if (secretId != null) {
            try {
                secretStore.removeSecret(secretId);
            } catch (SecretException e) {
                throw e;
            } catch (RuntimeException e) {
                throw new SecretException(
                        String.format("Failed to remove connection secret with ID '%s'.", secretId),
                        e);
            }
        }
    }
}
