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

package org.apache.flink.table.factories;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogConnection;
import org.apache.flink.table.catalog.SensitiveConnection;
import org.apache.flink.table.secret.ReadableSecretStore;
import org.apache.flink.table.secret.WritableSecretStore;
import org.apache.flink.table.secret.exceptions.SecretException;
import org.apache.flink.table.secret.exceptions.SecretNotFoundException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultConnectionFactory}. */
class DefaultConnectionFactoryTest {

    private final DefaultConnectionFactory factory = new DefaultConnectionFactory();

    // ---------------------------------------------------------------------------------------------
    // createConnection
    // ---------------------------------------------------------------------------------------------

    @Test
    void createConnectionWithoutSensitiveOptionsDoesNotCallSecretStore() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("host", "localhost");
        options.put("port", "9092");

        RecordingSecretStore secretStore = new RecordingSecretStore();
        CatalogConnection result =
                factory.createConnection(
                        SensitiveConnection.of(options, "my-comment"), secretStore);

        assertThat(secretStore.stored).isEmpty();
        assertThat(result.getOptions())
                .containsExactlyInAnyOrderEntriesOf(options)
                .doesNotContainKey(DefaultConnectionFactory.SECRET_REFERENCE_KEY);
        assertThat(result.getComment()).isEqualTo("my-comment");
    }

    @Test
    void createConnectionWithSensitiveOptionsStoresOnlySensitiveOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("host", "localhost");
        options.put("password", "p@ss");
        options.put("api-key", "abc");
        options.put("token", "tok");

        RecordingSecretStore secretStore = new RecordingSecretStore();
        CatalogConnection result =
                factory.createConnection(SensitiveConnection.of(options, null), secretStore);

        // secretStore.storeSecret called exactly once with only the sensitive subset
        assertThat(secretStore.stored).hasSize(1);
        Map<String, String> storedSecret = secretStore.stored.values().iterator().next();
        assertThat(storedSecret)
                .containsOnlyKeys("password", "api-key", "token")
                .containsEntry("password", "p@ss")
                .containsEntry("api-key", "abc")
                .containsEntry("token", "tok");

        // Returned connection retains only non-sensitive options plus a secret reference
        String secretId = secretStore.stored.keySet().iterator().next();
        assertThat(result.getOptions())
                .containsOnlyKeys("host", DefaultConnectionFactory.SECRET_REFERENCE_KEY)
                .containsEntry("host", "localhost")
                .containsEntry(DefaultConnectionFactory.SECRET_REFERENCE_KEY, secretId);
    }

    @Test
    void createConnectionRecognizesAllSensitiveFieldNames() {
        // Covers the default sensitive whitelist; failure here flags an accidental change to
        // the whitelist.
        List<String> sensitiveKeys =
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
                        "http-headers");

        Map<String, String> options = new LinkedHashMap<>();
        for (String key : sensitiveKeys) {
            options.put(key, "value-of-" + key);
        }

        RecordingSecretStore secretStore = new RecordingSecretStore();
        CatalogConnection result =
                factory.createConnection(SensitiveConnection.of(options, null), secretStore);

        assertThat(secretStore.stored).hasSize(1);
        Map<String, String> storedSecret = secretStore.stored.values().iterator().next();
        assertThat(storedSecret).containsExactlyInAnyOrderEntriesOf(options);
        assertThat(result.getOptions())
                .containsOnlyKeys(DefaultConnectionFactory.SECRET_REFERENCE_KEY);
    }

    @Test
    void createConnectionRejectsUserSuppliedReservedKey() {
        Map<String, String> options =
                Collections.singletonMap(
                        DefaultConnectionFactory.SECRET_REFERENCE_KEY, "user-injected");

        RecordingSecretStore secretStore = new RecordingSecretStore();
        assertThatThrownBy(
                        () ->
                                factory.createConnection(
                                        SensitiveConnection.of(options, null), secretStore))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(DefaultConnectionFactory.SECRET_REFERENCE_KEY)
                .hasMessageContaining("reserved");
        assertThat(secretStore.stored).isEmpty();
    }

    @Test
    void createConnectionPropagatesSecretException() {
        Map<String, String> options = Collections.singletonMap("password", "p@ss");
        SecretException original = new SecretException("downstream failure");

        RecordingSecretStore secretStore =
                new RecordingSecretStore() {
                    @Override
                    public String storeSecret(Map<String, String> secretData) {
                        throw original;
                    }
                };

        assertThatThrownBy(
                        () ->
                                factory.createConnection(
                                        SensitiveConnection.of(options, null), secretStore))
                .isSameAs(original);
    }

    @Test
    void createConnectionWrapsRuntimeExceptionFromStore() {
        Map<String, String> options = Collections.singletonMap("password", "p@ss");

        RecordingSecretStore secretStore =
                new RecordingSecretStore() {
                    @Override
                    public String storeSecret(Map<String, String> secretData) {
                        throw new IllegalStateException("kaboom");
                    }
                };

        assertThatThrownBy(
                        () ->
                                factory.createConnection(
                                        SensitiveConnection.of(options, null), secretStore))
                .isInstanceOf(SecretException.class)
                .hasMessageContaining("Failed to store connection secret")
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    // ---------------------------------------------------------------------------------------------
    // resolveConnection
    // ---------------------------------------------------------------------------------------------

    @Test
    void resolveConnectionWithoutSecretReferenceReturnsOptionsUnchanged() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("host", "localhost");
        options.put("port", "9092");

        RecordingSecretStore secretStore = new RecordingSecretStore();
        SensitiveConnection resolved =
                factory.resolveConnection(
                        CatalogConnection.of(options, "the-comment"), secretStore);

        assertThat(secretStore.retrieved).isEmpty();
        assertThat(resolved.getOptions()).containsExactlyInAnyOrderEntriesOf(options);
        assertThat(resolved.getComment()).isEqualTo("the-comment");
    }

    @Test
    void resolveConnectionMergesSecretsBackIntoOptions() {
        RecordingSecretStore secretStore = new RecordingSecretStore();
        Map<String, String> secrets = new HashMap<>();
        secrets.put("password", "p@ss");
        secrets.put("token", "tok");
        String secretId = secretStore.storeSecret(secrets);

        Map<String, String> catalogOptions = new LinkedHashMap<>();
        catalogOptions.put("host", "localhost");
        catalogOptions.put(DefaultConnectionFactory.SECRET_REFERENCE_KEY, secretId);

        SensitiveConnection resolved =
                factory.resolveConnection(CatalogConnection.of(catalogOptions, null), secretStore);

        assertThat(secretStore.retrieved).containsExactly(secretId);
        assertThat(resolved.getOptions())
                .containsOnlyKeys("host", "password", "token")
                .containsEntry("host", "localhost")
                .containsEntry("password", "p@ss")
                .containsEntry("token", "tok")
                .doesNotContainKey(DefaultConnectionFactory.SECRET_REFERENCE_KEY);
    }

    @Test
    void resolveConnectionTranslatesSecretNotFoundToValidationException() {
        // Empty stored map → fake's getSecret will throw SecretNotFoundException by default.
        RecordingSecretStore secretStore = new RecordingSecretStore();

        Map<String, String> catalogOptions =
                Collections.singletonMap(DefaultConnectionFactory.SECRET_REFERENCE_KEY, "missing");

        assertThatThrownBy(
                        () ->
                                factory.resolveConnection(
                                        CatalogConnection.of(catalogOptions, null), secretStore))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'missing'")
                .hasCauseInstanceOf(SecretNotFoundException.class);
    }

    @Test
    void resolveConnectionWrapsRuntimeExceptionFromStore() {
        RecordingSecretStore secretStore =
                new RecordingSecretStore() {
                    @Override
                    public Map<String, String> getSecret(String secretId) {
                        throw new IllegalStateException("transport down");
                    }
                };

        Map<String, String> catalogOptions =
                Collections.singletonMap(DefaultConnectionFactory.SECRET_REFERENCE_KEY, "abc");

        assertThatThrownBy(
                        () ->
                                factory.resolveConnection(
                                        CatalogConnection.of(catalogOptions, null), secretStore))
                .isInstanceOf(SecretException.class)
                .hasMessageContaining("'abc'")
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    // ---------------------------------------------------------------------------------------------
    // deleteSecrets
    // ---------------------------------------------------------------------------------------------

    @Test
    void deleteSecretsWithoutReferenceIsNoOp() {
        Map<String, String> catalogOptions = Collections.singletonMap("host", "localhost");

        RecordingSecretStore secretStore = new RecordingSecretStore();
        factory.deleteSecrets(CatalogConnection.of(catalogOptions, null), secretStore);

        assertThat(secretStore.removed).isEmpty();
    }

    @Test
    void deleteSecretsRemovesByReference() {
        RecordingSecretStore secretStore = new RecordingSecretStore();
        String secretId = secretStore.storeSecret(Collections.singletonMap("password", "p@ss"));

        Map<String, String> catalogOptions = new LinkedHashMap<>();
        catalogOptions.put("host", "localhost");
        catalogOptions.put(DefaultConnectionFactory.SECRET_REFERENCE_KEY, secretId);

        factory.deleteSecrets(CatalogConnection.of(catalogOptions, null), secretStore);

        assertThat(secretStore.removed).containsExactly(secretId);
        assertThat(secretStore.stored).doesNotContainKey(secretId);
    }

    @Test
    void deleteSecretsWrapsRuntimeExceptionFromStore() {
        RecordingSecretStore secretStore =
                new RecordingSecretStore() {
                    @Override
                    public void removeSecret(String secretId) {
                        throw new IllegalStateException("transport down");
                    }
                };

        Map<String, String> catalogOptions =
                Collections.singletonMap(DefaultConnectionFactory.SECRET_REFERENCE_KEY, "abc");

        assertThatThrownBy(
                        () ->
                                factory.deleteSecrets(
                                        CatalogConnection.of(catalogOptions, null), secretStore))
                .isInstanceOf(SecretException.class)
                .hasMessageContaining("'abc'")
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    // ---------------------------------------------------------------------------------------------
    // round-trip
    // ---------------------------------------------------------------------------------------------

    @Test
    void createAndResolveRoundTripPreservesOptionsAndComment() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("host", "localhost");
        options.put("port", "9092");
        options.put("password", "p@ss");
        options.put("token", "tok");

        RecordingSecretStore secretStore = new RecordingSecretStore();
        CatalogConnection persisted =
                factory.createConnection(
                        SensitiveConnection.of(options, "round-trip"), secretStore);
        SensitiveConnection resolved = factory.resolveConnection(persisted, secretStore);

        assertThat(resolved.getOptions()).containsExactlyInAnyOrderEntriesOf(options);
        assertThat(resolved.getComment()).isEqualTo("round-trip");
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    /**
     * Fake secret store that records all interactions; tests use the maps directly to verify how
     * {@link DefaultConnectionFactory} invokes the store.
     */
    private static class RecordingSecretStore implements WritableSecretStore, ReadableSecretStore {

        final Map<String, Map<String, String>> stored = new LinkedHashMap<>();
        final List<String> retrieved = new java.util.ArrayList<>();
        final List<String> removed = new java.util.ArrayList<>();

        @Override
        public String storeSecret(Map<String, String> secretData) {
            String id = UUID.randomUUID().toString();
            stored.put(id, new LinkedHashMap<>(secretData));
            return id;
        }

        @Override
        public void removeSecret(String secretId) {
            removed.add(secretId);
            stored.remove(secretId);
        }

        @Override
        public void updateSecret(String secretId, Map<String, String> newSecretData)
                throws SecretNotFoundException {
            if (!stored.containsKey(secretId)) {
                throw new SecretNotFoundException("Secret '" + secretId + "' not found.");
            }
            stored.put(secretId, new LinkedHashMap<>(newSecretData));
        }

        @Override
        public Map<String, String> getSecret(String secretId) throws SecretNotFoundException {
            retrieved.add(secretId);
            Map<String, String> result = stored.get(secretId);
            if (result == null) {
                throw new SecretNotFoundException("Secret '" + secretId + "' not found.");
            }
            return result;
        }
    }
}
