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

package org.apache.flink.table.secret;

import org.apache.flink.table.secret.exceptions.SecretNotFoundException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Test for {@link GenericInMemorySecretStore}. */
public class GenericInMemorySecretStoreTest {

    private GenericInMemorySecretStore secretStore;

    @BeforeEach
    void setUp() {
        secretStore = new GenericInMemorySecretStore();
    }

    @Test
    void testStoreAndGetSecret() throws SecretNotFoundException {
        Map<String, String> secretData = new HashMap<>();
        secretData.put("username", "testuser");
        secretData.put("password", "testpass");

        String secretId = secretStore.storeSecret(secretData);
        assertThat(secretId).isNotNull();

        Map<String, String> retrievedSecret = secretStore.getSecret(secretId);
        assertThat(retrievedSecret).isNotNull();
        assertThat(retrievedSecret.get("username")).isEqualTo("testuser");
        assertThat(retrievedSecret.get("password")).isEqualTo("testpass");
    }

    @Test
    void testGetNonExistentSecret() {
        assertThatThrownBy(() -> secretStore.getSecret("non-existent-id"))
                .isInstanceOf(SecretNotFoundException.class)
                .hasMessageContaining("Secret with ID 'non-existent-id' not found");
    }

    @Test
    void testGetSecretWithNullId() {
        assertThatThrownBy(() -> secretStore.getSecret(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Secret ID cannot be null");
    }

    @Test
    void testStoreSecretWithNullData() {
        assertThatThrownBy(() -> secretStore.storeSecret(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Secret data cannot be null");
    }

    @Test
    void testRemoveSecret() throws SecretNotFoundException {
        Map<String, String> secretData = new HashMap<>();
        secretData.put("key", "value");

        String secretId = secretStore.storeSecret(secretData);
        assertThat(secretStore.getSecret(secretId)).isNotNull();

        secretStore.removeSecret(secretId);
        assertThatThrownBy(() -> secretStore.getSecret(secretId))
                .isInstanceOf(SecretNotFoundException.class)
                .hasMessageContaining("Secret with ID '" + secretId + "' not found");
    }

    @Test
    void testRemoveSecretWithNullId() {
        assertThatThrownBy(() -> secretStore.removeSecret(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Secret ID cannot be null");
    }

    @Test
    void testRemoveNonExistentSecret() {
        // Should not throw exception, just silently remove nothing
        secretStore.removeSecret("non-existent-id");
    }

    @Test
    void testUpdateSecret() throws SecretNotFoundException {
        Map<String, String> originalData = new HashMap<>();
        originalData.put("username", "olduser");
        originalData.put("password", "oldpass");

        String secretId = secretStore.storeSecret(originalData);

        Map<String, String> updatedData = new HashMap<>();
        updatedData.put("username", "newuser");
        updatedData.put("password", "newpass");

        secretStore.updateSecret(secretId, updatedData);

        Map<String, String> retrievedSecret = secretStore.getSecret(secretId);
        assertThat(retrievedSecret.get("username")).isEqualTo("newuser");
        assertThat(retrievedSecret.get("password")).isEqualTo("newpass");
    }

    @Test
    void testUpdateNonExistentSecret() {
        Map<String, String> secretData = new HashMap<>();
        secretData.put("key", "value");

        assertThatThrownBy(() -> secretStore.updateSecret("non-existent-id", secretData))
                .isInstanceOf(SecretNotFoundException.class)
                .hasMessageContaining("Secret with ID 'non-existent-id' not found");
    }

    @Test
    void testUpdateSecretWithNullId() {
        Map<String, String> secretData = new HashMap<>();
        secretData.put("key", "value");

        assertThatThrownBy(() -> secretStore.updateSecret(null, secretData))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Secret ID cannot be null");
    }

    @Test
    void testUpdateSecretWithNullData() {
        Map<String, String> originalData = new HashMap<>();
        originalData.put("key", "value");
        String secretId = secretStore.storeSecret(originalData);

        assertThatThrownBy(() -> secretStore.updateSecret(secretId, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("New secret data cannot be null");
    }

    @Test
    void testClear() {
        Map<String, String> secretData1 = new HashMap<>();
        secretData1.put("key1", "value1");
        String secretId1 = secretStore.storeSecret(secretData1);

        Map<String, String> secretData2 = new HashMap<>();
        secretData2.put("key2", "value2");
        String secretId2 = secretStore.storeSecret(secretData2);

        secretStore.clear();

        assertThatThrownBy(() -> secretStore.getSecret(secretId1))
                .isInstanceOf(SecretNotFoundException.class);
        assertThatThrownBy(() -> secretStore.getSecret(secretId2))
                .isInstanceOf(SecretNotFoundException.class);
    }

    @Test
    void testStoreEmptySecret() throws SecretNotFoundException {
        Map<String, String> emptyData = new HashMap<>();
        String secretId = secretStore.storeSecret(emptyData);

        Map<String, String> retrievedSecret = secretStore.getSecret(secretId);
        assertThat(retrievedSecret).isNotNull();
        assertThat(retrievedSecret.isEmpty()).isTrue();
    }

    @Test
    void testStoreMultipleSecrets() throws SecretNotFoundException {
        Map<String, String> secret1 = new HashMap<>();
        secret1.put("user1", "pass1");

        Map<String, String> secret2 = new HashMap<>();
        secret2.put("user2", "pass2");

        String secretId1 = secretStore.storeSecret(secret1);
        String secretId2 = secretStore.storeSecret(secret2);

        assertThat(secretId1).isNotEqualTo(secretId2);

        Map<String, String> retrieved1 = secretStore.getSecret(secretId1);
        Map<String, String> retrieved2 = secretStore.getSecret(secretId2);

        assertThat(retrieved1.get("user1")).isEqualTo("pass1");
        assertThat(retrieved2.get("user2")).isEqualTo("pass2");
    }
}
