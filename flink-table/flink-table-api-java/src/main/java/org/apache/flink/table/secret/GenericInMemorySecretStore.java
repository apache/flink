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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.secret.exceptions.SecretNotFoundException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A generic in-memory implementation of both {@link ReadableSecretStore} and {@link
 * WritableSecretStore}.
 *
 * <p>This implementation stores secrets in memory as immutable Map objects. It is suitable for
 * testing and development purposes but should not be used in production environments as secrets are
 * not encrypted.
 */
@Internal
public class GenericInMemorySecretStore implements ReadableSecretStore, WritableSecretStore {

    private final Map<String, Map<String, String>> secrets;

    public GenericInMemorySecretStore() {
        this.secrets = new HashMap<>();
    }

    @Override
    public Map<String, String> getSecret(String secretId) throws SecretNotFoundException {
        checkNotNull(secretId, "Secret ID cannot be null");

        Map<String, String> secretData = secrets.get(secretId);
        if (secretData == null) {
            throw new SecretNotFoundException(
                    String.format("Secret with ID '%s' not found", secretId));
        }

        return secretData;
    }

    @Override
    public String storeSecret(Map<String, String> secretData) {
        checkNotNull(secretData, "Secret data cannot be null");

        String secretId = UUID.randomUUID().toString();
        secrets.put(secretId, Collections.unmodifiableMap(new HashMap<>(secretData)));
        return secretId;
    }

    @Override
    public void removeSecret(String secretId) {
        checkNotNull(secretId, "Secret ID cannot be null");
        secrets.remove(secretId);
    }

    @Override
    public void updateSecret(String secretId, Map<String, String> newSecretData)
            throws SecretNotFoundException {
        checkNotNull(secretId, "Secret ID cannot be null");
        checkNotNull(newSecretData, "New secret data cannot be null");

        if (!secrets.containsKey(secretId)) {
            throw new SecretNotFoundException(
                    String.format("Secret with ID '%s' not found", secretId));
        }

        secrets.put(secretId, Collections.unmodifiableMap(new HashMap<>(newSecretData)));
    }

    /** Clears all secrets from the store (for testing purposes). */
    void clear() {
        secrets.clear();
    }
}
