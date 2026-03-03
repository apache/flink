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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.secret.exceptions.SecretNotFoundException;

import java.util.Map;

/**
 * Interface for storing, updating, and removing secrets in a secret store.
 *
 * <p>This interface provides write operations for managing secrets, including adding new secrets,
 * updating existing ones, and removing secrets that are no longer needed.
 *
 * <p>Implementations should ensure that secret operations are performed securely and, where
 * applicable, atomically.
 */
@PublicEvolving
public interface WritableSecretStore extends SecretStore {

    /**
     * Stores a new secret in the secret store.
     *
     * @param secretData a map containing the secret data as key-value pairs to be stored
     * @return a unique identifier for the stored secret
     */
    String storeSecret(Map<String, String> secretData);

    /**
     * Removes a secret from the secret store.
     *
     * @param secretId the unique identifier of the secret to remove
     */
    void removeSecret(String secretId);

    /**
     * Atomically updates an existing secret with new data.
     *
     * <p>This operation replaces the entire secret data with the provided new data.
     *
     * @param secretId the unique identifier of the secret to update
     * @param newSecretData a map containing the new secret data as key-value pairs
     * @throws SecretNotFoundException if the secret with the given identifier does not exist
     */
    void updateSecret(String secretId, Map<String, String> newSecretData)
            throws SecretNotFoundException;
}
