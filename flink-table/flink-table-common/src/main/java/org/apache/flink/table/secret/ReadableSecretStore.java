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
 * Interface for retrieving secrets from a secret store.
 *
 * <p>This interface enables read-only access to stored secrets, allowing applications to retrieve
 * sensitive configuration data such as credentials, API tokens, and passwords.
 *
 * <p>Implementations of this interface should ensure secure retrieval and handling of secret data.
 */
@PublicEvolving
public interface ReadableSecretStore extends SecretStore {

    /**
     * Retrieves a secret from the store by its identifier.
     *
     * @param secretId the unique identifier of the secret to retrieve
     * @return a map containing the secret data as key-value pairs
     * @throws SecretNotFoundException if the secret with the given identifier does not exist
     */
    Map<String, String> getSecret(String secretId) throws SecretNotFoundException;
}
