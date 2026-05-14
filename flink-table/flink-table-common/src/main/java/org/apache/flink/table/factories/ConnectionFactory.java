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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.CatalogConnection;
import org.apache.flink.table.catalog.SensitiveConnection;
import org.apache.flink.table.secret.ReadableSecretStore;
import org.apache.flink.table.secret.WritableSecretStore;
import org.apache.flink.table.secret.exceptions.SecretException;

/**
 * Factory for creating and resolving connections, handling the encryption and decryption of
 * sensitive connection fields.
 *
 * <p>A {@code ConnectionFactory} is responsible for:
 *
 * <ul>
 *   <li>Extracting sensitive fields from a {@link SensitiveConnection} and storing them in a {@link
 *       WritableSecretStore}, returning a {@link CatalogConnection} that is safe to persist in a
 *       catalog.
 *   <li>Resolving a {@link CatalogConnection} from a catalog by retrieving its secrets from a
 *       {@link ReadableSecretStore} and returning a complete {@link SensitiveConnection}.
 * </ul>
 *
 * @see org.apache.flink.table.factories.DefaultConnectionFactory
 */
@PublicEvolving
public interface ConnectionFactory extends Factory {

    /**
     * The option key that identifies the connection type in a {@link SensitiveConnection}'s or
     * {@link CatalogConnection}'s options. The value of this option is matched against the {@link
     * #factoryIdentifier()} of registered {@code ConnectionFactory} implementations to discover the
     * factory that handles a given connection (see FLIP-529).
     */
    String CONNECTION_TYPE_KEY = "type";

    /**
     * Creates a {@link CatalogConnection} from a {@link SensitiveConnection} by extracting
     * sensitive fields and storing them in the provided {@link WritableSecretStore}.
     *
     * <p>The returned {@link CatalogConnection} contains only non-sensitive options plus a secret
     * reference that can be used to retrieve the sensitive fields later via {@link
     * #resolveConnection(CatalogConnection, ReadableSecretStore)}.
     *
     * @param connection the connection with all options including sensitive fields
     * @param secretStore the secret store where sensitive fields will be stored
     * @return a catalog-safe connection with sensitive fields replaced by a secret reference
     * @throws SecretException if storing the secret fails (e.g. underlying-store error)
     */
    CatalogConnection createConnection(
            SensitiveConnection connection, WritableSecretStore secretStore) throws SecretException;

    /**
     * Resolves a {@link CatalogConnection} into a {@link SensitiveConnection} by retrieving secrets
     * from the provided {@link ReadableSecretStore}.
     *
     * @param connection the catalog connection containing non-sensitive options and a secret
     *     reference
     * @param secretStore the secret store from which sensitive fields are retrieved
     * @return the complete connection with all options including sensitive fields
     * @throws SecretException if retrieving the secret fails (e.g. underlying-store error)
     */
    SensitiveConnection resolveConnection(
            CatalogConnection connection, ReadableSecretStore secretStore) throws SecretException;

    /**
     * Deletes any secrets associated with the given {@link CatalogConnection} from the provided
     * {@link WritableSecretStore}.
     *
     * <p>Implementations should locate the secret reference embedded in the connection (created by
     * {@link #createConnection(SensitiveConnection, WritableSecretStore)}) and remove the
     * corresponding entry from the secret store. This is intended to be called when a connection is
     * dropped or replaced (e.g. on alter), to avoid orphaned secrets.
     *
     * <p>The default implementation is a no-op for factories that do not externalize secrets.
     *
     * @param connection the catalog connection whose backing secrets should be removed
     * @param secretStore the secret store from which secrets should be deleted
     * @throws SecretException if removing the secret fails (e.g. underlying-store error)
     */
    default void deleteSecrets(CatalogConnection connection, WritableSecretStore secretStore)
            throws SecretException {}
}
