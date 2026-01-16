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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.secret.exceptions.SecretException;

import java.util.Collections;
import java.util.Set;

/**
 * Factory for creating {@link GenericInMemorySecretStore} instances.
 *
 * <p>This factory creates in-memory secret stores that are suitable for testing and development
 * purposes. Secrets are stored in plaintext JSON format in memory and are not persisted.
 */
@Internal
public class GenericInMemorySecretStoreFactory implements SecretStoreFactory {

    private GenericInMemorySecretStore secretStore;
    public static final String IDENTIFIER = "generic_in_memory";

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
    public SecretStore createSecretStore() {
        if (secretStore == null) {
            throw new IllegalStateException(
                    "SecretStoreFactory must be opened before creating a SecretStore");
        }
        return secretStore;
    }

    @Override
    public void open(Context context) throws SecretException {
        this.secretStore = new GenericInMemorySecretStore();
    }

    @Override
    public void close() throws CatalogException {
        if (secretStore != null) {
            secretStore.clear();
            secretStore = null;
        }
    }
}
