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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.secret.CommonSecretOptions;
import org.apache.flink.table.secret.SecretStore;
import org.apache.flink.table.secret.SecretStoreFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/** Utility for dealing with catalog store factories. */
@Internal
public class ApiFactoryUtil {

    /** Result holder for catalog store and factory. */
    @Internal
    public static class CatalogStoreResult {
        private final CatalogStore catalogStore;
        @Nullable private final CatalogStoreFactory catalogStoreFactory;

        public CatalogStoreResult(
                CatalogStore catalogStore, @Nullable CatalogStoreFactory catalogStoreFactory) {
            this.catalogStore = catalogStore;
            this.catalogStoreFactory = catalogStoreFactory;
        }

        public CatalogStore getCatalogStore() {
            return catalogStore;
        }

        @Nullable
        public CatalogStoreFactory getCatalogStoreFactory() {
            return catalogStoreFactory;
        }
    }

    /**
     * Gets or creates a {@link CatalogStore}. If a catalog store is provided in settings, it will
     * be used directly. Otherwise, a new catalog store will be created using the factory.
     *
     * @param providedCatalogStore the catalog store from settings, if present
     * @param configuration the configuration
     * @param classLoader the user classloader
     * @return a result containing the catalog store and factory (factory is null if store was
     *     provided)
     */
    public static CatalogStoreResult getOrCreateCatalogStore(
            Optional<CatalogStore> providedCatalogStore,
            Configuration configuration,
            ClassLoader classLoader) {
        if (providedCatalogStore.isPresent()) {
            return new CatalogStoreResult(providedCatalogStore.get(), null);
        } else {
            CatalogStoreFactory catalogStoreFactory =
                    findAndCreateCatalogStoreFactory(configuration, classLoader);
            CatalogStoreFactory.Context catalogStoreFactoryContext =
                    buildCatalogStoreFactoryContext(configuration, classLoader);
            catalogStoreFactory.open(catalogStoreFactoryContext);
            CatalogStore catalogStore = catalogStoreFactory.createCatalogStore();
            return new CatalogStoreResult(catalogStore, catalogStoreFactory);
        }
    }

    /**
     * Finds and creates a {@link CatalogStoreFactory} using the provided {@link Configuration} and
     * user classloader.
     *
     * <p>The configuration format should be as follows:
     *
     * <pre>{@code
     * table.catalog-store.kind: {identifier}
     * table.catalog-store.{identifier}.{param1}: xxx
     * table.catalog-store.{identifier}.{param2}: xxx
     * }</pre>
     */
    public static CatalogStoreFactory findAndCreateCatalogStoreFactory(
            Configuration configuration, ClassLoader classLoader) {
        String identifier = configuration.get(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND);

        CatalogStoreFactory catalogStoreFactory =
                FactoryUtil.discoverFactory(classLoader, CatalogStoreFactory.class, identifier);

        return catalogStoreFactory;
    }

    /**
     * Build a {@link CatalogStoreFactory.Context} for opening the {@link CatalogStoreFactory}.
     *
     * <p>The configuration format should be as follows:
     *
     * <pre>{@code
     * table.catalog-store.kind: {identifier}
     * table.catalog-store.{identifier}.{param1}: xxx
     * table.catalog-store.{identifier}.{param2}: xxx
     * }</pre>
     */
    public static CatalogStoreFactory.Context buildCatalogStoreFactoryContext(
            Configuration configuration, ClassLoader classLoader) {
        String identifier = configuration.get(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND);
        String catalogStoreOptionPrefix =
                CommonCatalogOptions.TABLE_CATALOG_STORE_OPTION_PREFIX + identifier + ".";
        Map<String, String> options =
                new DelegatingConfiguration(configuration, catalogStoreOptionPrefix).toMap();
        CatalogStoreFactory.Context context =
                new FactoryUtil.DefaultCatalogStoreContext(options, configuration, classLoader);

        return context;
    }

    /** Result holder for secret store and factory. */
    @Internal
    public static class SecretStoreResult {
        private final SecretStore secretStore;
        @Nullable private final SecretStoreFactory secretStoreFactory;

        public SecretStoreResult(
                SecretStore secretStore, @Nullable SecretStoreFactory secretStoreFactory) {
            this.secretStore = secretStore;
            this.secretStoreFactory = secretStoreFactory;
        }

        public SecretStore getSecretStore() {
            return secretStore;
        }

        @Nullable
        public SecretStoreFactory getSecretStoreFactory() {
            return secretStoreFactory;
        }
    }

    /**
     * Gets or creates a {@link SecretStore}. If a secret store is provided in settings, it will be
     * used directly. Otherwise, a new secret store will be created using the factory.
     *
     * @param providedSecretStore the secret store from settings, if present
     * @param configuration the configuration
     * @param classLoader the user classloader
     * @return a result containing the secret store and factory (factory is null if store was
     *     provided)
     */
    public static SecretStoreResult getOrCreateSecretStore(
            Optional<SecretStore> providedSecretStore,
            Configuration configuration,
            ClassLoader classLoader) {
        if (providedSecretStore.isPresent()) {
            return new SecretStoreResult(providedSecretStore.get(), null);
        } else {
            SecretStoreFactory secretStoreFactory =
                    findAndCreateSecretStoreFactory(configuration, classLoader);
            SecretStoreFactory.Context secretStoreFactoryContext =
                    buildSecretStoreFactoryContext(configuration, classLoader);
            secretStoreFactory.open(secretStoreFactoryContext);
            SecretStore secretStore = secretStoreFactory.createSecretStore();
            return new SecretStoreResult(secretStore, secretStoreFactory);
        }
    }

    /**
     * Finds and creates a {@link SecretStoreFactory} using the provided {@link Configuration} and
     * user classloader.
     *
     * <p>The configuration format should be as follows:
     *
     * <pre>{@code
     * table.secret-store.kind: {identifier}
     * table.secret-store.{identifier}.{param1}: xxx
     * table.secret-store.{identifier}.{param2}: xxx
     * }</pre>
     */
    @VisibleForTesting
    static SecretStoreFactory findAndCreateSecretStoreFactory(
            Configuration configuration, ClassLoader classLoader) {
        String identifier = configuration.get(CommonSecretOptions.TABLE_SECRET_STORE_KIND);

        SecretStoreFactory secretStoreFactory =
                FactoryUtil.discoverFactory(classLoader, SecretStoreFactory.class, identifier);

        return secretStoreFactory;
    }

    /**
     * Build a {@link SecretStoreFactory.Context} for opening the {@link SecretStoreFactory}.
     *
     * <p>The configuration format should be as follows:
     *
     * <pre>{@code
     * table.secret-store.kind: {identifier}
     * table.secret-store.{identifier}.{param1}: xxx
     * table.secret-store.{identifier}.{param2}: xxx
     * }</pre>
     */
    @VisibleForTesting
    static SecretStoreFactory.Context buildSecretStoreFactoryContext(
            Configuration configuration, ClassLoader classLoader) {
        String identifier = configuration.get(CommonSecretOptions.TABLE_SECRET_STORE_KIND);
        String secretStoreOptionPrefix =
                CommonSecretOptions.TABLE_SECRET_STORE_OPTION_PREFIX + identifier + ".";
        Map<String, String> options =
                new DelegatingConfiguration(configuration, secretStoreOptionPrefix).toMap();
        SecretStoreFactory.Context context =
                new FactoryUtil.DefaultSecretStoreContext(options, configuration, classLoader);

        return context;
    }
}
