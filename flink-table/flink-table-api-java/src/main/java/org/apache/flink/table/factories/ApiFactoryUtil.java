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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.secret.CommonSecretOptions;
import org.apache.flink.table.secret.SecretStoreFactory;

import java.util.Map;

/** Utility for dealing with catalog store factories. */
@Internal
public class ApiFactoryUtil {

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

    public static SecretStoreFactory findAndCreateSecretStoreFactory(
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
    public static SecretStoreFactory.Context buildSecretStoreFactoryContext(
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
