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

package org.apache.flink.table.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.CatalogStoreHolder;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;

import javax.annotation.Nullable;

/** Mock implementations of {@link CatalogManager} for testing purposes. */
public final class CatalogManagerMocks {

    public static final String DEFAULT_CATALOG =
            TableConfigOptions.TABLE_CATALOG_NAME.defaultValue();

    public static final String DEFAULT_DATABASE =
            TableConfigOptions.TABLE_DATABASE_NAME.defaultValue();

    public static CatalogManager createEmptyCatalogManager() {
        return createCatalogManager(null, null);
    }

    public static CatalogManager createCatalogManager(CatalogStore catalogStore) {
        return createCatalogManager(
                CatalogStoreHolder.newBuilder()
                        .catalogStore(catalogStore)
                        .classloader(CatalogManagerMocks.class.getClassLoader())
                        .config(new Configuration())
                        .build());
    }

    public static CatalogManager createCatalogManager(
            @Nullable CatalogStoreHolder catalogStoreHolder) {
        return createCatalogManager(null, catalogStoreHolder);
    }

    public static CatalogManager createCatalogManager(@Nullable Catalog catalog) {
        return createCatalogManager(catalog, null);
    }

    public static CatalogManager createCatalogManager(
            @Nullable Catalog catalog, @Nullable CatalogStoreHolder catalogStoreHolder) {
        final CatalogManager.Builder builder = preparedCatalogManager();
        if (catalog != null) {
            builder.defaultCatalog(DEFAULT_CATALOG, catalog);
        }
        if (catalogStoreHolder != null) {
            builder.catalogStoreHolder(catalogStoreHolder);
        }
        final CatalogManager catalogManager = builder.build();
        catalogManager.initSchemaResolver(true, ExpressionResolverMocks.dummyResolver());
        return catalogManager;
    }

    public static CatalogManager.Builder preparedCatalogManager() {
        return CatalogManager.newBuilder()
                .classLoader(CatalogManagerMocks.class.getClassLoader())
                .config(new Configuration())
                .defaultCatalog(DEFAULT_CATALOG, createEmptyCatalog())
                .catalogStoreHolder(
                        CatalogStoreHolder.newBuilder()
                                .classloader(CatalogManagerMocks.class.getClassLoader())
                                .config(new Configuration())
                                .catalogStore(new GenericInMemoryCatalogStore())
                                .build())
                .executionConfig(new ExecutionConfig());
    }

    public static Catalog createEmptyCatalog() {
        return new GenericInMemoryCatalog(DEFAULT_CATALOG, DEFAULT_DATABASE);
    }

    private CatalogManagerMocks() {
        // no instantiation
    }
}
