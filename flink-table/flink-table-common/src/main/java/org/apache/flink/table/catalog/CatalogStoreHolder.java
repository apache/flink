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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.CatalogStoreFactory;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A holder for a {@link CatalogStore} instance and the necessary information for creating and
 * initializing {@link Catalog} instances, including a {@link CatalogStoreFactory}, a {@link
 * ReadableConfig} instance, and a {@link ClassLoader} instance. This class provides automatic
 * resource management using the {@link AutoCloseable} interface, ensuring that the catalog-related
 * resources are properly closed and released when they are no longer needed.
 *
 * <p>A {@link CatalogStoreFactory} may create multiple {@link CatalogStore} instances, which can be
 * useful in SQL gateway scenarios where different sessions may use different catalog stores.
 * However, in some scenarios, a single {@link CatalogStore} instance may be sufficient, in which
 * case the {@link CatalogStoreFactory} can be stored in the holder to ensure that it is properly
 * closed when the {@link CatalogStore} is closed.
 */
@Internal
public class CatalogStoreHolder implements AutoCloseable {

    private CatalogStore catalogStore;

    private @Nullable CatalogStoreFactory factory;

    private ReadableConfig config;

    private ClassLoader classLoader;

    private CatalogStoreHolder(
            CatalogStore catalogStore,
            @Nullable CatalogStoreFactory factory,
            ReadableConfig config,
            ClassLoader classLoader) {
        this.catalogStore = catalogStore;
        this.factory = factory;
        this.config = config;
        this.classLoader = classLoader;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for a fluent definition of a {@link CatalogStoreHolder}. */
    @Internal
    public static final class Builder {

        private CatalogStore catalogStore;

        private ReadableConfig config;

        private @Nullable ClassLoader classLoader;

        private @Nullable CatalogStoreFactory factory;

        public Builder catalogStore(CatalogStore catalogStore) {
            this.catalogStore = catalogStore;
            return this;
        }

        public Builder config(ReadableConfig config) {
            this.config = config;
            return this;
        }

        public Builder classloader(ClassLoader classLoader) {
            this.classLoader = classLoader;
            return this;
        }

        public Builder factory(CatalogStoreFactory factory) {
            this.factory = factory;
            return this;
        }

        public CatalogStoreHolder build() {
            checkNotNull(catalogStore, "CatalogStore cannot be null");
            checkNotNull(config, "Config cannot be null");
            checkNotNull(classLoader, "Class loader cannot be null");
            CatalogStoreHolder catalogStoreHolder =
                    new CatalogStoreHolder(catalogStore, factory, config, classLoader);
            catalogStoreHolder.open();
            return catalogStoreHolder;
        }
    }

    public CatalogStore catalogStore() {
        return this.catalogStore;
    }

    public ReadableConfig config() {
        return this.config;
    }

    public ClassLoader classLoader() {
        return this.classLoader;
    }

    public void open() {
        this.catalogStore.open();
    }

    @Override
    public void close() throws Exception {
        this.catalogStore.close();

        if (this.factory != null) {
            this.factory.close();
        }
    }
}
