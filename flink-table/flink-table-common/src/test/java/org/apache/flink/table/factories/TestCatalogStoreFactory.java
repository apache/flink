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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.AbstractCatalogStore;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This class is an implementation of {@link CatalogStoreFactory} for testing purposes. All
 * instances of {@link CatalogStore} created by the same {@link CatalogStoreFactory} will share the
 * saved {@link CatalogDescriptor}s.
 */
public class TestCatalogStoreFactory implements CatalogStoreFactory {

    public static final String IDENTIFIER = "test-catalog-store";

    public static final Map<String, CatalogDescriptor> SHARED_DESCRIPTORS = new HashMap<>();

    @Override
    public CatalogStore createCatalogStore() {
        return new TestCatalogStore(SHARED_DESCRIPTORS);
    }

    @Override
    public void open(Context context) throws CatalogException {}

    @Override
    public void close() throws CatalogException {}

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

    /**
     * This class is a test catalog store that shares {@link CatalogDescriptor}s with other catalog
     * stores.
     */
    public static class TestCatalogStore extends AbstractCatalogStore {
        private final Map<String, CatalogDescriptor> sharedDescriptors;

        public TestCatalogStore(Map<String, CatalogDescriptor> sharedDescriptors) {
            this.sharedDescriptors = sharedDescriptors;
        }

        @Override
        public void storeCatalog(String catalogName, CatalogDescriptor catalog)
                throws CatalogException {
            sharedDescriptors.put(catalogName, catalog);
        }

        @Override
        public void removeCatalog(String catalogName, boolean ignoreIfNotExists)
                throws CatalogException {
            sharedDescriptors.remove(catalogName);
        }

        @Override
        public Optional<CatalogDescriptor> getCatalog(String catalogName) {
            return Optional.ofNullable(sharedDescriptors.get(catalogName));
        }

        @Override
        public Set<String> listCatalogs() {
            return sharedDescriptors.keySet();
        }

        @Override
        public boolean contains(String catalogName) {
            return sharedDescriptors.containsKey(catalogName);
        }
    }
}
