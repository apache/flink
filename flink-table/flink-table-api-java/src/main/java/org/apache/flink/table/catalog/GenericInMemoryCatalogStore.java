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
import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

/** A generic catalog store implementation that store all catalog configuration in memory. */
@Internal
public class GenericInMemoryCatalogStore extends AbstractCatalogStore {

    private final Map<String, CatalogDescriptor> descriptors;

    public GenericInMemoryCatalogStore() {
        descriptors = new HashMap<>();
    }

    @Override
    public void storeCatalog(String catalogName, CatalogDescriptor catalog)
            throws CatalogException {
        checkOpenState();
        if (descriptors.containsKey(catalogName)) {
            throw new CatalogException(
                    format("Catalog %s already exists in the catalog store.", catalogName));
        }
        descriptors.put(catalogName, catalog);
    }

    @Override
    public void removeCatalog(String catalogName, boolean ignoreIfNotExists)
            throws CatalogException {
        checkOpenState();
        if (descriptors.containsKey(catalogName)) {
            descriptors.remove(catalogName);
        } else if (!ignoreIfNotExists) {
            throw new CatalogException(
                    format("Catalog %s does not exist in the catalog store.", catalogName));
        }
    }

    @Override
    public Optional<CatalogDescriptor> getCatalog(String catalogName) {
        checkOpenState();
        return Optional.ofNullable(descriptors.get(catalogName));
    }

    @Override
    public Set<String> listCatalogs() {
        checkOpenState();
        return descriptors.keySet();
    }

    @Override
    public boolean contains(String catalogName) {
        checkOpenState();
        return descriptors.containsKey(catalogName);
    }
}
