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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.util.Optional;
import java.util.Set;

/**
 * The {@link CatalogStore} is used in {@link CatalogManager} to retrieve, save and remove {@link
 * CatalogDescriptor} at the external storage system.
 */
@PublicEvolving
public interface CatalogStore {

    /**
     * Store a catalog under the given catalog name. The catalog name must be unique.
     *
     * @param catalogName the given catalog name under which to store the given catalog
     * @param catalog catalog descriptor to store
     * @throws CatalogException throw when registration failed
     */
    void storeCatalog(String catalogName, CatalogDescriptor catalog) throws CatalogException;

    /**
     * Remove a catalog with the given catalog name. The catalog name must be existed.
     *
     * @param catalogName the given catalog name under which to remove the given catalog
     * @param ignoreIfNotExists whether throw an exception when the catalog does not exist
     * @throws CatalogException throw when the removal operation failed
     */
    void removeCatalog(String catalogName, boolean ignoreIfNotExists) throws CatalogException;

    /**
     * Get a catalog by name.
     *
     * @param catalogName name of the catalog to retrieve
     * @return the requested catalog or empty if the catalog does not exist
     */
    Optional<CatalogDescriptor> getCatalog(String catalogName);

    /**
     * Retrieves the names of all registered catalogs.
     *
     * @return the names of registered catalogs
     */
    Set<String> listCatalogs();

    /** Return whether the catalog exists in the catalog store. */
    boolean contains(String catalogName);

    /** Initialize the catalog store. */
    void open();

    /** Close the catalog store. */
    void close();
}
