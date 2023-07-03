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
 * This interface describes the behavior of retrieving, saving, or deleting Catalog configurations
 * from an external storage system.
 */
@PublicEvolving
public interface CatalogStore {

    /**
     * Store a catalog under the give name. The catalog name must be unique.
     *
     * @param catalogName name under which to register the given catalog
     * @param catalog catalog instance to store
     * @throws CatalogException if the registration of the catalog under the given name failed
     */
    void storeCatalog(String catalogName, CatalogDescriptor catalog) throws CatalogException;

    /**
     * Unregisters a catalog under the given name. The catalog name must be existed.
     *
     * @param catalogName name under which to unregister the given catalog.
     * @param ignoreIfNotExists If false exception will be thrown if the table or database or
     *     catalog to be altered does not exist.
     * @throws CatalogException if the unregistration of the catalog under the given name failed
     */
    void removeCatalog(String catalogName, boolean ignoreIfNotExists) throws CatalogException;

    /**
     * Gets a catalog by name.
     *
     * @param catalogName name of the catalog to retrieve
     * @return the requested catalog or empty if it does not exist
     */
    Optional<CatalogDescriptor> getCatalog(String catalogName);

    /**
     * Retrieves names of all registered catalogs.
     *
     * @return a set of names of registered catalogs
     */
    Set<String> listCatalogs();

    /**
     * Check if there is a corresponding catalog with the given name in CatalogStore.
     *
     * @return whether there is a corresponding Catalog with the given name
     */
    boolean contains(String catalogName);

    /** Initialization method for the CatalogStore. */
    void open();

    /** Tear-down method for the CatalogStore. */
    void close();
}
