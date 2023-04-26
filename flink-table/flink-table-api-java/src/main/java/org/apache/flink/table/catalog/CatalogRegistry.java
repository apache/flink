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
import org.apache.flink.table.api.CatalogNotExistException;

import java.util.Optional;

/** A catalog registry for dealing with catalogs. */
@PublicEvolving
public interface CatalogRegistry {

    /** Get the name of the current database. */
    String getCurrentDatabase();

    /** Gets the name of the current catalog. */
    String getCurrentCatalog();

    /**
     * Returns the full name of the given table path, this name may be padded with current
     * catalog/database name based on the {@code identifier's} length.
     *
     * @param identifier an unresolved identifier
     * @return a fully qualified object identifier
     */
    ObjectIdentifier qualifyIdentifier(UnresolvedIdentifier identifier);

    /**
     * Gets a catalog by name.
     *
     * @param catalogName name of the catalog to retrieve
     * @return the requested catalog
     * @throws CatalogNotExistException if the catalog does not exist
     */
    Catalog getCatalogOrError(String catalogName) throws CatalogNotExistException;

    /**
     * Retrieves a fully qualified table. If the path is not yet fully qualified use {@link
     * #qualifyIdentifier(UnresolvedIdentifier)} first.
     *
     * @param objectIdentifier full path of the table to retrieve
     * @return resolved table that the path points to or empty if it does not exist.
     */
    Optional<ResolvedCatalogBaseTable<?>> getCatalogBaseTable(ObjectIdentifier objectIdentifier);

    /**
     * Return whether the table with a fully qualified table path is temporary or not.
     *
     * @param objectIdentifier full path of the table
     * @return the table is temporary or not.
     */
    boolean isTemporaryTable(ObjectIdentifier objectIdentifier);

    /**
     * Retrieves a partition with a fully qualified table path and partition spec.
     *
     * @param tableIdentifier full path of the table to retrieve
     * @param partitionSpec full partition spec
     * @return partition in the table.
     */
    Optional<CatalogPartition> getPartition(
            ObjectIdentifier tableIdentifier, CatalogPartitionSpec partitionSpec);
}
