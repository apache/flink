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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Map;
import java.util.Optional;

/**
 * A common parent that describes the <i>unresolved</i> metadata of a table or view in a catalog.
 *
 * @see CatalogTable
 * @see CatalogView
 */
@PublicEvolving
public interface CatalogBaseTable {

    /** The kind of {@link CatalogBaseTable}. */
    enum TableKind {
        TABLE,
        VIEW
    }

    /** The kind of table this {@link CatalogBaseTable} describes. */
    TableKind getTableKind();

    /**
     * Returns a map of string-based options.
     *
     * <p>In case of {@link CatalogTable}, these options may determine the kind of connector and its
     * configuration for accessing the data in the external system. See {@link DynamicTableFactory}
     * for more information.
     */
    Map<String, String> getOptions();

    /**
     * @deprecated This method returns the deprecated {@link TableSchema} class. The old class was a
     *     hybrid of resolved and unresolved schema information. It has been replaced by the new
     *     {@link Schema} which is always unresolved and will be resolved by the framework later.
     */
    @Deprecated
    default TableSchema getSchema() {
        return null;
    }

    /**
     * Returns the schema of the table or view.
     *
     * <p>The schema can reference objects from other catalogs and will be resolved and validated by
     * the framework when accessing the table or view.
     *
     * @see ResolvedCatalogTable
     * @see ResolvedCatalogView
     */
    default Schema getUnresolvedSchema() {
        final TableSchema oldSchema = getSchema();
        if (oldSchema == null) {
            throw new UnsupportedOperationException(
                    "A CatalogBaseTable must implement getUnresolvedSchema().");
        }
        return oldSchema.toSchema();
    }

    /**
     * Get comment of the table or view.
     *
     * @return comment of the table/view.
     */
    String getComment();

    /**
     * Get a deep copy of the CatalogBaseTable instance.
     *
     * @return a copy of the CatalogBaseTable instance
     */
    CatalogBaseTable copy();

    /**
     * Get a brief description of the table or view.
     *
     * @return an optional short description of the table/view
     */
    Optional<String> getDescription();

    /**
     * Get a detailed description of the table or view.
     *
     * @return an optional long description of the table/view
     */
    Optional<String> getDetailedDescription();
}
