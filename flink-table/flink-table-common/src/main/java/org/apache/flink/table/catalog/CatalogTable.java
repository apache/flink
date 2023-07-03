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
import org.apache.flink.table.factories.DynamicTableFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Represents the unresolved metadata of a table in a {@link Catalog}.
 *
 * <p>It contains all characteristics that can be expressed in a SQL {@code CREATE TABLE} statement.
 * The framework will resolve instances of this interface to a {@link ResolvedCatalogTable} before
 * passing it to a {@link DynamicTableFactory} for creating a connector to an external system.
 *
 * <p>A catalog implementer can either use {@link #of(Schema, String, List, Map)} for a basic
 * implementation of this interface or create a custom class that allows passing catalog-specific
 * objects all the way down to the connector creation (if necessary).
 *
 * <p>Note: The default implementation that is available via {@link #of(Schema, String, List, Map)}
 * is always serializable. For example, it can be used for implementing a catalog that uses {@link
 * ResolvedCatalogTable#toProperties()} or for persisting compiled plans. An implementation of this
 * interface determines whether a catalog table can be serialized by providing a proper {@link
 * #getOptions()} method.
 */
@PublicEvolving
public interface CatalogTable extends CatalogBaseTable {

    /**
     * Creates a basic implementation of this interface.
     *
     * <p>The signature is similar to a SQL {@code CREATE TABLE} statement.
     *
     * @param schema unresolved schema
     * @param comment optional comment
     * @param partitionKeys list of partition keys or an empty list if not partitioned
     * @param options options to configure the connector
     */
    static CatalogTable of(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options) {
        return new DefaultCatalogTable(schema, comment, partitionKeys, options);
    }

    /**
     * Creates an instance of {@link CatalogTable} with a specific snapshot.
     *
     * @param schema unresolved schema
     * @param comment optional comment
     * @param partitionKeys list of partition keys or an empty list if not partitioned
     * @param options options to configure the connector
     * @param snapshot table snapshot of the table
     */
    static CatalogTable of(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options,
            @Nullable Long snapshot) {
        return new DefaultCatalogTable(schema, comment, partitionKeys, options, snapshot);
    }

    /**
     * Creates an instance of {@link CatalogTable} from a map of string properties that were
     * previously created with {@link ResolvedCatalogTable#toProperties()}.
     *
     * <p>Note that the serialization and deserialization of catalog tables are not symmetric. The
     * framework will resolve functions and perform other validation tasks. A catalog implementation
     * must not deal with this during a read operation.
     *
     * @param properties serialized version of a {@link CatalogTable} that includes schema,
     *     partition keys, and connector options
     */
    static CatalogTable fromProperties(Map<String, String> properties) {
        return CatalogPropertiesUtil.deserializeCatalogTable(properties);
    }

    @Override
    default TableKind getTableKind() {
        return TableKind.TABLE;
    }

    /**
     * Check if the table is partitioned or not.
     *
     * @return true if the table is partitioned; otherwise, false
     */
    boolean isPartitioned();

    /**
     * Get the partition keys of the table. This will be an empty set if the table is not
     * partitioned.
     *
     * @return partition keys of the table
     */
    List<String> getPartitionKeys();

    /**
     * Returns a copy of this {@code CatalogTable} with given table options {@code options}.
     *
     * @return a new copy of this table with replaced table options
     */
    CatalogTable copy(Map<String, String> options);

    /**
     * Serializes this instance into a map of string-based properties.
     *
     * <p>Compared to the pure table options in {@link #getOptions()}, the map includes schema,
     * partitioning, and other characteristics in a serialized form.
     *
     * @deprecated Only a {@link ResolvedCatalogTable} is serializable to properties.
     */
    @Deprecated
    default Map<String, String> toProperties() {
        return Collections.emptyMap();
    }

    /** Return the snapshot specified for the table. Return Optional.empty() if not specified. */
    default Optional<Long> getSnapshot() {
        return Optional.empty();
    }
}
