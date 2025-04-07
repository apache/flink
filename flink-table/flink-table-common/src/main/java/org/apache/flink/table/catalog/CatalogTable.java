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
import org.apache.flink.util.Preconditions;

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
 * <p>A catalog implementer can either use {@link #newBuilder()} for a basic implementation of this
 * interface or create a custom class that allows passing catalog-specific objects all the way down
 * to the connector creation (if necessary).
 *
 * <p>Note: The default implementation that is available via {@link #newBuilder()} is always
 * serializable. For example, it can be used for implementing a catalog that uses {@link
 * ResolvedCatalogTable#toProperties()} or for persisting compiled plans. An implementation of this
 * interface determines whether a catalog table can be serialized by providing a proper {@link
 * #getOptions()} method.
 */
@PublicEvolving
public interface CatalogTable extends CatalogBaseTable {

    /** Builder for configuring and creating instances of {@link CatalogTable}. */
    @PublicEvolving
    static CatalogTable.Builder newBuilder() {
        return new CatalogTable.Builder();
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

    /** Return the snapshot specified for the table. Return Optional.empty() if not specified. */
    default Optional<Long> getSnapshot() {
        return Optional.empty();
    }

    /** Returns the distribution of the table if the {@code DISTRIBUTED} clause is defined. */
    default Optional<TableDistribution> getDistribution() {
        return Optional.empty();
    }

    // --------------------------------------------------------------------------------------------

    /** Builder for configuring and creating instances of {@link CatalogTable}. */
    @PublicEvolving
    class Builder {
        private @Nullable Schema schema;
        private @Nullable String comment;
        private List<String> partitionKeys = Collections.emptyList();
        private Map<String, String> options = Collections.emptyMap();
        private @Nullable Long snapshot;
        private @Nullable TableDistribution distribution;

        private Builder() {}

        public Builder schema(Schema schema) {
            this.schema = Preconditions.checkNotNull(schema, "Schema must not be null.");
            return this;
        }

        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        public Builder partitionKeys(List<String> partitionKeys) {
            this.partitionKeys =
                    Preconditions.checkNotNull(partitionKeys, "Partition keys must not be null.");
            return this;
        }

        public Builder options(Map<String, String> options) {
            this.options = Preconditions.checkNotNull(options, "Options must not be null.");
            return this;
        }

        public Builder snapshot(@Nullable Long snapshot) {
            this.snapshot = snapshot;
            return this;
        }

        public Builder distribution(@Nullable TableDistribution distribution) {
            this.distribution = distribution;
            return this;
        }

        public CatalogTable build() {
            return new DefaultCatalogTable(
                    schema, comment, partitionKeys, options, snapshot, distribution);
        }
    }
}
