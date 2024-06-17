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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.utils.IntervalFreshnessUtils.convertFreshnessToDuration;

/**
 * Represents the unresolved metadata of a materialized table in a {@link Catalog}.
 *
 * <p>Materialized Table definition: In the context of streaming-batch unified storage, it provides
 * full history data and incremental changelog. By defining the data's production business logic and
 * freshness, data refresh is achieved through continuous or full refresh pipeline, while also
 * possessing the capability for both batch and incremental consumption.
 *
 * <p>The metadata for {@link CatalogMaterializedTable} includes the following four main parts:
 *
 * <ul>
 *   <li>Schema, comments, options and partition keys.
 *   <li>Data freshness, which determines when the data is generated and becomes visible for user.
 *   <li>Data production business logic, also known as the definition query.
 *   <li>Background refresh pipeline, either through a flink streaming or periodic flink batch job,
 *       it is initialized after materialized table is created.
 * </ul>
 */
@PublicEvolving
public interface CatalogMaterializedTable extends CatalogBaseTable {

    /** Builder for configuring and creating instances of {@link CatalogMaterializedTable}. */
    @PublicEvolving
    static CatalogMaterializedTable.Builder newBuilder() {
        return new CatalogMaterializedTable.Builder();
    }

    @Override
    default TableKind getTableKind() {
        return TableKind.MATERIALIZED_TABLE;
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
     * Returns a copy of this {@code CatalogMaterializedTable} with given table options {@code
     * options}.
     *
     * @return a new copy of this table with replaced table options
     */
    CatalogMaterializedTable copy(Map<String, String> options);

    /**
     * Returns a copy of this {@code CatalogDynamicTable} with given refresh info.
     *
     * @return a new copy of this table with replaced refresh info
     */
    CatalogMaterializedTable copy(
            RefreshStatus refreshStatus,
            String refreshHandlerDescription,
            byte[] serializedRefreshHandler);

    /** Return the snapshot specified for the table. Return Optional.empty() if not specified. */
    Optional<Long> getSnapshot();

    /**
     * The definition query text of materialized table, text is expanded in contrast to the original
     * SQL. This is needed because the context such as current DB is lost after the session, in
     * which view is defined, is gone. Expanded query text takes care of this, as an example.
     *
     * <p>For example, for a materialized table that is defined in the context of "default" database
     * with a query {@code select * from test1}, the expanded query text might become {@code select
     * `test1`.`name`, `test1`.`value` from `default`.`test1`}, where table test1 resides in
     * database "default" and has two columns ("name" and "value").
     *
     * @return the materialized table definition in expanded text.
     */
    String getDefinitionQuery();

    /**
     * Get the definition freshness of materialized table which is used to determine the physical
     * refresh mode.
     */
    IntervalFreshness getDefinitionFreshness();

    /**
     * Get the {@link Duration} value of materialized table definition freshness, it is converted
     * from {@link IntervalFreshness}.
     */
    default Duration getFreshness() {
        return convertFreshnessToDuration(getDefinitionFreshness());
    }

    /** Get the logical refresh mode of materialized table. */
    LogicalRefreshMode getLogicalRefreshMode();

    /** Get the physical refresh mode of materialized table. */
    RefreshMode getRefreshMode();

    /** Get the refresh status of materialized table. */
    RefreshStatus getRefreshStatus();

    /** Return summary description of refresh handler. */
    Optional<String> getRefreshHandlerDescription();

    /**
     * Return the serialized refresh handler of materialized table. This will not be used for
     * describe table.
     */
    @Nullable
    byte[] getSerializedRefreshHandler();

    /** The logical refresh mode of materialized table. */
    @PublicEvolving
    enum LogicalRefreshMode {
        /**
         * The refresh pipeline will be executed in continuous mode, corresponding to {@link
         * RefreshMode#CONTINUOUS}.
         */
        CONTINUOUS,

        /**
         * The refresh pipeline will be executed in full mode, corresponding to {@link
         * RefreshMode#FULL}.
         */
        FULL,

        /**
         * The refresh pipeline mode is determined by freshness of materialized table, either {@link
         * RefreshMode#FULL} or {@link RefreshMode#CONTINUOUS}.
         */
        AUTOMATIC
    }

    /** The physical refresh mode of materialized table. */
    @PublicEvolving
    enum RefreshMode {
        CONTINUOUS,
        FULL
    }

    /** Background refresh pipeline status of materialized table. */
    @PublicEvolving
    enum RefreshStatus {
        INITIALIZING,
        ACTIVATED,
        SUSPENDED
    }

    // --------------------------------------------------------------------------------------------

    /** Builder for configuring and creating instances of {@link CatalogMaterializedTable}. */
    @PublicEvolving
    class Builder {

        private Schema schema;
        private String comment;
        private List<String> partitionKeys = Collections.emptyList();
        private Map<String, String> options = Collections.emptyMap();
        private @Nullable Long snapshot;
        private String definitionQuery;
        private IntervalFreshness freshness;
        private LogicalRefreshMode logicalRefreshMode;
        private RefreshMode refreshMode;
        private RefreshStatus refreshStatus;
        private @Nullable String refreshHandlerDescription;
        private @Nullable byte[] serializedRefreshHandler;

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

        public Builder definitionQuery(String definitionQuery) {
            this.definitionQuery =
                    Preconditions.checkNotNull(
                            definitionQuery, "Definition query must not be null.");
            return this;
        }

        public Builder freshness(IntervalFreshness freshness) {
            this.freshness = Preconditions.checkNotNull(freshness, "Freshness must not be null.");
            return this;
        }

        public Builder logicalRefreshMode(LogicalRefreshMode logicalRefreshMode) {
            this.logicalRefreshMode =
                    Preconditions.checkNotNull(
                            logicalRefreshMode, "Logical refresh mode must not be null.");
            return this;
        }

        public Builder refreshMode(RefreshMode refreshMode) {
            this.refreshMode =
                    Preconditions.checkNotNull(refreshMode, "Refresh mode must not be null.");
            return this;
        }

        public Builder refreshStatus(RefreshStatus refreshStatus) {
            this.refreshStatus =
                    Preconditions.checkNotNull(refreshStatus, "Refresh status must not be null.");
            return this;
        }

        public Builder refreshHandlerDescription(@Nullable String refreshHandlerDescription) {
            this.refreshHandlerDescription = refreshHandlerDescription;
            return this;
        }

        public Builder serializedRefreshHandler(@Nullable byte[] serializedRefreshHandler) {
            this.serializedRefreshHandler = serializedRefreshHandler;
            return this;
        }

        public CatalogMaterializedTable build() {
            return new DefaultCatalogMaterializedTable(
                    schema,
                    comment,
                    partitionKeys,
                    options,
                    snapshot,
                    definitionQuery,
                    freshness,
                    logicalRefreshMode,
                    refreshMode,
                    refreshStatus,
                    refreshHandlerDescription,
                    serializedRefreshHandler);
        }
    }
}
