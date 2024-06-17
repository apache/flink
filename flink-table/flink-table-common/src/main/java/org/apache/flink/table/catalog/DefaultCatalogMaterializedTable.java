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
import org.apache.flink.table.api.Schema;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of a {@link CatalogMaterializedTable}. */
@Internal
public class DefaultCatalogMaterializedTable implements CatalogMaterializedTable {

    private final Schema schema;
    private final @Nullable String comment;
    private final List<String> partitionKeys;
    private final Map<String, String> options;

    private final @Nullable Long snapshot;

    private final String definitionQuery;
    private final IntervalFreshness freshness;
    private final LogicalRefreshMode logicalRefreshMode;
    private final RefreshMode refreshMode;
    private final RefreshStatus refreshStatus;
    private final @Nullable String refreshHandlerDescription;
    private final @Nullable byte[] serializedRefreshHandler;

    protected DefaultCatalogMaterializedTable(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options,
            @Nullable Long snapshot,
            String definitionQuery,
            IntervalFreshness freshness,
            LogicalRefreshMode logicalRefreshMode,
            RefreshMode refreshMode,
            RefreshStatus refreshStatus,
            @Nullable String refreshHandlerDescription,
            @Nullable byte[] serializedRefreshHandler) {
        this.schema = checkNotNull(schema, "Schema must not be null.");
        this.comment = comment;
        this.partitionKeys = checkNotNull(partitionKeys, "Partition keys must not be null.");
        this.options = checkNotNull(options, "Options must not be null.");
        this.snapshot = snapshot;
        this.definitionQuery = checkNotNull(definitionQuery, "Definition query must not be null.");
        this.freshness = checkNotNull(freshness, "Freshness must not be null.");
        this.logicalRefreshMode =
                checkNotNull(logicalRefreshMode, "Logical refresh mode must not be null.");
        this.refreshMode = checkNotNull(refreshMode, "Refresh mode must not be null.");
        this.refreshStatus = checkNotNull(refreshStatus, "Refresh status must not be null.");
        this.refreshHandlerDescription = refreshHandlerDescription;
        this.serializedRefreshHandler = serializedRefreshHandler;

        checkArgument(
                options.entrySet().stream()
                        .allMatch(e -> e.getKey() != null && e.getValue() != null),
                "Options cannot have null keys or values.");
    }

    @Override
    public Schema getUnresolvedSchema() {
        return schema;
    }

    @Override
    public String getComment() {
        return comment != null ? comment : "";
    }

    @Override
    public boolean isPartitioned() {
        return !partitionKeys.isEmpty();
    }

    @Override
    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public CatalogBaseTable copy() {
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

    @Override
    public CatalogMaterializedTable copy(Map<String, String> options) {
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

    @Override
    public CatalogMaterializedTable copy(
            RefreshStatus refreshStatus,
            String refreshHandlerDescription,
            byte[] serializedRefreshHandler) {
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

    @Override
    public Optional<String> getDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.empty();
    }

    @Override
    public Optional<Long> getSnapshot() {
        return Optional.ofNullable(snapshot);
    }

    @Override
    public String getDefinitionQuery() {
        return definitionQuery;
    }

    @Override
    public IntervalFreshness getDefinitionFreshness() {
        return freshness;
    }

    @Override
    public LogicalRefreshMode getLogicalRefreshMode() {
        return logicalRefreshMode;
    }

    @Override
    public RefreshMode getRefreshMode() {
        return refreshMode;
    }

    @Override
    public RefreshStatus getRefreshStatus() {
        return refreshStatus;
    }

    @Override
    public Optional<String> getRefreshHandlerDescription() {
        return Optional.ofNullable(refreshHandlerDescription);
    }

    @Nullable
    @Override
    public byte[] getSerializedRefreshHandler() {
        return serializedRefreshHandler;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultCatalogMaterializedTable that = (DefaultCatalogMaterializedTable) o;
        return Objects.equals(schema, that.schema)
                && Objects.equals(comment, that.comment)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(options, that.options)
                && Objects.equals(snapshot, that.snapshot)
                && Objects.equals(definitionQuery, that.definitionQuery)
                && Objects.equals(freshness, that.freshness)
                && logicalRefreshMode == that.logicalRefreshMode
                && refreshMode == that.refreshMode
                && refreshStatus == that.refreshStatus
                && Objects.equals(refreshHandlerDescription, that.refreshHandlerDescription)
                && Arrays.equals(serializedRefreshHandler, that.serializedRefreshHandler);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
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
                        refreshHandlerDescription);
        result = 31 * result + Arrays.hashCode(serializedRefreshHandler);
        return result;
    }

    @Override
    public String toString() {
        return "DefaultCatalogMaterializedTable{"
                + "schema="
                + schema
                + ", comment='"
                + comment
                + '\''
                + ", partitionKeys="
                + partitionKeys
                + ", options="
                + options
                + ", snapshot="
                + snapshot
                + ", definitionQuery='"
                + definitionQuery
                + '\''
                + ", freshness="
                + freshness
                + ", logicalRefreshMode="
                + logicalRefreshMode
                + ", refreshMode="
                + refreshMode
                + ", refreshStatus="
                + refreshStatus
                + ", refreshHandlerDescription='"
                + refreshHandlerDescription
                + '\''
                + ", serializedRefreshHandler="
                + Arrays.toString(serializedRefreshHandler)
                + '}';
    }
}
