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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A validated {@link CatalogMaterializedTable} that is backed by the original metadata coming from
 * the {@link Catalog} but resolved by the framework.
 *
 * <p>Note: This will be converted to {@link ResolvedCatalogTable} by framework during planner
 * optimize query phase.
 */
@PublicEvolving
public class ResolvedCatalogMaterializedTable
        implements ResolvedCatalogBaseTable<CatalogMaterializedTable>, CatalogMaterializedTable {

    private final CatalogMaterializedTable origin;

    private final ResolvedSchema resolvedSchema;

    private final RefreshMode refreshMode;

    private final IntervalFreshness freshness;

    public ResolvedCatalogMaterializedTable(
            CatalogMaterializedTable origin,
            ResolvedSchema resolvedSchema,
            RefreshMode refreshMode,
            IntervalFreshness freshness) {
        this.origin = checkNotNull(origin, "Original catalog materialized table must not be null.");
        this.resolvedSchema = checkNotNull(resolvedSchema, "Resolved schema must not be null.");
        this.refreshMode = checkNotNull(refreshMode, "Refresh mode must not be null.");
        this.freshness = checkNotNull(freshness, "Freshness must not be null.");
    }

    @Override
    public Map<String, String> getOptions() {
        return origin.getOptions();
    }

    @Override
    public String getComment() {
        return origin.getComment();
    }

    @Override
    public CatalogBaseTable copy() {
        return new ResolvedCatalogMaterializedTable(
                (CatalogMaterializedTable) origin.copy(), resolvedSchema, refreshMode, freshness);
    }

    @Override
    public ResolvedCatalogMaterializedTable copy(Map<String, String> options) {
        return new ResolvedCatalogMaterializedTable(
                origin.copy(options), resolvedSchema, refreshMode, freshness);
    }

    @Override
    public ResolvedCatalogMaterializedTable copy(
            RefreshStatus refreshStatus,
            String refreshHandlerDescription,
            byte[] serializedRefreshHandler) {
        return new ResolvedCatalogMaterializedTable(
                origin.copy(refreshStatus, refreshHandlerDescription, serializedRefreshHandler),
                resolvedSchema,
                refreshMode,
                freshness);
    }

    @Override
    public Optional<String> getDescription() {
        return origin.getDescription();
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return origin.getDetailedDescription();
    }

    @Override
    public boolean isPartitioned() {
        return origin.isPartitioned();
    }

    @Override
    public List<String> getPartitionKeys() {
        return origin.getPartitionKeys();
    }

    @Override
    public Optional<Long> getSnapshot() {
        return origin.getSnapshot();
    }

    @Override
    public String getOriginalQuery() {
        return origin.getOriginalQuery();
    }

    @Override
    public String getExpandedQuery() {
        return origin.getExpandedQuery();
    }

    @Override
    public CatalogMaterializedTable getOrigin() {
        return origin;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public @Nonnull IntervalFreshness getDefinitionFreshness() {
        return freshness;
    }

    @Override
    public LogicalRefreshMode getLogicalRefreshMode() {
        return origin.getLogicalRefreshMode();
    }

    @Override
    public @Nonnull RefreshMode getRefreshMode() {
        return refreshMode;
    }

    @Override
    public RefreshStatus getRefreshStatus() {
        return origin.getRefreshStatus();
    }

    @Override
    public Optional<String> getRefreshHandlerDescription() {
        return origin.getRefreshHandlerDescription();
    }

    @Override
    public Optional<TableDistribution> getDistribution() {
        return origin.getDistribution();
    }

    @Nullable
    @Override
    public byte[] getSerializedRefreshHandler() {
        return origin.getSerializedRefreshHandler();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResolvedCatalogMaterializedTable that = (ResolvedCatalogMaterializedTable) o;
        return Objects.equals(origin, that.origin)
                && Objects.equals(resolvedSchema, that.resolvedSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(origin, resolvedSchema);
    }

    @Override
    public String toString() {
        return "ResolvedCatalogMaterializedTable{"
                + "origin="
                + origin
                + ", resolvedSchema="
                + resolvedSchema
                + '}';
    }

    /** Convert this object to a {@link ResolvedCatalogTable} object for planner optimize query. */
    public ResolvedCatalogTable toResolvedCatalogTable() {
        return new ResolvedCatalogTable(
                CatalogTable.newBuilder()
                        .schema(getUnresolvedSchema())
                        .comment(getComment())
                        .distribution(getDistribution().orElse(null))
                        .partitionKeys(getPartitionKeys())
                        .options(getOptions())
                        .snapshot(getSnapshot().orElse(null))
                        .build(),
                getResolvedSchema());
    }
}
