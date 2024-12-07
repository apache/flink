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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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

    public ResolvedCatalogMaterializedTable(
            CatalogMaterializedTable origin, ResolvedSchema resolvedSchema) {
        this.origin =
                Preconditions.checkNotNull(
                        origin, "Original catalog materialized table must not be null.");
        this.resolvedSchema =
                Preconditions.checkNotNull(resolvedSchema, "Resolved schema must not be null.");
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
                (CatalogMaterializedTable) origin.copy(), resolvedSchema);
    }

    @Override
    public ResolvedCatalogMaterializedTable copy(Map<String, String> options) {
        return new ResolvedCatalogMaterializedTable(origin.copy(options), resolvedSchema);
    }

    @Override
    public ResolvedCatalogMaterializedTable copy(
            RefreshStatus refreshStatus,
            String refreshHandlerDescription,
            byte[] serializedRefreshHandler) {
        return new ResolvedCatalogMaterializedTable(
                origin.copy(refreshStatus, refreshHandlerDescription, serializedRefreshHandler),
                resolvedSchema);
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
    public CatalogMaterializedTable getOrigin() {
        return origin;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public String getDefinitionQuery() {
        return origin.getDefinitionQuery();
    }

    @Override
    public IntervalFreshness getDefinitionFreshness() {
        return origin.getDefinitionFreshness();
    }

    @Override
    public LogicalRefreshMode getLogicalRefreshMode() {
        return origin.getLogicalRefreshMode();
    }

    @Override
    public RefreshMode getRefreshMode() {
        return origin.getRefreshMode();
    }

    @Override
    public RefreshStatus getRefreshStatus() {
        return origin.getRefreshStatus();
    }

    @Override
    public Optional<String> getRefreshHandlerDescription() {
        return origin.getRefreshHandlerDescription();
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
                        .partitionKeys(getPartitionKeys())
                        .options(getOptions())
                        .snapshot(getSnapshot().orElse(null))
                        .build(),
                getResolvedSchema());
    }
}
