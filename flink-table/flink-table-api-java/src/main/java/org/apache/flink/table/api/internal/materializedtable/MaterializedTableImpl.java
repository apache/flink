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

package org.apache.flink.table.api.internal.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.MaterializedTable;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;

import java.util.Optional;

/** Implementation of {@link MaterializedTable}. */
@Internal
public final class MaterializedTableImpl implements MaterializedTable {

    private final TableEnvironmentInternal tableEnvironment;
    private final ContextResolvedTable snapshot;

    private MaterializedTableImpl(
            TableEnvironmentInternal tableEnvironment, ContextResolvedTable snapshot) {
        this.tableEnvironment = tableEnvironment;
        this.snapshot = snapshot;
    }

    /**
     * Returns a handle to the materialized table with the given identifier.
     *
     * @throws ValidationException if no object exists there, or it is not a materialized table
     */
    public static MaterializedTable of(
            TableEnvironmentInternal tableEnvironment, ObjectIdentifier identifier) {
        return new MaterializedTableImpl(
                tableEnvironment, lookUp(tableEnvironment.getCatalogManager(), identifier));
    }

    @Override
    public ObjectIdentifier getIdentifier() {
        return snapshot.getIdentifier();
    }

    @Override
    public RefreshStatus getRefreshStatus() {
        return lookUpResolvedTable().getRefreshStatus();
    }

    @Override
    public String getOriginalQuery() {
        return resolvedSnapshot().getOriginalQuery();
    }

    @Override
    public String getExpandedQuery() {
        return resolvedSnapshot().getExpandedQuery();
    }

    @Override
    public IntervalFreshness getFreshness() {
        return resolvedSnapshot().getDefinitionFreshness();
    }

    @Override
    public RefreshMode getRefreshMode() {
        return resolvedSnapshot().getRefreshMode();
    }

    @Override
    public Optional<String> getRefreshHandlerDescription() {
        return lookUpResolvedTable().getRefreshHandlerDescription();
    }

    private ResolvedCatalogMaterializedTable resolvedSnapshot() {
        return snapshot.getResolvedTable();
    }

    private ResolvedCatalogMaterializedTable lookUpResolvedTable() {
        return lookUp(tableEnvironment.getCatalogManager(), getIdentifier()).getResolvedTable();
    }

    private static ContextResolvedTable lookUp(
            CatalogManager catalogManager, ObjectIdentifier identifier) {
        final ContextResolvedTable table =
                catalogManager
                        .getTable(identifier)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Materialized table with identifier '%s' does not exist.",
                                                        identifier.asSummaryString())));
        if (table.getResolvedTable().getTableKind() != TableKind.MATERIALIZED_TABLE) {
            throw new ValidationException(
                    String.format(
                            "Table '%s' is not a materialized table, does not support materialized table related operation.",
                            identifier.asSummaryString()));
        }
        return table;
    }

    @Override
    public String toString() {
        return "MaterializedTable '" + getIdentifier().asSummaryString() + "'";
    }
}
