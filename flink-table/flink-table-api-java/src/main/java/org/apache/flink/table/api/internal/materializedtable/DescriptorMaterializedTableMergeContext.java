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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableMergeContext;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * The {@link MaterializedTableMergeContext} of {@code Table#materializeInto}, with the semantics of
 * the equivalent {@code CREATE OR ALTER MATERIALIZED TABLE} statement.
 */
@Internal
final class DescriptorMaterializedTableMergeContext implements MaterializedTableMergeContext {

    private final QueryOperation queryOperation;
    private final String serializedQuery;
    private final @Nullable IntervalFreshness freshness;
    private final ResolvedSchema querySchema;

    DescriptorMaterializedTableMergeContext(
            QueryOperation queryOperation,
            String serializedQuery,
            @Nullable IntervalFreshness freshness) {
        this.queryOperation = queryOperation;
        this.serializedQuery = serializedQuery;
        this.freshness = freshness;
        this.querySchema = toPhysicalSchema(queryOperation.getResolvedSchema());
    }

    @Override
    public boolean hasSchemaDefinition() {
        return false;
    }

    @Override
    public boolean hasConstraintDefinition() {
        return false;
    }

    @Override
    public Schema getMergedSchema() {
        return Schema.newBuilder().fromResolvedSchema(querySchema).build();
    }

    @Override
    public ResolvedSchema getQuerySchema() {
        return querySchema;
    }

    @Override
    public Map<String, String> getOptions() {
        return Map.of();
    }

    @Override
    public List<String> getPartitionKeys() {
        return List.of();
    }

    @Override
    public Optional<TableDistribution> getDistribution() {
        return Optional.empty();
    }

    @Override
    public @Nullable String getComment() {
        return null;
    }

    @Override
    public @Nullable IntervalFreshness getFreshness() {
        return freshness;
    }

    @Override
    public LogicalRefreshMode getLogicalRefreshMode() {
        return LogicalRefreshMode.AUTOMATIC;
    }

    @Override
    public @Nullable StartMode getStartMode() {
        return null;
    }

    @Override
    public String getOriginalQuery() {
        return serializedQuery;
    }

    @Override
    public String getExpandedQuery() {
        return serializedQuery;
    }

    @Override
    public QueryOperation getQueryOperation() {
        return queryOperation;
    }

    @Override
    public QueryOperation alignQuery(ResolvedCatalogMaterializedTable table) {
        // The table's schema is the query schema, so the query already lines up with the sink.
        return queryOperation;
    }

    /** Produces physical columns with default conversion classes. */
    private static ResolvedSchema toPhysicalSchema(ResolvedSchema schema) {
        final List<DataType> dataTypes =
                schema.getColumnDataTypes().stream()
                        .map(dataType -> fromLogicalToDataType(dataType.getLogicalType()))
                        .collect(Collectors.toList());
        return ResolvedSchema.physical(schema.getColumnNames(), dataTypes);
    }
}
