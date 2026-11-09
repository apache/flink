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

package org.apache.flink.table.operations.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.QueryOperation;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A context of a materialized table declaration including everything a {@code CREATE [OR ALTER]
 * MATERIALIZED TABLE ... AS <query>} statement declares besides the identifier.
 */
@Internal
public interface MaterializedTableMergeContext {

    /** Whether the declaration defines the table's columns. */
    boolean hasSchemaDefinition();

    /** Whether the declaration decides the primary key. */
    boolean hasConstraintDefinition();

    /** The declared schema merged onto the query schema. May be recomputed on each call. */
    Schema getMergedSchema();

    ResolvedSchema getQuerySchema();

    Map<String, String> getOptions();

    List<String> getPartitionKeys();

    Optional<TableDistribution> getDistribution();

    @Nullable
    String getComment();

    /** The declared freshness, or {@code null} to let the engine apply its default. */
    @Nullable
    IntervalFreshness getFreshness();

    /** The declared refresh mode. {@link LogicalRefreshMode#AUTOMATIC} lets the engine choose. */
    LogicalRefreshMode getLogicalRefreshMode();

    /** The declared start mode, or {@code null} for the configured default. */
    @Nullable
    StartMode getStartMode();

    String getOriginalQuery();

    /** The query text the refresh job runs. May be recomputed on each call. */
    String getExpandedQuery();

    QueryOperation getQueryOperation();

    /**
     * Aligns the query with the persisted columns of the resolved table it will be written to.
     * Called for create and convert only.
     */
    QueryOperation alignQuery(ResolvedCatalogMaterializedTable table);
}
