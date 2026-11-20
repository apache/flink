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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Optional;

/**
 * A handle on an existing materialized table, returned by {@link MaterializedPipeline#execute()}
 * and {@link TableEnvironment#fromMaterializedTable(String)}.
 *
 * <p>The handle keeps the definition that was read when it was created. {@link
 * #getOriginalQuery()}, {@link #getExpandedQuery()}, {@link #getFreshness()} and {@link
 * #getRefreshMode()} don't change when the materialized table is altered. To see a new definition
 * it needs to be looked up again. {@link #getRefreshStatus()} and {@link
 * #getRefreshHandlerDescription()} read the catalog on every call, and fail with a {@link
 * ValidationException} once the materialized table is dropped.
 *
 * <p>The following example shows how to work with a {@link MaterializedTable} object.
 *
 * <pre>{@code
 * TableEnvironment tableEnv = TableEnvironment.create(...);
 *
 * MaterializedTable revenue = tableEnv.fromMaterializedTable("revenue");
 *
 * RefreshStatus status = revenue.getRefreshStatus();
 * String query = revenue.getExpandedQuery();
 * }</pre>
 */
@PublicEvolving
public interface MaterializedTable {

    /** The fully qualified identifier of the materialized table. */
    ObjectIdentifier getIdentifier();

    /**
     * ACTIVATED, SUSPENDED or INITIALIZING.
     *
     * <p>ACTIVATED means the refresh pipeline was started (a continuous refresh job submitted, or a
     * full-refresh workflow scheduled), not that it is running.
     */
    RefreshStatus getRefreshStatus();

    /**
     * The query as written in SQL. For a materialized table declared with the Table API, it is the
     * generated SQL, the same as {@link #getExpandedQuery()}.
     */
    String getOriginalQuery();

    /** Expanded text of the original materialized table definition with resolved identifiers. */
    String getExpandedQuery();

    /** The declared freshness, or the configured default if none was declared. */
    IntervalFreshness getFreshness();

    /** CONTINUOUS or FULL. */
    RefreshMode getRefreshMode();

    /**
     * Display-only summary of the refresh handler. For a CONTINUOUS table this is the refresh job,
     * for a FULL table it is the scheduled workflow. Empty if the catalog records none.
     */
    Optional<String> getRefreshHandlerDescription();
}
