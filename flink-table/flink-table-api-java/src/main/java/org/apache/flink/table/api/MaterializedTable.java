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
 * <p>Every getter reads the current definition of the materialized table. This means a change made
 * elsewhere is visible on the next call. Separate calls are not read atomically.
 */
@PublicEvolving
public interface MaterializedTable extends Explainable<MaterializedTable> {

    /** The fully qualified identifier of the materialized table. */
    ObjectIdentifier getIdentifier();

    /**
     * The refresh status.
     *
     * <p>{@link RefreshStatus#ACTIVATED} means the refresh was started, not that it is running.
     *
     * @throws ValidationException if no materialized table exists at its path anymore.
     */
    RefreshStatus getRefreshStatus();

    /**
     * The query as written in SQL. For a materialized table declared with the Table API, it is the
     * SQL generated from the Table API query.
     *
     * @throws ValidationException if no materialized table exists at its path anymore.
     */
    String getOriginalQuery();

    /**
     * Expanded text of the original materialized table definition with resolved identifiers.
     *
     * @throws ValidationException if no materialized table exists at its path anymore.
     */
    String getExpandedQuery();

    /**
     * The declared freshness, or the configured default if none was declared.
     *
     * @throws ValidationException if no materialized table exists at its path anymore.
     */
    IntervalFreshness getFreshness();

    /**
     * The refresh mode used to keep the materialized table within its freshness.
     *
     * @throws ValidationException if no materialized table exists at its path anymore.
     */
    RefreshMode getRefreshMode();

    /**
     * A description of the refresh handler. Empty if the catalog records none.
     *
     * @throws ValidationException if no materialized table exists at its path anymore.
     */
    Optional<String> getRefreshHandlerDescription();
}
