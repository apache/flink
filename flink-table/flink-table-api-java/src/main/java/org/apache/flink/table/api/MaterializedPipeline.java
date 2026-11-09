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
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * A pending declaration of one materialized table, created by {@link
 * Table#materializeInto(String)}.
 */
@PublicEvolving
public interface MaterializedPipeline {

    /**
     * The fully qualified identifier this declaration will create or alter.
     *
     * <p>Resolved against the current catalog and database at {@link Table#materializeInto}.
     */
    ObjectIdentifier getIdentifier();

    /**
     * Submits this declaration with the semantics of {@code CREATE OR ALTER MATERIALIZED TABLE}.
     *
     * <p>If the materialized table exists and its continuous refresh job is running, the job is
     * stopped with a savepoint and a new refresh job is started, which does not restore that
     * savepoint. This also happens if the declaration is unchanged. The stop requires a savepoint
     * directory ({@code execution.checkpointing.savepoint-dir}); without one, this fails before the
     * stop. If the changes are rejected or cannot be written after the stop, the materialized table
     * is left suspended with its previous definition and the savepoint. If the new refresh job
     * starts but the catalog cannot record it, that job is cancelled and the previous refresh job
     * restarts from the savepoint. If the cancel fails, the materialized table is rolled back and
     * left suspended instead.
     *
     * <p>A suspended materialized table is altered and stays suspended, and its stored savepoint is
     * cleared, so a later {@code RESUME} starts the refresh job without state. A materialized table
     * in full refresh mode is altered without changing its refresh workflow.
     *
     * <p>An existing materialized table keeps its freshness, comment and partition keys, so a
     * freshness given to {@link Table#materializeInto(String, java.time.Duration)} is ignored. Its
     * options are reset and its distribution is dropped, as by a {@code CREATE OR ALTER
     * MATERIALIZED TABLE} without {@code WITH} and {@code DISTRIBUTED BY} clauses.
     *
     * <p>Returns once the catalog entry is written and, if a continuous refresh job is started,
     * once that job has been submitted.
     */
    MaterializedTable execute();
}
