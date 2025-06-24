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

package org.apache.flink.table.connector.sink.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.StagedTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;

/**
 * Interface for {@link DynamicTableSink}s that support atomic semantic for CTAS(CREATE TABLE AS
 * SELECT) or RTAS([CREATE OR] REPLACE TABLE AS SELECT) statement using a two-phase commit protocol.
 * The table sink is responsible for returning a {@link StagedTable} to tell the Flink how to
 * implement the atomicity semantics.
 *
 * <p>If the user turns on {@link TableConfigOptions#TABLE_RTAS_CTAS_ATOMICITY_ENABLED}, and the
 * {@link DynamicTableSink} implements {@link SupportsStaging}, the planner will call method {@link
 * #applyStaging(StagingContext)} to get the {@link StagedTable} returned by the sink, then the
 * {@link StagedTable} will be used by Flink to implement a two-phase commit with the actual
 * implementation of the {@link StagedTable}.
 */
@PublicEvolving
public interface SupportsStaging {

    /**
     * Provides a {@link StagingContext} for the sink modification and return a {@link StagedTable}.
     * The {@link StagedTable} provides transaction abstraction to support atomicity for CTAS/RTAS.
     * Flink will call the relevant API of StagedTable when the Job status switches,
     *
     * <p>Note: This method will be called at the compile stage.
     *
     * @param context The context for the sink modification
     * @return {@link StagedTable} that will be leveraged by Flink framework to provide atomicity
     *     semantics.
     */
    StagedTable applyStaging(StagingContext context);

    /**
     * The context is intended to tell DynamicTableSink the type of this operation. Currently, it'll
     * provide what kind of operation the staging sink is for. In this way, the DynamicTableSink can
     * return the corresponding implementation of StagedTable according to the specific operation.
     * More types of operations can be extended in the future.
     */
    @PublicEvolving
    interface StagingContext {
        StagingPurpose getStagingPurpose();
    }

    /**
     * The type of operation the staging sink is for.
     *
     * <p>Currently, two types of operation are supported:
     *
     * <ul>
     *   <li>CREATE_TABLE_AS - for the operation of CREATE TABLE AS SELECT statement
     *   <li>CREATE_TABLE_AS_IF_NOT_EXISTS - for the operation of CREATE TABLE AS SELECT IF NOT
     *       EXISTS statement.
     * </ul>
     */
    @PublicEvolving
    enum StagingPurpose {
        CREATE_TABLE_AS,
        CREATE_TABLE_AS_IF_NOT_EXISTS,
        REPLACE_TABLE_AS,
        CREATE_OR_REPLACE_TABLE_AS
    }
}
