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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * Enable to update existing data in a {@link DynamicTableSink} according to row-level changes.
 *
 * <p>The planner will call method {@link #applyRowLevelUpdate} to get the {@link
 * RowLevelUpdateInfo} that the sink returns, and rewrite the update operation based on gotten
 * {@link RowLevelUpdateInfo} to produce rows (may rows to be updated with update after values or
 * all rows including both rows be updated and the other rows without update need depending on
 * sink's implementation) to {@link DynamicTableSink}. The sink is expected to consume these rows to
 * achieve rows update purpose.
 */
public interface SupportsRowLevelUpdate {

    /**
     * Apply row level update with providing the updated columns and {@link
     * RowLevelModificationScanContext} passed by table source, then return {@link
     * RowLevelUpdateInfo} to guide the planner on how to rewrite the update operation.
     *
     * @param updatedColumns the columns updated by update operation in table column order.
     * @param context the context passed by table source which implement {@link
     *     SupportsRowLevelModificationScan}. there won't be any {@link
     *     RowLevelModificationScanContext} passed, so the {@param context} will be null.
     */
    RowLevelUpdateInfo applyRowLevelUpdate(
            List<Column> updatedColumns, @Nullable RowLevelModificationScanContext context);

    /** The information that guides the planer on how to rewrite the update operation. */
    @PublicEvolving
    interface RowLevelUpdateInfo {

        /**
         * The required columns that the sink expects for deletion, the rows consumed by sink will
         * contain the columns with the order consistent with the order of returned columns. If
         * return Optional.empty(), it will select all columns.
         */
        default Optional<List<Column>> requiredColumns() {
            return Optional.empty();
        }

        /**
         * Planner will rewrite the update operation to query base on the {@link
         * RowLevelUpdateMode}, keeps the query of update unchanged by default(in `UPDATED_ROWS`
         * mode), or change the query to union the updated rows and un-updated rows (in `ALL_ROWS`
         * mode).
         *
         * <p>Take the following SQL as an example:
         *
         * <pre>{@code
         * UPDATE t SET x = 1 WHERE y = 2;
         * }</pre>
         *
         * <p>If returns {@link RowLevelUpdateMode#UPDATED_ROWS}, the sink will get the update after
         * rows which match the filter [y = 2].
         *
         * <p>If returns {@link RowLevelUpdateMode#ALL_ROWS}, the sink will get the update after
         * rows which match the filter [y = 2] as well as the other rows that don't match the filter
         * [y = 2].
         *
         * <p>Note: All rows will have RowKind#UPDATE_AFTER when RowLevelUpdateMode is UPDATED_ROWS,
         * and RowKind#INSERT when RowLevelUpdateMode is ALL_ROWS.
         */
        default RowLevelUpdateMode getRowLevelUpdateMode() {
            return RowLevelUpdateMode.UPDATED_ROWS;
        }
    }

    /**
     * Type of delete modes that the sink expects for update purpose.
     *
     * <p>Currently, two modes are supported:
     *
     * <ul>
     *   <li>UPDATED_ROWS - in this mode, The sink will only get the rows that need to updated with
     *       updated value.
     *   <li>ALL_ROWS - in this mode, the sink will get all the rows containing the updated rows and
     *       the other rows that don't need to be updated.
     * </ul>
     */
    enum RowLevelUpdateMode {
        UPDATED_ROWS,
        ALL_ROWS
    }
}
