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
 * Interface for {@link DynamicTableSink}s that support update existing data according to row-level
 * changes. The table sink is responsible for telling planner how to produce the row changes, and
 * consuming them to achieve the purpose of row(s) update.
 *
 * <p>The planner will call method {@link #applyRowLevelUpdate(List,
 * RowLevelModificationScanContext)} to get the {@link RowLevelUpdateInfo} returned by sink, and
 * rewrite the update statement based on the retrieved {@link RowLevelUpdateInfo} to produce rows to
 * {@link DynamicTableSink}.
 */
@PublicEvolving
public interface SupportsRowLevelUpdate {

    /**
     * Applies row-level update with providing the updated columns and {@link
     * RowLevelModificationScanContext}, and return {@link RowLevelUpdateInfo}.
     *
     * @param updatedColumns the columns updated by update statement in table column order.
     * @param context the context passed by table source which implement {@link
     *     SupportsRowLevelModificationScan}. It'll be null if the table source doesn't implement
     *     it.
     */
    RowLevelUpdateInfo applyRowLevelUpdate(
            List<Column> updatedColumns, @Nullable RowLevelModificationScanContext context);

    /** The information that guides the planner on how to rewrite the update statement. */
    @PublicEvolving
    interface RowLevelUpdateInfo {

        /**
         * The required columns by the sink to perform row-level update. The rows consumed by sink
         * will contain the required columns in order. If return Optional.empty(), it will contain
         * all columns.
         */
        default Optional<List<Column>> requiredColumns() {
            return Optional.empty();
        }

        /**
         * Planner will rewrite the update statement to query base on the {@link
         * RowLevelUpdateMode}, keeping the query of update unchanged by default(in `UPDATED_ROWS`
         * mode), or changing the query to union the updated rows and the other rows (in `ALL_ROWS`
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
         * <p>If returns {@link RowLevelUpdateMode#ALL_ROWS}, the sink will get both the update
         * after rows which match the filter [y = 2] and the other rows that don't match the filter
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
     * Type of update modes that the sink expects for update purpose.
     *
     * <p>Currently, two modes are supported:
     *
     * <ul>
     *   <li>UPDATED_ROWS - in this mode, the sink will only get the update after rows.
     *   <li>ALL_ROWS - in this mode, the sink will get all the rows including both the update after
     *       rows and the other rows that don't need to be updated.
     * </ul>
     */
    @PublicEvolving
    enum RowLevelUpdateMode {
        UPDATED_ROWS,
        ALL_ROWS
    }
}
