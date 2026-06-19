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
 * Interface for {@link DynamicTableSink}s that support delete existing data according to row-level
 * changes. The table sink is responsible for telling planner how to produce the row changes, and
 * consuming them to achieve the purpose of row(s) deletion.
 *
 * <p>The planner will call the method {@link #applyRowLevelDelete(RowLevelModificationScanContext)}
 * to get the {@link RowLevelDeleteInfo} returned by sink, and rewrite the delete statement based on
 * the retrieved {@link RowLevelDeleteInfo} to produce rows to {@link DynamicTableSink}.
 *
 * <p>Note: For the cases where the table sink implement both {@link SupportsDeletePushDown} and
 * {@link SupportsRowLevelDelete}, the planner always prefers {@link SupportsDeletePushDown} over
 * {@link SupportsRowLevelDelete} on condition that {@link
 * SupportsDeletePushDown#applyDeleteFilters(List)} return true.
 */
@PublicEvolving
public interface SupportsRowLevelDelete {

    /**
     * Applies row-level delete with {@link RowLevelModificationScanContext}, and return a {@link
     * RowLevelDeleteInfo}.
     *
     * @param context the context passed by table source which implement {@link
     *     SupportsRowLevelModificationScan}. It'll be null if the table source doesn't implement
     *     it.
     */
    RowLevelDeleteInfo applyRowLevelDelete(@Nullable RowLevelModificationScanContext context);

    /** The information that guides the planner on how to rewrite the delete statement. */
    @PublicEvolving
    interface RowLevelDeleteInfo {

        /**
         * The required columns by the sink to perform row-level delete. The rows consumed by sink
         * will contain the required columns in order. If return Optional.empty(), it will contain
         * all columns.
         */
        default Optional<List<Column>> requiredColumns() {
            return Optional.empty();
        }

        /**
         * Planner will rewrite delete statement to query base on the {@link RowLevelDeleteInfo},
         * keeping the query of delete unchanged by default(in `DELETE_ROWS` mode), or changing the
         * query to the complementary set in REMAINING_ROWS mode.
         *
         * <p>Take the following SQL as an example:
         *
         * <pre>{@code
         * DELETE FROM t WHERE y = 2;
         * }</pre>
         *
         * <p>If returns {@link SupportsRowLevelDelete.RowLevelDeleteMode#DELETED_ROWS}, the sink
         * will get the rows to be deleted which match the filter [y = 2].
         *
         * <p>If returns {@link SupportsRowLevelDelete.RowLevelDeleteMode#REMAINING_ROWS}, the sink
         * will get the rows which don't match the filter [y = 2].
         *
         * <p>Note: All rows will be of RowKind#DELETE when RowLevelDeleteMode is DELETED_ROWS, and
         * RowKind#INSERT when RowLevelDeleteMode is REMAINING_ROWS.
         */
        default RowLevelDeleteMode getRowLevelDeleteMode() {
            return RowLevelDeleteMode.DELETED_ROWS;
        }
    }

    /**
     * Type of delete modes that the sink expects for delete purpose.
     *
     * <p>Currently, two modes are supported:
     *
     * <ul>
     *   <li>DELETED_ROWS - in this mode, the sink will only get the rows that need to be deleted.
     *   <li>REMAINING_ROWS - in this mode, the sink will only get the remaining rows after
     *       deletion.
     * </ul>
     */
    @PublicEvolving
    enum RowLevelDeleteMode {
        DELETED_ROWS,

        REMAINING_ROWS
    }
}
