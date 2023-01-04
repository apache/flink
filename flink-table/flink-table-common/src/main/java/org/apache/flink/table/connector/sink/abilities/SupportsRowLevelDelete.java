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
import org.apache.flink.table.connector.sink.DynamicTableSink;

import java.util.List;
import java.util.Optional;

/**
 * Enable to delete existing data in a {@link DynamicTableSink} according to row-level changes.
 *
 * <p>The planner will call method {@link #applyRowLevelDelete} to get the {@link
 * RowLevelDeleteInfo} that the sink returns, and rewrite the delete operation based on gotten
 * {@link RowLevelDeleteInfo} to produce rows (may rows to be deleted or remaining rows after the
 * delete operation depending on sink's implementation) to {@link DynamicTableSink}. The sink is
 * expected to consume these rows to achieve rows delete purpose.
 *
 * <p>Note: if the sink also implements {@link SupportsDeletePushDown}, the planner will always
 * prefer {@link SupportsDeletePushDown}, and only the filters aren't available or {@link
 * SupportsDeletePushDown#applyDeleteFilters} returns false, this interface will be considered and
 * to rewrite the delete operation to produce the rows to the sink.
 */
@PublicEvolving
public interface SupportsRowLevelDelete {

    /**
     * Apply row level delete, and return {@link RowLevelDeleteInfo} to guide the planner on how to
     * rewrite the delete operation.
     */
    RowLevelDeleteInfo applyRowLevelDelete();

    /** The information that guides the planer on how to rewrite the delete operation. */
    @PublicEvolving
    interface RowLevelDeleteInfo {

        /**
         * The required columns that the sink expects for deletion, the rows consumed by sink will
         * contain the columns with the order consistent with the order of returned columns. If
         * return Optional.empty(), it will select all columns.
         */
        default Optional<List<Column>> requiredColumns() {
            return Optional.empty();
        }

        /**
         * Planner will rewrite delete to query base on the {@link RowLevelDeleteInfo}, keeps the
         * query of delete unchanged by default(in `DELETE_ROWS` mode), or change the query to the
         * complementary set in REMAINING_ROWS mode.
         *
         * <p>Take the following SQL as an example:
         *
         * <pre>{@code
         * DELETE FROM t WHERE y = 2;
         * }</pre>
         *
         * <p>If returns {@link SupportsRowLevelDelete.RowLevelDeleteMode#DELETED_ROWS}, the sink
         * will get rows to be deleted which match the filter [y = 2].
         *
         * <p>If returns {@link SupportsRowLevelDelete.RowLevelDeleteMode#REMAINING_ROWS}, the sink
         * will get the rows which doesn't match the filter [y = 2].
         *
         * <p>Note: All rows will have RowKind#DELETE when RowLevelDeleteMode is DELETED_ROWS, and
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
     *   <li>REMAINING_ROWS - in this mode, the sink will only get the remaining rows as if the the
     *       delete operation had been done.
     * </ul>
     */
    @PublicEvolving
    enum RowLevelDeleteMode {
        DELETED_ROWS,
        REMAINING_ROWS
    }
}
