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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Optional;

/**
 * Provides call information about the table that has been passed to a table argument.
 *
 * <p>This class is only available for table arguments (i.e. arguments of a {@link
 * ProcessTableFunction} that are annotated with {@code @ArgumentHint(SET_SEMANTIC_TABLE)} or
 * {@code @ArgumentHint(ROW_SEMANTIC_TABLE)}).
 */
@PublicEvolving
public interface TableSemantics {

    /**
     * Data type of the passed table.
     *
     * <p>The returned data type might be the one that has been explicitly defined for the argument
     * or a {@link DataTypes#ROW} data type for polymorphic arguments that accept any type of row.
     *
     * <p>For example:
     *
     * <pre>{@code
     * // Function with explicit table argument type of row
     * class MyPTF extends ProcessTableFunction<String> {
     *   public void eval(Context ctx, @ArgumentHint(value = ArgumentTrait.SET_SEMANTIC_TABLE, type = "ROW < s STRING >") Row t) {
     *     TableSemantics semantics = ctx.tableSemanticsFor("t");
     *     // Always returns "ROW < s STRING >"
     *     semantics.dataType();
     *     ...
     *   }
     * }
     *
     * // Function with explicit table argument type of structured type "Customer"
     * class MyPTF extends ProcessTableFunction<String> {
     *   public void eval(Context ctx, @ArgumentHint(value = ArgumentTrait.SET_SEMANTIC_TABLE) Customer c) {
     *     TableSemantics semantics = ctx.tableSemanticsFor("c");
     *     // Always returns structured type of "Customer"
     *     semantics.dataType();
     *     ...
     *   }
     * }
     *
     * // Function with polymorphic table argument
     * class MyPTF extends ProcessTableFunction<String> {
     *   public void eval(Context ctx, @ArgumentHint(value = ArgumentTrait.SET_SEMANTIC_TABLE) Row t) {
     *     TableSemantics semantics = ctx.tableSemanticsFor("t");
     *     // Always returns "ROW" but content depends on the table that is passed into the call
     *     semantics.dataType();
     *     ...
     *   }
     * }
     * }</pre>
     */
    DataType dataType();

    /**
     * Returns information about how the passed table is partitioned. Applies only to table
     * arguments with set semantics.
     *
     * @return An array of indexes (0-based) that specify the PARTITION BY columns.
     */
    int[] partitionByColumns();

    /**
     * Returns information about how the passed table is ordered. Applies only to table arguments
     * with set semantics.
     *
     * @return An array of indexes (0-based) that specify the ORDER BY columns.
     */
    int[] orderByColumns();

    /**
     * Returns information about the sort direction for each ORDER BY column. Applies only to table
     * arguments with set semantics.
     *
     * <p>The returned array has the same length as {@link #orderByColumns()} and each element
     * corresponds to the sort direction of the column at the same index.
     *
     * @return An array of {@link SortDirection} values corresponding to the ORDER BY columns.
     */
    SortDirection[] orderByDirections();

    /**
     * Returns information about the time attribute of the passed table. The time attribute column
     * powers the concept of rowtime and timers. Applies to both table arguments with row and set
     * semantics.
     *
     * @return Position of the "ON_TIME" column. Returns -1 in case no time attribute has been
     *     passed. Returns an actual value when called during runtime. Returns empty during type
     *     inference phase as the time attribute is still unknown.
     */
    int timeColumn();

    /**
     * Actual changelog mode for the passed table. By default, table arguments take only {@link
     * ChangelogMode#insertOnly()}. They are able to take tables of other changelog modes, if
     * specified to do so (e.g. via an {@link ArgumentTrait}). This method returns the final
     * changelog mode determined by the planner.
     *
     * @return The definitive changelog mode expected for the passed table after physical
     *     optimization. Returns an actual value when called during runtime. Returns empty during
     *     type inference phase as the changelog mode is still unknown.
     */
    Optional<ChangelogMode> changelogMode();

    /**
     * Upsert key candidates derived from the passed table's metadata.
     *
     * <p>Returns a list of 0-based column index arrays that uniquely identify a row for upsert
     * semantics. Useful for functions that need to emit key-only deletes, match UPDATE_BEFORE /
     * UPDATE_AFTER pairs, or use a stable identifier to interact with state.
     *
     * <p>The upsert key describes row identity and is distinct from {@link #partitionByColumns()},
     * which describes distribution and co-location. They are independent and frequently disagree:
     *
     * <pre>{@code
     * -- Source declares PRIMARY KEY (id); the call partitions by region.
     * -- partitionByColumns() = [region]   (chosen by the caller)
     * -- upsertKeyColumns()   = [[id]]     (derived from the source's PK)
     * TO_CHANGELOG(input => TABLE source PARTITION BY region)
     * }</pre>
     *
     * <p>Returns an empty list when no upsert key is derivable, or when the planner has not yet
     * computed metadata (during type inference).
     *
     * <p>The planner may derive more than one candidate for the same input. For example, an inner
     * join of two tables each carrying their own primary key produces a result with both keys as
     * separate candidates. Picking which candidate to use is the function's responsibility, and the
     * choice must be stable across releases so PTF state stays consistent after job restarts and
     * upgrades. The order of the returned list is not part of the contract; PTF authors should not
     * depend on it. A typical stable choice is the smallest candidate by cardinality, with ties
     * broken by the column indices in ascending order.
     *
     * @return Candidate upsert keys of the passed table, or an empty list if none.
     */
    List<int[]> upsertKeyColumns();

    /** The sort direction for ORDER BY columns in table arguments with set semantics. */
    @PublicEvolving
    enum SortDirection {
        /** Ascending order with nulls first. */
        ASC_NULLS_FIRST(false, true),
        /** Ascending order with nulls last. */
        ASC_NULLS_LAST(false, false),
        /** Descending order with nulls first. */
        DESC_NULLS_FIRST(true, true),
        /** Descending order with nulls last. */
        DESC_NULLS_LAST(true, false);

        private final boolean descending;
        private final boolean nullsFirst;

        SortDirection(boolean descending, boolean nullsFirst) {
            this.descending = descending;
            this.nullsFirst = nullsFirst;
        }

        /** Returns true if this is a descending sort direction. */
        public boolean isDescending() {
            return descending;
        }

        /** Returns true if nulls should be sorted first. */
        public boolean isNullsFirst() {
            return nullsFirst;
        }

        /** Returns true if nulls should be sorted last. */
        public boolean isNullsLast() {
            return !nullsFirst;
        }
    }
}
