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
}
