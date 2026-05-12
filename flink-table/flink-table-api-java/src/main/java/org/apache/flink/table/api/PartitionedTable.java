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
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * A table that has been partitioned by a set of partition keys.
 *
 * <p>Currently, partitioned table objects are intended for table arguments of process table
 * functions (PTFs) that take table arguments with set semantics ({@link
 * ArgumentTrait#SET_SEMANTIC_TABLE}).
 *
 * @see ProcessTableFunction
 */
@PublicEvolving
public interface PartitionedTable {

    /**
     * Sorts the partitioned table within each partition.
     *
     * <p>This is useful for process table functions that need ordered input within each set scoped
     * by the key.
     *
     * <p>Note: The first sorting column must be a time attribute column in ascending order for
     * which a watermark has been declared. The watermarked column must be forwarded without any
     * modification to ensure event-time sorting. Secondary ordering columns can differ and are used
     * for deterministic ordering within the same timestamp.
     *
     * <p>Example:
     *
     * <pre>{@code
     * // Primary ordering by watermarked timestamp, secondary by score
     * env.fromCall(
     *   "MyPTF",
     *   table
     *     .partitionBy($("key"))
     *     .orderBy($("ts").asc(), $("score").desc())
     *     .asArgument("input_table")
     * )
     * }</pre>
     *
     * @param fields expressions for ordering (e.g., {@code $("ts").asc(), $("score").desc()})
     * @return a partitioned and ordered table
     * @see ProcessTableFunction
     */
    PartitionedTable orderBy(Expression... fields);

    /**
     * Converts this table object into a named argument.
     *
     * <p>This method is intended for calls to process table functions (PTFs) that take table
     * arguments.
     *
     * <p>Example:
     *
     * <pre>{@code
     * env.fromCall(
     *   "MyPTF",
     *   table.partitionBy($("key")).asArgument("input_table")
     * )
     * }</pre>
     *
     * @return an expression that can be passed into {@link TableEnvironment#fromCall}.
     * @see ProcessTableFunction
     */
    ApiExpression asArgument(String name);

    /**
     * Transforms the given table by passing it into a process table function (PTF) with set
     * semantics.
     *
     * <p>A PTF maps zero, one, or multiple tables to a new table. PTFs are the most powerful
     * function kind for Flink SQL and Table API. They enable implementing user-defined operators
     * that can be as feature-rich as built-in operations. PTFs have access to Flink's managed
     * state, event-time and timer services, and underlying table changelogs.
     *
     * <p>This method assumes a call to a previously registered function that takes exactly one
     * table argument with set semantics as the first argument. Additional scalar arguments can be
     * passed if necessary. Thus, this method is a shortcut for:
     *
     * <pre>{@code
     * env.fromCall(
     *   "MyPTF",
     *   THIS_TABLE,
     *   SOME_SCALAR_ARGUMENTS...
     * );
     * }</pre>
     *
     * <p>Example:
     *
     * <pre>{@code
     * env.createFunction("MyPTF", MyPTF.class);
     *
     * Table table = table
     *   .partitionBy($("key"))
     *   .process(
     *     "MyPTF"
     *     lit("Bob").asArgument("default_name"),
     *     lit(42).asArgument("default_threshold")
     *   );
     * }</pre>
     *
     * @param path The path of a function
     * @param arguments Table and scalar argument {@link Expressions}.
     * @return The {@link Table} object describing the pipeline for further transformations.
     * @see Expressions#call(String, Object...)
     * @see ProcessTableFunction
     */
    Table process(String path, Object... arguments);

    /**
     * Transforms the given table by passing it into a process table function (PTF) with set
     * semantics.
     *
     * <p>A PTF maps zero, one, or multiple tables to a new table. PTFs are the most powerful
     * function kind for Flink SQL and Table API. They enable implementing user-defined operators
     * that can be as feature-rich as built-in operations. PTFs have access to Flink's managed
     * state, event-time and timer services, and underlying table changelogs.
     *
     * <p>This method assumes a call to an unregistered, inline function that takes exactly one
     * table argument with set semantics as the first argument. Additional scalar arguments can be
     * passed if necessary. Thus, this method is a shortcut for:
     *
     * <pre>{@code
     * env.fromCall(
     *   MyPTF.class,
     *   THIS_TABLE,
     *   SOME_SCALAR_ARGUMENTS...
     * );
     * }</pre>
     *
     * <p>Example:
     *
     * <pre>{@code
     * Table table = table
     *   .partitionBy($("key"))
     *   .process(
     *     MyPTF.class,
     *     lit("Bob").asArgument("default_name"),
     *     lit(42).asArgument("default_threshold")
     *   );
     * }</pre>
     *
     * @param function The class containing the function's logic.
     * @param arguments Table and scalar argument {@link Expressions}.
     * @return The {@link Table} object describing the pipeline for further transformations.
     * @see Expressions#call(String, Object...)
     * @see ProcessTableFunction
     */
    Table process(Class<? extends UserDefinedFunction> function, Object... arguments);

    /**
     * Converts this partitioned dynamic table into an append-only table with an explicit operation
     * code column using the built-in {@code TO_CHANGELOG} process table function with set
     * semantics.
     *
     * <p>Each input row - regardless of its original change operation - is emitted as an
     * INSERT-only row with a string {@code "op"} column indicating the original operation (INSERT,
     * UPDATE_AFTER, DELETE, etc.). With set semantics, rows for the same partition key are
     * co-located in the same parallel operator instance.
     *
     * <p>For row semantics (each row processed independently), use {@link Table#toChangelog} on the
     * unpartitioned table.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * // Default: adds 'op' column and supports all changelog modes
     * Table result = table.partitionBy($("id")).toChangelog();
     *
     * // Custom op column name and mapping
     * Table result = table
     *     .partitionBy($("id"))
     *     .toChangelog(
     *         descriptor("op_code").asArgument("op"),
     *         map("INSERT", "I", "UPDATE_AFTER", "U").asArgument("op_mapping")
     *     );
     *
     * // Deletion flag pattern: comma-separated keys map multiple change operations to the same code
     * Table result = table
     *     .partitionBy($("id"))
     *     .toChangelog(
     *         descriptor("deleted").asArgument("op"),
     *         map("INSERT, UPDATE_AFTER", "false", "DELETE", "true").asArgument("op_mapping")
     *     );
     * }</pre>
     *
     * @param arguments optional named arguments for {@code op} and {@code op_mapping}
     * @return an append-only {@link Table} with output schema {@code [partition_keys, op,
     *     non_partition_input_columns]}
     * @see Table#toChangelog(Expression...)
     */
    Table toChangelog(Expression... arguments);

    /**
     * Converts this partitioned append-only table with an explicit operation code column into a
     * (potentially updating) dynamic table using the built-in {@code FROM_CHANGELOG} process table
     * function with set semantics.
     *
     * <p>Each input row is expected to have a string column that indicates the change operation.
     * The operation column is interpreted by the engine and removed from the output. With set
     * semantics, rows for the same partition key are co-located in the same parallel operator
     * instance, which is required when downstream operators are keyed on that column.
     *
     * <p>For row semantics (each row processed independently), use {@link Table#fromChangelog} on
     * the unpartitioned table.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * // Default: reads 'op' column with standard change operation names
     * Table result = cdcStream.partitionBy($("id")).fromChangelog();
     *
     * // With custom op column name
     * Table result = cdcStream
     *     .partitionBy($("id"))
     *     .fromChangelog(descriptor("operation").asArgument("op"));
     *
     * // With custom op_mapping
     * Table result = cdcStream
     *     .partitionBy($("id"))
     *     .fromChangelog(
     *         descriptor("op").asArgument("op"),
     *         map("c, r", "INSERT",
     *             "ub", "UPDATE_BEFORE",
     *             "ua", "UPDATE_AFTER",
     *             "d", "DELETE").asArgument("op_mapping")
     *     );
     *
     * // Silently skip rows with NULL or unmapped op codes instead of failing
     * Table result = cdcStream
     *     .partitionBy($("id"))
     *     .fromChangelog(lit("SKIP").asArgument("error_handling"));
     * }</pre>
     *
     * @param arguments optional named arguments for {@code op}, {@code op_mapping}, and {@code
     *     error_handling}
     * @return a dynamic {@link Table} with output schema {@code [partition_keys,
     *     non_partition_non_op_input_columns]}
     * @see Table#fromChangelog(Expression...)
     */
    Table fromChangelog(Expression... arguments);
}
