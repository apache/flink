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
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * A table that has been partitioned by a set of partition keys.
 *
 * <p>Currently, partitioned table objects are intended for table arguments of process table
 * functions (PTFs) that take table arguments with set semantics ({@link
 * ArgumentTrait#TABLE_AS_SET}).
 *
 * @see ProcessTableFunction
 */
@PublicEvolving
public interface PartitionedTable {

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
     * @see ProcessTableFunction
     * @return an expression that can be passed into {@link TableEnvironment#fromCall}.
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
}
