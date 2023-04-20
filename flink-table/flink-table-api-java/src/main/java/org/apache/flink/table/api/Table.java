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
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.DataType;

/**
 * The {@link Table} object is the core abstraction of the Table API. Similar to how the DataStream
 * API has {@code DataStream}s, the Table API is built around {@link Table}s.
 *
 * <p>A {@link Table} object describes a pipeline of data transformations. It does not contain the
 * data itself in any way. Instead, it describes how to read data from a {@link DynamicTableSource}
 * and how to eventually write data to a {@link DynamicTableSink}. The declared pipeline can be
 * printed, optimized, and eventually executed in a cluster. The pipeline can work with bounded or
 * unbounded streams which enables both streaming and batch scenarios.
 *
 * <p>By the definition above, a {@link Table} object can actually be considered as a <i>view</i> in
 * SQL terms.
 *
 * <p>The initial {@link Table} object is constructed by a {@link TableEnvironment}. For example,
 * {@link TableEnvironment#from(String)}) obtains a table from a catalog. Every {@link Table} object
 * has a schema that is available through {@link #getResolvedSchema()}. A {@link Table} object is
 * always associated with its original table environment during programming.
 *
 * <p>Every transformation (i.e. {@link #select(Expression...)} or {@link #filter(Expression)}) on a
 * {@link Table} object leads to a new {@link Table} object.
 *
 * <p>Use {@link #execute()} to execute the pipeline and retrieve the transformed data locally
 * during development. Otherwise, use {@link #executeInsert(String)} to write the data into a table
 * sink.
 *
 * <p>Many methods of this class take one or more {@link Expression}s as parameters. For fluent
 * definition of expressions and easier readability, we recommend to add a star import:
 *
 * <pre>
 *  import static org.apache.flink.table.api.Expressions.*;
 * </pre>
 *
 * <p>Check the documentation for more programming language specific APIs, for example, by using
 * Scala implicits.
 *
 * <p>The following example shows how to work with a {@link Table} object.
 *
 * <p>Java Example (with static import for expressions):
 *
 * <pre>{@code
 * TableEnvironment tableEnv = TableEnvironment.create(...);
 *
 * Table table = tableEnv.from("MyTable").select($("colA").trim(), $("colB").plus(12));
 *
 * table.execute().print();
 * }</pre>
 *
 * <p>Scala Example (with implicits for expressions):
 *
 * <pre>{@code
 * val tableEnv = TableEnvironment.create(...)
 *
 * val table = tableEnv.from("MyTable").select($"colA".trim(), $"colB" + 12)
 *
 * table.execute().print()
 * }</pre>
 */
@PublicEvolving
public interface Table extends Explainable<Table>, Executable {

    /**
     * Returns the schema of this table.
     *
     * @deprecated This method has been deprecated as part of FLIP-164. {@link TableSchema} has been
     *     replaced by two more dedicated classes {@link Schema} and {@link ResolvedSchema}. Use
     *     {@link Schema} for declaration in APIs. {@link ResolvedSchema} is offered by the
     *     framework after resolution and validation.
     */
    @Deprecated
    default TableSchema getSchema() {
        return TableSchema.fromResolvedSchema(getResolvedSchema());
    }

    /** Returns the resolved schema of this table. */
    ResolvedSchema getResolvedSchema();

    /** Prints the schema of this table to the console in a summary format. */
    void printSchema();

    /** Returns underlying logical representation of this table. */
    QueryOperation getQueryOperation();

    /**
     * Performs a selection operation. Similar to a SQL SELECT statement. The field expressions can
     * contain complex expressions and aggregations.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.select($("key"), $("value").avg().plus(" The average").as("average"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * tab.select($"key", $"value".avg + " The average" as "average")
     * }</pre>
     */
    Table select(Expression... fields);

    /**
     * Creates {@link TemporalTableFunction} backed up by this table as a history table. Temporal
     * Tables represent a concept of a table that changes over time and for which Flink keeps track
     * of those changes. {@link TemporalTableFunction} provides a way how to access those data.
     *
     * <p>For more information please check Flink's documentation on Temporal Tables.
     *
     * <p>Currently {@link TemporalTableFunction}s are only supported in streaming.
     *
     * @param timeAttribute Must points to a time indicator. Provides a way to compare which records
     *     are a newer or older version.
     * @param primaryKey Defines the primary key. With primary key it is possible to update a row or
     *     to delete it.
     * @return {@link TemporalTableFunction} which is an instance of {@link TableFunction}. It takes
     *     one single argument, the {@code timeAttribute}, for which it returns matching version of
     *     the {@link Table}, from which {@link TemporalTableFunction} was created.
     */
    TemporalTableFunction createTemporalTableFunction(
            Expression timeAttribute, Expression primaryKey);

    /**
     * Renames the fields of the expression result. Use this to disambiguate fields before joining
     * to operations.
     *
     * <p>Example:
     *
     * <pre>{@code
     * tab.as("a", "b")
     * }</pre>
     */
    Table as(String field, String... fields);

    /**
     * Renames the fields of the expression result. Use this to disambiguate fields before joining
     * to operations.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.as($("a"), $("b"))
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * tab.as($"a", $"b")
     * }</pre>
     *
     * @deprecated use {@link #as(String, String...)}
     */
    @Deprecated
    Table as(Expression... fields);

    /**
     * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE clause.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.filter($("name").isEqual("Fred"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * tab.filter($"name" === "Fred")
     * }</pre>
     */
    Table filter(Expression predicate);

    /**
     * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE clause.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.where($("name").isEqual("Fred"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * tab.where($"name" === "Fred")
     * }</pre>
     */
    Table where(Expression predicate);

    /**
     * Groups the elements on some grouping keys. Use this before a selection with aggregations to
     * perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.groupBy($("key")).select($("key"), $("value").avg());
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * tab.groupBy($"key").select($"key", $"value".avg)
     * }</pre>
     */
    GroupedTable groupBy(Expression... fields);

    /**
     * Removes duplicate values and returns only distinct (different) values.
     *
     * <p>Example:
     *
     * <pre>{@code
     * tab.select($("key"), $("value")).distinct();
     * }</pre>
     */
    Table distinct();

    /**
     * Joins two {@link Table}s. Similar to a SQL join. The fields of the two joined operations must
     * not overlap, use {@code as} to rename fields if necessary. You can use where and select
     * clauses after a join to further specify the behaviour of the join.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment} .
     *
     * <p>Example:
     *
     * <pre>{@code
     * left.join(right)
     *     .where($("a").isEqual($("b")).and($("c").isGreater(3))
     *     .select($("a"), $("b"), $("d"));
     * }</pre>
     */
    Table join(Table right);

    /**
     * Joins two {@link Table}s. Similar to a SQL join. The fields of the two joined operations must
     * not overlap, use {@code as} to rename fields if necessary.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment} .
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * left.join(right, $("a").isEqual($("b")))
     *     .select($("a"), $("b"), $("d"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * left.join(right, $"a" === $"b")
     *     .select($"a", $"b", $"d")
     * }</pre>
     */
    Table join(Table right, Expression joinPredicate);

    /**
     * Joins two {@link Table}s. Similar to a SQL left outer join. The fields of the two joined
     * operations must not overlap, use {@code as} to rename fields if necessary.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its {@code
     * TableConfig} must have null check enabled (default).
     *
     * <p>Example:
     *
     * <pre>{@code
     * left.leftOuterJoin(right)
     *     .select($("a"), $("b"), $("d"));
     * }</pre>
     */
    Table leftOuterJoin(Table right);

    /**
     * Joins two {@link Table}s. Similar to a SQL left outer join. The fields of the two joined
     * operations must not overlap, use {@code as} to rename fields if necessary.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its {@code
     * TableConfig} must have null check enabled (default).
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * left.leftOuterJoin(right, $("a").isEqual($("b")))
     *     .select($("a"), $("b"), $("d"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * left.leftOuterJoin(right, $"a" === $"b")
     *     .select($"a", $"b", $"d")
     * }</pre>
     */
    Table leftOuterJoin(Table right, Expression joinPredicate);

    /**
     * Joins two {@link Table}s. Similar to a SQL right outer join. The fields of the two joined
     * operations must not overlap, use {@code as} to rename fields if necessary.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its {@code
     * TableConfig} must have null check enabled (default).
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * left.rightOuterJoin(right, $("a").isEqual($("b")))
     *     .select($("a"), $("b"), $("d"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * left.rightOuterJoin(right, $"a" === $"b")
     *     .select($"a", $"b", $"d")
     * }</pre>
     */
    Table rightOuterJoin(Table right, Expression joinPredicate);

    /**
     * Joins two {@link Table}s. Similar to a SQL full outer join. The fields of the two joined
     * operations must not overlap, use {@code as} to rename fields if necessary.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its {@code
     * TableConfig} must have null check enabled (default).
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * left.fullOuterJoin(right, $("a").isEqual($("b")))
     *     .select($("a"), $("b"), $("d"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * left.fullOuterJoin(right, $"a" === $"b")
     *     .select($"a", $"b", $"d")
     * }</pre>
     */
    Table fullOuterJoin(Table right, Expression joinPredicate);

    /**
     * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
     * a SQL inner join with ON TRUE predicate but works with a table function. Each row of the
     * table is joined with all rows produced by the table function.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * class MySplitUDTF extends TableFunction<String> {
     *   public void eval(String str) {
     *     str.split("#").forEach(this::collect);
     *   }
     * }
     *
     * table.joinLateral(call(MySplitUDTF.class, $("c")).as("s"))
     *      .select($("a"), $("b"), $("c"), $("s"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * class MySplitUDTF extends TableFunction[String] {
     *   def eval(str: String): Unit = {
     *     str.split("#").foreach(collect)
     *   }
     * }
     *
     * val split = new MySplitUDTF()
     * table.joinLateral(split($"c") as "s")
     *      .select($"a", $"b", $"c", $"s")
     * }</pre>
     */
    Table joinLateral(Expression tableFunctionCall);

    /**
     * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
     * a SQL inner join but works with a table function. Each row of the table is joined with all
     * rows produced by the table function.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * class MySplitUDTF extends TableFunction<String> {
     *   public void eval(String str) {
     *     str.split("#").forEach(this::collect);
     *   }
     * }
     *
     * table.joinLateral(call(MySplitUDTF.class, $("c")).as("s"), $("a").isEqual($("s")))
     *      .select($("a"), $("b"), $("c"), $("s"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * class MySplitUDTF extends TableFunction[String] {
     *   def eval(str: String): Unit = {
     *     str.split("#").foreach(collect)
     *   }
     * }
     *
     * val split = new MySplitUDTF()
     * table.joinLateral(split($"c") as "s", $"a" === $"s")
     *      .select($"a", $"b", $"c", $"s")
     * }</pre>
     */
    Table joinLateral(Expression tableFunctionCall, Expression joinPredicate);

    /**
     * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
     * a SQL left outer join with ON TRUE predicate but works with a table function. Each row of the
     * table is joined with all rows produced by the table function. If the table function does not
     * produce any row, the outer row is padded with nulls.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * class MySplitUDTF extends TableFunction<String> {
     *   public void eval(String str) {
     *     str.split("#").forEach(this::collect);
     *   }
     * }
     *
     * table.leftOuterJoinLateral(call(MySplitUDTF.class, $("c")).as("s"))
     *      .select($("a"), $("b"), $("c"), $("s"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * class MySplitUDTF extends TableFunction[String] {
     *   def eval(str: String): Unit = {
     *     str.split("#").foreach(collect)
     *   }
     * }
     *
     * val split = new MySplitUDTF()
     * table.leftOuterJoinLateral(split($"c") as "s")
     *      .select($"a", $"b", $"c", $"s")
     * }</pre>
     */
    Table leftOuterJoinLateral(Expression tableFunctionCall);

    /**
     * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
     * a SQL left outer join with ON TRUE predicate but works with a table function. Each row of the
     * table is joined with all rows produced by the table function. If the table function does not
     * produce any row, the outer row is padded with nulls.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * class MySplitUDTF extends TableFunction<String> {
     *   public void eval(String str) {
     *     str.split("#").forEach(this::collect);
     *   }
     * }
     *
     * table.leftOuterJoinLateral(call(MySplitUDTF.class, $("c")).as("s"), $("a").isEqual($("s")))
     *      .select($("a"), $("b"), $("c"), $("s"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * class MySplitUDTF extends TableFunction[String] {
     *   def eval(str: String): Unit = {
     *     str.split("#").foreach(collect)
     *   }
     * }
     *
     * val split = new MySplitUDTF()
     * table.leftOuterJoinLateral(split($"c") as "s", $"a" === $"s")
     *      .select($"a", $"b", $"c", $"s")
     * }</pre>
     */
    Table leftOuterJoinLateral(Expression tableFunctionCall, Expression joinPredicate);

    /**
     * Minus of two {@link Table}s with duplicate records removed. Similar to a SQL EXCEPT clause.
     * Minus returns records from the left table that do not exist in the right table. Duplicate
     * records in the left table are returned exactly once, i.e., duplicates are removed. Both
     * tables must have identical field types.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * left.minus(right);
     * }</pre>
     */
    Table minus(Table right);

    /**
     * Minus of two {@link Table}s. Similar to a SQL EXCEPT ALL. Similar to a SQL EXCEPT ALL clause.
     * MinusAll returns the records that do not exist in the right table. A record that is present n
     * times in the left table and m times in the right table is returned (n - m) times, i.e., as
     * many duplicates as are present in the right table are removed. Both tables must have
     * identical field types.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * left.minusAll(right);
     * }</pre>
     */
    Table minusAll(Table right);

    /**
     * Unions two {@link Table}s with duplicate records removed. Similar to a SQL UNION. The fields
     * of the two union operations must fully overlap.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * left.union(right);
     * }</pre>
     */
    Table union(Table right);

    /**
     * Unions two {@link Table}s. Similar to a SQL UNION ALL. The fields of the two union operations
     * must fully overlap.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * left.unionAll(right);
     * }</pre>
     */
    Table unionAll(Table right);

    /**
     * Intersects two {@link Table}s with duplicate records removed. Intersect returns records that
     * exist in both tables. If a record is present in one or both tables more than once, it is
     * returned just once, i.e., the resulting table has no duplicate records. Similar to a SQL
     * INTERSECT. The fields of the two intersect operations must fully overlap.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * left.intersect(right);
     * }</pre>
     */
    Table intersect(Table right);

    /**
     * Intersects two {@link Table}s. IntersectAll returns records that exist in both tables. If a
     * record is present in both tables more than once, it is returned as many times as it is
     * present in both tables, i.e., the resulting table might have duplicate records. Similar to an
     * SQL INTERSECT ALL. The fields of the two intersect operations must fully overlap.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * left.intersectAll(right);
     * }</pre>
     */
    Table intersectAll(Table right);

    /**
     * Sorts the given {@link Table}. Similar to SQL {@code ORDER BY}.
     *
     * <p>The resulting Table is globally sorted across all parallel partitions.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.orderBy($("name").desc());
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * tab.orderBy($"name".desc)
     * }</pre>
     *
     * <p>For unbounded tables, this operation requires a sorting on a time attribute or a
     * subsequent fetch operation.
     */
    Table orderBy(Expression... fields);

    /**
     * Limits a (possibly sorted) result from an offset position.
     *
     * <p>This method can be combined with a preceding {@link #orderBy(Expression...)} call for a
     * deterministic order and a subsequent {@link #fetch(int)} call to return n rows after skipping
     * the first o rows.
     *
     * <pre>{@code
     * // skips the first 3 rows and returns all following rows.
     * tab.orderBy($("name").desc()).offset(3);
     * // skips the first 10 rows and returns the next 5 rows.
     * tab.orderBy($("name").desc()).offset(10).fetch(5);
     * }</pre>
     *
     * <p>For unbounded tables, this operation requires a subsequent fetch operation.
     *
     * @param offset number of records to skip
     */
    Table offset(int offset);

    /**
     * Limits a (possibly sorted) result to the first n rows.
     *
     * <p>This method can be combined with a preceding {@link #orderBy(Expression...)} call for a
     * deterministic order and {@link #offset(int)} call to return n rows after skipping the first o
     * rows.
     *
     * <pre>{@code
     * // returns the first 3 records.
     * tab.orderBy($("name").desc()).fetch(3);
     * // skips the first 10 rows and returns the next 5 rows.
     * tab.orderBy($("name").desc()).offset(10).fetch(5);
     * }</pre>
     *
     * @param fetch the number of records to return. Fetch must be >= 0.
     */
    Table fetch(int fetch);

    /**
     * Limits a (possibly sorted) result to the first n rows.
     *
     * <p>This method is a synonym for {@link #fetch(int)}.
     */
    default Table limit(int fetch) {
        return fetch(fetch);
    }

    /**
     * Limits a (possibly sorted) result to the first n rows from an offset position.
     *
     * <p>This method is a synonym for {@link #offset(int)} followed by {@link #fetch(int)}.
     */
    default Table limit(int offset, int fetch) {
        return offset(offset).fetch(fetch);
    }

    /**
     * Groups the records of a table by assigning them to windows defined by a time or row interval.
     *
     * <p>For streaming tables of infinite size, grouping into windows is required to define finite
     * groups on which group-based aggregates can be computed.
     *
     * <p>For batch tables of finite size, windowing essentially provides shortcuts for time-based
     * groupBy.
     *
     * <p><b>Note</b>: Computing windowed aggregates on a streaming table is only a parallel
     * operation if additional grouping attributes are added to the {@code groupBy(...)} clause. If
     * the {@code groupBy(...)} only references a GroupWindow alias, the streamed table will be
     * processed by a single task, i.e., with parallelism 1.
     *
     * @param groupWindow groupWindow that specifies how elements are grouped.
     * @return A windowed table.
     */
    GroupWindowedTable window(GroupWindow groupWindow);

    /**
     * Defines over-windows on the records of a table.
     *
     * <p>An over-window defines for each record an interval of records over which aggregation
     * functions can be computed.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * table
     *   .window(Over.partitionBy($("c")).orderBy($("rowTime")).preceding(lit(10).seconds()).as("ow")
     *   .select($("c"), $("b").count().over($("ow")), $("e").sum().over($("ow")));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * table
     *   .window(Over partitionBy $"c" orderBy $"rowTime" preceding 10.seconds as "ow")
     *   .select($"c", $"b".count over $"ow", $"e".sum over $"ow")
     * }</pre>
     *
     * <p><b>Note</b>: Computing over window aggregates on a streaming table is only a parallel
     * operation if the window is partitioned. Otherwise, the whole stream will be processed by a
     * single task, i.e., with parallelism 1.
     *
     * <p><b>Note</b>: Over-windows for batch tables are currently not supported.
     *
     * @param overWindows windows that specify the record interval over which aggregations are
     *     computed.
     * @return An OverWindowedTable to specify the aggregations.
     */
    OverWindowedTable window(OverWindow... overWindows);

    /**
     * Adds additional columns. Similar to a SQL SELECT statement. The field expressions can contain
     * complex expressions, but can not contain aggregations. It will throw an exception if the
     * added fields already exist.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.addColumns(
     *    $("a").plus(1).as("a1"),
     *    concat($("b"), "sunny").as("b1")
     * );
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * tab.addColumns(
     *    $"a" + 1 as "a1",
     *    concat($"b", "sunny") as "b1"
     * )
     * }</pre>
     */
    Table addColumns(Expression... fields);

    /**
     * Adds additional columns. Similar to a SQL SELECT statement. The field expressions can contain
     * complex expressions, but can not contain aggregations. Existing fields will be replaced. If
     * the added fields have duplicate field name, then the last one is used.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.addOrReplaceColumns(
     *    $("a").plus(1).as("a1"),
     *    concat($("b"), "sunny").as("b1")
     * );
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * tab.addOrReplaceColumns(
     *    $"a" + 1 as "a1",
     *    concat($"b", "sunny") as "b1"
     * )
     * }</pre>
     */
    Table addOrReplaceColumns(Expression... fields);

    /**
     * Renames existing columns. Similar to a field alias statement. The field expressions should be
     * alias expressions, and only the existing fields can be renamed.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.renameColumns(
     *    $("a").as("a1"),
     *    $("b").as("b1")
     * );
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * tab.renameColumns(
     *    $"a" as "a1",
     *    $"b" as "b1"
     * )
     * }</pre>
     */
    Table renameColumns(Expression... fields);

    /**
     * Drops existing columns. The field expressions should be field reference expressions.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.dropColumns($("a"), $("b"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * tab.dropColumns($"a", $"b")
     * }</pre>
     */
    Table dropColumns(Expression... fields);

    /**
     * Performs a map operation with an user-defined scalar function or built-in scalar function.
     * The output will be flattened if the output type is a composite type.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.map(call(MyMapFunction.class, $("c")))
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * val func = new MyMapFunction()
     * tab.map(func($"c"))
     * }</pre>
     */
    Table map(Expression mapFunction);

    /**
     * Performs a flatMap operation with an user-defined table function or built-in table function.
     * The output will be flattened if the output type is a composite type.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.flatMap(call(MyFlatMapFunction.class, $("c")))
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * val func = new MyFlatMapFunction()
     * tab.flatMap(func($"c"))
     * }</pre>
     */
    Table flatMap(Expression tableFunction);

    /**
     * Performs a global aggregate operation with an aggregate function. You have to close the
     * {@link #aggregate(Expression)} with a select statement. The output will be flattened if the
     * output type is a composite type.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.aggregate(call(MyAggregateFunction.class, $("a"), $("b")).as("f0", "f1", "f2"))
     *   .select($("f0"), $("f1"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * val aggFunc = new MyAggregateFunction
     * table.aggregate(aggFunc($"a", $"b") as ("f0", "f1", "f2"))
     *   .select($"f0", $"f1")
     * }</pre>
     */
    AggregatedTable aggregate(Expression aggregateFunction);

    /**
     * Perform a global flatAggregate without groupBy. FlatAggregate takes a TableAggregateFunction
     * which returns multiple rows. Use a selection after the flatAggregate.
     *
     * <p>Java Example:
     *
     * <pre>{@code
     * tab.flatAggregate(call(MyTableAggregateFunction.class, $("a"), $("b")).as("x", "y", "z"))
     *   .select($("x"), $("y"), $("z"));
     * }</pre>
     *
     * <p>Scala Example:
     *
     * <pre>{@code
     * val tableAggFunc: TableAggregateFunction = new MyTableAggregateFunction
     * tab.flatAggregate(tableAggFunc($"a", $"b") as ("x", "y", "z"))
     *   .select($"x", $"y", $"z")
     * }</pre>
     */
    FlatAggregateTable flatAggregate(Expression tableAggregateFunction);

    /**
     * Declares that the pipeline defined by the given {@link Table} object should be written to a
     * table (backed by a {@link DynamicTableSink}) that was registered under the specified path.
     *
     * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or {@link
     * TableEnvironment#useCatalog(String)} for the rules on the path resolution.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Table table = tableEnv.sqlQuery("SELECT * FROM MyTable");
     * TablePipeline tablePipeline = table.insertInto("MySinkTable");
     * TableResult tableResult = tablePipeline.execute();
     * tableResult.await();
     * }</pre>
     *
     * <p>One can execute the returned {@link TablePipeline} using {@link TablePipeline#execute()},
     * or compile it to a {@link CompiledPlan} using {@link TablePipeline#compilePlan()}.
     *
     * <p>If multiple pipelines should insert data into one or more sink tables as part of a single
     * execution, use a {@link StatementSet} (see {@link TableEnvironment#createStatementSet()}).
     *
     * @param tablePath The path of the registered table (backed by a {@link DynamicTableSink}).
     * @return The complete pipeline from one or more source tables to a sink table.
     */
    TablePipeline insertInto(String tablePath);

    /**
     * Declares that the pipeline defined by the given {@link Table} object should be written to a
     * table (backed by a {@link DynamicTableSink}) that was registered under the specified path.
     *
     * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or {@link
     * TableEnvironment#useCatalog(String)} for the rules on the path resolution.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Table table = tableEnv.sqlQuery("SELECT * FROM MyTable");
     * TablePipeline tablePipeline = table.insertInto("MySinkTable", true);
     * TableResult tableResult = tablePipeline.execute();
     * tableResult.await();
     * }</pre>
     *
     * <p>One can execute the returned {@link TablePipeline} using {@link TablePipeline#execute()},
     * or compile it to a {@link CompiledPlan} using {@link TablePipeline#compilePlan()}.
     *
     * <p>If multiple pipelines should insert data into one or more sink tables as part of a single
     * execution, use a {@link StatementSet} (see {@link TableEnvironment#createStatementSet()}).
     *
     * @param tablePath The path of the registered table (backed by a {@link DynamicTableSink}).
     * @param overwrite Indicates whether existing data should be overwritten.
     * @return The complete pipeline from one or more source tables to a sink table.
     */
    TablePipeline insertInto(String tablePath, boolean overwrite);

    /**
     * Declares that the pipeline defined by the given {@link Table} object should be written to a
     * table (backed by a {@link DynamicTableSink}) expressed via the given {@link TableDescriptor}.
     *
     * <p>The {@link TableDescriptor descriptor} won't be registered in the catalog, but it will be
     * propagated directly in the operation tree. Note that calling this method multiple times, even
     * with the same descriptor, results in multiple sink tables instances.
     *
     * <p>This method allows to declare a {@link Schema} for the sink descriptor. The declaration is
     * similar to a {@code CREATE TABLE} DDL in SQL and allows to:
     *
     * <ul>
     *   <li>overwrite automatically derived columns with a custom {@link DataType}
     *   <li>add metadata columns next to the physical columns
     *   <li>declare a primary key
     * </ul>
     *
     * <p>It is possible to declare a schema without physical/regular columns. In this case, those
     * columns will be automatically derived and implicitly put at the beginning of the schema
     * declaration.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * Schema schema = Schema.newBuilder()
     *   .column("f0", DataTypes.STRING())
     *   .build();
     *
     * Table table = tableEnv.from(TableDescriptor.forConnector("datagen")
     *   .schema(schema)
     *   .build());
     *
     * table.insertInto(TableDescriptor.forConnector("blackhole")
     *   .schema(schema)
     *   .build());
     * }</pre>
     *
     * <p>One can execute the returned {@link TablePipeline} using {@link TablePipeline#execute()},
     * or compile it to a {@link CompiledPlan} using {@link TablePipeline#compilePlan()}.
     *
     * <p>If multiple pipelines should insert data into one or more sink tables as part of a single
     * execution, use a {@link StatementSet} (see {@link TableEnvironment#createStatementSet()}).
     *
     * @param descriptor Descriptor describing the sink table into which data should be inserted.
     * @return The complete pipeline from one or more source tables to a sink table.
     */
    TablePipeline insertInto(TableDescriptor descriptor);

    /**
     * Declares that the pipeline defined by the given {@link Table} object should be written to a
     * table (backed by a {@link DynamicTableSink}) expressed via the given {@link TableDescriptor}.
     *
     * <p>The {@link TableDescriptor descriptor} won't be registered in the catalog, but it will be
     * propagated directly in the operation tree. Note that calling this method multiple times, even
     * with the same descriptor, results in multiple sink tables being registered.
     *
     * <p>This method allows to declare a {@link Schema} for the sink descriptor. The declaration is
     * similar to a {@code CREATE TABLE} DDL in SQL and allows to:
     *
     * <ul>
     *   <li>overwrite automatically derived columns with a custom {@link DataType}
     *   <li>add metadata columns next to the physical columns
     *   <li>declare a primary key
     * </ul>
     *
     * <p>It is possible to declare a schema without physical/regular columns. In this case, those
     * columns will be automatically derived and implicitly put at the beginning of the schema
     * declaration.
     *
     * <p>Examples:
     *
     * <pre>{@code
     * Schema schema = Schema.newBuilder()
     *   .column("f0", DataTypes.STRING())
     *   .build();
     *
     * Table table = tableEnv.from(TableDescriptor.forConnector("datagen")
     *   .schema(schema)
     *   .build());
     *
     * table.insertInto(TableDescriptor.forConnector("blackhole")
     *   .schema(schema)
     *   .build(), true);
     * }</pre>
     *
     * <p>One can execute the returned {@link TablePipeline} using {@link TablePipeline#execute()},
     * or compile it to a {@link CompiledPlan} using {@link TablePipeline#compilePlan()}.
     *
     * <p>If multiple pipelines should insert data into one or more sink tables as part of a single
     * execution, use a {@link StatementSet} (see {@link TableEnvironment#createStatementSet()}).
     *
     * @param descriptor Descriptor describing the sink table into which data should be inserted.
     * @param overwrite Indicates whether existing data should be overwritten.
     * @return The complete pipeline from one or more source tables to a sink table.
     */
    TablePipeline insertInto(TableDescriptor descriptor, boolean overwrite);

    /**
     * Shorthand for {@code tableEnv.insertInto(tablePath).execute()}.
     *
     * @see #insertInto(String)
     * @see TablePipeline#execute()
     */
    default TableResult executeInsert(String tablePath) {
        return insertInto(tablePath).execute();
    }

    /**
     * Shorthand for {@code tableEnv.insertInto(tablePath, overwrite).execute()}.
     *
     * @see #insertInto(String, boolean)
     * @see TablePipeline#execute()
     */
    default TableResult executeInsert(String tablePath, boolean overwrite) {
        return insertInto(tablePath, overwrite).execute();
    }

    /**
     * Shorthand for {@code tableEnv.insertInto(descriptor).execute()}.
     *
     * @see #insertInto(TableDescriptor)
     * @see TablePipeline#execute()
     */
    default TableResult executeInsert(TableDescriptor descriptor) {
        return insertInto(descriptor).execute();
    }

    /**
     * Shorthand for {@code tableEnv.insertInto(descriptor, overwrite).execute()}.
     *
     * @see #insertInto(TableDescriptor, boolean)
     * @see TablePipeline#execute()
     */
    default TableResult executeInsert(TableDescriptor descriptor, boolean overwrite) {
        return insertInto(descriptor, overwrite).execute();
    }
}
