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
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.sinks.TableSink;

/**
 * A Table is the core component of the Table API. Similar to how the batch and streaming APIs have
 * DataSet and DataStream, the Table API is built around {@link Table}.
 *
 * <p>Use the methods of {@link Table} to transform data. Use {@code TableEnvironment} to convert a
 * {@link Table} back to a {@code DataSet} or {@code DataStream}.
 *
 * <p>When using Scala a {@link Table} can also be converted using implicit conversions.
 *
 * <p>Java Example:
 *
 * <pre>{@code
 * ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
 * BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
 *
 * DataSet<Tuple2<String, Integer>> set = ...
 * tEnv.registerTable("MyTable", set, "a, b");
 *
 * Table table = tEnv.scan("MyTable").select(...);
 * ...
 * Table table2 = ...
 * DataSet<MyType> set2 = tEnv.toDataSet(table2, MyType.class);
 * }</pre>
 *
 * <p>Scala Example:
 *
 * <pre>{@code
 * val env = ExecutionEnvironment.getExecutionEnvironment
 * val tEnv = BatchTableEnvironment.create(env)
 *
 * val set: DataSet[(String, Int)] = ...
 * val table = set.toTable(tEnv, 'a, 'b)
 * ...
 * val table2 = ...
 * val set2: DataSet[MyType] = table2.toDataSet[MyType]
 * }</pre>
 *
 * <p>Operations such as {@code join}, {@code select}, {@code where} and {@code groupBy} either take
 * arguments in a Scala DSL or as an expression String. Please refer to the documentation for the
 * expression syntax.
 */
@PublicEvolving
public interface Table {

    /** Returns the schema of this table. */
    TableSchema getSchema();

    /** Prints the schema of this table to the console in a tree format. */
    void printSchema();

    /** Returns underlying logical representation of this table. */
    QueryOperation getQueryOperation();

    /**
     * Performs a selection operation. Similar to a SQL SELECT statement. The field expressions can
     * contain complex expressions and aggregations.
     *
     * <p>Example:
     *
     * <pre>{@code
     * tab.select("key, value.avg + ' The average' as average")
     * }</pre>
     *
     * @deprecated use {@link #select(Expression...)}
     */
    @Deprecated
    Table select(String fields);

    /**
     * Performs a selection operation. Similar to a SQL SELECT statement. The field expressions can
     * contain complex expressions and aggregations.
     *
     * <p>Scala Example:
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
     * @param timeAttribute Must points to a time attribute. Provides a way to compare which records
     *     are a newer or older version.
     * @param primaryKey Defines the primary key. With primary key it is possible to update a row or
     *     to delete it.
     * @return {@link TemporalTableFunction} which is an instance of {@link TableFunction}. It takes
     *     one single argument, the {@code timeAttribute}, for which it returns matching version of
     *     the {@link Table}, from which {@link TemporalTableFunction} was created.
     * @deprecated use {@link #createTemporalTableFunction(Expression, Expression)}
     */
    @Deprecated
    TemporalTableFunction createTemporalTableFunction(String timeAttribute, String primaryKey);

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
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * tab.filter("name = 'Fred'")
     * }</pre>
     *
     * @deprecated use {@link #filter(Expression)}
     */
    @Deprecated
    Table filter(String predicate);

    /**
     * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE clause.
     *
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * tab.where("name = 'Fred'")
     * }</pre>
     *
     * @deprecated use {@link #where(Expression)}
     */
    @Deprecated
    Table where(String predicate);

    /**
     * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE clause.
     *
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * tab.groupBy("key").select("key, value.avg")
     * }</pre>
     *
     * @deprecated use {@link #groupBy(Expression...)}
     */
    @Deprecated
    GroupedTable groupBy(String fields);

    /**
     * Groups the elements on some grouping keys. Use this before a selection with aggregations to
     * perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.
     *
     * <p>Scala Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * left.join(right, "a = b")
     * }</pre>
     *
     * @deprecated use {@link #join(Table, Expression)}
     */
    @Deprecated
    Table join(Table right, String joinPredicate);

    /**
     * Joins two {@link Table}s. Similar to a SQL join. The fields of the two joined operations must
     * not overlap, use {@code as} to rename fields if necessary.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment} .
     *
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * left.leftOuterJoin(right, "a = b").select("a, b, d")
     * }</pre>
     *
     * @deprecated use {@link #leftOuterJoin(Table, Expression)}
     */
    @Deprecated
    Table leftOuterJoin(Table right, String joinPredicate);

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
     * <p>Example:
     *
     * <pre>{@code
     * left.rightOuterJoin(right, "a = b").select("a, b, d")
     * }</pre>
     *
     * @deprecated use {@link #rightOuterJoin(Table, Expression)}
     */
    @Deprecated
    Table rightOuterJoin(Table right, String joinPredicate);

    /**
     * Joins two {@link Table}s. Similar to a SQL right outer join. The fields of the two joined
     * operations must not overlap, use {@code as} to rename fields if necessary.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its {@code
     * TableConfig} must have null check enabled (default).
     *
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * left.fullOuterJoin(right, "a = b").select("a, b, d")
     * }</pre>
     *
     * @deprecated use {@link #fullOuterJoin(Table, Expression)}
     */
    @Deprecated
    Table fullOuterJoin(Table right, String joinPredicate);

    /**
     * Joins two {@link Table}s. Similar to a SQL full outer join. The fields of the two joined
     * operations must not overlap, use {@code as} to rename fields if necessary.
     *
     * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its {@code
     * TableConfig} must have null check enabled (default).
     *
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * class MySplitUDTF extends TableFunction<String> {
     *   public void eval(String str) {
     *     str.split("#").forEach(this::collect);
     *   }
     * }
     *
     * TableFunction<String> split = new MySplitUDTF();
     * tableEnv.registerFunction("split", split);
     * table.joinLateral("split(c) as (s)").select("a, b, c, s");
     * }</pre>
     *
     * @deprecated use {@link #joinLateral(Expression)}
     */
    @Deprecated
    Table joinLateral(String tableFunctionCall);

    /**
     * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
     * a SQL inner join with ON TRUE predicate but works with a table function. Each row of the
     * table is joined with all rows produced by the table function.
     *
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * class MySplitUDTF extends TableFunction<String> {
     *   public void eval(String str) {
     *     str.split("#").forEach(this::collect);
     *   }
     * }
     *
     * TableFunction<String> split = new MySplitUDTF();
     * tableEnv.registerFunction("split", split);
     * table.joinLateral("split(c) as (s)", "a = s").select("a, b, c, s");
     * }</pre>
     *
     * @deprecated use {@link #joinLateral(Expression, Expression)}
     */
    @Deprecated
    Table joinLateral(String tableFunctionCall, String joinPredicate);

    /**
     * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
     * a SQL inner join but works with a table function. Each row of the table is joined with all
     * rows produced by the table function.
     *
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * class MySplitUDTF extends TableFunction<String> {
     *   public void eval(String str) {
     *     str.split("#").forEach(this::collect);
     *   }
     * }
     *
     * TableFunction<String> split = new MySplitUDTF();
     * tableEnv.registerFunction("split", split);
     * table.leftOuterJoinLateral("split(c) as (s)").select("a, b, c, s");
     * }</pre>
     *
     * @deprecated use {@link #leftOuterJoinLateral(Expression)}
     */
    @Deprecated
    Table leftOuterJoinLateral(String tableFunctionCall);

    /**
     * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
     * a SQL left outer join with ON TRUE predicate but works with a table function. Each row of the
     * table is joined with all rows produced by the table function. If the table function does not
     * produce any row, the outer row is padded with nulls.
     *
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * class MySplitUDTF extends TableFunction<String> {
     *   public void eval(String str) {
     *     str.split("#").forEach(this::collect);
     *   }
     * }
     *
     * TableFunction<String> split = new MySplitUDTF();
     * tableEnv.registerFunction("split", split);
     * table.leftOuterJoinLateral("split(c) as (s)", "a = s").select("a, b, c, s");
     * }</pre>
     *
     * @deprecated use {@link #leftOuterJoinLateral(Expression, Expression)}
     */
    @Deprecated
    Table leftOuterJoinLateral(String tableFunctionCall, String joinPredicate);

    /**
     * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
     * a SQL left outer join with ON TRUE predicate but works with a table function. Each row of the
     * table is joined with all rows produced by the table function. If the table function does not
     * produce any row, the outer row is padded with nulls.
     *
     * <p>Example:
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
     * Sorts the given {@link Table}. Similar to SQL ORDER BY. The resulting Table is sorted
     * globally sorted across all parallel partitions.
     *
     * <p>Example:
     *
     * <pre>{@code
     * tab.orderBy("name.desc")
     * }</pre>
     *
     * @deprecated use {@link #orderBy(Expression...)}
     */
    @Deprecated
    Table orderBy(String fields);

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
     * Writes the {@link Table} to a {@link TableSink} that was registered under the specified path.
     * For the path resolution algorithm see {@link TableEnvironment#useDatabase(String)}.
     *
     * <p>A batch {@link Table} can only be written to a {@code
     * org.apache.flink.table.sinks.BatchTableSink}, a streaming {@link Table} requires a {@code
     * org.apache.flink.table.sinks.AppendStreamTableSink}, a {@code
     * org.apache.flink.table.sinks.RetractStreamTableSink}, or an {@code
     * org.apache.flink.table.sinks.UpsertStreamTableSink}.
     *
     * @param tablePath The path of the registered {@link TableSink} to which the {@link Table} is
     *     written.
     * @deprecated use {@link #executeInsert(String)} for single sink, use {@link
     *     TableEnvironment#createStatementSet()} for multiple sinks.
     */
    @Deprecated
    void insertInto(String tablePath);

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
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * tab.addColumns("a + 1 as a1, concat(b, 'sunny') as b1")
     * }</pre>
     *
     * @deprecated use {@link #addColumns(Expression...)}
     */
    @Deprecated
    Table addColumns(String fields);

    /**
     * Adds additional columns. Similar to a SQL SELECT statement. The field expressions can contain
     * complex expressions, but can not contain aggregations. It will throw an exception if the
     * added fields already exist.
     *
     * <p>Example:
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
     * complex expressions, but can not contain aggregations. Existing fields will be replaced if
     * add columns name is the same as the existing column name. Moreover, if the added fields have
     * duplicate field name, then the last one is used.
     *
     * <p>Example:
     *
     * <pre>{@code
     * tab.addOrReplaceColumns("a + 1 as a1, concat(b, 'sunny') as b1")
     * }</pre>
     *
     * @deprecated use {@link #addOrReplaceColumns(Expression...)}
     */
    @Deprecated
    Table addOrReplaceColumns(String fields);

    /**
     * Adds additional columns. Similar to a SQL SELECT statement. The field expressions can contain
     * complex expressions, but can not contain aggregations. Existing fields will be replaced. If
     * the added fields have duplicate field name, then the last one is used.
     *
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * tab.renameColumns("a as a1, b as b1")
     * }</pre>
     *
     * @deprecated use {@link #renameColumns(Expression...)}
     */
    @Deprecated
    Table renameColumns(String fields);

    /**
     * Renames existing columns. Similar to a field alias statement. The field expressions should be
     * alias expressions, and only the existing fields can be renamed.
     *
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * tab.dropColumns("a, b")
     * }</pre>
     *
     * @deprecated use {@link #dropColumns(Expression...)}
     */
    @Deprecated
    Table dropColumns(String fields);

    /**
     * Drops existing columns. The field expressions should be field reference expressions.
     *
     * <p>Example:
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
     * Performs a map operation with an user-defined scalar function or a built-in scalar function.
     * The output will be flattened if the output type is a composite type.
     *
     * <p>Example:
     *
     * <pre>{@code
     * ScalarFunction func = new MyMapFunction();
     * tableEnv.registerFunction("func", func);
     * tab.map("func(c)");
     * }</pre>
     *
     * @deprecated use {@link #map(Expression)}
     */
    @Deprecated
    Table map(String mapFunction);

    /**
     * Performs a map operation with an user-defined scalar function or built-in scalar function.
     * The output will be flattened if the output type is a composite type.
     *
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * TableFunction func = new MyFlatMapFunction();
     * tableEnv.registerFunction("func", func);
     * table.flatMap("func(c)");
     * }</pre>
     *
     * @deprecated use {@link #flatMap(Expression)}
     */
    @Deprecated
    Table flatMap(String tableFunction);

    /**
     * Performs a flatMap operation with an user-defined table function or built-in table function.
     * The output will be flattened if the output type is a composite type.
     *
     * <p>Example:
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
     * {@link #aggregate(String)} with a select statement. The output will be flattened if the
     * output type is a composite type.
     *
     * <p>Example:
     *
     * <pre>{@code
     * AggregateFunction aggFunc = new MyAggregateFunction()
     * tableEnv.registerFunction("aggFunc", aggFunc);
     * table.aggregate("aggFunc(a, b) as (f0, f1, f2)")
     *   .select("f0, f1")
     * }</pre>
     *
     * @deprecated use {@link #aggregate(Expression)}
     */
    @Deprecated
    AggregatedTable aggregate(String aggregateFunction);

    /**
     * Performs a global aggregate operation with an aggregate function. You have to close the
     * {@link #aggregate(Expression)} with a select statement. The output will be flattened if the
     * output type is a composite type.
     *
     * <p>Example:
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
     * <p>Example:
     *
     * <pre>{@code
     * TableAggregateFunction tableAggFunc = new MyTableAggregateFunction();
     * tableEnv.registerFunction("tableAggFunc", tableAggFunc);
     * tab.flatAggregate("tableAggFunc(a, b) as (x, y, z)")
     *   .select("x, y, z")
     * }</pre>
     *
     * @deprecated use {@link #flatAggregate(Expression)}
     */
    @Deprecated
    FlatAggregateTable flatAggregate(String tableAggregateFunction);

    /**
     * Perform a global flatAggregate without groupBy. FlatAggregate takes a TableAggregateFunction
     * which returns multiple rows. Use a selection after the flatAggregate.
     *
     * <p>Example:
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
     * Writes the {@link Table} to a {@link TableSink} that was registered under the specified path,
     * and then execute the insert operation.
     *
     * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or {@link
     * TableEnvironment#useCatalog(String)} for the rules on the path resolution.
     *
     * <p>A batch {@link Table} can only be written to a {@code
     * org.apache.flink.table.sinks.BatchTableSink}, a streaming {@link Table} requires a {@code
     * org.apache.flink.table.sinks.AppendStreamTableSink}, a {@code
     * org.apache.flink.table.sinks.RetractStreamTableSink}, or an {@code
     * org.apache.flink.table.sinks.UpsertStreamTableSink}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Table table = tableEnv.fromQuery("select * from MyTable");
     * TableResult tableResult = table.executeInsert("MySink");
     * tableResult...
     * }</pre>
     *
     * @param tablePath The path of the registered TableSink to which the Table is written.
     * @return The insert operation execution result.
     */
    TableResult executeInsert(String tablePath);

    /**
     * Writes the {@link Table} to a {@link TableSink} that was registered under the specified path,
     * and then execute the insert operation.
     *
     * <p>See the documentation of {@link TableEnvironment#useDatabase(String)} or {@link
     * TableEnvironment#useCatalog(String)} for the rules on the path resolution.
     *
     * <p>A batch {@link Table} can only be written to a {@code
     * org.apache.flink.table.sinks.BatchTableSink}, a streaming {@link Table} requires a {@code
     * org.apache.flink.table.sinks.AppendStreamTableSink}, a {@code
     * org.apache.flink.table.sinks.RetractStreamTableSink}, or an {@code
     * org.apache.flink.table.sinks.UpsertStreamTableSink}.
     *
     * <p>Example:
     *
     * <pre>{@code
     * Table table = tableEnv.fromQuery("select * from MyTable");
     * TableResult tableResult = table.executeInsert("MySink", true);
     * tableResult...
     * }</pre>
     *
     * @param tablePath The path of the registered TableSink to which the Table is written.
     * @param overwrite The flag that indicates whether the insert should overwrite existing data or
     *     not.
     * @return The insert operation execution result.
     */
    TableResult executeInsert(String tablePath, boolean overwrite);

    /**
     * Collects the contents of the current table local client.
     *
     * <pre>{@code
     * Table table = tableEnv.fromQuery("select * from MyTable");
     * TableResult tableResult = table.execute();
     * tableResult...
     * }</pre>
     */
    TableResult execute();

    /**
     * Returns the AST of this table and the execution plan to compute the result of this table.
     *
     * @param extraDetails The extra explain details which the explain result should include, e.g.
     *     estimated cost, changelog mode for streaming
     * @return AST and the execution plan.
     */
    String explain(ExplainDetail... extraDetails);
}
