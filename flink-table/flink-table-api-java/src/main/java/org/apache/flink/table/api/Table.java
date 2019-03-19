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
import org.apache.flink.table.sinks.TableSink;

/**
 * A Table is the core component of the Table API.
 * Similar to how the batch and streaming APIs have DataSet and DataStream,
 * the Table API is built around {@link Table}.
 *
 * <p>Use the methods of {@link Table} to transform data. Use {@code TableEnvironment} to convert a
 * {@link Table} back to a {@code DataSet} or {@code DataStream}.
 *
 * <p>When using Scala a {@link Table} can also be converted using implicit conversions.
 *
 * <p>Java Example:
 *
 * <pre>
 * {@code
 *   ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
 *   BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
 *
 *   DataSet<Tuple2<String, Integer>> set = ...
 *   tEnv.registerTable("MyTable", set, "a, b");
 *
 *   Table table = tEnv.scan("MyTable").select(...);
 *   ...
 *   Table table2 = ...
 *   DataSet<MyType> set2 = tEnv.toDataSet(table2, MyType.class);
 * }
 * </pre>
 *
 * <p>Scala Example:
 *
 * <pre>
 * {@code
 *   val env = ExecutionEnvironment.getExecutionEnvironment
 *   val tEnv = BatchTableEnvironment.create(env)
 *
 *   val set: DataSet[(String, Int)] = ...
 *   val table = set.toTable(tEnv, 'a, 'b)
 *   ...
 *   val table2 = ...
 *   val set2: DataSet[MyType] = table2.toDataSet[MyType]
 * }
 * </pre>
 *
 * <p>Operations such as {@code join}, {@code select}, {@code where} and {@code groupBy} either
 * take arguments in a Scala DSL or as an expression String. Please refer to the documentation for
 * the expression syntax.
 */
@PublicEvolving
public interface Table {

	/**
	 * Returns the schema of this table.
	 */
	TableSchema getSchema();

	/**
	 * Prints the schema of this table to the console in a tree format.
	 */
	void printSchema();

	/**
	 * Performs a selection operation. Similar to an SQL SELECT statement. The field expressions
	 * can contain complex expressions and aggregations.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.select("key, value.avg + ' The average' as average")
	 * }
	 * </pre>
	 */
	Table select(String fields);

	/**
	 * Performs a selection operation. Similar to an SQL SELECT statement. The field expressions
	 * can contain complex expressions and aggregations.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.select('key, 'value.avg + " The average" as 'average)
	 * }
	 * </pre>
	 */
	Table select(Expression... fields);

	/**
	 * Creates {@link TemporalTableFunction} backed up by this table as a history table.
	 * Temporal Tables represent a concept of a table that changes over time and for which
	 * Flink keeps track of those changes. {@link TemporalTableFunction} provides a way how to
	 * access those data.
	 *
	 * <p>For more information please check Flink's documentation on Temporal Tables.
	 *
	 * <p>Currently {@link TemporalTableFunction}s are only supported in streaming.
	 *
	 * @param timeAttribute Must points to a time attribute. Provides a way to compare which
	 *                      records are a newer or older version.
	 * @param primaryKey    Defines the primary key. With primary key it is possible to update
	 *                      a row or to delete it.
	 * @return {@link TemporalTableFunction} which is an instance of {@link TableFunction}.
	 *        It takes one single argument, the {@code timeAttribute}, for which it returns
	 *        matching version of the {@link Table}, from which {@link TemporalTableFunction}
	 *        was created.
	 */
	TemporalTableFunction createTemporalTableFunction(String timeAttribute, String primaryKey);

	/**
	 * Creates {@link TemporalTableFunction} backed up by this table as a history table.
	 * Temporal Tables represent a concept of a table that changes over time and for which
	 * Flink keeps track of those changes. {@link TemporalTableFunction} provides a way how to
	 * access those data.
	 *
	 * <p>For more information please check Flink's documentation on Temporal Tables.
	 *
	 * <p>Currently {@link TemporalTableFunction}s are only supported in streaming.
	 *
	 * @param timeAttribute Must points to a time indicator. Provides a way to compare which
	 *                      records are a newer or older version.
	 * @param primaryKey    Defines the primary key. With primary key it is possible to update
	 *                      a row or to delete it.
	 * @return {@link TemporalTableFunction} which is an instance of {@link TableFunction}.
	 *        It takes one single argument, the {@code timeAttribute}, for which it returns
	 *        matching version of the {@link Table}, from which {@link TemporalTableFunction}
	 *        was created.
	 */
	TemporalTableFunction createTemporalTableFunction(Expression timeAttribute, Expression primaryKey);

	/**
	 * Renames the fields of the expression result. Use this to disambiguate fields before
	 * joining to operations.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.as("a, b")
	 * }
	 * </pre>
	 */
	Table as(String fields);

	/**
	 * Renames the fields of the expression result. Use this to disambiguate fields before
	 * joining to operations.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.as('a, 'b)
	 * }
	 * </pre>
	 */
	Table as(Expression... fields);

	/**
	 * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
	 * clause.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.filter("name = 'Fred'")
	 * }
	 * </pre>
	 */
	Table filter(String predicate);

	/**
	 * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
	 * clause.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.filter('name === "Fred")
	 * }
	 * </pre>
	 */
	Table filter(Expression predicate);

	/**
	 * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
	 * clause.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.where("name = 'Fred'")
	 * }
	 * </pre>
	 */
	Table where(String predicate);

	/**
	 * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
	 * clause.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.where('name === "Fred")
	 * }
	 * </pre>
	 */
	Table where(Expression predicate);

	/**
	 * Groups the elements on some grouping keys. Use this before a selection with aggregations
	 * to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.groupBy("key").select("key, value.avg")
	 * }
	 * </pre>
	 */
	GroupedTable groupBy(String fields);

	/**
	 * Groups the elements on some grouping keys. Use this before a selection with aggregations
	 * to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.groupBy('key).select('key, 'value.avg)
	 * }
	 * </pre>
	 */
	GroupedTable groupBy(Expression... fields);

	/**
	 * Removes duplicate values and returns only distinct (different) values.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.select("key, value").distinct()
	 * }
	 * </pre>
	 */
	Table distinct();

	/**
	 * Joins two {@link Table}s. Similar to an SQL join. The fields of the two joined
	 * operations must not overlap, use {@code as} to rename fields if necessary. You can use
	 * where and select clauses after a join to further specify the behaviour of the join.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment} .
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   left.join(right).where("a = b && c > 3").select("a, b, d")
	 * }
	 * </pre>
	 */
	Table join(Table right);

	/**
	 * Joins two {@link Table}s. Similar to an SQL join. The fields of the two joined
	 * operations must not overlap, use {@code as} to rename fields if necessary.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment} .
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   left.join(right, "a = b")
	 * }
	 * </pre>
	 */
	Table join(Table right, String joinPredicate);

	/**
	 * Joins two {@link Table}s. Similar to an SQL join. The fields of the two joined
	 * operations must not overlap, use {@code as} to rename fields if necessary.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment} .
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   left.join(right, 'a === 'b).select('a, 'b, 'd)
	 * }
	 * </pre>
	 */
	Table join(Table right, Expression joinPredicate);

	/**
	 * Joins two {@link Table}s. Similar to an SQL left outer join. The fields of the two joined
	 * operations must not overlap, use {@code as} to rename fields if necessary.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its
	 * {@code TableConfig} must have null check enabled (default).
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   left.leftOuterJoin(right).select("a, b, d")
	 * }
	 * </pre>
	 */
	Table leftOuterJoin(Table right);

	/**
	 * Joins two {@link Table}s. Similar to an SQL left outer join. The fields of the two joined
	 * operations must not overlap, use {@code as} to rename fields if necessary.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its
	 * {@code TableConfig} must have null check enabled (default).
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   left.leftOuterJoin(right, "a = b").select("a, b, d")
	 * }
	 * </pre>
	 */
	Table leftOuterJoin(Table right, String joinPredicate);

	/**
	 * Joins two {@link Table}s. Similar to an SQL left outer join. The fields of the two joined
	 * operations must not overlap, use {@code as} to rename fields if necessary.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its
	 * {@code TableConfig} must have null check enabled (default).
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   left.leftOuterJoin(right, 'a === 'b).select('a, 'b, 'd)
	 * }
	 * </pre>
	 */
	Table leftOuterJoin(Table right, Expression joinPredicate);

	/**
	 * Joins two {@link Table}s. Similar to an SQL right outer join. The fields of the two joined
	 * operations must not overlap, use {@code as} to rename fields if necessary.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its
	 * {@code TableConfig} must have null check enabled (default).
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   left.rightOuterJoin(right, "a = b").select("a, b, d")
	 * }
	 * </pre>
	 */
	Table rightOuterJoin(Table right, String joinPredicate);

	/**
	 * Joins two {@link Table}s. Similar to an SQL right outer join. The fields of the two joined
	 * operations must not overlap, use {@code as} to rename fields if necessary.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its
	 * {@code TableConfig} must have null check enabled (default).
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   left.rightOuterJoin(right, 'a === 'b).select('a, 'b, 'd)
	 * }
	 * </pre>
	 */
	Table rightOuterJoin(Table right, Expression joinPredicate);

	/**
	 * Joins two {@link Table}s. Similar to an SQL full outer join. The fields of the two joined
	 * operations must not overlap, use {@code as} to rename fields if necessary.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its
	 * {@code TableConfig} must have null check enabled (default).
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   left.fullOuterJoin(right, "a = b").select("a, b, d")
	 * }
	 * </pre>
	 */
	Table fullOuterJoin(Table right, String joinPredicate);

	/**
	 * Joins two {@link Table}s. Similar to an SQL full outer join. The fields of the two joined
	 * operations must not overlap, use {@code as} to rename fields if necessary.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment} and its
	 * {@code TableConfig} must have null check enabled (default).
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   left.fullOuterJoin(right, 'a === 'b).select('a, 'b, 'd)
	 * }
	 * </pre>
	 */
	Table fullOuterJoin(Table right, Expression joinPredicate);

	/**
	 * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
	 * a SQL inner join with ON TRUE predicate but works with a table function. Each row of the
	 * table is joined with all rows produced by the table function.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   class MySplitUDTF extends TableFunction<String> {
	 *     public void eval(String str) {
	 *       str.split("#").forEach(this::collect);
	 *     }
	 *   }
	 *
	 *   TableFunction<String> split = new MySplitUDTF();
	 *   tableEnv.registerFunction("split", split);
	 *   table.joinLateral("split(c) as (s)").select("a, b, c, s");
	 * }
	 * </pre>
	 */
	Table joinLateral(String tableFunctionCall);

	/**
	 * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
	 * a SQL inner join with ON TRUE predicate but works with a table function. Each row of the
	 * table is joined with all rows produced by the table function.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   class MySplitUDTF extends TableFunction[String] {
	 *     def eval(str: String): Unit = {
	 *       str.split("#").foreach(collect)
	 *     }
	 *   }
	 *
	 *   val split = new MySplitUDTF()
	 *   table.joinLateral(split('c) as ('s)).select('a, 'b, 'c, 's)
	 * }
	 * </pre>
	 */
	Table joinLateral(Expression tableFunctionCall);

	/**
	 * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
	 * a SQL inner join with ON TRUE predicate but works with a table function. Each row of the
	 * table is joined with all rows produced by the table function.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   class MySplitUDTF extends TableFunction<String> {
	 *     public void eval(String str) {
	 *       str.split("#").forEach(this::collect);
	 *     }
	 *   }
	 *
	 *   TableFunction<String> split = new MySplitUDTF();
	 *   tableEnv.registerFunction("split", split);
	 *   table.joinLateral("split(c) as (s)", "a = s").select("a, b, c, s");
	 * }
	 * </pre>
	 */
	Table joinLateral(String tableFunctionCall, String joinPredicate);

	/**
	 * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
	 * a SQL inner join with ON TRUE predicate but works with a table function. Each row of the
	 * table is joined with all rows produced by the table function.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   class MySplitUDTF extends TableFunction[String] {
	 *     def eval(str: String): Unit = {
	 *       str.split("#").foreach(collect)
	 *     }
	 *   }
	 *
	 *   val split = new MySplitUDTF()
	 *   table.joinLateral(split('c) as ('s), 'a === 's).select('a, 'b, 'c, 's)
	 * }
	 * </pre>
	 */
	Table joinLateral(Expression tableFunctionCall, Expression joinPredicate);

	/**
	 * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
	 * a SQL left outer join with ON TRUE predicate but works with a table function. Each row of
	 * the table is joined with all rows produced by the table function. If the table function does
	 * not produce any row, the outer row is padded with nulls.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   class MySplitUDTF extends TableFunction<String> {
	 *     public void eval(String str) {
	 *       str.split("#").forEach(this::collect);
	 *     }
	 *   }
	 *
	 *   TableFunction<String> split = new MySplitUDTF();
	 *   tableEnv.registerFunction("split", split);
	 *   table.leftOuterJoinLateral("split(c) as (s)").select("a, b, c, s");
	 * }
	 * </pre>
	 */
	Table leftOuterJoinLateral(String tableFunctionCall);

	/**
	 * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
	 * a SQL left outer join with ON TRUE predicate but works with a table function. Each row of
	 * the table is joined with all rows produced by the table function. If the table function does
	 * not produce any row, the outer row is padded with nulls.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   class MySplitUDTF extends TableFunction[String] {
	 *     def eval(str: String): Unit = {
	 *       str.split("#").foreach(collect)
	 *     }
	 *   }
	 *
	 *   val split = new MySplitUDTF()
	 *   table.leftOuterJoinLateral(split('c) as ('s)).select('a, 'b, 'c, 's)
	 * }
	 * </pre>
	 */
	Table leftOuterJoinLateral(Expression tableFunctionCall);

	/**
	 * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
	 * a SQL left outer join with ON TRUE predicate but works with a table function. Each row of
	 * the table is joined with all rows produced by the table function. If the table function does
	 * not produce any row, the outer row is padded with nulls.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   class MySplitUDTF extends TableFunction<String> {
	 *     public void eval(String str) {
	 *       str.split("#").forEach(this::collect);
	 *     }
	 *   }
	 *
	 *   TableFunction<String> split = new MySplitUDTF();
	 *   tableEnv.registerFunction("split", split);
	 *   table.leftOuterJoinLateral("split(c) as (s)", "a = s").select("a, b, c, s");
	 * }
	 * </pre>
	 */
	Table leftOuterJoinLateral(String tableFunctionCall, String joinPredicate);

	/**
	 * Joins this {@link Table} with an user-defined {@link TableFunction}. This join is similar to
	 * a SQL left outer join with ON TRUE predicate but works with a table function. Each row of
	 * the table is joined with all rows produced by the table function. If the table function does
	 * not produce any row, the outer row is padded with nulls.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   class MySplitUDTF extends TableFunction[String] {
	 *     def eval(str: String): Unit = {
	 *       str.split("#").foreach(collect)
	 *     }
	 *   }
	 *
	 *   val split = new MySplitUDTF()
	 *   table.leftOuterJoinLateral(split('c) as ('s), 'a === 's).select('a, 'b, 'c, 's)
	 * }
	 * </pre>
	 */
	Table leftOuterJoinLateral(Expression tableFunctionCall, Expression joinPredicate);

	/**
	 * Minus of two {@link Table}s with duplicate records removed.
	 * Similar to a SQL EXCEPT clause. Minus returns records from the left table that do not
	 * exist in the right table. Duplicate records in the left table are returned
	 * exactly once, i.e., duplicates are removed. Both tables must have identical field types.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment}.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   left.minus(right)
	 * }
	 * </pre>
	 */
	Table minus(Table right);

	/**
	 * Minus of two {@link Table}s. Similar to an SQL EXCEPT ALL.
	 * Similar to a SQL EXCEPT ALL clause. MinusAll returns the records that do not exist in
	 * the right table. A record that is present n times in the left table and m times
	 * in the right table is returned (n - m) times, i.e., as many duplicates as are present
	 * in the right table are removed. Both tables must have identical field types.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment}.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   left.minusAll(right)
	 * }
	 * </pre>
	 */
	Table minusAll(Table right);

	/**
	 * Unions two {@link Table}s with duplicate records removed.
	 * Similar to an SQL UNION. The fields of the two union operations must fully overlap.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment}.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   left.union(right)
	 * }
	 * </pre>
	 */
	Table union(Table right);

	/**
	 * Unions two {@link Table}s. Similar to an SQL UNION ALL. The fields of the two union
	 * operations must fully overlap.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment}.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   left.unionAll(right)
	 * }
	 * </pre>
	 */
	Table unionAll(Table right);

	/**
	 * Intersects two {@link Table}s with duplicate records removed. Intersect returns records that
	 * exist in both tables. If a record is present in one or both tables more than once, it is
	 * returned just once, i.e., the resulting table has no duplicate records. Similar to an
	 * SQL INTERSECT. The fields of the two intersect operations must fully overlap.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment}.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   left.intersect(right)
	 * }
	 * </pre>
	 */
	Table intersect(Table right);

	/**
	 * Intersects two {@link Table}s. IntersectAll returns records that exist in both tables.
	 * If a record is present in both tables more than once, it is returned as many times as it
	 * is present in both tables, i.e., the resulting table might have duplicate records. Similar
	 * to an SQL INTERSECT ALL. The fields of the two intersect operations must fully overlap.
	 *
	 * <p>Note: Both tables must be bound to the same {@code TableEnvironment}.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   left.intersectAll(right)
	 * }
	 * </pre>
	 */
	Table intersectAll(Table right);

	/**
	 * Sorts the given {@link Table}. Similar to SQL ORDER BY.
	 * The resulting Table is sorted globally sorted across all parallel partitions.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.orderBy("name.desc")
	 * }
	 * </pre>
	 */
	Table orderBy(String fields);

	/**
	 * Sorts the given {@link Table}. Similar to SQL ORDER BY.
	 * The resulting Table is globally sorted across all parallel partitions.
	 *
	 * <p>Scala Example:
	 *
	 * <pre>
	 * {@code
	 *   tab.orderBy('name.desc)
	 * }
	 * </pre>
	 */
	Table orderBy(Expression... fields);

	/**
	 * Limits a sorted result from an offset position.
	 * Similar to a SQL OFFSET clause. Offset is technically part of the Order By operator and
	 * thus must be preceded by it.
	 *
	 * {@link Table#offset(int offset)} can be combined with a subsequent
	 * {@link Table#fetch(int fetch)} call to return n rows after skipping the first o rows.
	 *
	 * <pre>
	 * {@code
	 *   // skips the first 3 rows and returns all following rows.
	 *   tab.orderBy("name.desc").offset(3)
	 *   // skips the first 10 rows and returns the next 5 rows.
	 *   tab.orderBy("name.desc").offset(10).fetch(5)
	 * }
	 * </pre>
	 *
	 * @param offset number of records to skip
	 */
	Table offset(int offset);

	/**
	 * Limits a sorted result to the first n rows.
	 * Similar to a SQL FETCH clause. Fetch is technically part of the Order By operator and
	 * thus must be preceded by it.
	 *
	 * {@link Table#fetch(int fetch)} can be combined with a preceding
	 * {@link Table#offset(int offset)} call to return n rows after skipping the first o rows.
	 *
	 * <pre>
	 * {@code
	 *   // returns the first 3 records.
	 *   tab.orderBy("name.desc").fetch(3)
	 *   // skips the first 10 rows and returns the next 5 rows.
	 *   tab.orderBy("name.desc").offset(10).fetch(5)
	 * }
	 * </pre>
	 *
	 * @param fetch the number of records to return. Fetch must be >= 0.
	 */
	Table fetch(int fetch);

	/**
	 * Writes the {@link Table} to a {@link TableSink} that was registered under the specified name.
	 *
	 * <p>A batch {@link Table} can only be written to a
	 * {@code org.apache.flink.table.sinks.BatchTableSink}, a streaming {@link Table} requires a
	 * {@code org.apache.flink.table.sinks.AppendStreamTableSink}, a
	 * {@code org.apache.flink.table.sinks.RetractStreamTableSink}, or an
	 * {@code org.apache.flink.table.sinks.UpsertStreamTableSink}.
	 *
	 * @param tableName Name of the registered {@link TableSink} to which the {@link Table} is
	 *                  written.
	 */
	void insertInto(String tableName);

	/**
	 * Writes the {@link Table} to a {@link TableSink} that was registered under the specified name.
	 *
	 * <p>A batch {@link Table} can only be written to a
	 * {@code org.apache.flink.table.sinks.BatchTableSink}, a streaming {@link Table} requires a
	 * {@code org.apache.flink.table.sinks.AppendStreamTableSink}, a
	 * {@code org.apache.flink.table.sinks.RetractStreamTableSink}, or an
	 * {@code org.apache.flink.table.sinks.UpsertStreamTableSink}.
	 *
	 * @param tableName Name of the {@link TableSink} to which the {@link Table} is written.
	 * @param conf The {@link QueryConfig} to use.
	 */
	void insertInto(String tableName, QueryConfig conf);

	/**
	 * Groups the records of a table by assigning them to windows defined by a time or row interval.
	 *
	 * <p>For streaming tables of infinite size, grouping into windows is required to define finite
	 * groups on which group-based aggregates can be computed.
	 *
	 * <p>For batch tables of finite size, windowing essentially provides shortcuts for time-based
	 * groupBy.
	 *
	 * <p><b>Note</b>: Computing windowed aggregates on a streaming table is only a parallel operation
	 * if additional grouping attributes are added to the {@code groupBy(...)} clause.
	 * If the {@code groupBy(...)} only references a GroupWindow alias, the streamed table will be
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
	 * <pre>
	 * {@code
	 *   table
	 *     .window(Over partitionBy 'c orderBy 'rowTime preceding 10.seconds as 'ow)
	 *     .select('c, 'b.count over 'ow, 'e.sum over 'ow)
	 * }
	 * </pre>
	 *
	 * <p><b>Note</b>: Computing over window aggregates on a streaming table is only a parallel
	 * operation if the window is partitioned. Otherwise, the whole stream will be processed by a
	 * single task, i.e., with parallelism 1.
	 *
	 * <p><b>Note</b>: Over-windows for batch tables are currently not supported.
	 *
	 * @param overWindows windows that specify the record interval over which aggregations are
	 *                    computed.
	 * @return An OverWindowedTable to specify the aggregations.
	 */
	OverWindowedTable window(OverWindow... overWindows);
}
