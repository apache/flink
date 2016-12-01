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
package org.apache.flink.api.table

import org.apache.calcite.rel.RelNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.table.expressions.{Asc, Expression, ExpressionParser, Ordering}
import org.apache.flink.api.table.plan.ProjectionTranslator._
import org.apache.flink.api.table.plan.logical._
import org.apache.flink.api.table.sinks.TableSink

import scala.collection.JavaConverters._

/**
  * A Table is the core component of the Table API.
  * Similar to how the batch and streaming APIs have DataSet and DataStream,
  * the Table API is built around [[Table]].
  *
  * Use the methods of [[Table]] to transform data. Use [[TableEnvironment]] to convert a [[Table]]
  * back to a DataSet or DataStream.
  *
  * When using Scala a [[Table]] can also be converted using implicit conversions.
  *
  * Example:
  *
  * {{{
  *   val env = ExecutionEnvironment.getExecutionEnvironment
  *   val tEnv = TableEnvironment.getTableEnvironment(env)
  *
  *   val set: DataSet[(String, Int)] = ...
  *   val table = set.toTable(tEnv, 'a, 'b)
  *   ...
  *   val table2 = ...
  *   val set2: DataSet[MyType] = table2.toDataSet[MyType]
  * }}}
  *
  * Operations such as [[join]], [[select]], [[where]] and [[groupBy]] either take arguments
  * in a Scala DSL or as an expression String. Please refer to the documentation for the expression
  * syntax.
  *
  * @param tableEnv The [[TableEnvironment]] to which the table is bound.
  * @param logicalPlan logical representation
  */
class Table(
    private[flink] val tableEnv: TableEnvironment,
    private[flink] val logicalPlan: LogicalNode) {

  def relBuilder = tableEnv.getRelBuilder

  def getRelNode: RelNode = logicalPlan.toRelNode(relBuilder)

  /**
    * Performs a selection operation. Similar to an SQL SELECT statement. The field expressions
    * can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   tab.select('key, 'value.avg + " The average" as 'average)
    * }}}
    */
  def select(fields: Expression*): Table = {

    val expandedFields = expandProjectList(fields, logicalPlan, tableEnv)
    val (projection, aggs, props) = extractAggregationsAndProperties(expandedFields, tableEnv)

    if (props.nonEmpty) {
      throw ValidationException("Window properties can only be used on windowed tables.")
    }

    if (aggs.nonEmpty) {
      new Table(tableEnv,
        Project(projection,
          Aggregate(Nil, aggs, logicalPlan).validate(tableEnv)).validate(tableEnv))
    } else {
      new Table(tableEnv,
        Project(projection, logicalPlan).validate(tableEnv))
    }
  }

  /**
    * Performs a selection operation. Similar to an SQL SELECT statement. The field expressions
    * can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   tab.select("key, value.avg + ' The average' as average")
    * }}}
    */
  def select(fields: String): Table = {
    val fieldExprs = ExpressionParser.parseExpressionList(fields)
    select(fieldExprs: _*)
  }

  /**
    * Renames the fields of the expression result. Use this to disambiguate fields before
    * joining to operations.
    *
    * Example:
    *
    * {{{
    *   tab.as('a, 'b)
    * }}}
    */
  def as(fields: Expression*): Table = {
    new Table(tableEnv, AliasNode(fields, logicalPlan).validate(tableEnv))
  }

  /**
    * Renames the fields of the expression result. Use this to disambiguate fields before
    * joining to operations.
    *
    * Example:
    *
    * {{{
    *   tab.as("a, b")
    * }}}
    */
  def as(fields: String): Table = {
    val fieldExprs = ExpressionParser.parseExpressionList(fields)
    as(fieldExprs: _*)
  }

  /**
    * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
    * clause.
    *
    * Example:
    *
    * {{{
    *   tab.filter('name === "Fred")
    * }}}
    */
  def filter(predicate: Expression): Table = {
    new Table(tableEnv, Filter(predicate, logicalPlan).validate(tableEnv))
  }

  /**
    * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
    * clause.
    *
    * Example:
    *
    * {{{
    *   tab.filter("name = 'Fred'")
    * }}}
    */
  def filter(predicate: String): Table = {
    val predicateExpr = ExpressionParser.parseExpression(predicate)
    filter(predicateExpr)
  }

  /**
    * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
    * clause.
    *
    * Example:
    *
    * {{{
    *   tab.where('name === "Fred")
    * }}}
    */
  def where(predicate: Expression): Table = {
    filter(predicate)
  }

  /**
    * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
    * clause.
    *
    * Example:
    *
    * {{{
    *   tab.where("name = 'Fred'")
    * }}}
    */
  def where(predicate: String): Table = {
    filter(predicate)
  }

  /**
    * Groups the elements on some grouping keys. Use this before a selection with aggregations
    * to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.
    *
    * Example:
    *
    * {{{
    *   tab.groupBy('key).select('key, 'value.avg)
    * }}}
    */
  def groupBy(fields: Expression*): GroupedTable = {
    new GroupedTable(this, fields)
  }

  /**
    * Groups the elements on some grouping keys. Use this before a selection with aggregations
    * to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.
    *
    * Example:
    *
    * {{{
    *   tab.groupBy("key").select("key, value.avg")
    * }}}
    */
  def groupBy(fields: String): GroupedTable = {
    val fieldsExpr = ExpressionParser.parseExpressionList(fields)
    groupBy(fieldsExpr: _*)
  }

  /**
    * Removes duplicate values and returns only distinct (different) values.
    *
    * Example:
    *
    * {{{
    *   tab.select("key, value").distinct()
    * }}}
    */
  def distinct(): Table = {
    new Table(tableEnv, Distinct(logicalPlan).validate(tableEnv))
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary. You can use
    * where and select clauses after a join to further specify the behaviour of the join.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.join(right).where('a === 'b && 'c > 3).select('a, 'b, 'd)
    * }}}
    */
  def join(right: Table): Table = {
    join(right, None, JoinType.INNER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.join(right, "a = b")
    * }}}
    */
  def join(right: Table, joinPredicate: String): Table = {
    join(right, joinPredicate, JoinType.INNER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.join(right, 'a === 'b).select('a, 'b, 'd)
    * }}}
    */
  def join(right: Table, joinPredicate: Expression): Table = {
    join(right, Some(joinPredicate), JoinType.INNER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL left outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have nullCheck enabled.
    *
    * Example:
    *
    * {{{
    *   left.leftOuterJoin(right, "a = b").select('a, 'b, 'd)
    * }}}
    */
  def leftOuterJoin(right: Table, joinPredicate: String): Table = {
    join(right, joinPredicate, JoinType.LEFT_OUTER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL left outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have nullCheck enabled.
    *
    * Example:
    *
    * {{{
    *   left.leftOuterJoin(right, 'a === 'b).select('a, 'b, 'd)
    * }}}
    */
  def leftOuterJoin(right: Table, joinPredicate: Expression): Table = {
    join(right, Some(joinPredicate), JoinType.LEFT_OUTER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL right outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have nullCheck enabled.
    *
    * Example:
    *
    * {{{
    *   left.rightOuterJoin(right, "a = b").select('a, 'b, 'd)
    * }}}
    */
  def rightOuterJoin(right: Table, joinPredicate: String): Table = {
    join(right, joinPredicate, JoinType.RIGHT_OUTER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL right outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have nullCheck enabled.
    *
    * Example:
    *
    * {{{
    *   left.rightOuterJoin(right, 'a === 'b).select('a, 'b, 'd)
    * }}}
    */
  def rightOuterJoin(right: Table, joinPredicate: Expression): Table = {
    join(right, Some(joinPredicate), JoinType.RIGHT_OUTER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL full outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have nullCheck enabled.
    *
    * Example:
    *
    * {{{
    *   left.fullOuterJoin(right, "a = b").select('a, 'b, 'd)
    * }}}
    */
  def fullOuterJoin(right: Table, joinPredicate: String): Table = {
    join(right, joinPredicate, JoinType.FULL_OUTER)
  }

  /**
    * Joins two [[Table]]s. Similar to an SQL full outer join. The fields of the two joined
    * operations must not overlap, use [[as]] to rename fields if necessary.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]] and its [[TableConfig]] must
    * have nullCheck enabled.
    *
    * Example:
    *
    * {{{
    *   left.fullOuterJoin(right, 'a === 'b).select('a, 'b, 'd)
    * }}}
    */
  def fullOuterJoin(right: Table, joinPredicate: Expression): Table = {
    join(right, Some(joinPredicate), JoinType.FULL_OUTER)
  }

  private def join(right: Table, joinPredicate: String, joinType: JoinType): Table = {
    val joinPredicateExpr = ExpressionParser.parseExpression(joinPredicate)
    join(right, Some(joinPredicateExpr), joinType)
  }

  private def join(right: Table, joinPredicate: Option[Expression], joinType: JoinType): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be joined.")
    }
    new Table(
      tableEnv,
      Join(this.logicalPlan, right.logicalPlan, joinType, joinPredicate).validate(tableEnv))
  }

  /**
    * Minus of two [[Table]]s with duplicate records removed.
    * Similar to a SQL EXCEPT clause. Minus returns records from the left table that do not
    * exist in the right table. Duplicate records in the left table are returned
    * exactly once, i.e., duplicates are removed. Both tables must have identical field types.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.minus(right)
    * }}}
    */
  def minus(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be " +
        "subtracted.")
    }
    new Table(tableEnv, Minus(logicalPlan, right.logicalPlan, all = false)
      .validate(tableEnv))
  }

  /**
    * Minus of two [[Table]]s. Similar to an SQL EXCEPT ALL.
    * Similar to a SQL EXCEPT ALL clause. MinusAll returns the records that do not exist in
    * the right table. A record that is present n times in the left table and m times
    * in the right table is returned (n - m) times, i.e., as many duplicates as are present
    * in the right table are removed. Both tables must have identical field types.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.minusAll(right)
    * }}}
    */
  def minusAll(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be " +
        "subtracted.")
    }
    new Table(tableEnv, Minus(logicalPlan, right.logicalPlan, all = true)
      .validate(tableEnv))
  }

  /**
    * Unions two [[Table]]s with duplicate records removed.
    * Similar to an SQL UNION. The fields of the two union operations must fully overlap.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.union(right)
    * }}}
    */
  def union(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be unioned.")
    }
    new Table(tableEnv, Union(logicalPlan, right.logicalPlan, all = false).validate(tableEnv))
  }

  /**
    * Unions two [[Table]]s. Similar to an SQL UNION ALL. The fields of the two union operations
    * must fully overlap.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.unionAll(right)
    * }}}
    */
  def unionAll(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be unioned.")
    }
    new Table(tableEnv, Union(logicalPlan, right.logicalPlan, all = true).validate(tableEnv))
  }

  /**
    * Intersects two [[Table]]s with duplicate records removed. Intersect returns records that
    * exist in both tables. If a record is present in one or both tables more than once, it is
    * returned just once, i.e., the resulting table has no duplicate records. Similar to an
    * SQL INTERSECT. The fields of the two intersect operations must fully overlap.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.intersect(right)
    * }}}
    */
  def intersect(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException(
        "Only tables from the same TableEnvironment can be intersected.")
    }
    new Table(tableEnv, Intersect(logicalPlan, right.logicalPlan, all = false).validate(tableEnv))
  }

  /**
    * Intersects two [[Table]]s. IntersectAll returns records that exist in both tables.
    * If a record is present in both tables more than once, it is returned as many times as it
    * is present in both tables, i.e., the resulting table might have duplicate records. Similar
    * to an SQL INTERSECT ALL. The fields of the two intersect operations must fully overlap.
    *
    * Note: Both tables must be bound to the same [[TableEnvironment]].
    *
    * Example:
    *
    * {{{
    *   left.intersectAll(right)
    * }}}
    */
  def intersectAll(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException(
        "Only tables from the same TableEnvironment can be intersected.")
    }
    new Table(tableEnv, Intersect(logicalPlan, right.logicalPlan, all = true).validate(tableEnv))
  }

  /**
    * Sorts the given [[Table]]. Similar to SQL ORDER BY.
    * The resulting Table is globally sorted across all parallel partitions.
    *
    * Example:
    *
    * {{{
    *   tab.orderBy('name.desc)
    * }}}
    */
  def orderBy(fields: Expression*): Table = {
    val order: Seq[Ordering] = fields.map {
      case o: Ordering => o
      case e => Asc(e)
    }
    new Table(tableEnv, Sort(order, logicalPlan).validate(tableEnv))
  }

  /**
    * Sorts the given [[Table]]. Similar to SQL ORDER BY.
    * The resulting Table is sorted globally sorted across all parallel partitions.
    *
    * Example:
    *
    * {{{
    *   tab.orderBy("name.desc")
    * }}}
    */
  def orderBy(fields: String): Table = {
    val parsedFields = ExpressionParser.parseExpressionList(fields)
    orderBy(parsedFields: _*)
  }

  /**
    * Limits a sorted result from an offset position.
    * Similar to a SQL LIMIT clause. Limit is technically part of the Order By operator and
    * thus must be preceded by it.
    *
    * Example:
    *
    * {{{
    *   // returns unlimited number of records beginning with the 4th record
    *   tab.orderBy('name.desc).limit(3)
    * }}}
    *
    * @param offset number of records to skip
    */
  def limit(offset: Int): Table = {
    new Table(tableEnv, Limit(offset = offset, child = logicalPlan).validate(tableEnv))
  }

  /**
    * Limits a sorted result to a specified number of records from an offset position.
    * Similar to a SQL LIMIT clause. Limit is technically part of the Order By operator and
    * thus must be preceded by it.
    *
    * Example:
    *
    * {{{
    *   // returns 5 records beginning with the 4th record
    *   tab.orderBy('name.desc).limit(3, 5)
    * }}}
    *
    * @param offset number of records to skip
    * @param fetch number of records to be returned
    */
  def limit(offset: Int, fetch: Int): Table = {
    new Table(tableEnv, Limit(offset, fetch, logicalPlan).validate(tableEnv))
  }

  /**
    * Writes the [[Table]] to a [[TableSink]]. A [[TableSink]] defines an external storage location.
    *
    * A batch [[Table]] can only be written to a
    * [[org.apache.flink.api.table.sinks.BatchTableSink]], a streaming [[Table]] requires a
    * [[org.apache.flink.api.table.sinks.StreamTableSink]].
    *
    * @param sink The [[TableSink]] to which the [[Table]] is written.
    * @tparam T The data type that the [[TableSink]] expects.
    */
  def writeToSink[T](sink: TableSink[T]): Unit = {

    // get schema information of table
    val rowType = getRelNode.getRowType
    val fieldNames: Array[String] = rowType.getFieldNames.asScala.toArray
    val fieldTypes: Array[TypeInformation[_]] = rowType.getFieldList.asScala
      .map(field => FlinkTypeFactory.toTypeInfo(field.getType)).toArray

    // configure the table sink
    val configuredSink = sink.configure(fieldNames, fieldTypes)

    // emit the table to the configured table sink
    tableEnv.writeToSink(this, configuredSink)
  }

  /**
    * Groups the records of a table by assigning them to windows defined by a time or row interval.
    *
    * For streaming tables of infinite size, grouping into windows is required to define finite
    * groups on which group-based aggregates can be computed.
    *
    * For batch tables of finite size, windowing essentially provides shortcuts for time-based
    * groupBy.
    *
    * __Note__: window on non-grouped streaming table is a non-parallel operation, i.e., all data
    * will be processed by a single operator.
    *
    * @param groupWindow group-window that specifies how elements are grouped.
    * @return A windowed table.
    */
  def window(groupWindow: GroupWindow): GroupWindowedTable = {
    new GroupWindowedTable(this, Seq(), groupWindow)
  }
}

/**
  * A table that has been grouped on a set of grouping keys.
  */
class GroupedTable(
  private[flink] val table: Table,
  private[flink] val groupKey: Seq[Expression]) {

  /**
    * Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
    * The field expressions can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   tab.groupBy('key).select('key, 'value.avg + " The average" as 'average)
    * }}}
    */
  def select(fields: Expression*): Table = {

    val (projection, aggs, props) = extractAggregationsAndProperties(fields, table.tableEnv)

    if (props.nonEmpty) {
      throw ValidationException("Window properties can only be used on windowed tables.")
    }

    val logical =
      Project(
        projection,
        Aggregate(
          groupKey,
          aggs,
          table.logicalPlan
        ).validate(table.tableEnv)
      ).validate(table.tableEnv)

    new Table(table.tableEnv, logical)
  }

  /**
    * Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
    * The field expressions can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   tab.groupBy("key").select("key, value.avg + ' The average' as average")
    * }}}
    */
  def select(fields: String): Table = {
    val fieldExprs = ExpressionParser.parseExpressionList(fields)
    select(fieldExprs: _*)
  }

  /**
    * Groups the records of a table by assigning them to windows defined by a time or row interval.
    *
    * For streaming tables of infinite size, grouping into windows is required to define finite
    * groups on which group-based aggregates can be computed.
    *
    * For batch tables of finite size, windowing essentially provides shortcuts for time-based
    * groupBy.
    *
    * @param groupWindow group-window that specifies how elements are grouped.
    * @return A windowed table.
    */
  def window(groupWindow: GroupWindow): GroupWindowedTable = {
    new GroupWindowedTable(table, groupKey, groupWindow)
  }
}

class GroupWindowedTable(
    private[flink] val table: Table,
    private[flink] val groupKey: Seq[Expression],
    private[flink] val window: GroupWindow) {

  /**
    * Performs a selection operation on a windowed table. Similar to an SQL SELECT statement.
    * The field expressions can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   groupWindowTable.select('key, 'window.start, 'value.avg + " The average" as 'average)
    * }}}
    */
  def select(fields: Expression*): Table = {

    val (projection, aggs, props) = extractAggregationsAndProperties(fields, table.tableEnv)

    val groupWindow = window.toLogicalWindow

    val logical =
      Project(
        projection,
        WindowAggregate(
          groupKey,
          groupWindow,
          props,
          aggs,
          table.logicalPlan
        ).validate(table.tableEnv)
      ).validate(table.tableEnv)

    new Table(table.tableEnv, logical)
  }

  /**
    * Performs a selection operation on a group-windows table. Similar to an SQL SELECT statement.
    * The field expressions can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   groupWindowTable.select("key, window.start, value.avg + ' The average' as average")
    * }}}
    */
  def select(fields: String): Table = {
    val fieldExprs = ExpressionParser.parseExpressionList(fields)
    select(fieldExprs: _*)
  }

}
