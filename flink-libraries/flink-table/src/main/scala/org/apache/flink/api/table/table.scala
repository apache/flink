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

import scala.collection.JavaConverters._

import org.apache.calcite.rel.RelNode

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.table.plan.RexNodeTranslator.extractAggregations
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.plan.logical._
import org.apache.flink.api.table.sinks.TableSink
import org.apache.flink.api.table.typeutils.TypeConverter

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
  * @param logicalPlan
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
    val projectionOnAggregates = fields.map(extractAggregations(_, tableEnv))
    val aggregations = projectionOnAggregates.flatMap(_._2)
    if (aggregations.nonEmpty) {
      new Table(tableEnv,
        Project(projectionOnAggregates.map(e => UnresolvedAlias(e._1)),
          Aggregate(Nil, aggregations, logicalPlan).validate(tableEnv)).validate(tableEnv))
    } else {
      new Table(tableEnv,
        Project(
          projectionOnAggregates.map(e => UnresolvedAlias(e._1)), logicalPlan).validate(tableEnv))
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
    if (tableEnv.isInstanceOf[StreamTableEnvironment]) {
      throw new TableException(s"Group by on stream tables is currently not supported.")
    }
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
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be joined.")
    }
    new Table(tableEnv,
      Join(this.logicalPlan, right.logicalPlan, JoinType.INNER, None).validate(tableEnv))
  }

  /**
    * Union two [[Table]]s with duplicate records removed.
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
    if (tableEnv.isInstanceOf[StreamTableEnvironment]) {
      throw new TableException(s"Union on stream tables is currently not supported.")
    }
    // check that right table belongs to the same TableEnvironment
    if (right.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be unioned.")
    }
    new Table(tableEnv, Union(logicalPlan, right.logicalPlan, false).validate(tableEnv))
  }

  /**
    * Union two [[Table]]s. Similar to an SQL UNION ALL. The fields of the two union operations
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
    new Table(tableEnv, Union(logicalPlan, right.logicalPlan, true).validate(tableEnv))
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
    val order: Seq[Ordering] = fields.map { case e =>
      e match {
        case o: Ordering => o
        case _ => Asc(e)
      }
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
    *   tab.orderBy("name DESC")
    * }}}
    */
  def orderBy(fields: String): Table = {
    val parsedFields = ExpressionParser.parseExpressionList(fields)
    orderBy(parsedFields: _*)
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
      .map(f => TypeConverter.sqlTypeToTypeInfo(f.getType.getSqlTypeName)).toArray

    // configure the table sink
    val configuredSink = sink.configure(fieldNames, fieldTypes)

    // emit the table to the configured table sink
    tableEnv.writeToSink(this, configuredSink)
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

    val projectionOnAggregates = fields.map(extractAggregations(_, table.tableEnv))
    val aggregations = projectionOnAggregates.flatMap(_._2)

    val logical = if (aggregations.nonEmpty) {
      Project(projectionOnAggregates.map(e => UnresolvedAlias(e._1)),
        Aggregate(groupKey, aggregations, table.logicalPlan).validate(table.tableEnv)
      )
    } else {
      Project(projectionOnAggregates.map(e => UnresolvedAlias(e._1)),
        Aggregate(groupKey, Nil, table.logicalPlan).validate(table.tableEnv))
    }

    new Table(table.tableEnv, logical.validate(table.tableEnv))
  }

  /**
    * Performs a selection operation on a grouped table. Similar to an SQL SELECT statement.
    * The field expressions can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   tab.groupBy("key").select("key, value.avg + " The average" as average")
    * }}}
    */
  def select(fields: String): Table = {
    val fieldExprs = ExpressionParser.parseExpressionList(fields)
    select(fieldExprs: _*)
  }
}
