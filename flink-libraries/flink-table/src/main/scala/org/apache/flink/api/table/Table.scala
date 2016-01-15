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
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilder.{AggCall, GroupKey}
import org.apache.flink.api.table.plan.RexNodeTranslator
import RexNodeTranslator.{toRexNode, extractAggCalls}
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.table.parser.ExpressionParser

import scala.collection.JavaConverters._

case class BaseTable(
    private[flink] val relNode: RelNode,
    private[flink] val relBuilder: RelBuilder)

/**
 * The abstraction for writing Table API programs. Similar to how the batch and streaming APIs
 * have [[org.apache.flink.api.scala.DataSet]] and
 * [[org.apache.flink.streaming.api.scala.DataStream]].
 *
 * Use the methods of [[Table]] to transform data. Use
 * [[org.apache.flink.api.java.table.TableEnvironment]] to convert a [[Table]] back to a DataSet
 * or DataStream.
 *
 * When using Scala a [[Table]] can also be converted using implicit conversions.
 *
 * Example:
 *
 * {{{
 *   val table = set.toTable('a, 'b)
 *   ...
 *   val table2 = ...
 *   val set = table2.toDataSet[MyType]
 * }}}
 *
 * Operations such as [[join]], [[select]], [[where]] and [[groupBy]] either take arguments
 * in a Scala DSL or as an expression String. Please refer to the documentation for the expression
 * syntax.
 */
class Table(
  private[flink] override val relNode: RelNode,
  private[flink] override val relBuilder: RelBuilder)
  extends BaseTable(relNode, relBuilder)
{

  /**
   * Performs a selection operation. Similar to an SQL SELECT statement. The field expressions
   * can contain complex expressions and aggregations.
   *
   * Example:
   *
   * {{{
   *   in.select('key, 'value.avg + " The average" as 'average, 'other.substring(0, 10))
   * }}}
   */
  def select(fields: Expression*): Table = {

    relBuilder.push(relNode)

    // separate aggregations and selection expressions
    val extractedAggCalls: List[(Expression, List[AggCall])] = fields
      .map(extractAggCalls(_, relBuilder)).toList

    // get aggregation calls
    val aggCalls: List[AggCall] = extractedAggCalls
      .map(_._2).reduce( (x,y) => x ::: y)

    // apply aggregations
    if (aggCalls.nonEmpty) {
      val emptyKey: GroupKey = relBuilder.groupKey()
      relBuilder.aggregate(emptyKey, aggCalls.toIterable.asJava)
    }

    // get selection expressions
    val exprs: List[RexNode] = extractedAggCalls
      .map(_._1)
      .map(toRexNode(_, relBuilder))

    relBuilder.project(exprs.toIterable.asJava)
    new Table(relBuilder.build(), relBuilder)
  }

  /**
   * Performs a selection operation. Similar to an SQL SELECT statement. The field expressions
   * can contain complex expressions and aggregations.
   *
   * Example:
   *
   * {{{
   *   in.select("key, value.avg + " The average" as average, other.substring(0, 10)")
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
   *   in.as('a, 'b)
   * }}}
   */
  def as(fields: Expression*): Table = {

    relBuilder.push(relNode)
    val expressions = fields.map(toRexNode(_, relBuilder)).toIterable.asJava
    val names = fields.map(_.name).toIterable.asJava
    relBuilder.project(expressions, names)
    new Table(relBuilder.build(), relBuilder)
  }

  /**
   * Renames the fields of the expression result. Use this to disambiguate fields before
   * joining to operations.
   *
   * Example:
   *
   * {{{
   *   in.as("a, b")
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
   *   in.filter('name === "Fred")
   * }}}
   */
  def filter(predicate: Expression): Table = {

    relBuilder.push(relNode)
    val pred = toRexNode(predicate, relBuilder)
    relBuilder.filter(pred)
    new Table(relBuilder.build(), relBuilder)
  }

  /**
   * Filters out elements that don't pass the filter predicate. Similar to a SQL WHERE
   * clause.
   *
   * Example:
   *
   * {{{
   *   in.filter("name = 'Fred'")
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
   *   in.where('name === "Fred")
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
   *   in.where("name = 'Fred'")
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
   *   in.groupBy('key).select('key, 'value.avg)
   * }}}
   */
  def groupBy(fields: Expression*): GroupedTable = {

    relBuilder.push(relNode)
    val groupExpr = fields.map(toRexNode(_, relBuilder)).toIterable.asJava
    val groupKey = relBuilder.groupKey(groupExpr)

    new GroupedTable(relBuilder.build(), relBuilder, groupKey)
  }

  /**
   * Groups the elements on some grouping keys. Use this before a selection with aggregations
   * to perform the aggregation on a per-group basis. Similar to a SQL GROUP BY statement.
   *
   * Example:
   *
   * {{{
   *   in.groupBy("key").select("key, value.avg")
   * }}}
   */
  def groupBy(fields: String): GroupedTable = {
    val fieldsExpr = ExpressionParser.parseExpressionList(fields)
    groupBy(fieldsExpr: _*)
  }

  /**
   * Joins two [[Table]]s. Similar to an SQL join. The fields of the two joined
   * operations must not overlap, use [[as]] to rename fields if necessary. You can use
   * where and select clauses after a join to further specify the behaviour of the join.
   *
   * Example:
   *
   * {{{
   *   left.join(right).where('a === 'b && 'c > 3).select('a, 'b, 'd)
   * }}}
   */
  def join(right: Table): Table = {

    // check that join inputs do not have overlapping field names
    val leftFields = relNode.getRowType.getFieldNames.asScala.toSet
    val rightFields = right.relNode.getRowType.getFieldNames.asScala.toSet
    if (leftFields.intersect(rightFields).nonEmpty) {
      throw new IllegalArgumentException("Overlapping fields names on join input.")
    }

    relBuilder.push(relNode)
    relBuilder.push(right.relNode)

    relBuilder.join(JoinRelType.INNER, relBuilder.literal(true))
    val join = relBuilder.build()
    val rowT = join.getRowType()
    new Table(join, relBuilder)
  }

  /**
   * Union two[[Table]]s. Similar to an SQL UNION ALL. The fields of the two union operations
   * must fully overlap.
   *
   * Example:
   *
   * {{{
   *   left.unionAll(right)
   * }}}
   */
  def unionAll(right: Table): Table = {

    val leftRowType: List[RelDataTypeField] = relNode.getRowType.getFieldList.asScala.toList
    val rightRowType: List[RelDataTypeField] = right.relNode.getRowType.getFieldList.asScala.toList

    if (leftRowType.length != rightRowType.length) {
      throw new IllegalArgumentException("Unioned tables have varying row schema.")
    }
    else {
      val zipped: List[(RelDataTypeField, RelDataTypeField)] = leftRowType.zip(rightRowType)
      zipped.foreach { case (x, y) =>
        if (!x.getName.equals(y.getName) || x.getType != y.getType) {
          throw new IllegalArgumentException("Unioned tables have varying row schema.")
        }
      }
    }

    relBuilder.push(relNode)
    relBuilder.push(right.relNode)

    relBuilder.union(true)
    new Table(relBuilder.build(), relBuilder)
  }

  /**
   * Get the process of the sql parsing, print AST and physical execution plan.The AST
   * show the structure of the supplied statement. The execution plan shows how the table 
   * referenced by the statement will be scanned.
   */
  def explain(extended: Boolean): String = {

    // TODO: enable once toDataSet() is working again

//    val ast = operation
//    val dataSet = this.toDataSet[Row]
//    val env = dataSet.getExecutionEnvironment
//    dataSet.output(new DiscardingOutputFormat[Row])
//    val jasonSqlPlan = env.getExecutionPlan()
//    val sqlPlan = PlanJsonParser.getSqlExecutionPlan(jasonSqlPlan, extended)
//    val result = "== Abstract Syntax Tree ==\n" + ast + "\n\n" + "== Physical Execution Plan ==" +
//      "\n" + sqlPlan
//    return result

    ""
  }

  def explain(): String = explain(false)
}

class GroupedTable(
    private[flink] override val relNode: RelNode,
    private[flink] override val relBuilder: RelBuilder,
    private[flink] val groupKey: GroupKey) extends BaseTable(relNode, relBuilder) {

  /**
    * Performs a selection operation. Similar to an SQL SELECT statement. The field expressions
    * can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   in.select('key, 'value.avg + " The average" as 'average, 'other.substring(0, 10))
    * }}}
    */
  def select(fields: Expression*): Table = {

    relBuilder.push(relNode)

    // separate aggregations and selection expressions
    val extractedAggCalls: List[(Expression, List[AggCall])] = fields
      .map(extractAggCalls(_, relBuilder)).toList

    // get aggregation calls
    val aggCalls: List[AggCall] = extractedAggCalls
      .map(_._2).reduce( (x,y) => x ::: y)

    // apply aggregations
    if (aggCalls.nonEmpty) {
      relBuilder.aggregate(groupKey, aggCalls.toIterable.asJava)
    }

    // get selection expressions
    val exprs: List[RexNode] = extractedAggCalls
      .map(_._1)
      .map(toRexNode(_, relBuilder))

    relBuilder.project(exprs.toIterable.asJava)
    new Table(relBuilder.build(), relBuilder)
  }

  /**
    * Performs a selection operation. Similar to an SQL SELECT statement. The field expressions
    * can contain complex expressions and aggregations.
    *
    * Example:
    *
    * {{{
    *   in.select("key, value.avg + " The average" as average, other.substring(0, 10)")
    * }}}
    */
  def select(fields: String): Table = {
    val fieldExprs = ExpressionParser.parseExpressionList(fields)
    select(fieldExprs: _*)
  }

}
