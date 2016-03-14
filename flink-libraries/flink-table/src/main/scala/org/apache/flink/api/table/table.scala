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

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.{RexInputRef, RexLiteral, RexCall, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilder.{AggCall, GroupKey}
import org.apache.calcite.util.NlsString
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.table.explain.PlanJsonParser
import org.apache.flink.api.table.plan.{PlanGenException, RexNodeTranslator}
import RexNodeTranslator.{toRexNode, extractAggCalls}
import org.apache.flink.api.table.expressions.{ExpressionParser, Naming, UnresolvedFieldReference, Expression}

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._

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
    val projected = relBuilder.build()

    if(relNode == projected) {
      // Calcite's RelBuilder does not translate identity projects even if they rename fields.
      //   Add a projection ourselves (will be automatically removed by translation rules).
      new Table(createRenamingProject(exprs), relBuilder)
    } else {
      new Table(projected, relBuilder)
    }

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

    val curNames = relNode.getRowType.getFieldNames.asScala

    // validate that AS has only field references
    if (! fields.forall( _.isInstanceOf[UnresolvedFieldReference] )) {
      throw new IllegalArgumentException("All expressions must be field references.")
    }
    // validate that we have not more field references than fields
    if ( fields.length > curNames.size) {
      throw new IllegalArgumentException("More field references than fields.")
    }

    val curFields = curNames.map(new UnresolvedFieldReference(_))

    val renamings = fields.zip(curFields).map {
      case (newName, oldName) => new Naming(oldName, newName.name)
    }
    val remaining = curFields.drop(fields.size)

    relBuilder.push(relNode)

    val exprs = (renamings ++ remaining).map(toRexNode(_, relBuilder))

    new Table(createRenamingProject(exprs), relBuilder)
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
    * Removes duplicate values and returns only distinct (different) values.
    *
    * Example:
    *
    * {{{
    *   in.select("key, value").distinct()
    * }}}
    */
  def distinct(): Table = {
    relBuilder.push(relNode)
    relBuilder.distinct()
    new Table(relBuilder.build(), relBuilder)
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
    new Table(join, relBuilder)
  }

  /**
    * Union two [[Table]]s. Similar to an SQL UNION ALL. The fields of the two union operations
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
  private[flink] def explain(extended: Boolean): String = {

    val ast = RelOptUtil.toString(relNode)
    val dataSet = this.toDataSet[Row]
    dataSet.output(new DiscardingOutputFormat[Row])
    val env = dataSet.getExecutionEnvironment
    val jasonSqlPlan = env.getExecutionPlan()
    val sqlPlan = PlanJsonParser.getSqlExecutionPlan(jasonSqlPlan, extended)
    val result = "== Abstract Syntax Tree ==\n" + ast + "\n" + "== Physical Execution Plan ==" +
      "\n" + sqlPlan
    return result

    ""
  }

  def explain(): String = explain(false)

  private def createRenamingProject(exprs: Seq[RexNode]): LogicalProject = {

    val names = exprs.map{ e =>
      e.getKind match {
        case SqlKind.AS =>
          e.asInstanceOf[RexCall].getOperands.get(1)
            .asInstanceOf[RexLiteral].getValue
            .asInstanceOf[NlsString].getValue
        case SqlKind.INPUT_REF =>
          relNode.getRowType.getFieldNames.get(e.asInstanceOf[RexInputRef].getIndex)
        case _ =>
          throw new PlanGenException("Unexpected expression type encountered.")
      }

    }

    LogicalProject.create(relNode, exprs.toList.asJava, names.toList.asJava)
  }

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
    *   in.select('key, 'value.avg + " The average" as 'average, 'other.substring(1, 10))
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
    relBuilder.aggregate(groupKey, aggCalls.toIterable.asJava)

    // get selection expressions
    val exprs: List[RexNode] = try {
      extractedAggCalls
        .map(_._1)
        .map(toRexNode(_, relBuilder))
    }
    catch {
      case iae: IllegalArgumentException  =>
        throw new IllegalArgumentException(
          "Only grouping fields and aggregations allowed after groupBy.", iae)
      case e: Exception => throw e
    }

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
    *   in.select("key, value.avg + " The average" as average, other.substring(1, 10)")
    * }}}
    */
  def select(fields: String): Table = {
    val fieldExprs = ExpressionParser.parseExpressionList(fields)
    select(fieldExprs: _*)
  }

}
