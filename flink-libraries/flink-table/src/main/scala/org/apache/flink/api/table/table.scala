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
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.tools.RelBuilder.{AggCall, GroupKey}
import org.apache.calcite.util.NlsString
import org.apache.flink.api.table.plan.PlanGenException
import org.apache.flink.api.table.plan.RexNodeTranslator.extractAggCalls
import org.apache.flink.api.table.expressions._

import scala.collection.mutable
import scala.collection.JavaConverters._

case class BaseTable(
  private[flink] val relNode: RelNode,
  private[flink] val tableEnv: TableEnvironment)

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
  * @param relNode The root node of the relational Calcite [[RelNode]] tree.
  * @param tableEnv The [[TableEnvironment]] to which the table is bound.
  */
class Table(
  private[flink] override val relNode: RelNode,
  private[flink] override val tableEnv: TableEnvironment)
  extends BaseTable(relNode, tableEnv)
{

  def relBuilder = tableEnv.getRelBuilder

  def getRelNode: RelNode = relNode

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

    checkUniqueNames(fields)

    relBuilder.push(relNode)

    // separate aggregations and selection expressions
    val extractedAggCalls: List[(Expression, List[AggCall])] = fields
      .map(extractAggCalls(_, tableEnv)).toList

    // get aggregation calls
    val aggCalls: List[AggCall] = extractedAggCalls.flatMap(_._2)

    // apply aggregations
    if (aggCalls.nonEmpty) {
      // aggregation on stream table is not currently supported
      tableEnv match {
        case _: StreamTableEnvironment =>
          throw new TableException("Aggregation on stream tables is currently not supported.")
        case _ =>
          val emptyKey: GroupKey = relBuilder.groupKey()
          relBuilder.aggregate(emptyKey, aggCalls.toIterable.asJava)
      }
    }

    // get selection expressions
    val exprs: List[RexNode] = extractedAggCalls.map(_._1.toRexNode(relBuilder))

    relBuilder.project(exprs.toIterable.asJava)
    val projected = relBuilder.build()

    if(relNode == projected) {
      // Calcite's RelBuilder does not translate identity projects even if they rename fields.
      //   Add a projection ourselves (will be automatically removed by translation rules).
      new Table(createRenamingProject(exprs), tableEnv)
    } else {
      new Table(projected, tableEnv)
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

    val exprs = (renamings ++ remaining).map(_.toRexNode(relBuilder))

    new Table(createRenamingProject(exprs), tableEnv)
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

    relBuilder.push(relNode)
    relBuilder.filter(predicate.toRexNode(relBuilder))

    new Table(relBuilder.build(), tableEnv)
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

    // group by on stream tables is currently not supported
    tableEnv match {
      case _: StreamTableEnvironment =>
        throw new TableException("Group by on stream tables is currently not supported.")
      case _ => {
        relBuilder.push(relNode)
        val groupExpr = fields.map(_.toRexNode(relBuilder)).toIterable.asJava
        val groupKey = relBuilder.groupKey(groupExpr)

        new GroupedTable(relBuilder.build(), tableEnv, groupKey)
      }
    }
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
    // distinct on stream table is not currently supported
    tableEnv match {
      case _: StreamTableEnvironment =>
        throw new TableException("Distinct on stream tables is currently not supported.")
      case _ =>
        relBuilder.push(relNode)
        relBuilder.distinct()
        new Table(relBuilder.build(), tableEnv)
    }
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

    // join on stream tables is currently not supported
    tableEnv match {
      case _: StreamTableEnvironment =>
        throw new TableException("Join on stream tables is currently not supported.")
      case _ => {
        // check that right table belongs to the same TableEnvironment
        if (right.tableEnv != this.tableEnv) {
          throw new TableException("Only tables from the same TableEnvironment can be joined.")
        }

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
        new Table(join, tableEnv)
      }
    }
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
      throw new TableException("Only tables from the same TableEnvironment can be unioned.")
    }

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
    new Table(relBuilder.build(), tableEnv)
  }

  /**
    * Sorts the given [[Table]]. Similar to SQL ORDER BY.
    * The resulting Table is sorted globally sorted across all parallel partitions.
    *
    * Example:
    *
    * {{{
    *   tab.orderBy('name.desc)
    * }}}
    */
  def orderBy(fields: Expression*): Table = {
    relBuilder.push(relNode)

    if (! fields.forall {
      case x : UnresolvedFieldReference => true
      case x : Ordering => x.child.isInstanceOf[UnresolvedFieldReference]
      case _ => false
    }) {
      throw new IllegalArgumentException("All expressions must be field references " +
        "or asc/desc expressions.")
    }

    val exprs = fields.map(_.toRexNode(relBuilder))

    relBuilder.sort(exprs.asJava)
    new Table(relBuilder.build(), tableEnv)

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

  private def checkUniqueNames(exprs: Seq[Expression]): Unit = {
    val names: mutable.Set[String] = mutable.Set()

    exprs.foreach {
      case n: Naming =>
        // explicit name
        if (names.contains(n.name)) {
          throw new IllegalArgumentException(s"Duplicate field name $n.name.")
        } else {
          names.add(n.name)
        }
      case u: UnresolvedFieldReference =>
        // simple field forwarding
        if (names.contains(u.name)) {
          throw new IllegalArgumentException(s"Duplicate field name $u.name.")
        } else {
          names.add(u.name)
        }
      case _ => // Do nothing
    }
  }

}

/**
  * A table that has been grouped on a set of grouping keys.
  *
  * @param relNode The root node of the relational Calcite [[RelNode]] tree.
  * @param tableEnv The [[TableEnvironment]] to which the table is bound.
  * @param groupKey The Calcite [[GroupKey]] of this table.
  */
class GroupedTable(
  private[flink] override val relNode: RelNode,
  private[flink] override val tableEnv: TableEnvironment,
  private[flink] val groupKey: GroupKey) extends BaseTable(relNode, tableEnv) {

  def relBuilder = tableEnv.getRelBuilder

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

    relBuilder.push(relNode)

    // separate aggregations and selection expressions
    val extractedAggCalls: List[(Expression, List[AggCall])] = fields
      .map(extractAggCalls(_, tableEnv)).toList

    // get aggregation calls
    val aggCalls: List[AggCall] = extractedAggCalls.flatMap(_._2)

    // apply aggregations
    relBuilder.aggregate(groupKey, aggCalls.toIterable.asJava)

    // get selection expressions
    val exprs: List[RexNode] = try {
      extractedAggCalls.map(_._1.toRexNode(relBuilder))
    } catch {
      case iae: IllegalArgumentException  =>
        throw new IllegalArgumentException(
          "Only grouping fields and aggregations allowed after groupBy.", iae)
      case e: Exception => throw e
    }

    relBuilder.project(exprs.toIterable.asJava)

    new Table(relBuilder.build(), tableEnv)
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
