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

package org.apache.flink.table.api

import _root_.java.util.Collections.emptyList
import org.apache.flink.table.expressions.{Expression, ExpressionParser, LookupCallResolver}
import org.apache.flink.table.functions.TemporalTableFunction
import org.apache.flink.table.operations.JoinQueryOperation.JoinType
import org.apache.flink.table.operations.OperationExpressionsUtils._
import org.apache.flink.table.operations.{OperationTreeBuilder, QueryOperation}
import org.apache.flink.table.plan.QueryOperationConverter
import org.apache.flink.table.util.JavaScalaConversionUtil.toJava

import org.apache.calcite.rel.RelNode

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._

/**
  * The implementation of the [[Table]].
  *
  * NOTE: Currently, [[TableImpl]] is just a wrapper for RelNode
  * and all the methods in the class are not implemented. This is
  * used to support end-to-end tests for Blink planner. It will be
  * implemented when we support full stack Table API for Blink planner.
  *
  * @param tableEnv The [[TableEnvironment]] to which the table is bound.
  * @param operationTree logical representation
  */
class TableImpl(
    private[flink] val tableEnv: TableEnvironment,
    private[flink] val operationTree: QueryOperation) extends Table {

  private[flink] val operationTreeBuilder: OperationTreeBuilder = tableEnv.operationTreeBuilder

  private lazy val tableSchema: TableSchema = operationTree.getTableSchema

  private val toRelNodeConverter = new QueryOperationConverter(
    tableEnv.getRelBuilder, tableEnv.functionCatalog)

  private[flink] val callResolver = new LookupCallResolver(tableEnv.functionCatalog)

  /**
    * Returns the Calcite RelNode represent this Table.
    */
  def getRelNode: RelNode = operationTree.accept(toRelNodeConverter)

  override def getSchema: TableSchema = tableSchema

  override def printSchema(): Unit = print(tableSchema.toString)

  override def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields).asScala: _*)
  }

  override def select(fields: Expression*): Table = {
    val expressionsWithResolvedCalls = fields.map(_.accept(callResolver)).asJava
    val extracted = extractAggregationsAndProperties(expressionsWithResolvedCalls)

    if (!extracted.getWindowProperties.isEmpty) {
      throw new ValidationException("Window properties can only be used on windowed tables.")
    }

    if (!extracted.getAggregations.isEmpty) {
      wrap(
        operationTreeBuilder.project(extracted.getProjections,
          operationTreeBuilder.aggregate(emptyList[Expression], extracted.getAggregations,
            operationTree
          )
        )
      )
    } else {
      wrap(operationTreeBuilder.project(expressionsWithResolvedCalls, operationTree))
    }
  }

  override def createTemporalTableFunction(
    timeAttribute: String,
    primaryKey: String): TemporalTableFunction = ???

  override def createTemporalTableFunction(
    timeAttribute: Expression,
    primaryKey: Expression): TemporalTableFunction = ???

  override def as(fields: String): Table = {
    as(ExpressionParser.parseExpressionList(fields).asScala: _*)
  }

  override def as(fields: Expression*): Table = {
    new TableImpl(tableEnv, operationTreeBuilder.alias(fields.asJava, operationTree))
  }

  override def filter(predicate: String): Table = {
    filter(ExpressionParser.parseExpression(predicate))
  }

  override def filter(predicate: Expression): Table = {
    val resolvedCallPredicate = predicate.accept(callResolver)
    new TableImpl(tableEnv, operationTreeBuilder.filter(resolvedCallPredicate, operationTree))
  }

  override def where(predicate: String): Table = {
    filter(predicate)
  }

  override def where(predicate: Expression): Table = {
    filter(predicate)
  }

  override def groupBy(fields: String): GroupedTable = {
    groupBy(ExpressionParser.parseExpressionList(fields).asScala: _*)
  }

  override def groupBy(fields: Expression*): GroupedTable = {
    new GroupedTableImpl(this, fields)
  }

  override def distinct(): Table = {
    new TableImpl(tableEnv, operationTreeBuilder.distinct(operationTree))
  }

  override def join(right: Table): Table = {
    joinInternal(right, None, JoinType.INNER)
  }

  override def join(right: Table, joinPredicate: String): Table = {
    join(right, ExpressionParser.parseExpression(joinPredicate))
  }

  override def join(right: Table, joinPredicate: Expression): Table = {
    joinInternal(right, Some(joinPredicate), JoinType.INNER)
  }

  override def leftOuterJoin(right: Table): Table = {
    joinInternal(right, None, JoinType.LEFT_OUTER)
  }

  override def leftOuterJoin(right: Table, joinPredicate: String): Table = {
    leftOuterJoin(right, ExpressionParser.parseExpression(joinPredicate))
  }

  override def leftOuterJoin(right: Table, joinPredicate: Expression): Table = {
    joinInternal(right, Some(joinPredicate), JoinType.LEFT_OUTER)
  }

  override def rightOuterJoin(right: Table, joinPredicate: String): Table = {
    rightOuterJoin(right, ExpressionParser.parseExpression(joinPredicate))
  }

  override def rightOuterJoin(right: Table, joinPredicate: Expression): Table = {
    joinInternal(right, Some(joinPredicate), JoinType.RIGHT_OUTER)
  }

  override def fullOuterJoin(right: Table, joinPredicate: String): Table = {
    fullOuterJoin(right, ExpressionParser.parseExpression(joinPredicate))
  }

  override def fullOuterJoin(right: Table, joinPredicate: Expression): Table = {
    joinInternal(right, Some(joinPredicate), JoinType.FULL_OUTER)
  }

  private def joinInternal(
      right: Table,
      joinPredicate: Option[Expression],
      joinType: JoinType)
  : Table = {
    // check that the TableEnvironment of right table is not null
    // and right table belongs to the same TableEnvironment
    if (right.asInstanceOf[TableImpl].tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be joined.")
    }

    wrap(operationTreeBuilder.join(
      this.operationTree,
      right.getQueryOperation,
      joinType,
      toJava(joinPredicate),
      correlated = false))
  }

  override def joinLateral(tableFunctionCall: String): Table = {
    joinLateral(ExpressionParser.parseExpression(tableFunctionCall))
  }

  override def joinLateral(tableFunctionCall: Expression): Table = {
    joinLateralInternal(tableFunctionCall, None, JoinType.INNER)
  }

  override def joinLateral(tableFunctionCall: String, joinPredicate: String): Table = {
    joinLateral(
      ExpressionParser.parseExpression(tableFunctionCall),
      ExpressionParser.parseExpression(joinPredicate))
  }

  override def joinLateral(tableFunctionCall: Expression, joinPredicate: Expression): Table = {
    joinLateralInternal(tableFunctionCall, Some(joinPredicate), JoinType.INNER)
  }

  override def leftOuterJoinLateral(tableFunctionCall: String): Table = {
    leftOuterJoinLateral(ExpressionParser.parseExpression(tableFunctionCall))
  }

  override def leftOuterJoinLateral(tableFunctionCall: Expression): Table = {
    joinLateralInternal(tableFunctionCall, None, JoinType.LEFT_OUTER)
  }

  override def leftOuterJoinLateral(tableFunctionCall: String, joinPredicate: String): Table = {
    leftOuterJoinLateral(
      ExpressionParser.parseExpression(tableFunctionCall),
      ExpressionParser.parseExpression(joinPredicate))
  }

  override def leftOuterJoinLateral(
      tableFunctionCall: Expression, joinPredicate: Expression): Table = {
    joinLateralInternal(tableFunctionCall, Some(joinPredicate), JoinType.LEFT_OUTER)
  }

  private def joinLateralInternal(
      callExpr: Expression,
      joinPredicate: Option[Expression],
      joinType: JoinType): Table = {

    // check join type
    if (joinType != JoinType.INNER && joinType != JoinType.LEFT_OUTER) {
      throw new ValidationException(
        "Table functions are currently only supported for inner and left outer lateral joins.")
    }
    wrap(operationTreeBuilder.joinLateral(
      operationTree,
      callExpr,
      joinType,
      toJava(joinPredicate)
    ))
  }

  override def minus(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    val rightImpl = right.asInstanceOf[TableImpl]
    if (rightImpl.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be " +
          "subtracted.")
    }
    wrap(operationTreeBuilder.minus(operationTree, rightImpl.operationTree, all = false))
  }

  override def minusAll(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    val rightImpl = right.asInstanceOf[TableImpl]
    if (rightImpl.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be " +
          "subtracted.")
    }

    wrap(operationTreeBuilder.minus(operationTree, rightImpl.operationTree, all = true))
  }

  override def union(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    val rightImpl = right.asInstanceOf[TableImpl]
    if (rightImpl.tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be unioned.")
    }

    wrap(operationTreeBuilder.union(operationTree, rightImpl.operationTree, all = false))
  }

  override def unionAll(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    val rightImpl = right.asInstanceOf[TableImpl]
    if (right.asInstanceOf[TableImpl].tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be unioned.")
    }

    wrap(operationTreeBuilder.union(operationTree, rightImpl.operationTree, all = true))
  }

  override def intersect(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    val rightImpl = right.asInstanceOf[TableImpl]
    if (rightImpl.tableEnv != this.tableEnv) {
      throw new ValidationException(
        "Only tables from the same TableEnvironment can be intersected.")
    }

    wrap(operationTreeBuilder.intersect(operationTree, rightImpl.operationTree, all = false))
  }

  override def intersectAll(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    val rightImpl = right.asInstanceOf[TableImpl]
    if (rightImpl.tableEnv != this.tableEnv) {
      throw new ValidationException(
        "Only tables from the same TableEnvironment can be intersected.")
    }

    wrap(operationTreeBuilder.intersect(operationTree, rightImpl.operationTree, all = true))
  }

  override def orderBy(fields: String): Table = {
    orderBy(ExpressionParser.parseExpressionList(fields).asScala: _*)
  }

  override def orderBy(fields: Expression*): Table = {
    wrap(operationTreeBuilder.sort(fields.map(_.accept(callResolver)).asJava, operationTree))
  }

  override def offset(offset: Int): Table = {
    wrap(operationTreeBuilder.limitWithOffset(offset, operationTree))
  }

  override def fetch(fetch: Int): Table = {
    if (fetch < 0) {
      throw new ValidationException("FETCH count must be equal or larger than 0.")
    }
    wrap(operationTreeBuilder.limitWithFetch(fetch, operationTree))
  }

  override def insertInto(tablePath: String, tablePathContinued: String*): Unit = ???

  override def insertInto(
    conf: QueryConfig,
    tablePath: String,
    tablePathContinued: String*): Unit = ???

  override def insertInto(
    tableName: String,
    conf: QueryConfig): Unit = ???

  override def window(groupWindow: GroupWindow): GroupWindowedTable = ???

  override def window(overWindows: OverWindow*): OverWindowedTable = {
    new OverWindowedTableImpl(this, overWindows)
  }

  override def addColumns(fields: String): Table = ???

  override def addColumns(fields: Expression*): Table = ???

  override def addOrReplaceColumns(fields: String): Table = ???

  override def addOrReplaceColumns(fields: Expression*): Table = ???

  override def renameColumns(fields: String): Table = ???

  override def renameColumns(fields: Expression*): Table = ???

  override def dropColumns(fields: String): Table = ???

  override def dropColumns(fields: Expression*): Table = ???

  override def map(mapFunction: String): Table = ???

  override def map(mapFunction: Expression): Table = ???

  override def flatMap(tableFunction: String): Table = ???

  override def flatMap(tableFunction: Expression): Table = ???

  override def getQueryOperation: QueryOperation = operationTree

  override def aggregate(aggregateFunction: String): AggregatedTable = {
    aggregate(ExpressionParser.parseExpression(aggregateFunction))
  }

  override def aggregate(aggregateFunction: Expression): AggregatedTable = {
    groupBy().aggregate(aggregateFunction)
  }

  override def flatAggregate(tableAggregateFunction: String): FlatAggregateTable = ???

  override def flatAggregate(tableAggregateFunction: Expression): FlatAggregateTable = ???

  private def wrap(operation: QueryOperation): Table = {
    new TableImpl(tableEnv, operation)
  }
}

/**
  * The implementation of a [[GroupedTable]] that has been grouped on a set of grouping keys.
  */
class GroupedTableImpl(
    private[flink] val table: Table,
    private[flink] val groupKeys: Seq[Expression])
    extends GroupedTable {

  private val tableImpl = table.asInstanceOf[TableImpl]

  override def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields).asScala: _*)
  }

  override def select(fields: Expression*): Table = {
    val expressionsWithResolvedCalls = fields.map(_.accept(tableImpl.callResolver)).asJava
    val extracted = extractAggregationsAndProperties(expressionsWithResolvedCalls)

    if (!extracted.getWindowProperties.isEmpty) {
      throw new ValidationException("Window properties can only be used on windowed tables.")
    }

    new TableImpl(tableImpl.tableEnv,
      tableImpl.operationTreeBuilder.project(extracted.getProjections,
        tableImpl.operationTreeBuilder.aggregate(
          groupKeys.asJava,
          extracted.getAggregations,
          tableImpl.operationTree
        )
      ))
  }

  override def aggregate(aggregateFunction: String): AggregatedTable = {
    aggregate(ExpressionParser.parseExpression(aggregateFunction))
  }

  override def aggregate(aggregateFunction: Expression): AggregatedTable = {
    new AggregatedTableImpl(table, groupKeys, aggregateFunction)
  }

  override def flatAggregate(tableAggregateFunction: String): FlatAggregateTable = {
    flatAggregate(ExpressionParser.parseExpression(tableAggregateFunction))
  }

  override def flatAggregate(tableAggregateFunction: Expression): FlatAggregateTable = {
    ???
  }
}

/**
  * The implementation of an [[AggregatedTable]] that has been performed on an aggregate function.
  */
class AggregatedTableImpl(
    private[flink] val table: Table,
    private[flink] val groupKeys: Seq[Expression],
    private[flink] val aggregateFunction: Expression)
    extends AggregatedTable {

  private val tableImpl = table.asInstanceOf[TableImpl]

  override def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields).asScala: _*)
  }

  override def select(fields: Expression*): Table = {
    new TableImpl(tableImpl.tableEnv,
      tableImpl.operationTreeBuilder.project(fields,
        tableImpl.operationTreeBuilder.aggregate(
          groupKeys.asJava,
          aggregateFunction,
          tableImpl.operationTree
        )
      ))
  }
}

/**
  * The implementation of an [[OverWindowedTable]] that has been windowed for [[OverWindow]]s.
  */
class OverWindowedTableImpl(
    private[flink] val table: Table,
    private[flink] val overWindows: Seq[OverWindow])
  extends OverWindowedTable{

  private val tableImpl = table.asInstanceOf[TableImpl]

  override def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields).asScala: _*)
  }

  override def select(fields: Expression*): Table = {
    new TableImpl(
      tableImpl.tableEnv,
      tableImpl.operationTreeBuilder
        .project(fields.asJava,
          tableImpl.operationTree,
          overWindows.asJava)
    )
  }
}
