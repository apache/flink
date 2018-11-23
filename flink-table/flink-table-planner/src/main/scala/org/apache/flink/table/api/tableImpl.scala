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
import _root_.java.util.function.Supplier

import org.apache.calcite.rel.RelNode
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.table.expressions.ApiExpressionUtils.{extractAggregationsAndProperties, extractFieldReferences, replaceAggregationsAndProperties}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.{TemporalTableFunction, TemporalTableFunctionImpl}
import org.apache.flink.table.operations.TableOperation
import org.apache.flink.table.plan.OperationTreeBuilder
import org.apache.flink.table.plan.ProjectionTranslator.getLeadingNonAliasExpr
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.util.JavaScalaConversionUtil.toJava

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._

/**
  * The implementation of the [[Table]].
  *
  * In [[TableImpl]], string expressions are parsed by [[ExpressionParser]] into [[Expression]]s.
  *
  * __NOTE__: Currently, the implementation depends on Calcite.
  *
  * @param tableEnv The [[TableEnvironment]] to which the table is bound.
  * @param operationTree logical representation
  */
class TableImpl(
    private[flink] val tableEnv: TableEnvironment,
    private[flink] val operationTree: TableOperation)
  extends Table {

  private[flink] val operationTreeBuilder: OperationTreeBuilder = tableEnv.operationTreeBuilder

  private[flink] val callResolver = new LookupCallResolver(tableEnv.functionCatalog)

  private lazy val tableSchema: TableSchema = operationTree.getTableSchema

  var tableName: String = _

  def getRelNode: RelNode = operationTree.asInstanceOf[LogicalNode]
    .toRelNode(tableEnv.getRelBuilder)

  override def getSchema: TableSchema = tableSchema

  override def printSchema(): Unit = print(tableSchema.toString)

  override def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields).asScala: _*)
  }

  override def select(fields: Expression*): Table = {
    selectInternal(fields)
  }

  private[flink] def getUniqueAttributeSupplier: Supplier[String] = {
    new Supplier[String] {
      override def get(): String = tableEnv.createUniqueAttributeName()
    }
  }

  private def selectInternal(fields: Seq[Expression]): Table = {
    val expressionsWithResolvedCalls = fields.map(_.accept(callResolver)).asJava
    val extracted = extractAggregationsAndProperties(
      expressionsWithResolvedCalls,
      getUniqueAttributeSupplier)

    val aggNames = extracted.getAggregations
    val propNames = extracted.getWindowProperties
    if (!propNames.isEmpty) {
      throw new ValidationException("Window properties can only be used on windowed tables.")
    }

    if (!aggNames.isEmpty) {
      val projectsOnAgg =
        replaceAggregationsAndProperties(
          expressionsWithResolvedCalls,
          aggNames,
          propNames)
      val projectFields = extractFieldReferences(expressionsWithResolvedCalls)

      wrap(
        operationTreeBuilder.project(projectsOnAgg,
          operationTreeBuilder.aggregate(emptyList[Expression], aggNames,
            operationTreeBuilder.project(projectFields, operationTree.asInstanceOf[LogicalNode])
          )
        )
      )
    } else {
      wrap(operationTreeBuilder.project(expressionsWithResolvedCalls, operationTree))
    }
  }

  override def createTemporalTableFunction(
      timeAttribute: String,
      primaryKey: String)
    : TemporalTableFunction = {
    createTemporalTableFunction(
      ExpressionParser.parseExpression(timeAttribute),
      ExpressionParser.parseExpression(primaryKey))
  }

  override def createTemporalTableFunction(
      timeAttribute: Expression,
      primaryKey: Expression)
    : TemporalTableFunction = {
    val temporalTable = operationTreeBuilder.createTemporalTable(
      timeAttribute,
      primaryKey,
      operationTree)

    TemporalTableFunctionImpl.create(
      operationTree,
      temporalTable.timeAttribute,
      temporalTable.primaryKey)
  }

  override def as(fields: String): Table = {
    as(ExpressionParser.parseExpressionList(fields).asScala: _*)
  }

  override def as(fields: Expression*): Table = {
    asInternal(fields)
  }

  private def asInternal(fields: Seq[Expression]): Table = {
    new TableImpl(tableEnv, operationTreeBuilder.alias(fields.asJava, operationTree))
  }

  override def filter(predicate: String): Table = {
    filter(ExpressionParser.parseExpression(predicate))
  }

  override def filter(predicate: Expression): Table = {
    filterInternal(predicate)
  }

  private def filterInternal(predicate: Expression): Table = {
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
    groupByInternal(fields)
  }

  private def groupByInternal(fields: Seq[Expression]): GroupedTable = {
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
        right.asInstanceOf[TableImpl].operationTree,
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
    joinLateralInternal(
      tableFunctionCall,
      Some(joinPredicate),
      JoinType.INNER)
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
    joinLateralInternal(
      tableFunctionCall,
      Some(joinPredicate),
      JoinType.LEFT_OUTER)
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
        callExpr.accept(callResolver),
        joinType,
        toJava(joinPredicate.map(_.accept(callResolver))
      )))
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
    orderByInternal(fields)
  }

  private def orderByInternal(fields: Seq[Expression]): Table = {
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

  override def insertInto(tableName: String): Unit = {
    insertInto(tableName, tableEnv.queryConfig)
  }

  override def insertInto(tableName: String, conf: QueryConfig): Unit = {
    tableEnv.insertInto(this, tableName, conf)
  }

  override def window(window: GroupWindow): GroupWindowedTable = {
    new GroupWindowedTableImpl(this, window)
  }

  override def window(overWindows: OverWindow*): OverWindowedTable = {

    if (tableEnv.isInstanceOf[BatchTableEnvironment]) {
      throw new TableException("Over-windows for batch tables are currently not supported.")
    }

    if (overWindows.size != 1) {
      throw new TableException("Over-Windows are currently only supported single window.")
    }

    new OverWindowedTableImpl(this, overWindows)
  }

  override def addColumns(fields: String): Table = {
    addColumns(ExpressionParser.parseExpressionList(fields): _*);
  }

  override def addColumns(fields: Expression*): Table = {
    addColumnsOperation(false, fields: _*)
  }

  override def addOrReplaceColumns(fields: String): Table = {
    addOrReplaceColumns(ExpressionParser.parseExpressionList(fields): _*)
  }

  override def addOrReplaceColumns(fields: Expression*): Table = {
    addColumnsOperation(true, fields: _*)
  }

  private def addColumnsOperation(replaceIfExist: Boolean, fields: Expression*): Table = {
    val expressionsWithResolvedCalls = fields.map(_.accept(callResolver)).asJava
    val extracted = extractAggregationsAndProperties(
      expressionsWithResolvedCalls,
      getUniqueAttributeSupplier)

    val aggNames = extracted.getAggregations

    if(aggNames.nonEmpty){
      throw new TableException(
        s"The added field expression cannot be an aggregation, found [${aggNames.head}].")
    }

    wrap(operationTreeBuilder.addColumns(
      replaceIfExist, expressionsWithResolvedCalls, operationTree))
  }

  override def renameColumns(fields: String): Table = {
    renameColumns(ExpressionParser.parseExpressionList(fields): _*)
  }

  override def renameColumns(fields: Expression*): Table = {
    wrap(operationTreeBuilder.renameColumns(fields, operationTree))
  }

  override def dropColumns(fields: String): Table = {
    dropColumns(ExpressionParser.parseExpressionList(fields): _*)
  }

  override def dropColumns(fields: Expression*): Table = {
    wrap(operationTreeBuilder.dropColumns(fields, operationTree))
  }

  override def map(mapFunction: String): Table = {
    map(ExpressionParser.parseExpression(mapFunction))
  }

  override def map(mapFunction: Expression): Table = {
    val resolvedMapFunction = mapFunction.accept(callResolver)
    getLeadingNonAliasExpr(resolvedMapFunction) match {
      case callExpr: CallExpression if callExpr.getFunctionDefinition.getType ==
        FunctionDefinition.Type.SCALAR_FUNCTION =>
      case _ =>
        throw new ValidationException("Only ScalarFunction can be used in the map operator.")
    }

    wrap(operationTreeBuilder.map(resolvedMapFunction, operationTree))
  }

  /**
    * Registers an unique table name under the table environment
    * and return the registered table name.
    */
  override def toString: String = {
    if (tableName == null) {
      tableName = "UnnamedTable$" + tableEnv.attrNameCntr.getAndIncrement()
      tableEnv.registerTable(tableName, this)
    }
    tableName
  }

  private def wrap(operation: TableOperation): Table = {
    new TableImpl(tableEnv, operation)
  }
}

/**
  * The implementation of a [[GroupedTable]] that has been grouped on a set of grouping keys.
  */
class GroupedTableImpl(
    private[flink] val table: Table,
    private[flink] val groupKey: Seq[Expression])
  extends GroupedTable {

  private val tableImpl = table.asInstanceOf[TableImpl]

  override def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields).asScala: _*)
  }

  override def select(fields: Expression*): Table = {
    selectInternal(fields)
  }

  private def selectInternal(fields: Seq[Expression]): Table = {
    val expressionsWithResolvedCalls = fields.map(_.accept(tableImpl.callResolver)).asJava
    val extracted = extractAggregationsAndProperties(expressionsWithResolvedCalls,
      tableImpl.getUniqueAttributeSupplier)

    val aggNames = extracted.getAggregations
    val propNames = extracted.getWindowProperties
    if (!propNames.isEmpty) {
      throw new ValidationException("Window properties can only be used on windowed tables.")
    }

    val projectsOnAgg =
      replaceAggregationsAndProperties(
        expressionsWithResolvedCalls,
        aggNames,
        propNames)
    val projectFields = extractFieldReferences((expressionsWithResolvedCalls.asScala ++ groupKey)
      .asJava)

    new TableImpl(tableImpl.tableEnv,
      tableImpl.operationTreeBuilder.project(projectsOnAgg,
        tableImpl.operationTreeBuilder.aggregate(
          groupKey.asJava,
          aggNames,
          tableImpl.operationTreeBuilder.project(projectFields, tableImpl.operationTree)
        )
      ))
  }
}

/**
  * The implementation of a [[GroupWindowedTable]] that has been windowed for [[GroupWindow]]s.
  */
class GroupWindowedTableImpl(
    private[flink] val table: Table,
    private[flink] val window: GroupWindow)
  extends GroupWindowedTable {

  override def groupBy(fields: String): WindowGroupedTable = {
    groupBy(ExpressionParser.parseExpressionList(fields).asScala: _*)
  }

  override def groupBy(fields: Expression*): WindowGroupedTable = {
    val fieldsWithoutWindow = fields.filterNot(window.getAlias.equals(_))
    if (fields.size != fieldsWithoutWindow.size + 1) {
      throw new ValidationException("GroupBy must contain exactly one window alias.")
    }

    new WindowGroupedTableImpl(table, fieldsWithoutWindow, window)
  }
}

/**
  * The implementation of a [[WindowGroupedTable]] that has been windowed and grouped for
  * [[GroupWindow]]s.
  */
class WindowGroupedTableImpl(
    private[flink] val table: Table,
    private[flink] val groupKeys: Seq[Expression],
    private[flink] val window: GroupWindow)
  extends WindowGroupedTable {

  private val tableImpl = table.asInstanceOf[TableImpl]

  override def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields).asScala: _*)
  }

  override def select(fields: Expression*): Table = {
    selectInternal(
      groupKeys,
      window,
      fields)
  }

  private def selectInternal(
      groupKeys: Seq[Expression],
      window: GroupWindow,
      fields: Seq[Expression])
    : Table = {
    val expressionsWithResolvedCalls = fields.map(_.accept(tableImpl.callResolver)).asJava
    val extracted = extractAggregationsAndProperties(
      expressionsWithResolvedCalls,
      tableImpl.getUniqueAttributeSupplier)

    val aggNames = extracted.getAggregations
    val propNames = extracted.getWindowProperties

    val projectsOnAgg =
      replaceAggregationsAndProperties(
        expressionsWithResolvedCalls,
        aggNames,
        propNames)
    val projectFields = extractFieldReferences(
      (expressionsWithResolvedCalls.asScala ++ groupKeys :+ this.window.getTimeField)
        .asJava)

    new TableImpl(tableImpl.tableEnv,
      tableImpl.operationTreeBuilder.project(
        projectsOnAgg,
        tableImpl.operationTreeBuilder.windowAggregate(
          groupKeys.asJava,
          window,
          propNames,
          aggNames,
          tableImpl.operationTreeBuilder.project(projectFields, tableImpl.operationTree)
        ),
        // required for proper resolution of the time attribute in multi-windows
        explicitAlias = true
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
    selectInternal(
      fields,
      overWindows)
  }

  private def selectInternal(
    fields: Seq[Expression],
    logicalOverWindows: Seq[OverWindow])
  : Table = {
    val expressionsWithResolvedCalls = fields.map(_.accept(tableImpl.callResolver))

    new TableImpl(
      tableImpl.tableEnv,
      tableImpl.operationTreeBuilder
        .project(expressionsWithResolvedCalls.asJava,
          tableImpl.operationTree,
          logicalOverWindows.asJava)
    )
  }
}
