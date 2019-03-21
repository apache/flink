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

import org.apache.calcite.rel.RelNode
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.table.calcite.FlinkRelBuilder
import org.apache.flink.table.expressions.{Alias, Asc, Expression, ExpressionBridge,
  ExpressionParser, Ordering, PlannerExpression, ResolvedFieldReference, UnresolvedAlias,
  WindowProperty}
import org.apache.flink.table.functions.{TemporalTableFunction, TemporalTableFunctionImpl}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.plan.ProjectionTranslator._
import org.apache.flink.table.plan.logical.{Minus, _}
import org.apache.flink.table.util.JavaScalaConversionUtil

import _root_.scala.collection.JavaConversions._

/**
  * The implementation of the [[Table]].
  *
  * In [[TableImpl]], string expressions are parsed by [[ExpressionParser]] into [[Expression]]s.
  *
  * __NOTE__: Currently, the implementation depends on Calcite.
  *
  * @param tableEnv The [[TableEnvironment]] to which the table is bound.
  * @param logicalPlan logical representation
  */
class TableImpl(
    private[flink] val tableEnv: TableEnvironment,
    private[flink] val logicalPlan: LogicalNode) extends Table {

  private[flink] val expressionBridge: ExpressionBridge[PlannerExpression] =
    tableEnv.expressionBridge

  private lazy val tableSchema: TableSchema = new TableSchema(
    logicalPlan.output.map(_.name).toArray,
    logicalPlan.output.map(_.resultType).toArray)

  var tableName: String = _

  def relBuilder: FlinkRelBuilder = tableEnv.getRelBuilder

  def getRelNode: RelNode = logicalPlan.toRelNode(relBuilder)

  override def getSchema: TableSchema = tableSchema

  override def printSchema(): Unit = print(tableSchema.toString)

  override def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields): _*)
  }

  override def select(fields: Expression*): Table = {
    selectInternal(fields.map(expressionBridge.bridge))
  }

  private def selectInternal(fields: Seq[PlannerExpression]): Table = {
    val expandedFields = expandProjectList(fields, logicalPlan, tableEnv)
    val (aggNames, propNames) = extractAggregationsAndProperties(expandedFields, tableEnv)
    if (propNames.nonEmpty) {
      throw new ValidationException("Window properties can only be used on windowed tables.")
    }

    if (aggNames.nonEmpty) {
      val projectsOnAgg = replaceAggregationsAndProperties(
        expandedFields, tableEnv, aggNames, propNames)
      val projectFields = extractFieldReferences(expandedFields)

      new TableImpl(tableEnv,
        Project(projectsOnAgg,
          Aggregate(Nil, aggNames.map(a => Alias(a._1, a._2)).toSeq,
            Project(projectFields, logicalPlan).validate(tableEnv)
          ).validate(tableEnv)
        ).validate(tableEnv)
      )
    } else {
      new TableImpl(tableEnv,
        Project(expandedFields.map(UnresolvedAlias), logicalPlan).validate(tableEnv))
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
    createTemporalTableFunctionInternal(
      expressionBridge.bridge(timeAttribute),
      expressionBridge.bridge(primaryKey))
  }

  private def createTemporalTableFunctionInternal(
      timeAttribute: PlannerExpression,
      primaryKey: PlannerExpression)
    : TemporalTableFunction = {
    val temporalTable = TemporalTable(timeAttribute, primaryKey, logicalPlan)
      .validate(tableEnv)
      .asInstanceOf[TemporalTable]

    TemporalTableFunctionImpl.create(
      this,
      temporalTable.timeAttribute,
      validatePrimaryKeyExpression(temporalTable.primaryKey))
  }

  private def validatePrimaryKeyExpression(expression: Expression): String = {
    expression match {
      case fieldReference: ResolvedFieldReference =>
        fieldReference.name
      case _ => throw new ValidationException(
        s"Unsupported expression [$expression] as primary key. " +
          s"Only top-level (not nested) field references are supported.")
    }
  }

  override def as(fields: String): Table = {
    as(ExpressionParser.parseExpressionList(fields): _*)
  }

  override def as(fields: Expression*): Table = {
    asInternal(fields.map(tableEnv.expressionBridge.bridge))
  }

  private def asInternal(fields: Seq[PlannerExpression]): Table = {
    new TableImpl(tableEnv, AliasNode(fields, logicalPlan).validate(tableEnv))
  }

  override def filter(predicate: String): Table = {
    filter(ExpressionParser.parseExpression(predicate))
  }

  override def filter(predicate: Expression): Table = {
    filterInternal(expressionBridge.bridge(predicate))
  }

  private def filterInternal(predicate: PlannerExpression): Table = {
    new TableImpl(tableEnv, Filter(predicate, logicalPlan).validate(tableEnv))
  }

  override def where(predicate: String): Table = {
    filter(predicate)
  }

  override def where(predicate: Expression): Table = {
    filter(predicate)
  }

  override def groupBy(fields: String): GroupedTable = {
    groupBy(ExpressionParser.parseExpressionList(fields): _*)
  }

  override def groupBy(fields: Expression*): GroupedTable = {
    groupByInternal(fields.map(expressionBridge.bridge))
  }

  private def groupByInternal(fields: Seq[PlannerExpression]): GroupedTable = {
    new GroupedTableImpl(this, fields)
  }

  override def distinct(): Table = {
    new TableImpl(tableEnv, Distinct(logicalPlan).validate(tableEnv))
  }

  override def join(right: Table): Table = {
    joinInternal(right, None, JoinType.INNER)
  }

  override def join(right: Table, joinPredicate: String): Table = {
    join(right, ExpressionParser.parseExpression(joinPredicate))
  }

  override def join(right: Table, joinPredicate: Expression): Table = {
    joinInternal(right, Some(expressionBridge.bridge(joinPredicate)), JoinType.INNER)
  }

  override def leftOuterJoin(right: Table): Table = {
    joinInternal(right, None, JoinType.LEFT_OUTER)
  }

  override def leftOuterJoin(right: Table, joinPredicate: String): Table = {
    leftOuterJoin(right, ExpressionParser.parseExpression(joinPredicate))
  }

  override def leftOuterJoin(right: Table, joinPredicate: Expression): Table = {
    joinInternal(right, Some(expressionBridge.bridge(joinPredicate)), JoinType.LEFT_OUTER)
  }

  override def rightOuterJoin(right: Table, joinPredicate: String): Table = {
    rightOuterJoin(right, ExpressionParser.parseExpression(joinPredicate))
  }

  override def rightOuterJoin(right: Table, joinPredicate: Expression): Table = {
    joinInternal(right, Some(expressionBridge.bridge(joinPredicate)), JoinType.RIGHT_OUTER)
  }

  override def fullOuterJoin(right: Table, joinPredicate: String): Table = {
    fullOuterJoin(right, ExpressionParser.parseExpression(joinPredicate))
  }

  override def fullOuterJoin(right: Table, joinPredicate: Expression): Table = {
    joinInternal(right, Some(expressionBridge.bridge(joinPredicate)), JoinType.FULL_OUTER)
  }

  private def joinInternal(
      right: Table,
      joinPredicate: Option[PlannerExpression],
      joinType: JoinType)
    : Table = {
    // check that the TableEnvironment of right table is not null
    // and right table belongs to the same TableEnvironment
    if (right.asInstanceOf[TableImpl].tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be joined.")
    }

    new TableImpl(
      tableEnv,
      Join(
        this.logicalPlan,
        right.asInstanceOf[TableImpl].logicalPlan,
        joinType,
        joinPredicate,
        correlated = false).validate(tableEnv))
  }

  override def joinLateral(tableFunctionCall: String): Table = {
    joinLateral(ExpressionParser.parseExpression(tableFunctionCall))
  }

  override def joinLateral(tableFunctionCall: Expression): Table = {
    joinLateralInternal(expressionBridge.bridge(tableFunctionCall), None, JoinType.INNER)
  }

  override def joinLateral(tableFunctionCall: String, joinPredicate: String): Table = {
    joinLateral(
      ExpressionParser.parseExpression(tableFunctionCall),
      ExpressionParser.parseExpression(joinPredicate))
  }

  override def joinLateral(tableFunctionCall: Expression, joinPredicate: Expression): Table = {
    joinLateralInternal(
      expressionBridge.bridge(tableFunctionCall),
      Some(expressionBridge.bridge(joinPredicate)),
      JoinType.INNER)
  }

  override def leftOuterJoinLateral(tableFunctionCall: String): Table = {
    leftOuterJoinLateral(ExpressionParser.parseExpression(tableFunctionCall))
  }

  override def leftOuterJoinLateral(tableFunctionCall: Expression): Table = {
    joinLateralInternal(expressionBridge.bridge(tableFunctionCall), None, JoinType.LEFT_OUTER)
  }

  override def leftOuterJoinLateral(tableFunctionCall: String, joinPredicate: String): Table = {
    leftOuterJoinLateral(
      ExpressionParser.parseExpression(tableFunctionCall),
      ExpressionParser.parseExpression(joinPredicate))
  }

  override def leftOuterJoinLateral(
    tableFunctionCall: Expression, joinPredicate: Expression): Table = {
    joinLateralInternal(
      expressionBridge.bridge(tableFunctionCall),
      Some(expressionBridge.bridge(joinPredicate)),
      JoinType.LEFT_OUTER)
  }

  private def joinLateralInternal(
      callExpr: PlannerExpression,
      joinPredicate: Option[PlannerExpression],
      joinType: JoinType): Table = {

    // check join type
    if (joinType != JoinType.INNER && joinType != JoinType.LEFT_OUTER) {
      throw new ValidationException(
        "Table functions are currently only supported for inner and left outer lateral joins.")
    }

    val logicalCall = UserDefinedFunctionUtils.createLogicalFunctionCall(
      callExpr,
      logicalPlan)
    val validatedLogicalCall = logicalCall.validate(tableEnv)

    new TableImpl(
      tableEnv,
      Join(
        logicalPlan,
        validatedLogicalCall,
        joinType,
        joinPredicate,
        correlated = true
      ).validate(tableEnv))
  }

  override def minus(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.asInstanceOf[TableImpl].tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be " +
        "subtracted.")
    }
    new TableImpl(
      tableEnv, Minus(logicalPlan, right.asInstanceOf[TableImpl].logicalPlan, all = false)
      .validate(tableEnv))
  }

  override def minusAll(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.asInstanceOf[TableImpl].tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be " +
        "subtracted.")
    }
    new TableImpl(
      tableEnv, Minus(logicalPlan, right.asInstanceOf[TableImpl].logicalPlan, all = true)
      .validate(tableEnv))
  }

  override def union(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.asInstanceOf[TableImpl].tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be unioned.")
    }
    new TableImpl(
      tableEnv, Union(logicalPlan, right.asInstanceOf[TableImpl].logicalPlan, all = false)
        .validate(tableEnv))
  }

  override def unionAll(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.asInstanceOf[TableImpl].tableEnv != this.tableEnv) {
      throw new ValidationException("Only tables from the same TableEnvironment can be unioned.")
    }
    new TableImpl(
      tableEnv, Union(logicalPlan, right.asInstanceOf[TableImpl].logicalPlan, all = true)
        .validate(tableEnv))
  }

  override def intersect(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.asInstanceOf[TableImpl].tableEnv != this.tableEnv) {
      throw new ValidationException(
        "Only tables from the same TableEnvironment can be intersected.")
    }
    new TableImpl(
      tableEnv, Intersect(logicalPlan, right.asInstanceOf[TableImpl].logicalPlan, all = false)
        .validate(tableEnv))
  }

  override def intersectAll(right: Table): Table = {
    // check that right table belongs to the same TableEnvironment
    if (right.asInstanceOf[TableImpl].tableEnv != this.tableEnv) {
      throw new ValidationException(
        "Only tables from the same TableEnvironment can be intersected.")
    }
    new TableImpl(
      tableEnv, Intersect(logicalPlan, right.asInstanceOf[TableImpl].logicalPlan, all = true)
        .validate(tableEnv))
  }

  override def orderBy(fields: String): Table = {
    orderBy(ExpressionParser.parseExpressionList(fields): _*)
  }

  override def orderBy(fields: Expression*): Table = {
    orderByInternal(fields.map(expressionBridge.bridge))
  }

  private def orderByInternal(fields: Seq[PlannerExpression]): Table = {
    val order: Seq[Ordering] = fields.map {
      case o: Ordering => o
      case e => Asc(e)
    }
    new TableImpl(tableEnv, Sort(order, logicalPlan).validate(tableEnv))
  }

  override def offset(offset: Int): Table = {
    new TableImpl(tableEnv, Limit(offset, -1, logicalPlan).validate(tableEnv))
  }

  override def fetch(fetch: Int): Table = {
    if (fetch < 0) {
      throw new ValidationException("FETCH count must be equal or larger than 0.")
    }
    this.logicalPlan match {
      case Limit(o, -1, c) =>
        // replace LIMIT without FETCH by LIMIT with FETCH
        new TableImpl(tableEnv, Limit(o, fetch, c).validate(tableEnv))
      case Limit(_, _, _) =>
        throw new ValidationException("FETCH is already defined.")
      case _ =>
        new TableImpl(tableEnv, Limit(0, fetch, logicalPlan).validate(tableEnv))
    }
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
}

/**
  * The implementation of a [[GroupedTable]] that has been grouped on a set of grouping keys.
  */
class GroupedTableImpl(
    private[flink] val table: Table,
    private[flink] val groupKey: Seq[PlannerExpression])
  extends GroupedTable {

  private val tableImpl = table.asInstanceOf[TableImpl]

  override def select(fields: String): Table = {
    select(ExpressionParser.parseExpressionList(fields): _*)
  }

  override def select(fields: Expression*): Table = {
    selectInternal(fields.map(tableImpl.expressionBridge.bridge))
  }

  private def selectInternal(fields: Seq[PlannerExpression]): Table = {
    val expandedFields = expandProjectList(fields, tableImpl.logicalPlan, tableImpl.tableEnv)
    val (aggNames, propNames) = extractAggregationsAndProperties(expandedFields, tableImpl.tableEnv)
    if (propNames.nonEmpty) {
      throw new ValidationException("Window properties can only be used on windowed tables.")
    }

    val projectsOnAgg = replaceAggregationsAndProperties(
      expandedFields, tableImpl.tableEnv, aggNames, propNames)
    val projectFields = extractFieldReferences(expandedFields ++ groupKey)

    new TableImpl(tableImpl.tableEnv,
      Project(projectsOnAgg,
        Aggregate(groupKey, aggNames.map(a => Alias(a._1, a._2)).toSeq,
          Project(projectFields, tableImpl.logicalPlan).validate(tableImpl.tableEnv)
        ).validate(tableImpl.tableEnv)
      ).validate(tableImpl.tableEnv))
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
    groupBy(ExpressionParser.parseExpressionList(fields): _*)
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
    select(ExpressionParser.parseExpressionList(fields): _*)
  }

  override def select(fields: Expression*): Table = {
    selectInternal(
      groupKeys.map(tableImpl.expressionBridge.bridge),
      createLogicalWindow(),
      fields.map(tableImpl.expressionBridge.bridge))
  }

  private def selectInternal(
      groupKeys: Seq[PlannerExpression],
      window: LogicalWindow,
      fields: Seq[PlannerExpression]): Table = {
    val expandedFields = expandProjectList(fields, tableImpl.logicalPlan, tableImpl.tableEnv)
    val (aggNames, propNames) = extractAggregationsAndProperties(expandedFields, tableImpl.tableEnv)

    val projectsOnAgg = replaceAggregationsAndProperties(
      expandedFields, tableImpl.tableEnv, aggNames, propNames)

    val projectFields = extractFieldReferences(expandedFields ++ groupKeys :+ window.timeAttribute)

    new TableImpl(tableImpl.tableEnv,
      Project(
        projectsOnAgg,
        WindowAggregate(
          groupKeys,
          window,
          propNames.map(a => Alias(a._1, a._2)).toSeq,
          aggNames.map(a => Alias(a._1, a._2)).toSeq,
          Project(projectFields, tableImpl.logicalPlan).validate(tableImpl.tableEnv)
        ).validate(tableImpl.tableEnv),
        // required for proper resolution of the time attribute in multi-windows
        explicitAlias = true
      ).validate(tableImpl.tableEnv))
  }

  /**
    * Converts an API class to a logical window for planning.
    */
  private def createLogicalWindow(): LogicalWindow = window match {
    case tw: TumbleWithSizeOnTimeWithAlias =>
      TumblingGroupWindow(
        tableImpl.expressionBridge.bridge(tw.getAlias),
        tableImpl.expressionBridge.bridge(tw.getTimeField),
        tableImpl.expressionBridge.bridge(tw.getSize))
    case sw: SlideWithSizeAndSlideOnTimeWithAlias =>
      SlidingGroupWindow(
        tableImpl.expressionBridge.bridge(sw.getAlias),
        tableImpl.expressionBridge.bridge(sw.getTimeField),
        tableImpl.expressionBridge.bridge(sw.getSize),
        tableImpl.expressionBridge.bridge(sw.getSlide))
    case sw: SessionWithGapOnTimeWithAlias =>
      SessionGroupWindow(
        tableImpl.expressionBridge.bridge(sw.getAlias),
        tableImpl.expressionBridge.bridge(sw.getTimeField),
        tableImpl.expressionBridge.bridge(sw.getGap))
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
    select(ExpressionParser.parseExpressionList(fields): _*)
  }

  override def select(fields: Expression*): Table = {
    selectInternal(
      fields.map(tableImpl.expressionBridge.bridge),
      overWindows.map(createLogicalWindow))
  }

  private def selectInternal(
      fields: Seq[PlannerExpression],
      logicalOverWindows: Seq[LogicalOverWindow])
    : Table = {

    val expandedFields = expandProjectList(
      fields,
      tableImpl.logicalPlan,
      tableImpl.tableEnv)

    if (fields.exists(_.isInstanceOf[WindowProperty])){
      throw new ValidationException(
        "Window start and end properties are not available for Over windows.")
    }

    val expandedOverFields =
      resolveOverWindows(expandedFields, logicalOverWindows, tableImpl.tableEnv)

    new TableImpl(
      tableImpl.tableEnv,
      Project(
        expandedOverFields.map(UnresolvedAlias),
        tableImpl.logicalPlan,
        // required for proper projection push down
        explicitAlias = true)
        .validate(tableImpl.tableEnv)
    )
  }

  /**
    * Converts an API class to a logical window for planning.
    */
  private def createLogicalWindow(overWindow: OverWindow): LogicalOverWindow = {
    LogicalOverWindow(
      tableImpl.expressionBridge.bridge(overWindow.getAlias),
      overWindow.getPartitioning.map(tableImpl.expressionBridge.bridge),
      tableImpl.expressionBridge.bridge(overWindow.getOrder),
      tableImpl.expressionBridge.bridge(overWindow.getPreceding),
      JavaScalaConversionUtil
        .toScala(overWindow.getFollowing).map(tableImpl.expressionBridge.bridge)
    )
  }
}
