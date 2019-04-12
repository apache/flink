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

package org.apache.flink.table.operations

import java.util.{Collections, Optional, List => JList}

import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.ExpressionResolver.resolverFor
import org.apache.flink.table.expressions.FunctionDefinition.Type.SCALAR_FUNCTION
import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.catalog.FunctionDefinitionCatalog
import org.apache.flink.table.expressions.lookups.TableReferenceLookup
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.plan.logical.{Minus => LMinus, _}
import org.apache.flink.table.util.JavaScalaConversionUtil
import org.apache.flink.table.util.JavaScalaConversionUtil.toScala
import org.apache.flink.util.Preconditions

import _root_.scala.collection.JavaConverters._

/**
  * Builder for [[[Operation]] tree.
  */
class OperationTreeBuilder(private val tableEnv: TableEnvironment) {

  private val expressionBridge: ExpressionBridge[PlannerExpression] = tableEnv.expressionBridge
  private val functionCatalog: FunctionDefinitionCatalog = tableEnv.functionCatalog

  private val projectionOperationFactory = new ProjectionOperationFactory(expressionBridge)
  private val noWindowPropertyChecker = new NoWindowPropertyChecker(
    "Window start and end properties are not available for Over windows.")

  private def bridgeExpression(expression: Expression): PlannerExpression = {
    val expr = expressionBridge.bridge(expression)
    if (!expr.valid) {
      throw new ValidationException(s"Could not validate expression: $expression")
    }
    expr
  }

  private val tableCatalog = new TableReferenceLookup {
    override def lookupTable(name: String): Optional[TableReferenceExpression] =
      JavaScalaConversionUtil
      .toJava(tableEnv.scanInternal(Array(name))
        .map(op => new TableReferenceExpression(name, op.getTableOperation)))
  }

  def project(
      projectList: JList[Expression],
      child: TableOperation,
      explicitAlias: Boolean = false)
    : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]

    projectInternal(projectList, childNode, explicitAlias, Collections.emptyList())
  }

  def project(
      projectList: JList[Expression],
      child: TableOperation,
      overWindows: JList[OverWindow])
    : TableOperation = {

    Preconditions.checkArgument(!overWindows.isEmpty)

    val childNode = child.asInstanceOf[LogicalNode]

    projectList.asScala.map(_.accept(noWindowPropertyChecker))

    projectInternal(projectList,
      childNode,
      explicitAlias = true,
      overWindows)
  }

  private def projectInternal(
      projectList: JList[Expression],
      child: LogicalNode,
      explicitAlias: Boolean,
      overWindows: JList[OverWindow])
    : LogicalNode = {
    val resolver = resolverFor(tableCatalog, functionCatalog, child).withOverWindows(overWindows)
      .build
    val projections = resolver.resolve(projectList)

    projectionOperationFactory.create(projections, child, explicitAlias)
  }

  /**
    * Adds additional columns. Existing fields will be replaced if replaceIfExist is true.
    */
  def addColumns(
      replaceIfExist: Boolean,
      fieldLists: JList[Expression],
      child: TableOperation)
    : TableOperation = {
    val newColumns = if (replaceIfExist) {
      val fieldNames = child.getTableSchema.getFieldNames.toList.asJava
      ColumnOperationUtils.addOrReplaceColumns(fieldNames, fieldLists)
    } else {
      (new UnresolvedReferenceExpression("*") +: fieldLists.asScala).asJava
    }
    project(newColumns, child)
  }

  def renameColumns(
      aliases: JList[Expression],
      child: TableOperation)
    : TableOperation = {

    val inputFieldNames = child.getTableSchema.getFieldNames.toList.asJava
    val validateAliases = ColumnOperationUtils.renameColumns(inputFieldNames, aliases)

    project(validateAliases, child)
  }

  def dropColumns(
      fieldLists: JList[Expression],
      child: TableOperation)
    : TableOperation = {

    val inputFieldNames = child.getTableSchema.getFieldNames.toList.asJava
    val finalFields = ColumnOperationUtils.dropFields(inputFieldNames, fieldLists)

    project(finalFields, child)
  }

  def aggregate(
      groupingExpressions: JList[Expression],
      aggregates: JList[Expression],
      child: TableOperation)
    : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]
    val resolver = resolverFor(tableCatalog, functionCatalog, child).build

    val convertedGroupings = resolveExpressions(groupingExpressions, resolver)
    val convertedAggregates = resolveExpressions(aggregates, resolver)

    Aggregate(convertedGroupings, convertedAggregates, childNode).validate(tableEnv)
  }

  private def resolveExpressions(
      expressions: JList[Expression],
      resolver: ExpressionResolver)
    : Seq[PlannerExpression] = {
    resolver.resolve(expressions).asScala.map(bridgeExpression)
  }

  def windowAggregate(
      groupingExpressions: JList[Expression],
      window: GroupWindow,
      windowProperties: JList[Expression],
      aggregates: JList[Expression],
      child: TableOperation)
    : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]
    val resolver = resolverFor(tableCatalog, functionCatalog, child).withGroupWindow(window).build

    val convertedGroupings = resolveExpressions(groupingExpressions, resolver)

    val convertedAggregates = resolveExpressions(aggregates, resolver)

    val convertedProperties = resolveExpressions(windowProperties, resolver)

    WindowAggregate(
        convertedGroupings,
        resolver.resolveGroupWindow(window),
        convertedProperties,
        convertedAggregates,
        childNode)
      .validate(tableEnv)
  }

  def join(
      left: TableOperation,
      right: TableOperation,
      joinType: JoinType,
      condition: Optional[Expression],
      correlated: Boolean)
    : TableOperation = {

    val leftNode = left.asInstanceOf[LogicalNode]
    val rightNode = right.asInstanceOf[LogicalNode]

    val resolver = resolverFor(tableCatalog, functionCatalog, leftNode, rightNode).build()

    val resolvedCondition = toScala(condition).map(c => resolver.resolve(List(c).asJava)) match {
      case Some(resolvedExprs) if resolvedExprs.size != 1 =>
        throw new ValidationException(s"Invalid join condition $condition")
      case Some(resolvedExprs) =>
        Some(resolvedExprs.get(0))
      case None => None
    }

    val plannerExpression = resolvedCondition.map(bridgeExpression)

    Join(leftNode, rightNode, joinType, plannerExpression, correlated).validate(tableEnv)
  }

  def joinLateral(
      left: TableOperation,
      tableFunction: Expression,
      joinType: JoinType,
      condition: Optional[Expression])
    : TableOperation = {

    val leftNode = left.asInstanceOf[LogicalNode]

    val resolver = resolverFor(tableCatalog, functionCatalog, leftNode).build()
    val resolvedFunction = resolveSingleExpression(tableFunction, resolver)

    val temporalTable = UserDefinedFunctionUtils.createLogicalFunctionCall(
      expressionBridge.bridge(resolvedFunction),
      leftNode).validate(tableEnv)

    join(left, temporalTable, joinType, condition, correlated = true)
  }

  def resolveExpression(expression: Expression, tableOperation: TableOperation*)
    : PlannerExpression = {
    val resolver = resolverFor(tableCatalog, functionCatalog, tableOperation: _*).build()

    resolveSingleExpression(expression, resolver)
  }

  private def resolveSingleExpression(
      expression: Expression,
      resolver: ExpressionResolver)
    : PlannerExpression = {
    val resolvedTimeAttributes = resolver.resolve(List(expression).asJava)
    if (resolvedTimeAttributes.size() != 1) {
      throw new ValidationException("Expected single expression")
    } else {
      expressionBridge.bridge(resolvedTimeAttributes.get(0))
    }
  }

  def sort(
      fields: JList[Expression],
      child: TableOperation)
    : TableOperation = {
    val childNode = child.asInstanceOf[LogicalNode]

    val resolver = resolverFor(tableCatalog, functionCatalog, childNode).build()
    val resolvedExpressions = resolver.resolve(fields)
    val order = SortOperationUtils.wrapInOrder(resolvedExpressions).asScala.map(bridgeExpression)

    Sort(order, childNode).validate(tableEnv)
  }

  def limitWithOffset(offset: Int, child: TableOperation): TableOperation = {
      limit(offset, -1, child)
  }

  def limitWithFetch(fetch: Int, child: TableOperation): TableOperation = {
      applyFetch(fetch, child)
  }

  private def applyFetch(fetch: Int, child: TableOperation): TableOperation = {
    child match {
      case Limit(o, -1, c) =>
        // replace LIMIT without FETCH by LIMIT with FETCH
        limit(o, fetch, c)
      case Limit(_, _, _) =>
        throw new ValidationException("FETCH is already defined.")
      case _ =>
        limit(0, fetch, child)
    }
  }

  def limit(offset: Int, fetch: Int, child: TableOperation): TableOperation = {
    Limit(offset, fetch, child.asInstanceOf[LogicalNode]).validate(tableEnv)
  }

  def alias(
      fields: JList[Expression],
      child: TableOperation)
    : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]
    val convertedFields = fields.asScala.map(expressionBridge.bridge)

    AliasNode(convertedFields, childNode).validate(tableEnv)
  }

  def filter(
      condition: Expression,
      child: TableOperation)
    : TableOperation = {

    val childNode = child.asInstanceOf[LogicalNode]
    val resolver = resolverFor(tableCatalog, functionCatalog, childNode).build()
    val convertedFields = expressionBridge.bridge(resolveSingleExpression(condition, resolver))

    Filter(convertedFields, childNode).validate(tableEnv)
  }

  def distinct(
      child: TableOperation)
    : TableOperation = {
    Distinct(child.asInstanceOf[LogicalNode]).validate(tableEnv)
  }

  def minus(
      left: TableOperation,
      right: TableOperation,
      all: Boolean)
    : TableOperation = {
    LMinus(left.asInstanceOf[LogicalNode], right.asInstanceOf[LogicalNode], all).validate(tableEnv)
  }

  def intersect(
      left: TableOperation,
      right: TableOperation,
      all: Boolean)
    : TableOperation = {
    Intersect(left.asInstanceOf[LogicalNode], right.asInstanceOf[LogicalNode], all)
      .validate(tableEnv)
  }

  def union(
      left: TableOperation,
      right: TableOperation,
      all: Boolean)
    : TableOperation = {
    Union(left.asInstanceOf[LogicalNode], right.asInstanceOf[LogicalNode], all).validate(tableEnv)
  }

  def map(mapFunction: Expression, child: TableOperation): TableOperation = {

    if (!isScalarFunction(mapFunction)) {
      throw new ValidationException("Only ScalarFunction can be used in the map operator.")
    }

    val expandedFields = new CallExpression(BuiltInFunctionDefinitions.FLATTEN,
      List(mapFunction).asJava)
    project(Collections.singletonList(expandedFields), child)
  }

  private def isScalarFunction(mapFunction: Expression) = {
    mapFunction.isInstanceOf[CallExpression] &&
      mapFunction.asInstanceOf[CallExpression].getFunctionDefinition.getType == SCALAR_FUNCTION
  }

  class NoWindowPropertyChecker(val exceptionMessage: String)
    extends ApiExpressionDefaultVisitor[Void] {
    override def visitCall(call: CallExpression): Void = {
      val functionDefinition = call.getFunctionDefinition
      if (BuiltInFunctionDefinitions.WINDOW_PROPERTIES
        .contains(functionDefinition)) {
        throw new ValidationException(exceptionMessage)
      }
      call.getChildren.asScala.foreach(expr => expr.accept(this))
      null
    }

    override protected def defaultMethod(expression: Expression): Void = null
  }
}
