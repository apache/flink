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

import org.apache.flink.table.api._
import org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral
import org.apache.flink.table.expressions.ExpressionResolver.resolverFor
import org.apache.flink.table.expressions.ExpressionUtils.isFunctionOfType
import org.apache.flink.table.expressions.FunctionDefinition.Type.{SCALAR_FUNCTION, TABLE_FUNCTION}
import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.catalog.FunctionDefinitionCatalog
import org.apache.flink.table.expressions.lookups.TableReferenceLookup
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.operations.AliasOperationUtils.createAliasList
import org.apache.flink.table.operations.JoinTableOperation.JoinType
import org.apache.flink.table.operations.SetTableOperation.SetTableOperationType._
import org.apache.flink.table.util.JavaScalaConversionUtil
import org.apache.flink.table.util.JavaScalaConversionUtil.toScala
import org.apache.flink.util.Preconditions

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._

/**
  * Builder for [[[Operation]] tree.
  */
class OperationTreeBuilder(private val tableEnv: TableEnvImpl) {

  private val expressionBridge: ExpressionBridge[PlannerExpression] = tableEnv.expressionBridge
  private val functionCatalog: FunctionDefinitionCatalog = tableEnv.functionCatalog

  private val isStreaming = tableEnv.isInstanceOf[StreamTableEnvImpl]
  private val projectionOperationFactory = new ProjectionOperationFactory(expressionBridge)
  private val sortOperationFactory = new SortOperationFactory(isStreaming)
  private val calculatedTableFactory = new CalculatedTableFactory()
  private val setOperationFactory = new SetOperationFactory(isStreaming)
  private val aggregateOperationFactory = new AggregateOperationFactory(expressionBridge,
    isStreaming)
  private val joinOperationFactory = new JoinOperationFactory(expressionBridge)

  private val noWindowPropertyChecker = new NoWindowPropertyChecker(
    "Window start and end properties are not available for Over windows.")

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
    projectInternal(projectList, child, explicitAlias, Collections.emptyList())
  }

  def project(
      projectList: JList[Expression],
      child: TableOperation,
      overWindows: JList[OverWindow])
    : TableOperation = {

    Preconditions.checkArgument(!overWindows.isEmpty)

    projectList.asScala.map(_.accept(noWindowPropertyChecker))

    projectInternal(projectList,
      child,
      explicitAlias = true,
      overWindows)
  }

  private def projectInternal(
      projectList: JList[Expression],
      child: TableOperation,
      explicitAlias: Boolean,
      overWindows: JList[OverWindow])
    : TableOperation = {
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

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val inputFieldNames = child.getTableSchema.getFieldNames.toList.asJava
    val validateAliases =
      ColumnOperationUtils.renameColumns(inputFieldNames, resolver.resolve(aliases))

    project(validateAliases, child)
  }

  def dropColumns(
      fieldLists: JList[Expression],
      child: TableOperation)
    : TableOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val inputFieldNames = child.getTableSchema.getFieldNames.toList.asJava
    val finalFields = ColumnOperationUtils.dropFields(inputFieldNames, resolver.resolve(fieldLists))

    project(finalFields, child)
  }

  def aggregate(
      groupingExpressions: JList[Expression],
      aggregates: JList[Expression],
      child: TableOperation)
    : TableOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build

    val resolvedGroupings = resolver.resolve(groupingExpressions)
    val resolvedAggregates = resolver.resolve(aggregates)

    aggregateOperationFactory.createAggregate(resolvedGroupings, resolvedAggregates, child)
  }

  def windowAggregate(
      groupingExpressions: JList[Expression],
      window: GroupWindow,
      windowProperties: JList[Expression],
      aggregates: JList[Expression],
      child: TableOperation)
    : TableOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val resolvedWindow = aggregateOperationFactory.createResolvedWindow(window, resolver)

    val resolverWithWindowReferences = resolverFor(tableCatalog, functionCatalog, child)
      .withLocalReferences(
        new LocalReferenceExpression(
          resolvedWindow.getAlias,
          resolvedWindow.getTimeAttribute.getResultType))
      .build

    val convertedGroupings = resolverWithWindowReferences.resolve(groupingExpressions)

    val convertedAggregates = resolverWithWindowReferences.resolve(aggregates)

    val convertedProperties = resolverWithWindowReferences.resolve(windowProperties)

    aggregateOperationFactory.createWindowAggregate(
      convertedGroupings,
      convertedAggregates,
      convertedProperties,
      resolvedWindow,
      child)
  }

  def join(
      left: TableOperation,
      right: TableOperation,
      joinType: JoinType,
      condition: Optional[Expression],
      correlated: Boolean)
    : TableOperation = {
    val resolver = resolverFor(tableCatalog, functionCatalog, left, right).build()
    val resolvedCondition = toScala(condition).map(expr => resolveSingleExpression(expr, resolver))

    joinOperationFactory
      .create(left, right, joinType, resolvedCondition.getOrElse(valueLiteral(true)), correlated)
  }

  def joinLateral(
      left: TableOperation,
      tableFunction: Expression,
      joinType: JoinType,
      condition: Optional[Expression])
    : TableOperation = {
    val resolver = resolverFor(tableCatalog, functionCatalog, left).build()
    val resolvedFunction = resolveSingleExpression(tableFunction, resolver)

    val temporalTable = calculatedTableFactory.create(resolvedFunction)

    join(left, temporalTable, joinType, condition, correlated = true)
  }

  def resolveExpression(expression: Expression, tableOperation: TableOperation*)
    : Expression = {
    val resolver = resolverFor(tableCatalog, functionCatalog, tableOperation: _*).build()

    resolveSingleExpression(expression, resolver)
  }

  private def resolveSingleExpression(
      expression: Expression,
      resolver: ExpressionResolver)
    : Expression = {
    val resolvedExpression = resolver.resolve(List(expression).asJava)
    if (resolvedExpression.size() != 1) {
      throw new ValidationException("Expected single expression")
    } else {
      resolvedExpression.get(0)
    }
  }

  def sort(
      fields: JList[Expression],
      child: TableOperation)
    : TableOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val resolvedFields = resolver.resolve(fields)

    sortOperationFactory.createSort(resolvedFields, child)
  }

  def limitWithOffset(offset: Int, child: TableOperation): TableOperation = {
    sortOperationFactory.createLimitWithOffset(offset, child)
  }

  def limitWithFetch(fetch: Int, child: TableOperation): TableOperation = {
    sortOperationFactory.createLimitWithFetch(fetch, child)
  }

  def alias(
      fields: JList[Expression],
      child: TableOperation)
    : TableOperation = {

    val newFields = createAliasList(fields, child)

    project(newFields, child, explicitAlias = true)
  }

  def filter(
      condition: Expression,
      child: TableOperation)
    : TableOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val resolvedExpression = resolveSingleExpression(condition, resolver)
    val convertedCondition = expressionBridge.bridge(resolvedExpression)
    if (convertedCondition.resultType != Types.BOOLEAN) {
      throw new ValidationException(s"Filter operator requires a boolean expression as input," +
        s" but $condition is of type ${convertedCondition.resultType}")
    }

    new FilterTableOperation(resolvedExpression, child)
  }

  def distinct(
      child: TableOperation)
    : TableOperation = {
    new DistinctTableOperation(child)
  }

  def minus(
      left: TableOperation,
      right: TableOperation,
      all: Boolean)
    : TableOperation = {
    setOperationFactory.create(MINUS, left, right, all)
  }

  def intersect(
      left: TableOperation,
      right: TableOperation,
      all: Boolean)
    : TableOperation = {
    setOperationFactory.create(INTERSECT, left, right, all)
  }

  def union(
      left: TableOperation,
      right: TableOperation,
      all: Boolean)
    : TableOperation = {
    setOperationFactory.create(UNION, left, right, all)
  }

  def map(mapFunction: Expression, child: TableOperation): TableOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val resolvedMapFunction = resolveSingleExpression(mapFunction, resolver)

    if (!isFunctionOfType(resolvedMapFunction, SCALAR_FUNCTION)) {
      throw new ValidationException("Only ScalarFunction can be used in the map operator.")
    }

    val expandedFields = new CallExpression(BuiltInFunctionDefinitions.FLATTEN,
      List(resolvedMapFunction).asJava)
    project(Collections.singletonList(expandedFields), child)
  }

  def flatMap(tableFunction: Expression, child: TableOperation): TableOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val resolvedTableFunction = resolveSingleExpression(tableFunction, resolver)

    if (!isFunctionOfType(resolvedTableFunction, TABLE_FUNCTION)) {
      throw new ValidationException("Only TableFunction can be used in the flatMap operator.")
    }

    val originFieldNames: Seq[String] =
      resolvedTableFunction.asInstanceOf[CallExpression].getFunctionDefinition match {
        case tfd: TableFunctionDefinition =>
          UserDefinedFunctionUtils.getFieldInfo(tfd.getResultType)._1
      }

    def getUniqueName(inputName: String, usedFieldNames: Seq[String]): String = {
      var i = 0
      var resultName = inputName
      while (usedFieldNames.contains(resultName)) {
        resultName = resultName + "_" + i
        i += 1
      }
      resultName
    }

    val usedFieldNames = child.getTableSchema.getFieldNames.toBuffer
    val newFieldNames = originFieldNames.map({ e =>
      val resultName = getUniqueName(e, usedFieldNames)
      usedFieldNames.append(resultName)
      resultName
    })

    val renamedTableFunction = ApiExpressionUtils.call(
      BuiltInFunctionDefinitions.AS,
      resolvedTableFunction +: newFieldNames.map(ApiExpressionUtils.valueLiteral(_)): _*)
    val joinNode = joinLateral(child, renamedTableFunction, JoinType.INNER, Optional.empty())
    val rightNode = dropColumns(
      child.getTableSchema.getFieldNames.map(a => new UnresolvedReferenceExpression(a)).toList,
      joinNode)
    alias(originFieldNames.map(a => new UnresolvedReferenceExpression(a)), rightNode)
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
