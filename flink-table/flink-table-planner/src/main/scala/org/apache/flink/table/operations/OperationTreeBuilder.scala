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
import org.apache.flink.table.catalog.FunctionLookup
import org.apache.flink.table.expressions.ApiExpressionUtils.{unresolvedCall, unresolvedRef, valueLiteral}
import org.apache.flink.table.expressions.ExpressionResolver.resolverFor
import org.apache.flink.table.expressions.ApiExpressionUtils.isFunctionOfKind
import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.lookups.TableReferenceLookup
import org.apache.flink.table.functions.FunctionKind.{SCALAR, TABLE}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.functions.{AggregateFunctionDefinition, BuiltInFunctionDefinitions, TableFunctionDefinition}
import org.apache.flink.table.operations.AliasOperationUtils.createAliasList
import org.apache.flink.table.operations.JoinQueryOperation.JoinType
import org.apache.flink.table.operations.OperationExpressionsUtils.extractAggregationsAndProperties
import org.apache.flink.table.operations.SetQueryOperation.SetQueryOperationType._
import org.apache.flink.table.util.JavaScalaConversionUtil
import org.apache.flink.table.util.JavaScalaConversionUtil.toScala
import org.apache.flink.util.Preconditions

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._

/**
  * Builder for [[[Operation]] tree.
  *
  * The operation tree builder resolves expressions such that factories only work with fully
  * [[ResolvedExpression]]s.
  */
class OperationTreeBuilder(private val tableEnv: TableEnvImpl) {

  private val expressionBridge: ExpressionBridge[PlannerExpression] = tableEnv.expressionBridge
  private val functionCatalog: FunctionLookup = tableEnv.functionCatalog
  private val lookupResolver = new LookupCallResolver(tableEnv.functionCatalog)

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
        .map(op => new TableReferenceExpression(name, op)))
  }

  def project(
      projectList: JList[Expression],
      child: QueryOperation,
      explicitAlias: Boolean = false)
    : QueryOperation = {
    projectInternal(projectList, child, explicitAlias, Collections.emptyList())
  }

  def project(
      projectList: JList[Expression],
      child: QueryOperation,
      overWindows: JList[OverWindow])
    : QueryOperation = {

    Preconditions.checkArgument(!overWindows.isEmpty)

    projectList.asScala.map(_.accept(noWindowPropertyChecker))

    projectInternal(projectList,
      child,
      explicitAlias = true,
      overWindows)
  }

  private def projectInternal(
      projectList: JList[Expression],
      child: QueryOperation,
      explicitAlias: Boolean,
      overWindows: JList[OverWindow])
    : QueryOperation = {

    validateProjectList(projectList, overWindows)

    val resolver = resolverFor(tableCatalog, functionCatalog, child)
      .withOverWindows(overWindows)
      .build
    val projections = resolver.resolve(projectList)
    projectionOperationFactory.create(
      projections,
      child,
      explicitAlias,
      resolver.postResolverFactory())
  }

  /**
    * Window properties and aggregate function should not exist in the plain project, i.e., window
    * properties should exist in the window operators and aggregate functions should exist in
    * the aggregate operators.
    */
  private def validateProjectList(
      projectList: JList[Expression],
      overWindows: JList[OverWindow])
    : Unit = {

    val expressionsWithResolvedCalls = projectList.map(_.accept(lookupResolver)).asJava
    val extracted = extractAggregationsAndProperties(expressionsWithResolvedCalls)
    if (!extracted.getWindowProperties.isEmpty) {
      throw new ValidationException("Window properties can only be used on windowed tables.")
    }

    // aggregate functions can't exist in the plain project except for the over window case
    if (!extracted.getAggregations.isEmpty && overWindows.isEmpty) {
      throw new ValidationException("Aggregate functions are not supported in the select right" +
        " after the aggregate or flatAggregate operation.")
    }
  }

  /**
    * Adds additional columns. Existing fields will be replaced if replaceIfExist is true.
    */
  def addColumns(
      replaceIfExist: Boolean,
      fieldLists: JList[Expression],
      child: QueryOperation)
    : QueryOperation = {
    val newColumns = if (replaceIfExist) {
      val fieldNames = child.getTableSchema.getFieldNames.toList.asJava
      ColumnOperationUtils.addOrReplaceColumns(fieldNames, fieldLists)
    } else {
      (unresolvedRef("*") +: fieldLists.asScala).asJava
    }
    project(newColumns, child)
  }

  def renameColumns(
      aliases: JList[Expression],
      child: QueryOperation)
    : QueryOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child)
      .build

    val inputFieldNames = child.getTableSchema.getFieldNames.toList.asJava
    val validateAliases = ColumnOperationUtils.renameColumns(
      inputFieldNames,
      resolver.resolveExpanding(aliases))

    project(validateAliases, child)
  }

  def dropColumns(
      fieldList: JList[Expression],
      child: QueryOperation)
    : QueryOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child)
      .build

    val inputFieldNames = child.getTableSchema.getFieldNames.toList.asJava
    val finalFields = ColumnOperationUtils.dropFields(
      inputFieldNames,
      resolver.resolveExpanding(fieldList))

    project(finalFields, child)
  }

  def aggregate(
      groupingExpressions: JList[Expression],
      aggregates: JList[Expression],
      child: QueryOperation)
    : QueryOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build

    val resolvedGroupings = resolver.resolve(groupingExpressions)
    val resolvedAggregates = resolver.resolve(aggregates)

    aggregateOperationFactory.createAggregate(resolvedGroupings, resolvedAggregates, child)
  }

  /**
    * Row based aggregate that will flatten the output if it is a composite type.
    */
  def aggregate(
      groupingExpressions: JList[Expression],
      aggregate: Expression,
      child: QueryOperation)
    : QueryOperation = {
    val resolvedAggregate = aggregate.accept(lookupResolver)

    // extract alias and aggregate function
    var alias: Seq[String] = Seq()
    val aggWithoutAlias = resolvedAggregate match {
      case c: UnresolvedCallExpression
          if c.getFunctionDefinition == BuiltInFunctionDefinitions.AS =>
        alias = c.getChildren
          .drop(1)
          .map(e => ExpressionUtils.extractValue(e, classOf[String]).get())
        c.getChildren.get(0)
      case c: UnresolvedCallExpression
        if c.getFunctionDefinition.isInstanceOf[AggregateFunctionDefinition] =>
        if (alias.isEmpty) alias = UserDefinedFunctionUtils.getFieldInfo(
          c.getFunctionDefinition.asInstanceOf[AggregateFunctionDefinition].getResultTypeInfo)._1
        c
      case e => e
    }

    // turn agg to a named agg, because it will be verified later.
    var cnt = 0
    val childNames = child.getTableSchema.getFieldNames
    while (childNames.contains("TMP_" + cnt)) {
      cnt += 1
    }
    val aggWithNamedAlias = unresolvedCall(
      BuiltInFunctionDefinitions.AS,
      aggWithoutAlias,
      valueLiteral("TMP_" + cnt))

    // get agg table
    val aggQueryOperation = this.aggregate(groupingExpressions, Seq(aggWithNamedAlias), child)

    // flatten the aggregate function
    val aggNames = aggQueryOperation.getTableSchema.getFieldNames
    val flattenExpressions = aggNames.take(groupingExpressions.size())
      .map(e => unresolvedRef(e)) ++
      Seq(unresolvedCall(BuiltInFunctionDefinitions.FLATTEN, unresolvedRef(aggNames.last)))
    val flattenedOperation = this.project(flattenExpressions.toList, aggQueryOperation)

    // add alias
    aliasBackwardFields(flattenedOperation, alias, groupingExpressions.size())
  }

  def tableAggregate(
      groupingExpressions: JList[Expression],
      tableAggFunction: Expression,
      child: QueryOperation)
    : QueryOperation = {

    // Step1: add a default name to the call in the grouping expressions, e.g., groupBy(a % 5) to
    // groupBy(a % 5 as TMP_0). We need a name for every column so that to perform alias for the
    // table aggregate function in Step4.
    val newGroupingExpressions = addAliasToTheCallInGroupings(
      child.getTableSchema.getFieldNames,
      groupingExpressions)

    // Step2: resolve expressions
    val resolver = resolverFor(tableCatalog, functionCatalog, child).build
    val resolvedGroupings = resolver.resolve(newGroupingExpressions)
    val resolvedFunctionAndAlias = aggregateOperationFactory.extractTableAggFunctionAndAliases(
      resolveSingleExpression(tableAggFunction, resolver))

    // Step3: create table agg operation
    val tableAggOperation = aggregateOperationFactory
      .createAggregate(resolvedGroupings, Seq(resolvedFunctionAndAlias.f0), child)

    // Step4: add a top project to alias the output fields of the table aggregate.
    aliasBackwardFields(tableAggOperation, resolvedFunctionAndAlias.f1, groupingExpressions.size())
  }

  def windowAggregate(
      groupingExpressions: JList[Expression],
      window: GroupWindow,
      windowProperties: JList[Expression],
      aggregates: JList[Expression],
      child: QueryOperation)
    : QueryOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val resolvedWindow = aggregateOperationFactory.createResolvedWindow(window, resolver)

    val resolverWithWindowReferences = resolverFor(tableCatalog, functionCatalog, child)
      .withLocalReferences(
        new LocalReferenceExpression(
          resolvedWindow.getAlias,
          resolvedWindow.getTimeAttribute.getOutputDataType))
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

  def windowTableAggregate(
    groupingExpressions: JList[Expression],
    window: GroupWindow,
    windowProperties: JList[Expression],
    tableAggFunction: Expression,
    child: QueryOperation)
  : QueryOperation = {

    // Step1: add a default name to the call in the grouping expressions, e.g., groupBy(a % 5) to
    // groupBy(a % 5 as TMP_0). We need a name for every column so that to perform alias for the
    // table aggregate function in Step4.
    val newGroupingExpressions = addAliasToTheCallInGroupings(
      child.getTableSchema.getFieldNames,
      groupingExpressions)

    // Step2: resolve expressions, including grouping, aggregates and window properties.
    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val resolvedWindow = aggregateOperationFactory.createResolvedWindow(window, resolver)

    val resolverWithWindowReferences = resolverFor(tableCatalog, functionCatalog, child)
      .withLocalReferences(
        new LocalReferenceExpression(
          resolvedWindow.getAlias,
          resolvedWindow.getTimeAttribute.getOutputDataType))
      .build

    val convertedGroupings = resolverWithWindowReferences.resolve(newGroupingExpressions)
    val convertedAggregates = resolverWithWindowReferences.resolve(Seq(tableAggFunction))
    val convertedProperties = resolverWithWindowReferences.resolve(windowProperties)
    val resolvedFunctionAndAlias = aggregateOperationFactory.extractTableAggFunctionAndAliases(
      convertedAggregates.get(0))

    // Step3: create window table agg operation
    val tableAggOperation = aggregateOperationFactory.createWindowAggregate(
      convertedGroupings,
      Seq(resolvedFunctionAndAlias.f0),
      convertedProperties,
      resolvedWindow,
      child)

    // Step4: add a top project to alias the output fields of the table aggregate. Also, project the
    // window attribute.
    aliasBackwardFields(tableAggOperation, resolvedFunctionAndAlias.f1, groupingExpressions.size())
  }

  def join(
      left: QueryOperation,
      right: QueryOperation,
      joinType: JoinType,
      condition: Optional[Expression],
      correlated: Boolean)
    : QueryOperation = {
    val resolver = resolverFor(tableCatalog, functionCatalog, left, right).build()
    val resolvedCondition = toScala(condition).map(expr => resolveSingleExpression(expr, resolver))

    joinOperationFactory
      .create(left, right, joinType, resolvedCondition.getOrElse(valueLiteral(true)), correlated)
  }

  def joinLateral(
      left: QueryOperation,
      tableFunction: Expression,
      joinType: JoinType,
      condition: Optional[Expression])
    : QueryOperation = {
    val resolver = resolverFor(tableCatalog, functionCatalog, left).build()
    val resolvedFunction = resolveSingleExpression(tableFunction, resolver)

    val temporalTable =
      calculatedTableFactory.create(resolvedFunction, left.getTableSchema.getFieldNames)

    join(left, temporalTable, joinType, condition, correlated = true)
  }

  def resolveExpression(expression: Expression, queryOperation: QueryOperation*)
    : Expression = {
    val resolver = resolverFor(tableCatalog, functionCatalog, queryOperation: _*).build()

    resolveSingleExpression(expression, resolver)
  }

  private def resolveSingleExpression(
      expression: Expression,
      resolver: ExpressionResolver)
    : ResolvedExpression = {
    val resolvedExpression = resolver.resolve(List(expression).asJava)
    if (resolvedExpression.size() != 1) {
      throw new ValidationException("Expected single expression")
    } else {
      resolvedExpression.get(0)
    }
  }

  def sort(
      fields: JList[Expression],
      child: QueryOperation)
    : QueryOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val resolvedFields = resolver.resolve(fields)

    sortOperationFactory.createSort(resolvedFields, child, resolver.postResolverFactory())
  }

  def limitWithOffset(offset: Int, child: QueryOperation): QueryOperation = {
    sortOperationFactory.createLimitWithOffset(offset, child)
  }

  def limitWithFetch(fetch: Int, child: QueryOperation): QueryOperation = {
    sortOperationFactory.createLimitWithFetch(fetch, child)
  }

  def alias(
      fields: JList[Expression],
      child: QueryOperation)
    : QueryOperation = {

    val newFields = createAliasList(fields, child)

    project(newFields, child, explicitAlias = true)
  }

  def filter(
      condition: Expression,
      child: QueryOperation)
    : QueryOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val resolvedExpression = resolveSingleExpression(condition, resolver)
    val convertedCondition = expressionBridge.bridge(resolvedExpression)
    if (convertedCondition.resultType != Types.BOOLEAN) {
      throw new ValidationException(s"Filter operator requires a boolean expression as input," +
        s" but $condition is of type ${convertedCondition.resultType}")
    }

    new FilterQueryOperation(resolvedExpression, child)
  }

  def distinct(
      child: QueryOperation)
    : QueryOperation = {
    new DistinctQueryOperation(child)
  }

  def minus(
      left: QueryOperation,
      right: QueryOperation,
      all: Boolean)
    : QueryOperation = {
    setOperationFactory.create(MINUS, left, right, all)
  }

  def intersect(
      left: QueryOperation,
      right: QueryOperation,
      all: Boolean)
    : QueryOperation = {
    setOperationFactory.create(INTERSECT, left, right, all)
  }

  def union(
      left: QueryOperation,
      right: QueryOperation,
      all: Boolean)
    : QueryOperation = {
    setOperationFactory.create(UNION, left, right, all)
  }

  def map(mapFunction: Expression, child: QueryOperation): QueryOperation = {

    val resolvedMapFunction = mapFunction.accept(lookupResolver)

    if (!isFunctionOfKind(resolvedMapFunction, SCALAR)) {
      throw new ValidationException("Only a scalar function can be used in the map operator.")
    }

    val expandedFields = unresolvedCall(BuiltInFunctionDefinitions.FLATTEN, resolvedMapFunction)
    project(Collections.singletonList(expandedFields), child)
  }

  def flatMap(tableFunction: Expression, child: QueryOperation): QueryOperation = {

    val resolvedTableFunction = tableFunction.accept(lookupResolver)

    if (!isFunctionOfKind(resolvedTableFunction, TABLE)) {
      throw new ValidationException("Only a table function can be used in the flatMap operator.")
    }

    val originFieldNames: Seq[String] =
      resolvedTableFunction.asInstanceOf[UnresolvedCallExpression].getFunctionDefinition match {
        case tfd: TableFunctionDefinition =>
          UserDefinedFunctionUtils.getFieldInfo(tfd.getResultType)._1
      }

    val usedFieldNames = child.getTableSchema.getFieldNames.toBuffer
    val newFieldNames = originFieldNames.map({ e =>
      val resultName = getUniqueName(e, usedFieldNames)
      usedFieldNames.append(resultName)
      resultName
    })

    val renamedTableFunction = unresolvedCall(
      BuiltInFunctionDefinitions.AS,
      resolvedTableFunction +: newFieldNames.map(ApiExpressionUtils.valueLiteral(_)): _*)
    val joinNode = joinLateral(child, renamedTableFunction, JoinType.INNER, Optional.empty())
    val rightNode = dropColumns(
      child.getTableSchema.getFieldNames.map(ApiExpressionUtils.unresolvedRef).toList,
      joinNode)
    alias(originFieldNames.map(a => unresolvedRef(a)), rightNode)
  }

  /**
    * Return a unique name that does not exist in usedFieldNames according to the input name.
    */
  private def getUniqueName(inputName: String, usedFieldNames: Seq[String]): String = {
    var i = 0
    var resultName = inputName
    while (usedFieldNames.contains(resultName)) {
      resultName = resultName + "_" + i
      i += 1
    }
    resultName
  }

  /**
    * Add a default name to the call in the grouping expressions, e.g., groupBy(a % 5) to
    * groupBy(a % 5 as TMP_0).
    */
  private def addAliasToTheCallInGroupings(
      inputFieldNames: Seq[String],
      groupingExpressions: JList[Expression])
    : JList[Expression] = {

    var attrNameCntr: Int = 0
    val usedFieldNames = inputFieldNames.toBuffer
    groupingExpressions.map {
      case c: UnresolvedCallExpression
          if c.getFunctionDefinition != BuiltInFunctionDefinitions.AS =>
        val tempName = getUniqueName("TMP_" + attrNameCntr, usedFieldNames)
        usedFieldNames.append(tempName)
        attrNameCntr += 1
        unresolvedCall(
          BuiltInFunctionDefinitions.AS,
          c,
          valueLiteral(tempName)
        )
      case e => e
    }
  }

  /**
    * Rename fields in the input [[QueryOperation]].
    */
  private def aliasBackwardFields(
    inputOperation: QueryOperation,
    alias: Seq[String],
    aliasStartIndex: Int)
  : QueryOperation = {

    if (alias.nonEmpty) {
      val namesBeforeAlias = inputOperation.getTableSchema.getFieldNames
      val namesAfterAlias = namesBeforeAlias.take(aliasStartIndex) ++ alias ++
        namesBeforeAlias.takeRight(namesBeforeAlias.length - alias.size - aliasStartIndex)
      this.alias(namesAfterAlias.map(e => unresolvedRef(e)).toList, inputOperation)
    } else {
      inputOperation
    }
  }

  class NoWindowPropertyChecker(val exceptionMessage: String)
    extends ApiExpressionDefaultVisitor[Void] {
    override def visit(unresolvedCall: UnresolvedCallExpression): Void = {
      val functionDefinition = unresolvedCall.getFunctionDefinition
      if (BuiltInFunctionDefinitions.WINDOW_PROPERTIES
        .contains(functionDefinition)) {
        throw new ValidationException(exceptionMessage)
      }
      unresolvedCall.getChildren.asScala.foreach(expr => expr.accept(this))
      null
    }

    override protected def defaultMethod(expression: Expression): Void = null
  }
}
