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

import org.apache.flink.table.api.{StreamTableEnvironment, TableEnvironment, ValidationException}
import org.apache.flink.table.expressions.ApiExpressionUtils.{call, valueLiteral}
import org.apache.flink.table.expressions.ExpressionResolver.resolverFor
import org.apache.flink.table.expressions.catalog.FunctionDefinitionCatalog
import org.apache.flink.table.expressions.lookups.TableReferenceLookup
import org.apache.flink.table.expressions.{AggregateFunctionDefinition, BuiltInFunctionDefinitions, CallExpression, Expression, ExpressionResolver, ExpressionUtils, LookupCallResolver, TableReferenceExpression, UnresolvedReferenceExpression}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.operations.AliasOperationUtils.createAliasList
import org.apache.flink.table.operations.JoinQueryOperation.JoinType
import org.apache.flink.table.operations.OperationExpressionsUtils.extractAggregationsAndProperties
import org.apache.flink.table.operations.SetQueryOperation.SetQueryOperationType.{INTERSECT, MINUS, UNION}
import org.apache.flink.table.types.logical.LogicalTypeRoot
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.util.JavaScalaConversionUtil
import org.apache.flink.table.util.JavaScalaConversionUtil.toScala

import java.util.{Optional, List => JList}

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._

/**
  * Builder for [[[QueryOperation]] tree.
  */
class OperationTreeBuilder(private val tableEnv: TableEnvironment) {

  private val functionCatalog: FunctionDefinitionCatalog = tableEnv.functionCatalog

  private val isStreaming = tableEnv.isInstanceOf[StreamTableEnvironment]
  private val projectionOperationFactory = new ProjectionOperationFactory()
  private val sortOperationFactory = new SortOperationFactory(isStreaming)
  private val calculatedTableFactory = new CalculatedTableFactory()
  private val setOperationFactory = new SetOperationFactory(isStreaming)
  private val aggregateOperationFactory = new AggregateOperationFactory(isStreaming)
  private val joinOperationFactory = new JoinOperationFactory()

  private val tableCatalog = new TableReferenceLookup {
    override def lookupTable(name: String): Optional[TableReferenceExpression] =
      JavaScalaConversionUtil
        .toJava(tableEnv.scanInternal(Array(name))
          .map(t => new TableReferenceExpression(name, t.getQueryOperation)))
  }

  def project(
      projectList: JList[Expression],
      child: QueryOperation,
      explicitAlias: Boolean = false): QueryOperation = {
    projectInternal(projectList, child, explicitAlias)
  }

  private def projectInternal(
      projectList: JList[Expression],
      child: QueryOperation,
      explicitAlias: Boolean): QueryOperation = {

    validateProjectList(projectList)

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build
    val projections = resolver.resolve(projectList)
    projectionOperationFactory.create(projections, child, explicitAlias)
  }

  /**
    * Window properties and aggregate function should not exist in the plain project, i.e., window
    * properties should exist in the window operators and aggregate functions should exist in
    * the aggregate operators.
    */
  private def validateProjectList(projectList: JList[Expression]): Unit = {

    val callResolver = new LookupCallResolver(functionCatalog)
    val expressionsWithResolvedCalls = projectList.map(_.accept(callResolver)).asJava
    val extracted = extractAggregationsAndProperties(expressionsWithResolvedCalls)
    if (!extracted.getWindowProperties.isEmpty) {
      throw new ValidationException("Window properties can only be used on windowed tables.")
    }

    // aggregate functions can't exist in the plain project except for the over window case
    if (!extracted.getAggregations.isEmpty) {
      throw new ValidationException("Aggregate functions are not supported in the select right" +
        " after the aggregate or flatAggregate operation.")
    }
  }

  def alias(
      fields: JList[Expression],
      child: QueryOperation): QueryOperation = {

    val newFields = createAliasList(fields, child)

    project(newFields, child, explicitAlias = true)
  }

  def filter(
      condition: Expression,
      child: QueryOperation): QueryOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val resolvedExpression = resolveSingleExpression(condition, resolver)
    val returnTypeRoot = ExpressionTypeInfer.infer(resolvedExpression).getLogicalType.getTypeRoot
    if (returnTypeRoot != LogicalTypeRoot.BOOLEAN) {
      throw new ValidationException(s"Filter operator requires a boolean expression as input," +
          s" but $condition is of type $returnTypeRoot")
    }

    new FilterQueryOperation(resolvedExpression, child)
  }

  def distinct(
      child: QueryOperation): QueryOperation = {
    new DistinctQueryOperation(child)
  }

  def minus(
      left: QueryOperation,
      right: QueryOperation,
      all: Boolean): QueryOperation = {
    setOperationFactory.create(MINUS, left, right, all)
  }

  def intersect(
      left: QueryOperation,
      right: QueryOperation,
      all: Boolean): QueryOperation = {
    setOperationFactory.create(INTERSECT, left, right, all)
  }

  def union(
      left: QueryOperation,
      right: QueryOperation,
      all: Boolean): QueryOperation = {
    setOperationFactory.create(UNION, left, right, all)
  }

  def sort(
      fields: JList[Expression],
      child: QueryOperation): QueryOperation = {

    val resolver = resolverFor(tableCatalog, functionCatalog, child).build()
    val resolvedFields = resolver.resolve(fields)

    sortOperationFactory.createSort(resolvedFields, child)
  }

  def limitWithOffset(offset: Int, child: QueryOperation): QueryOperation = {
    sortOperationFactory.createLimitWithOffset(offset, child)
  }

  def limitWithFetch(fetch: Int, child: QueryOperation): QueryOperation = {
    sortOperationFactory.createLimitWithFetch(fetch, child)
  }

  private def resolveSingleExpression(
      expression: Expression,
      resolver: ExpressionResolver): Expression = {
    val resolvedExpression = resolver.resolve(List(expression).asJava)
    if (resolvedExpression.size() != 1) {
      throw new ValidationException("Expected single expression")
    } else {
      resolvedExpression.get(0)
    }
  }

  def resolveExpression(expression: Expression, queryOperation: QueryOperation*): Expression = {
    val resolver = resolverFor(tableCatalog, functionCatalog, queryOperation: _*).build()

    resolveSingleExpression(expression, resolver)
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
      this.alias(namesAfterAlias.map(e =>
        new UnresolvedReferenceExpression(e)).toList, inputOperation)
    } else {
      inputOperation
    }
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
    // resolve for java string case, i.e., turn LookupCallExpression to CallExpression.
    val resolvedAggregate = this.resolveExpression(aggregate, child)

    // extract alias and aggregate function
    var alias: Seq[String] = Seq()
    val aggWithoutAlias = resolvedAggregate match {
      case c: CallExpression
        if c.getFunctionDefinition.getName == BuiltInFunctionDefinitions.AS.getName =>
        alias = c.getChildren
            .drop(1)
            .map(e => ExpressionUtils.extractValue(e, classOf[String]).get())
        c.getChildren.get(0)
      case c: CallExpression
        if c.getFunctionDefinition.isInstanceOf[AggregateFunctionDefinition] =>
        if (alias.isEmpty) alias = UserDefinedFunctionUtils.getFieldInfo(
          fromLegacyInfoToDataType(
            c.getFunctionDefinition.asInstanceOf[AggregateFunctionDefinition]
                .getResultTypeInfo))._1
        c
      case e => e
    }

    // turn agg to a named agg, because it will be verified later.
    var cnt = 0
    val childNames = child.getTableSchema.getFieldNames
    while (childNames.contains("TMP_" + cnt)) {
      cnt += 1
    }
    val aggWithNamedAlias = call(
      BuiltInFunctionDefinitions.AS,
      aggWithoutAlias,
      valueLiteral("TMP_" + cnt))

    // get agg table
    val aggQueryOperation = this.aggregate(groupingExpressions, Seq(aggWithNamedAlias), child)

    // flatten the aggregate function
    val aggNames = aggQueryOperation.getTableSchema.getFieldNames
    val flattenExpressions = aggNames.take(groupingExpressions.size())
        .map(e => new UnresolvedReferenceExpression(e)) ++
        Seq(new CallExpression(BuiltInFunctionDefinitions.FLATTEN,
          Seq(new UnresolvedReferenceExpression(aggNames.last))))
    val flattenedOperation = this.project(flattenExpressions.toList, aggQueryOperation)

    // add alias
    aliasBackwardFields(flattenedOperation, alias, groupingExpressions.size())
  }

  def join(
      left: QueryOperation,
      right: QueryOperation,
      joinType: JoinType,
      condition: Optional[Expression],
      correlated: Boolean): QueryOperation = {
    val resolver = resolverFor(tableCatalog, functionCatalog, left, right).build()
    val resolvedCondition = toScala(condition).map(expr => resolveSingleExpression(expr, resolver))

    joinOperationFactory
        .create(left, right, joinType, resolvedCondition.getOrElse(valueLiteral(true)), correlated)
  }

  def joinLateral(
      left: QueryOperation,
      tableFunction: Expression,
      joinType: JoinType,
      condition: Optional[Expression]): QueryOperation = {
    val resolver = resolverFor(tableCatalog, functionCatalog, left).build()
    val resolvedFunction = resolveSingleExpression(tableFunction, resolver)

    val temporalTable = calculatedTableFactory.create(resolvedFunction)

    join(left, temporalTable, joinType, condition, correlated = true)
  }
}
