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

import java.util.{Optional, List => JList}
import org.apache.flink.table.api.{TableEnvironment, ValidationException}
import org.apache.flink.table.expressions.ExpressionResolver.resolverFor
import org.apache.flink.table.expressions.catalog.FunctionDefinitionCatalog
import org.apache.flink.table.expressions.lookups.TableReferenceLookup
import org.apache.flink.table.expressions.{Expression, LookupCallResolver, TableReferenceExpression}
import org.apache.flink.table.operations.OperationExpressionsUtils.extractAggregationsAndProperties
import org.apache.flink.table.util.JavaScalaConversionUtil

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._

/**
  * Builder for [[[QueryOperation]] tree.
  */
class OperationTreeBuilder(private val tableEnv: TableEnvironment) {

  private val functionCatalog: FunctionDefinitionCatalog = tableEnv.functionCatalog

  private val projectionOperationFactory = new ProjectionOperationFactory(tableEnv.getRelBuilder,
    functionCatalog)

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

}
