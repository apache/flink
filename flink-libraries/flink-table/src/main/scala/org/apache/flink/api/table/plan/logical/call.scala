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
package org.apache.flink.api.table.plan.logical

import java.lang.reflect.Method
import java.util

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table._
import org.apache.flink.api.table.expressions.{Attribute, Expression, ResolvedFieldReference}
import org.apache.flink.api.table.functions.TableFunction
import org.apache.flink.api.table.functions.utils.TableSqlFunction
import org.apache.flink.api.table.functions.utils.UserDefinedFunctionUtils.{getEvalMethod, signaturesToString, signatureToString, getFieldInfo, checkNotSingleton, checkForInstantiation}
import org.apache.flink.api.table.plan.schema.FlinkTableFunctionImpl

import scala.collection.JavaConverters._

/**
  * General logical node for unresolved user-defined table function calls.
  */
case class UnresolvedTableFunctionCall(functionName: String, args: Seq[Expression])
  extends LogicalNode {

  override def output: Seq[Attribute] =
    throw UnresolvedException("Invalid call to output on UnresolvedTableFunctionCall")

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder =
    throw UnresolvedException("Invalid call to construct on UnresolvedTableFunctionCall")

  override private[flink] def children: Seq[LogicalNode] =
    throw UnresolvedException("Invalid call to children on UnresolvedTableFunctionCall")
}

/**
  * LogicalNode for calling a user-defined table functions.
  * @param functionName function name
  * @param tableFunction table function to be called (might be overloaded)
  * @param parameters actual parameters
  * @param fieldNames output field names
  * @param child child logical node
  */
case class LogicalTableFunctionCall(
    functionName: String,
    tableFunction: TableFunction[_],
    parameters: Seq[Expression],
    resultType: TypeInformation[_],
    fieldNames: Array[String],
    child: LogicalNode)
  extends UnaryNode {

  val (_, fieldIndexes, fieldTypes) = getFieldInfo(resultType)
  var evalMethod: Method = _

  override def output: Seq[Attribute] = fieldNames.zip(fieldTypes).map {
    case (n, t) => ResolvedFieldReference(n, t)
  }

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    val node = super.validate(tableEnv).asInstanceOf[LogicalTableFunctionCall]
    // check not Scala object
    checkNotSingleton(tableFunction.getClass)
    // check could be instantiated
    checkForInstantiation(tableFunction.getClass)
    // look for a signature that matches the input types
    val signature = node.parameters.map(_.resultType)
    val foundMethod = getEvalMethod(tableFunction.getClass, signature)
    if (foundMethod.isEmpty) {
      failValidation(
        s"Given parameters of function '$functionName' do not match any signature. \n" +
          s"Actual: ${signatureToString(signature)} \n" +
          s"Expected: ${signaturesToString(tableFunction.getClass)}")
    } else {
      node.evalMethod = foundMethod.get
    }
    node
  }

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    val fieldIndexes = getFieldInfo(resultType)._2
    val function = new FlinkTableFunctionImpl(resultType, fieldIndexes, fieldNames, evalMethod)
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val sqlFunction = TableSqlFunction(
      tableFunction.toString,
      tableFunction,
      resultType,
      typeFactory,
      function)

    val scan = LogicalTableFunctionScan.create(
      relBuilder.peek().getCluster,
      new util.ArrayList[RelNode](),
      relBuilder.call(sqlFunction, parameters.map(_.toRexNode(relBuilder)).asJava),
      function.getElementType(null),
      function.getRowType(relBuilder.getTypeFactory, null),
      null)

    relBuilder.push(scan)
  }
}
