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

package org.apache.flink.table.planner.codegen.calls

import org.apache.calcite.rex.RexCall
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils.newName
import org.apache.flink.table.planner.codegen.GeneratedExpression.NEVER_NULL
import org.apache.flink.table.planner.codegen.calls.ScalarFunctionCallGen.prepareFunctionArgs
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils.getEvalMethodSignature
import org.apache.flink.table.planner.functions.utils.{TableSqlFunction, UserDefinedFunctionUtils}
import org.apache.flink.table.planner.plan.schema.FlinkTableFunction
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.types.PlannerTypeUtils
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.LogicalType

import scala.collection.JavaConversions._

/**
  * Generates a call to user-defined [[TableFunction]].
  *
  * @param tableFunction user-defined [[TableFunction]] that might be overloaded
  */
class TableFunctionCallGen(
    rexCall: RexCall,
    tableFunction: TableFunction[_])
  extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {

    val functionReference = ctx.addReusableFunction(tableFunction)

    val resultCollectorTerm = generateResultCollector(ctx)

    val setCollectorCode = s"""
      |$functionReference.setCollector($resultCollectorTerm);
      |""".stripMargin
    ctx.addReusableOpenStatement(setCollectorCode)

    // generate function call
    // convert parameters for function (output boxing)
    val parameters = prepareUDFArgs(ctx, operands, tableFunction)
    val functionCallCode =
      s"""
        |${parameters.map(_.code).mkString("\n")}
        |$functionReference.eval(${parameters.map(_.resultTerm).mkString(", ")});
        |""".stripMargin

    // has no result
    GeneratedExpression(
      resultCollectorTerm,
      NEVER_NULL,
      functionCallCode,
      returnType)
  }

  def prepareUDFArgs(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      func: TableFunction[_]): Array[GeneratedExpression] = {
    // get the expanded parameter types
    val paramClasses = getEvalMethodSignature(func, operands.map(_.resultType).toArray)
    prepareFunctionArgs(ctx, operands, paramClasses, func.getParameterTypes(paramClasses))
  }

  def getExternalDataType: DataType = {
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    val arguments = UserDefinedFunctionUtils.transformRexNodes(rexCall.operands)
    val operandTypes = rexCall.operands
        .map(_.getType)
        .map(FlinkTypeFactory.toLogicalType).toArray
    val func = sqlFunction.makeFunction(arguments, operandTypes)
    val argTypes = getEvalMethodSignature(
      func,
      rexCall.operands
        .map(_.getType)
        .map(FlinkTypeFactory.toLogicalType).toArray)
    sqlFunction
        .getFunction
        .asInstanceOf[FlinkTableFunction]
        .getExternalResultType(func, arguments, argTypes)
  }

  /**
   * Generates a collector that converts the output of a table function (possibly as an atomic type)
   * into an internal row type. Returns a collector term for referencing the collector.
   */
  def generateResultCollector(ctx: CodeGeneratorContext): String = {
    val externalDataType = getExternalDataType
    val pojoFieldMapping = Some(UserDefinedFunctionUtils.getFieldInfo(externalDataType)._2)
    val externalType = fromDataTypeToLogicalType(externalDataType)
    val wrappedInternalType = PlannerTypeUtils.toRowType(externalType)

    val collectorCtx = CodeGeneratorContext(ctx.tableConfig)
    val externalTerm = newName("externalRecord")
    val resultGenerator = new ExprCodeGenerator(collectorCtx, true)
      .bindInput(externalType, externalTerm, pojoFieldMapping)
    val wrappedResult = resultGenerator.generateConverterResultExpression(
      wrappedInternalType,
      classOf[GenericRow])

    val collectorCode =
      s"""
       |${wrappedResult.code}
       |outputResult(${wrappedResult.resultTerm});
       |""".stripMargin

    val resultCollector = CollectorCodeGenerator.generateWrappingCollector(
      collectorCtx,
      "TableFunctionResultConverterCollector",
      externalType,
      externalTerm,
      CodeGenUtils.genToInternal(ctx, externalDataType),
      collectorCode)
    val resultCollectorTerm = newName("resultConverterCollector")
    CollectorCodeGenerator.addToContext(ctx, resultCollectorTerm, resultCollector)
    resultCollectorTerm
  }
}
