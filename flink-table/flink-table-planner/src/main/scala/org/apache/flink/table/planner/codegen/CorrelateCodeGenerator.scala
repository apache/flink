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
package org.apache.flink.table.planner.codegen

import org.apache.flink.api.common.functions.Function
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.utils.JoinedRowData
import org.apache.flink.table.functions.FunctionKind
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.utils.TableSqlFunction
import org.apache.flink.table.planner.plan.nodes.exec.utils.{ExecNodeUtil, TransformationMetadata}
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.operators.join.FlinkJoinType
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.runtime.util.StreamRecordCollector
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rex._

object CorrelateCodeGenerator {

  def generateCorrelateTransformation(
      tableConfig: ReadableConfig,
      operatorCtx: CodeGeneratorContext,
      inputTransformation: Transformation[RowData],
      inputType: RowType,
      invocation: RexCall,
      condition: Option[RexNode],
      outputType: RowType,
      joinType: FlinkJoinType,
      parallelism: Int,
      retainHeader: Boolean,
      opName: String,
      transformationMeta: TransformationMetadata,
      parallelismConfigured: Boolean): Transformation[RowData] = {

    // according to the SQL standard, every scalar function should also be a table function
    // but we don't allow that for now
    invocation.getOperator match {
      case func: BridgingSqlFunction if func.getDefinition.getKind == FunctionKind.TABLE => // ok
      case _: TableSqlFunction => // ok
      case f @ _ =>
        throw new ValidationException(
          s"Invalid use of function '$f'. " +
            s"Currently, only table functions can be used in a correlate operation.")
    }

    // adjust indicies of InputRefs to adhere to schema expected by generator
    val changeInputRefIndexShuttle = new RexShuttle {
      override def visitInputRef(inputRef: RexInputRef): RexNode = {
        new RexInputRef(inputType.getFieldCount + inputRef.getIndex, inputRef.getType)
      }
    }

    val substituteStreamOperator = generateOperator(
      operatorCtx,
      tableConfig,
      inputType,
      condition.map(_.accept(changeInputRefIndexShuttle)),
      outputType,
      joinType,
      invocation,
      opName,
      retainHeader)

    ExecNodeUtil.createOneInputTransformation(
      inputTransformation,
      transformationMeta,
      substituteStreamOperator,
      InternalTypeInfo.of(outputType),
      parallelism,
      0,
      parallelismConfigured)
  }

  /** Generates the flat map operator to run the user-defined table function. */
  private[flink] def generateOperator[T <: Function](
      ctx: CodeGeneratorContext,
      tableConfig: ReadableConfig,
      inputType: RowType,
      condition: Option[RexNode],
      returnType: RowType,
      joinType: FlinkJoinType,
      rexCall: RexCall,
      ruleDescription: String,
      retainHeader: Boolean = true): CodeGenOperatorFactory[RowData] = {

    val functionResultType = FlinkTypeFactory.toLogicalRowType(rexCall.getType)

    // 1. prepare collectors

    // 1.1 compile correlate collector
    val correlateCollectorTerm = generateCorrelateCollector(
      ctx,
      tableConfig,
      inputType,
      functionResultType,
      returnType,
      condition,
      retainHeader)

    // 1.2 compile result conversion collector
    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(inputType)
    val call = exprGenerator.generateExpression(rexCall)
    val resultCollectorTerm = call.resultTerm
    val setCollectors =
      s"""
         |$correlateCollectorTerm.setCollector(
         | new ${classOf[StreamRecordCollector[_]].getCanonicalName}(
         |     ${CodeGenUtils.DEFAULT_OPERATOR_COLLECTOR_TERM}));
         |$resultCollectorTerm.setCollector($correlateCollectorTerm);
         |""".stripMargin
    ctx.addReusableOpenStatement(setCollectors)

    // 2. call function
    var body =
      s"""
         |$correlateCollectorTerm.setInput(${exprGenerator.input1Term});
         |$correlateCollectorTerm.reset();
         |${call.code}
         |""".stripMargin

    // 3. left join
    if (joinType == FlinkJoinType.LEFT) {
      // output all fields of left and right
      // in case of left outer join and the returned row of table function is empty,
      // fill all fields of row with null
      val joinedRowTerm = CodeGenUtils.newName("joinedRow")
      val nullRowTerm = CodeGenUtils.newName("nullRow")
      ctx.addReusableOutputRecord(returnType, classOf[JoinedRowData], joinedRowTerm)
      ctx.addReusableNullRow(nullRowTerm, functionResultType.getFieldCount)
      val header = if (retainHeader) {
        s"$joinedRowTerm.setRowKind(${exprGenerator.input1Term}.getRowKind());"
      } else {
        ""
      }
      body +=
        s"""
           |boolean hasOutput = $correlateCollectorTerm.isCollected();
           |if (!hasOutput) {
           |  $joinedRowTerm.replace(${exprGenerator.input1Term}, $nullRowTerm);
           |  $header
           |  $correlateCollectorTerm.outputResult($joinedRowTerm);
           |}
           |""".stripMargin
    } else if (joinType != FlinkJoinType.INNER) {
      throw new TableException(s"Unsupported JoinRelType: $joinType for correlate join.")
    }

    val genOperator = OperatorCodeGenerator.generateOneInputStreamOperator[RowData, RowData](
      ctx,
      ruleDescription,
      body,
      inputType)
    new CodeGenOperatorFactory(genOperator)
  }

  /**
   * Generates a collector that correlates input and converted table function results. Returns a
   * collector term for referencing the collector.
   */
  private def generateCorrelateCollector(
      ctx: CodeGeneratorContext,
      tableConfig: ReadableConfig,
      inputType: RowType,
      functionResultType: RowType,
      resultType: RowType,
      condition: Option[RexNode],
      retainHeader: Boolean = true): String = {

    val correlateCollectorTerm = newName("correlateCollector")
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val udtfInputTerm = CodeGenUtils.DEFAULT_INPUT2_TERM

    val collectorCtx = new CodeGeneratorContext(tableConfig, ctx.classLoader)

    val body = {
      // completely output left input + right
      val joinedRowTerm = CodeGenUtils.newName("joinedRow")
      collectorCtx.addReusableOutputRecord(resultType, classOf[JoinedRowData], joinedRowTerm)

      val header = if (retainHeader) {
        s"$joinedRowTerm.setRowKind($inputTerm.getRowKind());"
      } else {
        ""
      }
      s"""
         |$joinedRowTerm.replace($inputTerm, $udtfInputTerm);
         |$header
         |outputResult($joinedRowTerm);
      """.stripMargin
    }

    val collectorCode = if (condition.isEmpty) {
      body
    } else {
      val filterGenerator = new ExprCodeGenerator(collectorCtx, false)
        .bindInput(inputType, inputTerm)
        .bindSecondInput(functionResultType, udtfInputTerm)
      val filterCondition = filterGenerator.generateExpression(condition.get)

      s"""
         |${filterCondition.code}
         |if (${filterCondition.resultTerm}) {
         |  $body
         |}
         |""".stripMargin
    }

    val correlateCollector = CollectorCodeGenerator.generateTableFunctionCollector(
      collectorCtx,
      "TableFunctionCollector",
      collectorCode,
      inputType,
      functionResultType,
      inputTerm = inputTerm,
      collectedTerm = udtfInputTerm)

    CollectorCodeGenerator.addToContext(ctx, correlateCollectorTerm, correlateCollector)

    correlateCollectorTerm
  }
}
