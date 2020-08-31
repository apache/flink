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
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{TableConfig, TableException, ValidationException}
import org.apache.flink.table.data.{GenericRowData, JoinedRowData, RowData}
import org.apache.flink.table.functions.FunctionKind
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.utils.TableSqlFunction
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.runtime.util.StreamRecordCollector
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex._

import scala.collection.JavaConversions._

object CorrelateCodeGenerator {

  private[flink] def generateCorrelateTransformation(
      config: TableConfig,
      operatorCtx: CodeGeneratorContext,
      inputTransformation: Transformation[RowData],
      inputRelType: RelDataType,
      projectProgram: Option[RexProgram],
      scan: FlinkLogicalTableFunctionScan,
      condition: Option[RexNode],
      outRelType: RelDataType,
      joinType: JoinRelType,
      parallelism: Int,
      retainHeader: Boolean,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String,
      opName: String,
      transformationName: String)
    : Transformation[RowData] = {

    val funcRel = scan.asInstanceOf[FlinkLogicalTableFunctionScan]
    val rexCall = funcRel.getCall.asInstanceOf[RexCall]
    val inputType = FlinkTypeFactory.toLogicalRowType(inputRelType)
    val returnType = FlinkTypeFactory.toLogicalRowType(outRelType)

    // according to the SQL standard, every scalar function should also be a table function
    // but we don't allow that for now
    rexCall.getOperator match {
      case func: BridgingSqlFunction if func.getDefinition.getKind == FunctionKind.TABLE => // ok
      case _: TableSqlFunction => // ok
      case f@_ =>
        throw new ValidationException(
          s"Invalid use of function '$f'. " +
            s"Currently, only table functions can be used in a correlate operation.")
    }

    val swallowInputOnly = if (projectProgram.isDefined) {
      val program = projectProgram.get
      val selects = program.getProjectList.map(_.getIndex)
      val inputFieldCnt = program.getInputRowType.getFieldCount
      val swallowInputOnly = selects.head > inputFieldCnt &&
        (inputFieldCnt - outRelType.getFieldCount == inputRelType.getFieldCount)
      // partial output or output right only
      swallowInputOnly
    } else {
      // completely output left input + right
      false
    }

    // adjust indicies of InputRefs to adhere to schema expected by generator
    val changeInputRefIndexShuttle = new RexShuttle {
      override def visitInputRef(inputRef: RexInputRef): RexNode = {
        new RexInputRef(inputRelType.getFieldCount + inputRef.getIndex, inputRef.getType)
      }
    }

    val substituteStreamOperator = generateOperator(
      operatorCtx,
      config,
      inputType,
      projectProgram,
      swallowInputOnly,
      condition.map(_.accept(changeInputRefIndexShuttle)),
      returnType,
      joinType,
      rexCall,
      opName,
      classOf[ProcessFunction[RowData, RowData]],
      retainHeader)

    ExecNode.createOneInputTransformation(
      inputTransformation,
      transformationName,
      substituteStreamOperator,
      InternalTypeInfo.of(returnType),
      parallelism)
  }

  /**
    * Generates the flat map operator to run the user-defined table function.
    */
  private[flink] def generateOperator[T <: Function](
      ctx: CodeGeneratorContext,
      config: TableConfig,
      inputType: RowType,
      projectProgram: Option[RexProgram],
      swallowInputOnly: Boolean = false,
      condition: Option[RexNode],
      returnType: RowType,
      joinType: JoinRelType,
      rexCall: RexCall,
      ruleDescription: String,
      functionClass: Class[T],
      retainHeader: Boolean = true)
    : CodeGenOperatorFactory[RowData] = {

    val functionResultType = FlinkTypeFactory.toLogicalRowType(rexCall.getType)

    // 1. prepare collectors

    // 1.1 compile correlate collector
    val correlateCollectorTerm = generateCorrelateCollector(
      ctx,
      config,
      inputType,
      projectProgram,
      swallowInputOnly,
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
         |     ${CodeGenUtils.DEFAULT_OPERATOR_COLLECTOR_TERM }));
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
    if (joinType == JoinRelType.LEFT) {
      if (swallowInputOnly) {
        // and the returned row table function is empty, collect a null
        val nullRowTerm = CodeGenUtils.newName("nullRow")
        ctx.addReusableOutputRecord(functionResultType, classOf[GenericRowData], nullRowTerm)
        ctx.addReusableNullRow(nullRowTerm, functionResultType.getFieldCount)
        val header = if (retainHeader) {
          s"$nullRowTerm.setRowKind(${exprGenerator.input1Term}.getRowKind());"
        } else {
          ""
        }
        body +=
          s"""
             |boolean hasOutput = $correlateCollectorTerm.isCollected();
             |if (!hasOutput) {
             |  $header
             |  $correlateCollectorTerm.outputResult($nullRowTerm);
             |}
             |""".stripMargin
      } else if (projectProgram.isDefined) {
        // output partial fields of left and right
        val outputTerm = CodeGenUtils.newName("projectOut")
        ctx.addReusableOutputRecord(returnType, classOf[GenericRowData], outputTerm)

        val header = if (retainHeader) {
          s"$outputTerm.setRowKind(${CodeGenUtils.DEFAULT_INPUT1_TERM}.getRowKind());"
        } else {
          ""
        }
        val projectionExpression = generateProjectResultExpr(
          ctx,
          config,
          inputType,
          functionResultType,
          udtfAlwaysNull = true,
          returnType,
          outputTerm,
          projectProgram.get)

        body +=
          s"""
             |boolean hasOutput = $correlateCollectorTerm.isCollected();
             |if (!hasOutput) {
             |  ${projectionExpression.code}
             |  $header
             |  $correlateCollectorTerm.outputResult($outputTerm);
             |}
             |""".stripMargin

      } else {
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

        }
    } else if (joinType != JoinRelType.INNER) {
      throw new TableException(s"Unsupported JoinRelType: $joinType for correlate join.")
    }

    val genOperator = OperatorCodeGenerator.generateOneInputStreamOperator[RowData, RowData](
      ctx, ruleDescription, body, inputType)
    new CodeGenOperatorFactory(genOperator)
  }

  private def generateProjectResultExpr(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      input1Type: RowType,
      functionResultType: RowType,
      udtfAlwaysNull: Boolean,
      returnType: RowType,
      outputTerm: String,
      program: RexProgram): GeneratedExpression = {
    val projectExprGenerator = new ExprCodeGenerator(ctx, udtfAlwaysNull)
      .bindInput(input1Type, CodeGenUtils.DEFAULT_INPUT1_TERM)
    if (udtfAlwaysNull) {
      val udtfNullRow = CodeGenUtils.newName("udtfNullRow")
      ctx.addReusableNullRow(udtfNullRow, functionResultType.getFieldCount)

      projectExprGenerator.bindSecondInput(
        functionResultType,
        udtfNullRow)
    } else {
      projectExprGenerator.bindSecondInput(
        functionResultType)
    }
    val projection = program.getProjectList.map(program.expandLocalRef)
    val projectionExprs = projection.map(projectExprGenerator.generateExpression)
    projectExprGenerator.generateResultExpression(
      projectionExprs, returnType, classOf[GenericRowData], outputTerm)
  }

  /**
   * Generates a collector that correlates input and converted table function results. Returns a
   * collector term for referencing the collector.
   */
  private def generateCorrelateCollector(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      inputType: RowType,
      projectProgram: Option[RexProgram],
      swallowInputOnly: Boolean,
      functionResultType: RowType,
      resultType: RowType,
      condition: Option[RexNode],
      retainHeader: Boolean = true)
    : String = {

    val correlateCollectorTerm = newName("correlateCollector")
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val udtfInputTerm = CodeGenUtils.DEFAULT_INPUT2_TERM

    val collectorCtx = CodeGeneratorContext(config)

    val body = if (projectProgram.isDefined) {
      // partial output
      if (swallowInputOnly) {
        // output right only
        val header = if (retainHeader) {
          s"$udtfInputTerm.setRowKind($inputTerm.getRowKind());"
        } else {
          ""
        }
        s"""
           |$header
           |outputResult($udtfInputTerm);
        """.stripMargin
      } else {
        val outputTerm = CodeGenUtils.newName("projectOut")
        collectorCtx.addReusableOutputRecord(resultType, classOf[GenericRowData], outputTerm)

        val header = if (retainHeader) {
          s"$outputTerm.setRowKind($inputTerm.getRowKind());"
        } else {
          ""
        }
        val projectionExpression = generateProjectResultExpr(
          collectorCtx,
          config,
          inputType,
          functionResultType,
          udtfAlwaysNull = false,
          resultType,
          outputTerm,
          projectProgram.get)

        s"""
           |$header
           |${projectionExpression.code}
           |outputResult(${projectionExpression.resultTerm});
        """.stripMargin
      }
    } else {
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
