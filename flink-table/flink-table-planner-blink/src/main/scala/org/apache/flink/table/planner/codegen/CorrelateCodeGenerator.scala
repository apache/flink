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
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow, JoinedRow}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils.getEvalMethodSignature
import org.apache.flink.table.planner.functions.utils.{TableSqlFunction, UserDefinedFunctionUtils}
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.planner.plan.schema.FlinkTableFunction
import org.apache.flink.table.runtime.collector.TableFunctionCollector
import org.apache.flink.table.runtime.generated.GeneratedCollector
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.types.PlannerTypeUtils
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.runtime.util.StreamRecordCollector
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LogicalType, RowType}

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex._

import scala.collection.JavaConversions._

object CorrelateCodeGenerator {

  private[flink] def generateCorrelateTransformation(
      config: TableConfig,
      operatorCtx: CodeGeneratorContext,
      inputTransformation: Transformation[BaseRow],
      inputRelType: RelDataType,
      projectProgram: Option[RexProgram],
      scan: FlinkLogicalTableFunctionScan,
      condition: Option[RexNode],
      outDataType: RelDataType,
      joinType: JoinRelType,
      parallelism: Int,
      retainHeader: Boolean,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String,
      opName: String,
      transformationName: String): Transformation[BaseRow] = {
    val funcRel = scan.asInstanceOf[FlinkLogicalTableFunctionScan]
    val rexCall = funcRel.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    // we need result Type to do code generation
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
    val udtfExternalType = sqlFunction
        .getFunction
        .asInstanceOf[FlinkTableFunction]
        .getExternalResultType(func, arguments, argTypes)
    val pojoFieldMapping = Some(UserDefinedFunctionUtils.getFieldInfo(udtfExternalType)._2)
    val inputType = FlinkTypeFactory.toLogicalRowType(inputRelType)
    val (returnType, swallowInputOnly ) = if (projectProgram.isDefined) {
      val program = projectProgram.get
      val selects = program.getProjectList.map(_.getIndex)
      val inputFieldCnt = program.getInputRowType.getFieldCount
      val swallowInputOnly = selects.head > inputFieldCnt &&
        (inputFieldCnt - outDataType.getFieldCount == inputRelType.getFieldCount)
      // partial output or output right only
      (FlinkTypeFactory.toLogicalRowType(outDataType), swallowInputOnly)
    } else {
      // completely output left input + right
      (FlinkTypeFactory.toLogicalRowType(outDataType), false)
    }
    // adjust indicies of InputRefs to adhere to schema expected by generator
    val changeInputRefIndexShuttle = new RexShuttle {
      override def visitInputRef(inputRef: RexInputRef): RexNode = {
        new RexInputRef(inputRelType.getFieldCount + inputRef.getIndex, inputRef.getType)
      }
    }

    val collectorCtx = CodeGeneratorContext(config)
    val collector = generateCollector(
      collectorCtx,
      config,
      inputType,
      projectProgram,
      swallowInputOnly,
      udtfExternalType,
      returnType,
      condition.map(_.accept(changeInputRefIndexShuttle)),
      pojoFieldMapping,
      retainHeader)

    val substituteStreamOperator = generateOperator(
      operatorCtx,
      collectorCtx,
      config,
      inputType,
      projectProgram,
      swallowInputOnly,
      fromDataTypeToLogicalType(udtfExternalType),
      returnType,
      joinType,
      rexCall,
      pojoFieldMapping,
      opName,
      classOf[ProcessFunction[BaseRow, BaseRow]],
      collector,
      retainHeader)

    ExecNode.createOneInputTransformation(
      inputTransformation,
      transformationName,
      substituteStreamOperator,
      BaseRowTypeInfo.of(returnType),
      parallelism)
  }

  /**
    * Generates the flat map operator to run the user-defined table function.
    */
  private[flink] def generateOperator[T <: Function](
      ctx: CodeGeneratorContext,
      collectorCtx: CodeGeneratorContext,
      config: TableConfig,
      inputType: RowType,
      projectProgram: Option[RexProgram],
      swallowInputOnly: Boolean = false,
      udtfType: LogicalType,
      returnType: RowType,
      joinType: JoinRelType,
      rexCall: RexCall,
      pojoFieldMapping: Option[Array[Int]],
      ruleDescription: String,
      functionClass: Class[T],
      udtfCollector: GeneratedCollector[TableFunctionCollector[_]],
      retainHeader: Boolean = true): CodeGenOperatorFactory[BaseRow] = {
    ctx.references ++= collectorCtx.references
    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(inputType)
      .bindSecondInput(udtfType, inputFieldMapping = pojoFieldMapping)

    // 1.compile and init udtf collector
    val udtfCollectorTerm = newName("udtfCollectorTerm")
    ctx.addReusableMember(s"private ${udtfCollector.getClassName} $udtfCollectorTerm = null;")
    ctx.addReusableInnerClass(udtfCollector.getClassName, udtfCollector.getCode)

    val call = exprGenerator.generateExpression(rexCall)
    val openUDTFCollector =
      s"""
         |$udtfCollectorTerm = new ${udtfCollector.getClassName}();
         |$udtfCollectorTerm.setRuntimeContext(getRuntimeContext());
         |$udtfCollectorTerm.open(new ${className[Configuration]}());
         |$udtfCollectorTerm.setCollector(
         | new ${classOf[StreamRecordCollector[_]].getCanonicalName}(
         |     ${CodeGenUtils.DEFAULT_OPERATOR_COLLECTOR_TERM }));
         |${call.resultTerm}.setCollector($udtfCollectorTerm);
         |""".stripMargin
    ctx.addReusableOpenStatement(openUDTFCollector)

    // 2. call udtf
    var body =
      s"""
         |$udtfCollectorTerm.setInput(${exprGenerator.input1Term});
         |$udtfCollectorTerm.reset();
         |${call.code}
         |""".stripMargin

    // 3. left join
    if (joinType == JoinRelType.LEFT) {
      if (swallowInputOnly) {
        // and the returned row table function is empty, collect a null
        val nullRowTerm = CodeGenUtils.newName("nullRow")
        ctx.addReusableOutputRecord(
          PlannerTypeUtils.toRowType(udtfType), classOf[GenericRow], nullRowTerm)
        ctx.addReusableNullRow(nullRowTerm, PlannerTypeUtils.getArity(udtfType))
        val header = if (retainHeader) {
          s"$nullRowTerm.setHeader(${exprGenerator.input1Term}.getHeader());"
        } else {
          ""
        }
        body +=
          s"""
             |boolean hasOutput = $udtfCollectorTerm.isCollected();
             |if (!hasOutput) {
             |  $header
             |  $udtfCollectorTerm.outputResult($nullRowTerm);
             |}
             |""".stripMargin
      } else if (projectProgram.isDefined) {
        // output partial fields of left and right
        val outputTerm = CodeGenUtils.newName("projectOut")
        ctx.addReusableOutputRecord(returnType, classOf[GenericRow], outputTerm)

        val header = if (retainHeader) {
          s"$outputTerm.setHeader(${CodeGenUtils.DEFAULT_INPUT1_TERM}.getHeader());"
        } else {
          ""
        }
        val projectionExpression = generateProjectResultExpr(
          ctx,
          config,
          inputType,
          udtfType,
          pojoFieldMapping,
          udtfAlwaysNull = true,
          returnType,
          outputTerm,
          projectProgram.get)

        body +=
          s"""
             |boolean hasOutput = $udtfCollectorTerm.isCollected();
             |if (!hasOutput) {
             |  ${projectionExpression.code}
             |  $header
             |  $udtfCollectorTerm.outputResult($outputTerm);
             |}
             |""".stripMargin

      } else {
        // output all fields of left and right
        // in case of left outer join and the returned row of table function is empty,
        // fill all fields of row with null
        val joinedRowTerm = CodeGenUtils.newName("joinedRow")
        val nullRowTerm = CodeGenUtils.newName("nullRow")
        ctx.addReusableOutputRecord(returnType, classOf[JoinedRow], joinedRowTerm)
        ctx.addReusableNullRow(nullRowTerm, PlannerTypeUtils.getArity(udtfType))
        val header = if (retainHeader) {
          s"$joinedRowTerm.setHeader(${exprGenerator.input1Term}.getHeader());"
        } else {
          ""
        }
        body +=
          s"""
             |boolean hasOutput = $udtfCollectorTerm.isCollected();
             |if (!hasOutput) {
             |  $joinedRowTerm.replace(${exprGenerator.input1Term}, $nullRowTerm);
             |  $header
             |  $udtfCollectorTerm.outputResult($joinedRowTerm);
             |}
             |""".stripMargin

        }
    } else if (joinType != JoinRelType.INNER) {
      throw new TableException(s"Unsupported JoinRelType: $joinType for correlate join.")
    }

    val genOperator = OperatorCodeGenerator.generateOneInputStreamOperator[BaseRow, BaseRow](
      ctx, ruleDescription, body, inputType)
    new CodeGenOperatorFactory(genOperator)
  }

  private def generateProjectResultExpr(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      input1Type: RowType,
      udtfType: LogicalType,
      udtfPojoFieldMapping: Option[Array[Int]],
      udtfAlwaysNull: Boolean,
      returnType: RowType,
      outputTerm: String,
      program: RexProgram): GeneratedExpression = {
    val projectExprGenerator = new ExprCodeGenerator(ctx, udtfAlwaysNull)
      .bindInput(input1Type, CodeGenUtils.DEFAULT_INPUT1_TERM)
    if (udtfAlwaysNull) {
      val udtfNullRow = CodeGenUtils.newName("udtfNullRow")
      ctx.addReusableNullRow(udtfNullRow, PlannerTypeUtils.getArity(udtfType))

      projectExprGenerator.bindSecondInput(
        PlannerTypeUtils.toRowType(udtfType),
        udtfNullRow,
        inputFieldMapping = udtfPojoFieldMapping)
    } else {
      projectExprGenerator.bindSecondInput(
        udtfType,
        inputFieldMapping = udtfPojoFieldMapping)
    }
    val projection = program.getProjectList.map(program.expandLocalRef)
    val projectionExprs = projection.map(projectExprGenerator.generateExpression)
    projectExprGenerator.generateResultExpression(
      projectionExprs, returnType, classOf[GenericRow], outputTerm)
  }

  /**
    * Generates table function collector.
    */
  private[flink] def generateCollector(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      inputType: RowType,
      projectProgram: Option[RexProgram],
      swallowInputOnly: Boolean,
      udtfExternalType: DataType,
      resultType: RowType,
      condition: Option[RexNode],
      pojoFieldMapping: Option[Array[Int]],
      retainHeader: Boolean = true): GeneratedCollector[TableFunctionCollector[_]] = {
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val udtfInputTerm = CodeGenUtils.DEFAULT_INPUT2_TERM

    val udtfType = fromDataTypeToLogicalType(udtfExternalType)
    val exprGenerator = new ExprCodeGenerator(ctx, false).bindInput(
      udtfType, inputTerm = udtfInputTerm, inputFieldMapping = pojoFieldMapping)

    val udtfBaseRowType = PlannerTypeUtils.toRowType(udtfType)
    val udtfResultExpr = exprGenerator.generateConverterResultExpression(
      udtfBaseRowType, classOf[GenericRow])

    val body = if (projectProgram.isDefined) {
      // partial output
      if (swallowInputOnly) {
        // output right only
        val header = if (retainHeader) {
          s"${udtfResultExpr.resultTerm}.setHeader($inputTerm.getHeader());"
        } else {
          ""
        }
        s"""
           |${udtfResultExpr.code}
           |$header
           |outputResult(${udtfResultExpr.resultTerm});
        """.stripMargin
      } else {
        val outputTerm = CodeGenUtils.newName("projectOut")
        ctx.addReusableOutputRecord(resultType, classOf[GenericRow], outputTerm)

        val header = if (retainHeader) {
          s"$outputTerm.setHeader($inputTerm.getHeader());"
        } else {
          ""
        }
        val projectionExpression = generateProjectResultExpr(
          ctx,
          config,
          inputType,
          udtfType,
          pojoFieldMapping,
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
      ctx.addReusableOutputRecord(resultType, classOf[JoinedRow], joinedRowTerm)

      val header = if (retainHeader) {
        s"$joinedRowTerm.setHeader($inputTerm.getHeader());"
      } else {
        ""
      }
      s"""
        |${udtfResultExpr.code}
        |$joinedRowTerm.replace($inputTerm, ${udtfResultExpr.resultTerm});
        |$header
        |outputResult($joinedRowTerm);
      """.stripMargin
    }

    val collectorCode = if (condition.isEmpty) {
      body
    } else {

      val filterGenerator = new ExprCodeGenerator(ctx, false)
        .bindInput(inputType, inputTerm)
        .bindSecondInput(udtfType, udtfInputTerm, pojoFieldMapping)
      val filterCondition = filterGenerator.generateExpression(condition.get)

      s"""
         |${filterCondition.code}
         |if (${filterCondition.resultTerm}) {
         |  $body
         |}
         |""".stripMargin
    }

    CollectorCodeGenerator.generateTableFunctionCollector(
      ctx,
      "TableFunctionCollector",
      collectorCode,
      inputType,
      udtfType,
      inputTerm = inputTerm,
      collectedTerm = udtfInputTerm,
      converter = CodeGenUtils.genToInternal(ctx, udtfExternalType))
  }

}
