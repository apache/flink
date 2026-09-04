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

import org.apache.flink.api.common.functions.{FlatMapFunction, Function}
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.data.{BoxedWrapperRowData, RowData}
import org.apache.flink.table.functions.FunctionKind
import org.apache.flink.table.planner.calcite.{FlinkRexBuilder, FlinkTypeFactory}
import org.apache.flink.table.planner.codegen.CodeGenUtils.className
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.RowKind

import org.apache.calcite.rex._

import scala.collection.JavaConverters._

object CalcCodeGenerator {

  def generateCalcOperator(
      ctx: CodeGeneratorContext,
      inputType: RowType,
      outputType: RowType,
      projection: Seq[RexNode],
      condition: Option[RexNode],
      partialDeleteKeys: Array[Int],
      typeFactory: FlinkTypeFactory,
      retainHeader: Boolean = false,
      opName: String): CodeGenOperatorFactory[RowData] = {
    // filter out time attributes
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val processCode = generateProcessCode(
      ctx,
      inputType,
      outputType,
      classOf[BoxedWrapperRowData],
      projection,
      condition,
      Option(partialDeleteKeys),
      typeFactory,
      inputTerm,
      CodeGenUtils.DEFAULT_OPERATOR_COLLECTOR_TERM,
      eagerInputUnboxingCode = true,
      retainHeader = retainHeader,
      outputDirectly = false
    )

    val genOperator =
      OperatorCodeGenerator.generateOneInputStreamOperator[RowData, RowData](
        ctx,
        opName,
        processCode,
        inputType,
        inputTerm = inputTerm,
        lazyInputUnboxingCode = true)

    new CodeGenOperatorFactory(genOperator)
  }

  private[flink] def generateFunction[T <: Function](
      inputType: RowType,
      name: String,
      returnType: RowType,
      outRowClass: Class[_ <: RowData],
      calcProjection: Seq[RexNode],
      calcCondition: Option[RexNode],
      tableConfig: ReadableConfig,
      classLoader: ClassLoader,
      typeFactory: FlinkTypeFactory): GeneratedFunction[FlatMapFunction[RowData, RowData]] = {
    val ctx = new CodeGeneratorContext(tableConfig, classLoader)
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val collectorTerm = CodeGenUtils.DEFAULT_COLLECTOR_TERM
    val processCode = generateProcessCode(
      ctx,
      inputType,
      returnType,
      outRowClass,
      calcProjection,
      calcCondition,
      None,
      typeFactory,
      inputTerm,
      collectorTerm = collectorTerm,
      eagerInputUnboxingCode = false,
      outputDirectly = true
    )

    FunctionCodeGenerator.generateFunction(
      ctx,
      name,
      classOf[FlatMapFunction[RowData, RowData]],
      processCode,
      returnType,
      inputType,
      input1Term = inputTerm,
      collectorTerm = collectorTerm)
  }

  private[flink] def generateProcessCode(
      ctx: CodeGeneratorContext,
      inputType: RowType,
      outRowType: RowType,
      outRowClass: Class[_ <: RowData],
      projection: Seq[RexNode],
      condition: Option[RexNode],
      partialDeleteKeys: Option[Array[Int]],
      typeFactory: FlinkTypeFactory,
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      collectorTerm: String = CodeGenUtils.DEFAULT_OPERATOR_COLLECTOR_TERM,
      eagerInputUnboxingCode: Boolean,
      retainHeader: Boolean = false,
      outputDirectly: Boolean = false): String = {

    // according to the SQL standard, every table function should also be a scalar function
    // but we don't allow that for now
    projection.foreach(_.accept(ScalarFunctionsValidator))
    condition.foreach(_.accept(ScalarFunctionsValidator))

    val rexProgram = buildRexProgram(typeFactory, inputType, projection, condition)

    val exprGenerator = new ExprCodeGenerator(ctx, false, rexProgram)
      .bindInput(inputType, inputTerm = inputTerm)

    val onlyFilter = projection.lengthCompare(inputType.getFieldCount) == 0 &&
      projection.zipWithIndex.forall {
        case (rexNode, index) =>
          rexNode.isInstanceOf[RexInputRef] && rexNode.asInstanceOf[RexInputRef].getIndex == index
      }

    def produceOutputCode(resultTerm: String): String = if (outputDirectly) {
      s"$collectorTerm.collect($resultTerm);"
    } else {
      s"${OperatorCodeGenerator.generateCollect(resultTerm)}"
    }

    def produceFullProjectionCode: String = {
      val fullProjectList = rexProgram.getProjectList.asScala

      val expressions = fullProjectList.map(exprGenerator.generateExpression)
      val resultExpression =
        exprGenerator.generateResultExpression(expressions, outRowType, outRowClass)

      val projectionExpressionCode = resultExpression.code

      val header = if (retainHeader) {
        s"${resultExpression.resultTerm}.setRowKind($inputTerm.getRowKind());"
      } else {
        ""
      }

      s"""
         |$header
         |$projectionExpressionCode
         |${produceOutputCode(resultExpression.resultTerm)}
         |""".stripMargin
    }

    def produceProjectionCode: String = partialDeleteKeys match {
      case Some(keys) =>
        // In case of partial deletes, the calc must only forward the key columns, non-key
        // expressions must not be evaluated.
        //
        // Any RexLocalRef sub-expression evaluated while generating either branch (e.g. the
        // BinaryRowWriter code backing a ROW(...) constructor) would otherwise be hoisted, by
        // default, to the bottom (unconditional) local-ref cache scope and run for *every* row
        // regardless of which branch is actually taken. Each branch is therefore generated
        // inside its own pushed local-ref scope (mirroring
        // ExprCodeGenerator.visitOperandInScopedCache, used for CASE/AND/OR short-circuiting) so
        // that such code is folded into that branch only and never runs unconditionally.
        ctx.pushLocalRefScope()
        val keyProjection = buildKeyProjections(exprGenerator, rexProgram, outRowType, keys)
        val resultExpression =
          exprGenerator.generateResultExpression(keyProjection, outRowType, outRowClass)
        val keyScopedCode = ctx.popLocalRefScope().values.map(_.code).mkString("\n")

        val keyHeader = if (retainHeader) {
          s"${resultExpression.resultTerm}.setRowKind($inputTerm.getRowKind());"
        } else {
          ""
        }

        ctx.pushLocalRefScope()
        val fullProjectionCode = produceFullProjectionCode
        val fullScopedCode = ctx.popLocalRefScope().values.map(_.code).mkString("\n")

        s"""
           |if ($inputTerm.getRowKind() == ${className[RowKind]}.DELETE) {
           |  $keyScopedCode
           |  ${resultExpression.code}
           |  $keyHeader
           |  ${produceOutputCode(resultExpression.resultTerm)}
           |} else {
           |  $fullScopedCode
           |  $fullProjectionCode
           |}
           |""".stripMargin
      case None =>
        produceFullProjectionCode
    }

    if (condition.isEmpty && onlyFilter) {
      throw new TableException(
        "This calc has no useful projection and no filter. " +
          "It should be removed by CalcRemoveRule.")
    } else if (condition.isEmpty) { // only projection
      val projectionCode = produceProjectionCode
      val localRefCode = ctx.reuseLocalRefCode()
      s"""
         |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
         |$localRefCode
         |$projectionCode
         |""".stripMargin
    } else {
      val filterCondition = exprGenerator.generateExpression(rexProgram.getCondition)
      // only filter
      if (onlyFilter) {
        val localRefCode = ctx.reuseLocalRefCode()
        s"""
           |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
           |$localRefCode
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           |  ${produceOutputCode(inputTerm)}
           |}
           |""".stripMargin
      } else { // both filter and projection
        val filterInputCode = ctx.reuseInputUnboxingCode()
        val filterInputSet = Set(ctx.reusableInputUnboxingExprs.keySet.toSeq: _*)

        val filterLocalRefSet: Set[Int] = ctx.getReusableLocalRefExprBottomScope.keySet.toSet

        // if any filter conditions, projection code will enter an new scope
        val projectionCode = produceProjectionCode

        val projectionInputCode = ctx.reusableInputUnboxingExprs
          .filter { case (k, _) => !filterInputSet.contains(k) }
          .values
          .map(_.code)
          .mkString("\n")

        val filterLocalRefCode = ctx.getReusableLocalRefExprBottomScope
          .filter { case (k, _) => filterLocalRefSet.contains(k) }
          .values
          .map(_.code)
          .mkString("\n")
        val projectionLocalRefCode = ctx.getReusableLocalRefExprBottomScope
          .filter { case (k, _) => !filterLocalRefSet.contains(k) }
          .values
          .map(_.code)
          .mkString("\n")

        s"""
           |${if (eagerInputUnboxingCode) filterInputCode else ""}
           |$filterLocalRefCode
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           | ${if (eagerInputUnboxingCode) projectionInputCode else ""}
           |  $projectionLocalRefCode
           |  $projectionCode
           |}
           |""".stripMargin
      }
    }
  }

  /**
   * Builds the key-only projection's per-output-column expressions for a Calc forwarding partial
   * delete changes. Output columns listed in `partialDeleteKeys` are evaluated from the regular
   * projection; every other column is generated directly as a typed `NULL` so that its (potentially
   * unsafe) expression is never evaluated on a delete-by-key tombstone, whose non-key columns may
   * not be present.
   */
  private def buildKeyProjections(
      exprGenerator: ExprCodeGenerator,
      rexProgram: RexProgram,
      outRowType: RowType,
      partialDeleteKeys: Array[Int]): Seq[GeneratedExpression] = {
    val keyIndices = partialDeleteKeys.toSet
    rexProgram.getProjectList.asScala.zipWithIndex.map {
      case (projectRef, idx) =>
        if (keyIndices.contains(idx)) {
          exprGenerator.generateExpression(projectRef)
        } else {
          GenerateUtils.generateNullLiteral(outRowType.getTypeAt(idx).copy(true))
        }
    }
  }

  private def buildRexProgram(
      typeFactory: FlinkTypeFactory,
      inputType: RowType,
      projection: Seq[RexNode],
      condition: Option[RexNode]
  ): RexProgram = {
    val rexBuilder = new FlinkRexBuilder(typeFactory)
    val relInputType = typeFactory.createFieldTypeFromLogicalType(inputType)
    val builder = new RexProgramBuilder(relInputType, rexBuilder)
    projection.foreach(p => builder.addProject(p, null))
    if (condition.isDefined) {
      builder.addCondition(condition.get)
    }
    builder.getProgram
  }

  private object ScalarFunctionsValidator extends RexVisitorImpl[Unit](true) {
    override def visitCall(call: RexCall): Unit = {
      super.visitCall(call)
      call.getOperator match {
        case bsf: BridgingSqlFunction if bsf.getDefinition.getKind != FunctionKind.SCALAR =>
          throw new ValidationException(
            s"Invalid use of function '$bsf'. " +
              s"Currently, only scalar functions can be used in a projection or filter operation.")
        case _ => // ok
      }
    }
  }
}
