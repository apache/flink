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
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.data.{BoxedWrapperRowData, RowData}
import org.apache.flink.table.functions.FunctionKind
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rex._
import org.apache.calcite.rex.RexUtil

import scala.collection.JavaConverters._
import scala.collection.mutable

object CalcCodeGenerator {

  def generateCalcOperator(
      ctx: CodeGeneratorContext,
      inputTransform: Transformation[RowData],
      outputType: RowType,
      projection: Seq[RexNode],
      condition: Option[RexNode],
      retainHeader: Boolean = false,
      opName: String): CodeGenOperatorFactory[RowData] = {
    val inputType = inputTransform.getOutputType
      .asInstanceOf[InternalTypeInfo[RowData]]
      .toRowType
    // filter out time attributes
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val processCode = generateProcessCode(
      ctx,
      inputType,
      outputType,
      classOf[BoxedWrapperRowData],
      projection,
      condition,
      eagerInputUnboxingCode = true,
      retainHeader = retainHeader)

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
      classLoader: ClassLoader): GeneratedFunction[FlatMapFunction[RowData, RowData]] = {
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
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      collectorTerm: String = CodeGenUtils.DEFAULT_OPERATOR_COLLECTOR_TERM,
      eagerInputUnboxingCode: Boolean,
      retainHeader: Boolean = false,
      outputDirectly: Boolean = false): String = {

    // according to the SQL standard, every table function should also be a scalar function
    // but we don't allow that for now
    projection.foreach(_.accept(ScalarFunctionsValidator))
    condition.foreach(_.accept(ScalarFunctionsValidator))

    val exprGenerator = new ExprCodeGenerator(ctx, false)
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

    def produceProjectionCode: String = {
      val projectionExprs = projection.map(exprGenerator.generateExpression)
      val projectionExpression =
        exprGenerator.generateResultExpression(projectionExprs, outRowType, outRowClass)

      val projectionExpressionCode = projectionExpression.code

      val header = if (retainHeader) {
        s"${projectionExpression.resultTerm}.setRowKind($inputTerm.getRowKind());"
      } else {
        ""
      }

      s"""
         |$header
         |$projectionExpressionCode
         |${produceOutputCode(projectionExpression.resultTerm)}
         |""".stripMargin
    }

    if (condition.isEmpty && onlyFilter) {
      throw new TableException(
        "This calc has no useful projection and no filter. " +
          "It should be removed by CalcRemoveRule.")
    } else if (condition.isEmpty) { // only projection
      val projectionCode = produceProjectionCode
      s"""
         |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
         |$projectionCode
         |""".stripMargin
    } else if (onlyFilter) {
      // only filter - no projection
      val filterCondition = exprGenerator.generateExpression(condition.get)
      s"""
         |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
         |${filterCondition.code}
         |if (${filterCondition.resultTerm}) {
         |  ${produceOutputCode(inputTerm)}
         |}
         |""".stripMargin
    } else {
      // both filter and projection
      // Find and pre-generate non-deterministic subexpressions that appear
      // in both filter and projection to avoid duplicate evaluation.
      // This MUST be called BEFORE generating filterCondition to populate the cache.
      val commonNonDetExprsCode =
        preGenerateCommonNonDeterministicExprs(ctx, exprGenerator, condition.get, projection)

      // Now generate the filter condition - it will use cached non-det expressions
      val filterCondition = exprGenerator.generateExpression(condition.get)

      val filterInputCode = ctx.reuseInputUnboxingCode()
      val filterInputSet = Set(ctx.reusableInputUnboxingExprs.keySet.toSeq: _*)

      // if any filter conditions, projection code will enter an new scope
      val projectionCode = produceProjectionCode

      val projectionInputCode = ctx.reusableInputUnboxingExprs
        .filter(entry => !filterInputSet.contains(entry._1))
        .values
        .map(_.code)
        .mkString("\n")
      s"""
         |${if (eagerInputUnboxingCode) filterInputCode else ""}
         |$commonNonDetExprsCode
         |${filterCondition.code}
         |if (${filterCondition.resultTerm}) {
         |  ${if (eagerInputUnboxingCode) projectionInputCode else ""}
         |  $projectionCode
         |}
         |""".stripMargin
    }
  }

  /**
   * Pre-generates non-deterministic subexpressions that appear in both the filter condition and
   * projection. This ensures they are evaluated only once per row.
   *
   * @param ctx
   *   The code generator context
   * @param exprGenerator
   *   The expression code generator
   * @param condition
   *   The filter condition
   * @param projection
   *   The projection expressions
   * @return
   *   Generated code for the common non-deterministic expressions
   */
  private def preGenerateCommonNonDeterministicExprs(
      ctx: CodeGeneratorContext,
      exprGenerator: ExprCodeGenerator,
      condition: RexNode,
      projection: Seq[RexNode]): String = {

    // Collect all non-deterministic RexCall subexpressions from the condition
    val conditionNonDetExprs = collectNonDeterministicCalls(condition)

    if (conditionNonDetExprs.isEmpty) {
      return ""
    }

    // Collect all non-deterministic RexCall subexpressions from all projection expressions
    val projectionNonDetExprs = projection.flatMap(collectNonDeterministicCalls).toSet

    // Find common non-deterministic expressions (by their string representation)
    val commonExprs = conditionNonDetExprs.filter {
      expr => projectionNonDetExprs.exists(_.toString == expr.toString)
    }

    if (commonExprs.isEmpty) {
      return ""
    }

    // Pre-generate each common non-deterministic expression and cache it
    val codeBuilder = new StringBuilder
    for (expr <- commonExprs) {
      val exprKey = expr.toString
      if (ctx.getReusableSubExpr(exprKey).isEmpty) {
        // Generate the expression
        val generated = exprGenerator.generateExpression(expr)
        // Cache it for reuse
        ctx.addReusableSubExpr(exprKey, generated)
        // Add the code to evaluate the expression
        codeBuilder.append(generated.code).append("\n")
      }
    }

    codeBuilder.toString()
  }

  /** Collects all non-deterministic RexCall subexpressions from a RexNode tree. */
  private def collectNonDeterministicCalls(node: RexNode): Set[RexCall] = {
    val result = mutable.Set[RexCall]()

    node.accept(new RexVisitorImpl[Unit](true) {
      override def visitCall(call: RexCall): Unit = {
        if (!RexUtil.isDeterministic(call)) {
          result += call
        }
        // Continue visiting children
        super.visitCall(call)
      }
    })

    result.toSet
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
