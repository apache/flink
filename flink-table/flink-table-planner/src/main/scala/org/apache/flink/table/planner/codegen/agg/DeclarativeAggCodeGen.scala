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
package org.apache.flink.table.planner.codegen.agg

import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.ApiExpressionUtils.localRef
import org.apache.flink.table.functions.DeclarativeAggregateFunction
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator.DISTINCT_KEY_TERM
import org.apache.flink.table.planner.expressions.{DeclarativeExpressionResolver, RexNodeExpression}
import org.apache.flink.table.planner.expressions.DeclarativeExpressionResolver.{toRexDistinctKey, toRexInputRef}
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter
import org.apache.flink.table.planner.functions.aggfunctions.SizeBasedWindowFunction
import org.apache.flink.table.planner.plan.utils.AggregateInfo
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.{fromDataTypeToLogicalType, fromLogicalTypeToDataType}
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.tools.RelBuilder

/**
 * It is for code generate aggregation functions that are specified using expressions. The aggregate
 * buffer is embedded inside of a larger shared aggregation buffer.
 *
 * @param ctx
 *   the code gen context
 * @param aggInfo
 *   the aggregate information
 * @param filterExpression
 *   filter argument access expression, none if no filter
 * @param mergedAccOffset
 *   the mergedAcc may come from local aggregate, this is the first buffer offset in the row
 * @param aggBufferOffset
 *   the offset in the buffers of this aggregate
 * @param aggBufferSize
 *   the total size of aggregate buffers
 * @param inputTypes
 *   the input field type infos
 * @param constants
 *   the constant literals
 * @param relBuilder
 *   the rel builder to translate expressions to calcite rex nodes
 */
class DeclarativeAggCodeGen(
    ctx: CodeGeneratorContext,
    aggInfo: AggregateInfo,
    filterExpression: Option[Expression],
    mergedAccOffset: Int,
    aggBufferOffset: Int,
    aggBufferSize: Int,
    inputTypes: Seq[LogicalType],
    constants: Seq[RexLiteral],
    relBuilder: RelBuilder)
  extends AggCodeGen {

  private val function = aggInfo.function.asInstanceOf[DeclarativeAggregateFunction]

  private val bufferTypes = aggInfo.externalAccTypes.map(fromDataTypeToLogicalType)
  private val bufferIndexes = Array.range(aggBufferOffset, aggBufferOffset + bufferTypes.length)
  private val bufferTerms = function.aggBufferAttributes
    .map(a => s"agg${aggInfo.aggIndex}_${a.getName}")

  private val rexNodeGen = new ExpressionConverter(relBuilder)

  private val windowSizeTerm = function match {
    case f: SizeBasedWindowFunction =>
      val exprCodegen = new ExprCodeGenerator(ctx, false)
      exprCodegen.generateExpression(f.windowSizeAttribute().accept(rexNodeGen))
      f.windowSizeAttribute().getName
    case _ => null
  }

  private val bufferNullTerms = {
    val exprCodegen = new ExprCodeGenerator(ctx, false)
    bufferTerms
      .zip(bufferTypes)
      .map { case (name, t) => localRef(name, fromLogicalTypeToDataType(t)) }
      .map(_.accept(rexNodeGen))
      .map(exprCodegen.generateExpression)
      .map(_.nullTerm)
  }

  private val argIndexes = aggInfo.argIndexes
  private val argTypes = {
    val types = inputTypes ++ constants.map(t => FlinkTypeFactory.toLogicalType(t.getType))
    argIndexes.map(types(_))
  }

  def createAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    function.initialValuesExpressions
      .map(expr => generator.generateExpression(expr.accept(rexNodeGen)))
  }

  def setAccumulator(generator: ExprCodeGenerator): String = {
    val aggBufferAccesses = function.aggBufferAttributes.zipWithIndex
      .map {
        case (attr, index) =>
          toRexInputRef(relBuilder, bufferIndexes(index), bufferTypes(index))
      }
      .map(expr => generator.generateExpression(expr.accept(rexNodeGen)))

    val setters = aggBufferAccesses.zipWithIndex.map {
      case (access, index) =>
        s"""
           |${access.copyResultTermToTargetIfChanged(ctx, bufferTerms(index))};
           |${bufferNullTerms(index)} = ${access.nullTerm};
         """.stripMargin
    }

    setters.mkString("\n")
  }

  override def resetAccumulator(generator: ExprCodeGenerator): String = {
    val initialExprs = function.initialValuesExpressions
      .map(expr => generator.generateExpression(expr.accept(rexNodeGen)))
    val codes = initialExprs.zipWithIndex.map {
      case (init, index) =>
        val memberName = bufferTerms(index)
        val memberNullTerm = bufferNullTerms(index)
        s"""
           |${init.code}
           |$memberName = ${init.resultTerm};
           |$memberNullTerm = ${init.nullTerm};
         """.stripMargin
    }
    codes.mkString("\n")
  }

  def getAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    bufferTypes.zipWithIndex.map {
      case (bufferType, index) =>
        GeneratedExpression(bufferTerms(index), bufferNullTerms(index), "", bufferType)
    }
  }

  def accumulate(generator: ExprCodeGenerator): String = {
    val isDistinctMerge = generator.input1Term.startsWith(DISTINCT_KEY_TERM)
    val resolvedExprs = function.accumulateExpressions
      .map(_.accept(ResolveReference(isDistinctMerge = isDistinctMerge)))

    val exprs = resolvedExprs
      .map(_.accept(rexNodeGen)) // rex nodes
      .map(generator.generateExpression) // generated expressions

    val codes = exprs.zipWithIndex.map {
      case (expr, index) =>
        s"""
           |${expr.code}
           |${expr.copyResultTermToTargetIfChanged(ctx, bufferTerms(index))};
           |${bufferNullTerms(index)} = ${expr.nullTerm};
       """.stripMargin
    }

    filterExpression match {
      case Some(expr) =>
        val generated = generator.generateExpression(expr.accept(rexNodeGen))
        s"""
           |if (${generated.resultTerm}) {
           |  ${codes.mkString("\n")}
           |}
         """.stripMargin
      case None =>
        codes.mkString("\n")
    }
  }

  def retract(generator: ExprCodeGenerator): String = {
    val isDistinctMerge = generator.input1Term.startsWith(DISTINCT_KEY_TERM)
    val resolvedExprs = function.retractExpressions
      .map(_.accept(ResolveReference(isDistinctMerge = isDistinctMerge)))

    val exprs = resolvedExprs
      .map(_.accept(rexNodeGen)) // rex nodes
      .map(generator.generateExpression) // generated expressions

    val codes = exprs.zipWithIndex.map {
      case (expr, index) =>
        s"""
           |${expr.code}
           |${expr.copyResultTermToTargetIfChanged(ctx, bufferTerms(index))};
           |${bufferNullTerms(index)} = ${expr.nullTerm};
       """.stripMargin
    }

    filterExpression match {
      case Some(expr) =>
        val generated = generator.generateExpression(expr.accept(rexNodeGen))
        s"""
           |if (${generated.resultTerm}) {
           |  ${codes.mkString("\n")}
           |}
         """.stripMargin
      case None =>
        codes.mkString("\n")
    }
  }

  def merge(generator: ExprCodeGenerator): String = {
    val exprs = function.mergeExpressions
      .map(_.accept(ResolveReference(isMerge = true)))
      .map(_.accept(rexNodeGen)) // rex nodes
      .map(generator.generateExpression) // generated expressions

    val codes = exprs.zipWithIndex.map {
      case (expr, index) =>
        s"""
           |${expr.code}
           |${expr.copyResultTermToTargetIfChanged(ctx, bufferTerms(index))};
           |${bufferNullTerms(index)} = ${expr.nullTerm};
       """.stripMargin
    }

    codes.mkString("\n")
  }

  def getValue(generator: ExprCodeGenerator): GeneratedExpression = {
    val resolvedGetValueExpression = function.getValueExpression
      .accept(ResolveReference())
    generator.generateExpression(resolvedGetValueExpression.accept(rexNodeGen))
  }

  /**
   * Resolves the given expression to a resolved Expression.
   *
   * @param isMerge
   *   this is called from merge() method
   */
  private case class ResolveReference(isMerge: Boolean = false, isDistinctMerge: Boolean = false)
    extends DeclarativeExpressionResolver(relBuilder, function, isMerge) {

    override def toMergeInputExpr(name: String, localIndex: Int): ResolvedExpression = {
      // in merge case, the input1 is mergedAcc
      toRexInputRef(
        relBuilder,
        mergedAccOffset + bufferIndexes(localIndex),
        bufferTypes(localIndex))
    }

    override def toAccInputExpr(name: String, localIndex: Int): ResolvedExpression = {
      val inputIndex = argIndexes(localIndex)
      if (inputIndex >= inputTypes.length) { // it is a constant
        val constantIndex = inputIndex - inputTypes.length
        val constant = constants(constantIndex)
        new RexNodeExpression(
          constant,
          fromLogicalTypeToDataType(FlinkTypeFactory.toLogicalType(constant.getType)),
          null,
          null)
      } else { // it is a input field
        if (isDistinctMerge) { // this is called from distinct merge
          if (function.operandCount == 1) {
            // the distinct key is a BoxedValue
            val t = argTypes(localIndex)
            toRexDistinctKey(relBuilder, name, t)
          } else {
            // the distinct key is a RowData
            toRexInputRef(relBuilder, localIndex, argTypes(localIndex))
          }
        } else {
          // the input is the inputRow
          toRexInputRef(relBuilder, argIndexes(localIndex), argTypes(localIndex))
        }
      }
    }

    override def toAggBufferExpr(name: String, localIndex: Int): ResolvedExpression = {
      // name => agg${aggInfo.aggIndex}_$name"
      localRef(bufferTerms(localIndex), fromLogicalTypeToDataType(bufferTypes(localIndex)))
    }
  }

  override def checkNeededMethods(
      needAccumulate: Boolean = false,
      needRetract: Boolean = false,
      needMerge: Boolean = false,
      needReset: Boolean = false,
      needEmitValue: Boolean = false): Unit = {
    // skip the check for DeclarativeAggregateFunction for now
  }

  override def setWindowSize(generator: ExprCodeGenerator): String = {
    function match {
      case _: SizeBasedWindowFunction =>
        s"""this.$windowSizeTerm = ${generator.input1Term};"""
      case _ =>
        ""
    }
  }
}
