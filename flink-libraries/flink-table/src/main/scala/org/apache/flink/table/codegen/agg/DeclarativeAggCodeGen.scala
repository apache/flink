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
package org.apache.flink.table.codegen.agg

import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.functions.DeclarativeAggregateFunction
import org.apache.flink.table.api.types.{DataType, DataTypes, InternalType}
import org.apache.flink.table.codegen.CodeGenUtils.primitiveTypeTermForType
import org.apache.flink.table.codegen.{CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.codegen.agg.AggsHandlerCodeGenerator._
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.util.AggregateInfo

/**
  * It is for code generate aggregation functions that are specified using expressions.
  * The aggregate buffer is embedded inside of a larger shared aggregation buffer.
  *
  * @param ctx the code gen context
  * @param aggInfo  the aggregate information
  * @param filterExpression filter argument access expression, none if no filter
  * @param mergedAccOffset the mergedAcc may come from local aggregate,
  *                        this is the first buffer offset in the row
  * @param aggBufferOffset  the offset in the buffers of this aggregate
  * @param aggBufferSize  the total size of aggregate buffers
  * @param inputTypes   the input field type infos
  * @param constantExprs  the constant expressions
  * @param relBuilder  the rel builder to translate expressions to calcite rex nodes
  */
class DeclarativeAggCodeGen(
    ctx: CodeGeneratorContext,
    aggInfo: AggregateInfo,
    filterExpression: Option[Expression],
    mergedAccOffset: Int,
    aggBufferOffset: Int,
    aggBufferSize: Int,
    inputTypes: Seq[InternalType],
    constantExprs: Seq[GeneratedExpression],
    relBuilder: RelBuilder)
  extends AggCodeGen {

  val function: DeclarativeAggregateFunction =
    aggInfo.function.asInstanceOf[DeclarativeAggregateFunction]

  val bufferTypes: Array[DataType] = aggInfo.externalAccTypes
  val bufferIndexes: Array[Int] = Array.range(
    aggBufferOffset, aggBufferOffset + bufferTypes.length)
  val bufferTerms: Array[String] = function.aggBufferAttributes
    .map(a => s"agg${aggInfo.aggIndex}_${a.name}").toArray
  val bufferNullTerms: Array[String] = bufferTerms.map(_ + "_isNull")

  val argIndexes: Array[Int] = aggInfo.argIndexes
  val argTypes: Array[InternalType] = {
    val types = inputTypes ++ constantExprs.map(_.resultType)
    argIndexes.map(types(_))
  }

  def createAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    function.initialValuesExpressions
      .map(expr => generator.generateExpression(expr.toRexNode(relBuilder)))
  }

  def setAccumulator(generator: ExprCodeGenerator): String = {
    val aggBufferAccesses = function.aggBufferAttributes.zipWithIndex
      .map { case (attr, index) =>
        ResolvedAggInputReference(
          attr.name, bufferIndexes(index), bufferTypes(index).toInternalType)
      }
      .map(expr => generator.generateExpression(expr.toRexNode(relBuilder)))

    val setters = aggBufferAccesses.zipWithIndex.map {
      case (access, index) =>
        val typeTerm = primitiveTypeTermForType(access.resultType)
        val memberName = bufferTerms(index)
        val memberNullTerm = bufferNullTerms(index)
        ctx.addReusableMember(s"private $typeTerm $memberName;")
        ctx.addReusableMember(s"private boolean $memberNullTerm;")
        s"""
           |${access.copyResultTermToTargetIfChanged(ctx, memberName)};
           |$memberNullTerm = ${access.nullTerm};
         """.stripMargin
    }

    setters.mkString("\n")
  }

  override def resetAccumulator(generator: ExprCodeGenerator): String = {
    val initialExprs = function.initialValuesExpressions
      .map(expr => generator.generateExpression(expr.toRexNode(relBuilder)))
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
    bufferTypes.zipWithIndex.map { case (bufferType, index) =>
      GeneratedExpression(
        bufferTerms(index), bufferNullTerms(index), "", bufferType.toInternalType)
    }
  }

  def accumulate(generator: ExprCodeGenerator): String = {
    val resolvedExprs = if (generator.input1Term.startsWith(DISTINCT_KEY_TERM)) {
      // called from distinct merge
      function.accumulateExpressions
        .map(_.postOrderTransform(resolveReference(isDistinctMerge = true)))
    } else {
      // called from accumulate
      function.accumulateExpressions
        .map(_.postOrderTransform(resolveReference()))
    }

    val exprs = resolvedExprs
      .map(_.toRexNode(relBuilder)) // rex nodes
      .map(generator.generateExpression) // generated expressions

    val codes = exprs.zipWithIndex.map { case (expr, index) =>
      s"""
         |${expr.code}
         |${expr.copyResultTermToTargetIfChanged(ctx, bufferTerms(index))};
         |${bufferNullTerms(index)} = ${expr.nullTerm};
       """.stripMargin
    }

    filterExpression match {
      case Some(expr) =>
        val generated = generator.generateExpression(expr.toRexNode(relBuilder))
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
    val resolvedExprs = if (generator.input1Term.startsWith(DISTINCT_KEY_TERM)) {
      // called from distinct merge
      function.retractExpressions
        .map(_.postOrderTransform(resolveReference(isDistinctMerge = true)))
    } else {
      // called from retract
      function.retractExpressions
        .map(_.postOrderTransform(resolveReference()))
    }

    val exprs = resolvedExprs
      .map(_.toRexNode(relBuilder)) // rex nodes
      .map(generator.generateExpression) // generated expressions

    val codes = exprs.zipWithIndex.map { case (expr, index) =>
      s"""
         |${expr.code}
         |${expr.copyResultTermToTargetIfChanged(ctx, bufferTerms(index))};
         |${bufferNullTerms(index)} = ${expr.nullTerm};
       """.stripMargin
    }

    filterExpression match {
      case Some(expr) =>
        val generated = generator.generateExpression(expr.toRexNode(relBuilder))
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
      .map(_.postOrderTransform(resolveReference(isMerge = true)))
      .map(_.toRexNode(relBuilder)) // rex nodes
      .map(generator.generateExpression) // generated expressions

    val codes = exprs.zipWithIndex.map { case (expr, index) =>
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
      .postOrderTransform(resolveReference())
    generator.generateExpression(resolvedGetValueExpression.toRexNode(relBuilder))
  }

  /**
    * Resolves the given expression to a [[NamedExpression]].
    *
    * @param isMerge this is called from merge() method
    * @param isDistinctMerge this is called from distinct merge method
    */
  private def resolveReference(isMerge: Boolean = false, isDistinctMerge: Boolean = false)
  : PartialFunction[Expression, Expression] = {
    case input: UnresolvedFieldReference =>
      // We always use UnresolvedFieldReference to represent reference of input field.
      // In non-merge case, the input is the operand of the aggregate function.
      // In merge case, the input is the aggregate buffers sent by local aggregate.
      if (isMerge) {
        val localIndex = function.inputAggBufferAttributes.indexOf(input)
        // in merge case, the input1 is mergedAcc
        ResolvedAggInputReference(
          input.name,
          mergedAccOffset + bufferIndexes(localIndex),
          bufferTypes(localIndex).toInternalType)
      } else {
        val localIndex = function.operands.indexOf(input)
        val inputIndex = argIndexes(localIndex)
        // index to constant
        if (inputIndex >= inputTypes.length) {
          val constantIndex = inputIndex - inputTypes.length
          val constantTerm = constantExprs(constantIndex).resultTerm
          val nullTerm = constantExprs(constantIndex).nullTerm
          val constantType = constantExprs(constantIndex).resultType
          // constant is reused as member variable
          ResolvedAggLocalReference(constantTerm, nullTerm, constantType)
        } else {
          if (isDistinctMerge) {  // this is called from distinct merge
            if (function.inputCount == 1) {
              // the distinct key is a BoxedValue
              ResolvedDistinctKeyReference(input.name, argTypes(0))
            } else {
              // the distinct key is a BaseRow
              ResolvedAggInputReference(input.name, localIndex, argTypes(localIndex))
            }
          } else {
            // the input is the inputRow
            ResolvedAggInputReference(input.name, argIndexes(localIndex), argTypes(localIndex))
          }
        }
      }

    case buffer: UnresolvedAggBufferReference =>
      val localIndex = function.aggBufferAttributes.indexOf(buffer)
      val name = bufferTerms(localIndex)
      val nullTerm = bufferNullTerms(localIndex)
      // buffer access is reused as member variable
      ResolvedAggLocalReference(name, nullTerm, bufferTypes(localIndex).toInternalType)
  }

  def checkNeededMethods(
     needAccumulate: Boolean = false,
     needRetract: Boolean = false,
     needMerge: Boolean = false,
     needReset: Boolean = false): Unit = {
    // skip the check for DeclarativeAggregateFunction for now
  }
}
