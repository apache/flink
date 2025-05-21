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
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{BundledAggregateFunction, FunctionContext, ImperativeAggregateFunction}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator._
import org.apache.flink.table.planner.expressions.DeclarativeExpressionResolver.toRexInputRef
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter
import org.apache.flink.table.planner.plan.utils.AggregateInfo
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.types.FieldsDataType
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.tools.RelBuilder

import scala.collection.mutable.ArrayBuffer

class BundledImperativeAggCodeGen(
    ctx: CodeGeneratorContext,
    aggInfo: AggregateInfo,
    filterExpression: Option[Expression],
    accIndexes: (Int, Int),
    inputTypes: Seq[LogicalType],
    constantExprs: Seq[GeneratedExpression],
    relBuilder: RelBuilder,
    inputFieldCopy: Boolean)
  extends AggCodeGen {
  val function = aggInfo.function.asInstanceOf[ImperativeAggregateFunction[_, _]]
  val functionTerm: String = ctx.addReusableFunction(
    function,
    classOf[FunctionContext],
    Seq(s"$STORE_TERM.getRuntimeContext()"))
  val aggIndex: Int = aggInfo.aggIndex
  val externalAccType = aggInfo.externalAccTypes(0)
  private val internalAccType = fromDataTypeToLogicalType(externalAccType)

  /**
   * whether the acc type is an internal type. Currently we only support GenericRowData as internal
   * acc type
   */
  val isAccTypeInternal: Boolean =
    classOf[RowData].isAssignableFrom(externalAccType.getConversionClass)

  private val externalResultType = aggInfo.externalResultType
  private val internalResultType = fromDataTypeToLogicalType(externalResultType)

  private val rexNodeGen = new ExpressionConverter(relBuilder)

  override def createAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    Seq(GeneratedExpression("null", NEVER_NULL, NO_CODE, internalAccType, Option.empty))
  }

  override def setAccumulator(generator: ExprCodeGenerator): String = {
    ""
  }

  override def getAccumulator(generator: ExprCodeGenerator): Seq[GeneratedExpression] = {
    Seq(GeneratedExpression("null", NEVER_NULL, NO_CODE, internalAccType, Option.empty))
  }

  override def resetAccumulator(generator: ExprCodeGenerator): String = {
    ""
  }

  override def setWindowSize(generator: ExprCodeGenerator): String = {
    ""
  }

  override def accumulate(generator: ExprCodeGenerator): String = {
    ""
  }

  override def retract(generator: ExprCodeGenerator): String = {
    ""
  }

  private def bundledParametersCode(generator: ExprCodeGenerator): (String, String) = {
    val externalInputTypes = aggInfo.externalArgTypes
    var codes: ArrayBuffer[String] = ArrayBuffer.empty[String]

    val inputRowFields = aggInfo.argIndexes.zipWithIndex
      .map {
        case (f, index) =>
          if (f >= inputTypes.length) {
            // index to constant
            val expr = constantExprs(f - inputTypes.length)
            genToExternalConverterAll(ctx, externalInputTypes(index), expr)
          } else {
            // index to input field
            val inputRef = {
              // called from accumulate
              toRexInputRef(relBuilder, f, inputTypes(f))
            }
            var inputExpr = generator.generateExpression(inputRef.accept(rexNodeGen))
            if (inputFieldCopy) inputExpr = inputExpr.deepCopy(ctx)
            codes += inputExpr.code
            genToExternalConverterAll(ctx, externalInputTypes(index), inputExpr)
          }
      }
      .mkString(", ")

    val loop =
      s"""
         |   java.util.List inputRows = new java.util.ArrayList();
         |   for ($ROW_DATA $BATCH_ENTRY_TERM : $BATCH_INPUT_TERM.getRows()) {
         |      ${ctx.reuseInputUnboxingCode(BATCH_ENTRY_TERM)}
         |      ${ctx.reusePerRecordCode()}
         |      inputRows.add($GENERIC_ROW.ofKind(
         |          $BATCH_ENTRY_TERM.getRowKind(), $inputRowFields));
         |   }
         |
         |   $BUNDLED_KEY_SEGMENT aggCall${aggInfo.aggIndex} =
         |       $BUNDLED_KEY_SEGMENT.of(
         |         $BATCH_INPUT_TERM.getKey(),
         |         inputRows,
         |         $BATCH_INPUT_TERM.getAccumulator() == null ?
         |             null :
         |             // Grab the segment of the accumulator belonging to this one
         |             $BATCH_INPUT_TERM.getAccumulator().getRow(${accIndexes._1},
         |                  ${externalAccType.asInstanceOf[FieldsDataType].getChildren.size()}),
         |         $BATCH_INPUT_TERM.getUpdatedValuesAfterEachRow());
         |""".stripMargin

    codes += loop
    val inputFields = Seq(
      s"""future${aggInfo.aggIndex}""".stripMargin,
      s"""aggCall${aggInfo.aggIndex}""".stripMargin)
    (inputFields.mkString(", "), codes.mkString("\n"))
  }

  override def bundledAccumulateRetract(generator: ExprCodeGenerator): String = {
    val (parameters, code) = bundledParametersCode(generator)

    val call =
      s"""
         | $functionTerm.bundledAccumulateRetract($parameters);
   """.stripMargin
    filterExpression match {
      case None =>
        s"""
           |$code
           |$call
     """.stripMargin
      case Some(expr) =>
        throw new UnsupportedOperationException(
          "Filter operations not handled on bundled aggregates yet");
    }
  }

  override def merge(generator: ExprCodeGenerator): String = {
    ""
  }

  override def getValue(generator: ExprCodeGenerator): GeneratedExpression = {
    GeneratedExpression("null", NEVER_NULL, NO_CODE, internalResultType, Option.empty)
  }

  override def checkNeededMethods(
      needAccumulate: Boolean,
      needRetract: Boolean,
      needMerge: Boolean,
      needReset: Boolean,
      needBundled: Boolean,
      needEmitValue: Boolean): Unit = {

    if (needRetract) {
      function match {
        case f: BundledAggregateFunction if !f.canRetract =>
          throw new UnsupportedOperationException(
            s"Retract functionality is not implemented for the aggregate function: ${f.getClass.getName}")
        case _ =>
      }
    }
  }
}
