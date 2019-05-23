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

package org.apache.flink.table.plan.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.`type`.RowType
import org.apache.flink.table.`type`.TypeConverters.createInternalTypeFromTypeInfo
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils.{DEFAULT_INPUT1_TERM, GENERIC_ROW}
import org.apache.flink.table.codegen.OperatorCodeGenerator.generateCollect
import org.apache.flink.table.codegen.{CodeGenUtils, CodeGeneratorContext, ExprCodeGenerator, OperatorCodeGenerator}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.runtime.CodeGenOperatorFactory
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._

/**
  * Util for [[TableScan]]s.
  */
object ScanUtil {

  private[flink] def hasTimeAttributeField(indexes: Array[Int]) =
    indexes.contains(TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER)||
        indexes.contains(TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER)||
        indexes.contains(TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER)||
        indexes.contains(TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER)

  private[flink] def needsConversion(dataType: TypeInformation[_], clz: Class[_]): Boolean =
    createInternalTypeFromTypeInfo(dataType) match {
      case _: RowType => !CodeGenUtils.isInternalClass(clz, dataType)
      case _ => true
    }

  private[flink] def convertToInternalRow(
      ctx: CodeGeneratorContext,
      input: StreamTransformation[Any],
      fieldIndexes: Array[Int],
      inputType: TypeInformation[_],
      outRowType: RelDataType,
      qualifiedName: Seq[String],
      config: TableConfig,
      rowtimeExpr: Option[RexNode] = None,
      beforeConvert: String = "",
      afterConvert: String = ""): StreamTransformation[BaseRow] = {

    val outputRowType = FlinkTypeFactory.toInternalRowType(outRowType)

    // conversion
    val convertName = "SourceConversion"
    // type convert
    val inputTerm = DEFAULT_INPUT1_TERM
    val internalInType = createInternalTypeFromTypeInfo(inputType)
    val (inputTermConverter, inputRowType) = {
      val convertFunc = CodeGenUtils.genToInternal(ctx, inputType)
      internalInType match {
        case rt: RowType => (convertFunc, rt)
        case _ => ((record: String) => s"$GENERIC_ROW.of(${convertFunc(record)})",
            new RowType(internalInType))
      }
    }

    val processCode =
      if ((inputRowType.getFieldTypes sameElements outputRowType.getFieldTypes) &&
          (inputRowType.getFieldNames sameElements outputRowType.getFieldNames) &&
          !hasTimeAttributeField(fieldIndexes)) {
        s"${generateCollect(inputTerm)}"
      } else {

        // field index change (pojo) or has time attribute field
        val conversion = new ExprCodeGenerator(ctx, false)
            .bindInput(inputRowType, inputTerm = inputTerm, inputFieldMapping = Some(fieldIndexes))
            .generateConverterResultExpression(
              outputRowType, classOf[GenericRow], rowtimeExpression = rowtimeExpr)

        s"""
           |$beforeConvert
           |${conversion.code}
           |${generateCollect(conversion.resultTerm)}
           |$afterConvert
           |""".stripMargin
      }

    val generatedOperator = OperatorCodeGenerator.generateOneInputStreamOperator[Any, BaseRow](
      ctx,
      convertName,
      processCode,
      "",
      outputRowType,
      config,
      converter = inputTermConverter)

    val substituteStreamOperator = new CodeGenOperatorFactory[BaseRow](generatedOperator)

    new OneInputTransformation(
      input,
      getOperatorName(qualifiedName, outRowType),
      substituteStreamOperator,
      outputRowType.toTypeInfo,
      input.getParallelism)
  }

  /**
    * @param qualifiedName qualified name for table
    */
  private[flink] def getOperatorName(qualifiedName: Seq[String], rowType: RelDataType): String = {
    val s = s"table:$qualifiedName, fields:(${rowType.getFieldNames.mkString(", ")})"
    s"SourceConversion($s)"
  }
}
