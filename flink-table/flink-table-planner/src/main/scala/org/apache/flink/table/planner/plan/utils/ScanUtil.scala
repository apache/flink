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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, CodeGenUtils, ExprCodeGenerator, OperatorCodeGenerator}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{DEFAULT_INPUT1_TERM, GENERIC_ROW}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.generateCollect
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

/** Util for [[TableScan]]s. */
object ScanUtil {

  def hasTimeAttributeField(indexes: Array[Int]) =
    indexes.contains(TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER) ||
      indexes.contains(TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER) ||
      indexes.contains(TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER) ||
      indexes.contains(TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER)

  private[flink] def needsConversion(source: TableSource[_]): Boolean = {
    needsConversion(source.getProducedDataType)
  }

  def needsConversion(dataType: DataType): Boolean =
    fromDataTypeToLogicalType(dataType) match {
      case _: RowType => !CodeGenUtils.isInternalClass(dataType)
      case _ => true
    }

  def convertToInternalRow(
      ctx: CodeGeneratorContext,
      input: Transformation[Any],
      fieldIndexes: Array[Int],
      inputType: DataType,
      outputRowType: RowType,
      qualifiedName: util.List[String],
      nameFormatter: (String, String) => String,
      descriptionFormatter: String => String,
      rowtimeExpr: Option[RexNode] = None,
      beforeConvert: String = "",
      afterConvert: String = ""): Transformation[RowData] = {
    // conversion
    val convertName = "SourceConversion"
    // type convert
    val inputTerm = DEFAULT_INPUT1_TERM
    val internalInType = fromDataTypeToLogicalType(inputType)
    val (inputTermConverter, inputRowType) = {
      val convertFunc = CodeGenUtils.genToInternalConverter(ctx, inputType)
      internalInType match {
        case rt: RowType => (convertFunc, rt)
        case _ =>
          (
            (record: String) => s"$GENERIC_ROW.of(${convertFunc(record)})",
            RowType.of(internalInType))
      }
    }

    val processCode =
      if (
        (inputRowType.getChildren == outputRowType.getChildren) &&
        (inputRowType.getFieldNames == outputRowType.getFieldNames) &&
        !hasTimeAttributeField(fieldIndexes)
      ) {
        s"${generateCollect(inputTerm)}"
      } else {

        // field index change (pojo) or has time attribute field
        val conversion = new ExprCodeGenerator(ctx, false)
          .bindInput(inputRowType, inputTerm = inputTerm, inputFieldMapping = Some(fieldIndexes))
          .generateConverterResultExpression(
            outputRowType,
            classOf[GenericRowData],
            rowtimeExpression = rowtimeExpr)

        s"""
           |$beforeConvert
           |${conversion.code}
           |${generateCollect(conversion.resultTerm)}
           |$afterConvert
           |""".stripMargin
      }

    val generatedOperator = OperatorCodeGenerator.generateOneInputStreamOperator[Any, RowData](
      ctx,
      convertName,
      processCode,
      outputRowType,
      converter = inputTermConverter)

    val substituteStreamOperator = new CodeGenOperatorFactory[RowData](generatedOperator)

    val description = descriptionFormatter(getOperatorDescription(qualifiedName, outputRowType))
    val name = nameFormatter(description, "SourceConversion");
    ExecNodeUtil.createOneInputTransformation(
      input.asInstanceOf[Transformation[RowData]],
      name,
      description,
      substituteStreamOperator,
      InternalTypeInfo.of(outputRowType),
      input.getParallelism,
      0,
      false)
  }

  /** @param qualifiedName qualified name for table */
  private[flink] def getOperatorDescription(
      qualifiedName: Seq[String],
      rowType: RowType): String = {
    val tableQualifiedName = qualifiedName.mkString(".")
    val fieldNames = rowType.getFieldNames.mkString(", ")
    s"SourceConversion(table=[$tableQualifiedName], fields=[$fieldNames])"
  }

  /** Returns the field indices of primary key in given fields. */
  def getPrimaryKeyIndices(
      fieldNames: util.List[String],
      keyFields: util.List[String]): Array[Int] = {
    // we must use the output field names of scan node instead of the original schema
    // to calculate the primary key indices, because the scan node maybe projection pushed down
    keyFields.map {
      k =>
        val index = fieldNames.indexOf(k)
        if (index < 0) {
          // primary key shouldn't be pruned, otherwise it's a bug
          throw new TableException(
            s"Can't find primary key field $k in the input fields $fieldNames. " +
              s"This is a bug, please file an issue.")
        }
        index
    }.toArray
  }
}
