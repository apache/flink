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

package org.apache.flink.table.plan.nodes.dataset

import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.{FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.plan.nodes.CommonScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.MapRunner
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.types.Row

trait BatchScan extends CommonScan[Row] with DataSetRel {

  protected def convertToInternalRow(
      schema: RowSchema,
      input: DataSet[Any],
      fieldIdxs: Array[Int],
      config: TableConfig,
      rowtimeExpression: Option[RexNode]): DataSet[Row] = {

    val inputType = input.getType
    val internalType = schema.typeInfo

    val hasTimeIndicator = fieldIdxs.exists(f =>
      f == TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER ||
      f == TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER)

    // conversion
    if (inputType != internalType || hasTimeIndicator) {

      val function = generateConversionMapper(
        config,
        inputType,
        internalType,
        "DataSetSourceConversion",
        schema.fieldNames,
        fieldIdxs,
        rowtimeExpression)

      val runner = new MapRunner[Any, Row](
        function.name,
        function.code,
        function.returnType)

      val opName = s"from: (${schema.fieldNames.mkString(", ")})"

      input.map(runner).name(opName)
    }
    // no conversion necessary, forward
    else {
      input.asInstanceOf[DataSet[Row]]
    }
  }

  private def generateConversionMapper(
      config: TableConfig,
      inputType: TypeInformation[Any],
      outputType: TypeInformation[Row],
      conversionOperatorName: String,
      fieldNames: Seq[String],
      inputFieldMapping: Array[Int],
      rowtimeExpression: Option[RexNode]): GeneratedFunction[MapFunction[Any, Row], Row] = {

    val generator = new FunctionCodeGenerator(
      config,
      false,
      inputType,
      None,
      Some(inputFieldMapping))

    val conversion = generator.generateConverterResultExpression(
      outputType,
      fieldNames,
      rowtimeExpression)

    val body =
      s"""
         |${conversion.code}
         |return ${conversion.resultTerm};
         |""".stripMargin

    generator.generateFunction(
      "DataSetSourceConversion",
      classOf[MapFunction[Any, Row]],
      body,
      outputType)
  }
}
