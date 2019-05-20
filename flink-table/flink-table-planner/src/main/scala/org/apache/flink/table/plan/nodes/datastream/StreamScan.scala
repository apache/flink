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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.{FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.plan.nodes.CommonScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.types.Row
import org.apache.flink.table.runtime.CRowOutputProcessRunner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

trait StreamScan extends CommonScan[CRow] with DataStreamRel {

  protected def convertToInternalRow(
      schema: RowSchema,
      input: DataStream[Any],
      fieldIdxs: Array[Int],
      config: TableConfig,
      rowtimeExpression: Option[RexNode]): DataStream[CRow] = {

    val inputType = input.getType
    val internalType = schema.typeInfo
    val cRowType = CRowTypeInfo(internalType)

    val hasTimeIndicator = fieldIdxs.exists(f =>
      f == TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER ||
      f == TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER)

    if (input.getType == cRowType && !hasTimeIndicator) {
      // input is already a CRow with correct type
      input.asInstanceOf[DataStream[CRow]]

    } else if (input.getType == internalType && !hasTimeIndicator) {
      // input is already of correct type. Only need to wrap it as CRow
      input.asInstanceOf[DataStream[Row]].map(new RichMapFunction[Row, CRow] {
        @transient private var outCRow: CRow = null
        override def open(parameters: Configuration): Unit = {
          outCRow = new CRow(null, change = true)
        }

        override def map(v: Row): CRow = {
          outCRow.row = v
          outCRow
        }
      }).returns(cRowType)

    } else {
      // input needs to be converted and wrapped as CRow or time indicators need to be generated

      val function = generateConversionProcessFunction(
        config,
        inputType,
        internalType,
        "DataStreamSourceConversion",
        schema.fieldNames,
        fieldIdxs,
        rowtimeExpression
      )

      val processFunc = new CRowOutputProcessRunner(
        function.name,
        function.code,
        cRowType)

      val opName = s"from: (${schema.fieldNames.mkString(", ")})"

      input.process(processFunc).name(opName).returns(cRowType)
    }
  }

  private def generateConversionProcessFunction(
      config: TableConfig,
      inputType: TypeInformation[Any],
      outputType: TypeInformation[Row],
      conversionOperatorName: String,
      fieldNames: Seq[String],
      inputFieldMapping: Array[Int],
      rowtimeExpression: Option[RexNode]): GeneratedFunction[ProcessFunction[Any, Row], Row] = {

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
         |${generator.collectorTerm}.collect(${conversion.resultTerm});
         |""".stripMargin

    generator.generateFunction(
      "DataStreamSourceConversion",
      classOf[ProcessFunction[Any, Row]],
      body,
      outputType)
  }
}
