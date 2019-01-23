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
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{TableConfig, Types}
import org.apache.flink.table.codegen.{FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.plan.nodes.CommonScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.types.Row
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import java.lang.{Boolean => JBool}

import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.runtime.conversion.{ExternalTypeToCRowProcessRunner, JavaTupleToCRowProcessRunner, ScalaTupleToCRowProcessRunner}

trait StreamScan extends CommonScan[CRow] with DataStreamRel {

  protected def convertUpsertToInternalRow(
      schema: RowSchema,
      input: DataStream[Any],
      fieldIdxs: Array[Int],
      config: TableConfig,
      rowtimeExpression: Option[RexNode]): DataStream[CRow] = {

    val internalType = schema.typeInfo
    val cRowType = CRowTypeInfo(internalType)

    val hasTimeIndicator = fieldIdxs.exists(f =>
      f == TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER ||
        f == TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER)

    val dsType = input.getType

    dsType match {
        // Scala tuple
      case t: CaseClassTypeInfo[_]
        if t.getTypeClass == classOf[(_, _)] && t.getTypeAt(0) == Types.BOOLEAN =>

        val inputType = t.getTypeAt[Any](1)
        if (inputType == internalType && !hasTimeIndicator) {
          // input is already of correct type. Only need to wrap it as CRow
          input.asInstanceOf[DataStream[(Boolean, Row)]]
            .map(new RichMapFunction[(Boolean, Row), CRow] {
              @transient private var outCRow: CRow = _
              override def open(parameters: Configuration): Unit = {
                outCRow = new CRow(null, change = true)
              }

              override def map(v: (Boolean, Row)): CRow = {
                outCRow.row = v._2
                outCRow.change = v._1
                outCRow
              }
            }).returns(cRowType)

        } else {
          // input needs to be converted and wrapped as CRow or time indicators need to be generated

          val function = generateConversionProcessFunction(
            config,
            inputType.asInstanceOf[TypeInformation[Any]],
            internalType,
            "UpsertStreamSourceConversion",
            schema.fieldNames,
            fieldIdxs,
            rowtimeExpression
          )

          val processFunc = new ScalaTupleToCRowProcessRunner(
            function.name,
            function.code,
            cRowType)

          val opName = s"from: (${schema.fieldNames.mkString(", ")})"

          input
            .asInstanceOf[DataStream[(Boolean, Any)]]
            .process(processFunc).name(opName).returns(cRowType)
        }

      // Java tuple
      case t: TupleTypeInfo[_]
        if t.getTypeClass == classOf[JTuple2[_, _]] && t.getTypeAt(0) == Types.BOOLEAN =>

        val inputType = t.getTypeAt[Any](1)
        if (inputType == internalType && !hasTimeIndicator) {
          // input is already of correct type. Only need to wrap it as CRow
          input.asInstanceOf[DataStream[JTuple2[JBool, Row]]]
            .map(new RichMapFunction[JTuple2[JBool, Row], CRow] {
              @transient private var outCRow: CRow = _
              override def open(parameters: Configuration): Unit = {
                outCRow = new CRow(null, change = true)
              }

              override def map(v: JTuple2[JBool, Row]): CRow = {
                outCRow.row = v.f1
                outCRow.change = v.f0
                outCRow
              }
            }).returns(cRowType)

        } else {
          // input needs to be converted and wrapped as CRow or time indicators need to be generated

          val function = generateConversionProcessFunction(
            config,
            inputType.asInstanceOf[TypeInformation[Any]],
            internalType,
            "UpsertStreamSourceConversion",
            schema.fieldNames,
            fieldIdxs,
            rowtimeExpression
          )

          val processFunc = new JavaTupleToCRowProcessRunner(
            function.name,
            function.code,
            cRowType)

          val opName = s"from: (${schema.fieldNames.mkString(", ")})"

          input
            .asInstanceOf[DataStream[JTuple2[JBool, Any]]]
            .process(processFunc).name(opName).returns(cRowType)
        }
    }
  }

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
        @transient private var outCRow: CRow = _
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
        "AppendStreamSourceConversion",
        schema.fieldNames,
        fieldIdxs,
        rowtimeExpression
      )

      val processFunc = new ExternalTypeToCRowProcessRunner(
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
