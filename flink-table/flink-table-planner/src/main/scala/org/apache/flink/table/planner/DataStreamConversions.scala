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

package org.apache.flink.table.planner

import java.lang.{Boolean => JBool}

import org.apache.calcite.rel.RelNode
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.runtime.conversion._
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.runtime.{CRowMapRunner, OutputRowtimeProcessFunction}

object DataStreamConversions {

  /**
    * Translates a [[DataStream]] of internal [[CRow]] type into a [[DataStream]] of requested type.
    *
    * @param inputDataStream     The input [[DataStream]] for the conversion.
    * @param logicalType         The logical row type of the [[DataStream]]. This is needed because
    *                            field naming might be lost during optimization.
    * @param withChangeFlag      Set to true to emit records with change flags.
    * @param requestedOutputType The [[TypeInformation]] of the resulting [[DataStream]].
    * @tparam A The type of the resulting [[DataStream]].
    * @return The [[DataStream]] of requested type.
    */
  def convert[A](
      inputDataStream: DataStream[CRow],
      logicalType: TableSchema,
      withChangeFlag: Boolean,
      requestedOutputType: TypeInformation[A],
      config: TableConfig)
    : DataStream[A] = {

    val rowtimeFields = logicalType.getFieldTypes.zip(logicalType.getFieldNames).zipWithIndex
      .filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f._1._1))

    // convert the input type for the conversion mapper
    // the input will be changed in the OutputRowtimeProcessFunction later
    val convType = if (rowtimeFields.length > 1) {
      throw new TableException(
        s"Found more than one rowtime field: [${rowtimeFields.map(_._1._2).mkString(", ")}] in " +
          s"the table that should be converted to a DataStream.\n" +
          s"Please select the rowtime field that should be used as event-time timestamp for the " +
          s"DataStream by casting all other fields to TIMESTAMP.")
    } else if (rowtimeFields.length == 1) {
      val origRowType = inputDataStream.getType.asInstanceOf[CRowTypeInfo].rowType
      val convFieldTypes = origRowType.getFieldTypes.map { t =>
        if (FlinkTypeFactory.isRowtimeIndicatorType(t)) {
          SqlTimeTypeInfo.TIMESTAMP
        } else {
          t
        }
      }
      CRowTypeInfo(new RowTypeInfo(convFieldTypes, origRowType.getFieldNames))
    } else {
      inputDataStream.getType
    }

    // convert CRow to output type
    val conversion: MapFunction[CRow, A] = if (withChangeFlag) {
      getConversionMapperWithChanges(
        convType,
        logicalType,
        requestedOutputType,
        "DataStreamSinkConversion",
        config)
    } else {
      getConversionMapper(
        convType,
        logicalType,
        requestedOutputType,
        "DataStreamSinkConversion",
        config)
    }

    val rootParallelism = inputDataStream.getParallelism

    val withRowtime = if (rowtimeFields.isEmpty) {
      // no rowtime field to set
      inputDataStream.map(conversion)
    } else {
      // set the only rowtime field as event-time timestamp for DataStream
      // and convert it to SQL timestamp
      inputDataStream
        .process(new OutputRowtimeProcessFunction[A](conversion, rowtimeFields.head._2))
    }

    withRowtime
      .returns(requestedOutputType)
      .name(s"to: ${requestedOutputType.getTypeClass.getSimpleName}")
      .setParallelism(rootParallelism)
  }

  /**
    * Creates a final converter that maps the internal row type to external type.
    *
    * @param physicalInputType   the input of the sink
    * @param logicalInputSchema  the input schema with correct field names (esp. for POJO field
    *                            mapping)
    * @param requestedOutputType the output type of the sink
    * @param functionName        name of the map function. Must not be unique but has to be a
    *                            valid Java class identifier.
    */
  private def getConversionMapper[OUT](
      physicalInputType: TypeInformation[CRow],
      logicalInputSchema: TableSchema,
      requestedOutputType: TypeInformation[OUT],
      functionName: String,
      config: TableConfig)
    : MapFunction[CRow, OUT] = {

    val converterFunction = Conversions.generateRowConverterFunction[OUT](
      physicalInputType.asInstanceOf[CRowTypeInfo].rowType,
      logicalInputSchema,
      requestedOutputType,
      functionName,
      config
    )

    converterFunction match {

      case Some(func) =>
        new CRowMapRunner[OUT](func.name, func.code, func.returnType)

      case _ =>
        new CRowToRowMapFunction().asInstanceOf[MapFunction[CRow, OUT]]
    }
  }

  /**
    * Creates a converter that maps the internal CRow type to Scala or Java Tuple2 with change flag.
    *
    * @param physicalInputType   the input of the sink
    * @param logicalInputSchema  the input schema with correct field names (esp. for POJO field
    *                            mapping)
    * @param requestedOutputType the output type of the sink.
    * @param functionName        name of the map function. Must not be unique but has to be a
    *                            valid Java class identifier.
    */
  private def getConversionMapperWithChanges[OUT](
      physicalInputType: TypeInformation[CRow],
      logicalInputSchema: TableSchema,
      requestedOutputType: TypeInformation[OUT],
      functionName: String,
      config: TableConfig)
    : MapFunction[CRow, OUT] = requestedOutputType match {

    // Scala tuple
    case t: CaseClassTypeInfo[_]
      if t.getTypeClass == classOf[(_, _)] && t.getTypeAt(0) == Types.BOOLEAN =>

      val reqType = t.getTypeAt[Any](1)

      // convert Row into requested type and wrap result in Tuple2
      val converterFunction = Conversions.generateRowConverterFunction(
        physicalInputType.asInstanceOf[CRowTypeInfo].rowType,
        logicalInputSchema,
        reqType,
        functionName,
        config
      )

      converterFunction match {

        case Some(func) =>
          new CRowToScalaTupleMapRunner(
            func.name,
            func.code,
            requestedOutputType.asInstanceOf[TypeInformation[(Boolean, Any)]]
          ).asInstanceOf[MapFunction[CRow, OUT]]

        case _ =>
          new CRowToScalaTupleMapFunction().asInstanceOf[MapFunction[CRow, OUT]]
      }

    // Java tuple
    case t: TupleTypeInfo[_]
      if t.getTypeClass == classOf[JTuple2[_, _]] && t.getTypeAt(0) == Types.BOOLEAN =>

      val reqType = t.getTypeAt[Any](1)

      // convert Row into requested type and wrap result in Tuple2
      val converterFunction = Conversions.generateRowConverterFunction(
        physicalInputType.asInstanceOf[CRowTypeInfo].rowType,
        logicalInputSchema,
        reqType,
        functionName,
        config
      )

      converterFunction match {

        case Some(func) =>
          new CRowToJavaTupleMapRunner(
            func.name,
            func.code,
            requestedOutputType.asInstanceOf[TypeInformation[JTuple2[JBool, Any]]]
          ).asInstanceOf[MapFunction[CRow, OUT]]

        case _ =>
          new CRowToJavaTupleMapFunction().asInstanceOf[MapFunction[CRow, OUT]]
      }
  }
}
