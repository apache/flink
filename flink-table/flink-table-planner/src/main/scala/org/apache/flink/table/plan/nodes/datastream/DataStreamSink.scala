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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink => StreamingDataStreamSink}
import org.apache.flink.table.api.{TableException, TableSchema, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.Sink
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.table.planner.{DataStreamConversions, StreamPlanner}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.sinks.{AppendStreamTableSink, DataStreamTableSink, RetractStreamTableSink, TableSink, UpsertStreamTableSink}
import org.apache.flink.table.types.utils.TypeConversions

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode

import _root_.java.lang.{Boolean => JBool}

import _root_.scala.collection.JavaConverters._


/**
  * Stream physical RelNode to to write data into an external sink defined by a [[TableSink]].
  */
class DataStreamSink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sink: TableSink[_],
    sinkName: String)
  extends Sink(cluster, traitSet, inputRel, sink, sinkName)
  with DataStreamRel {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamSink(cluster, traitSet, inputs.get(0), sink, sinkName)
  }

  override def translateToPlan(planner: StreamPlanner): DataStream[CRow] = {
    val inputTransform = writeToSink(planner).asInstanceOf[Transformation[CRow]]
    new DataStream(planner.getExecutionEnvironment, inputTransform)
  }

  private def writeToSink[T](planner: StreamPlanner): Transformation[_] = {
    sink match {
      case retractSink: RetractStreamTableSink[T] =>
        val resultSink = writeToRetractSink(retractSink, planner)
        resultSink.getTransformation

      case upsertSink: UpsertStreamTableSink[T] =>
        val resultSink = writeToUpsertSink(upsertSink, planner)
        resultSink.getTransformation

      case appendSink: AppendStreamTableSink[T] =>
        val resultSink = writeToAppendSink(appendSink, planner)
        resultSink.getTransformation

      case dataStreamTableSink: DataStreamTableSink[_] =>
        // if no change flags are requested, verify table is an insert-only (append-only) table.
        if (!dataStreamTableSink.withChangeFlag && !UpdatingPlanChecker.isAppendOnly(getInput)) {
          throw new ValidationException(
            "Table is not an append-only table. " +
              "Use the toRetractStream() in order to handle add and retract messages.")
        }
        val dataStream = translateInput(
          planner,
          getTableSchema,
          dataStreamTableSink.getOutputType,
          dataStreamTableSink.withChangeFlag)
        dataStream.getTransformation

      case _ =>
        throw new ValidationException("Stream Tables can only be emitted by AppendStreamTableSink, "
          + "RetractStreamTableSink, or UpsertStreamTableSink.")
    }
  }

  private def writeToRetractSink[T](
      sink: RetractStreamTableSink[T],
      planner: StreamPlanner): StreamingDataStreamSink[_]= {
    // retraction sink can always be used
    val outputType = TypeConversions.fromDataTypeToLegacyInfo(sink.getConsumedDataType)
      .asInstanceOf[TypeInformation[JTuple2[JBool, T]]]
    // translate the Table into a DataStream and provide the type that the TableSink expects.
    val result: DataStream[JTuple2[JBool, T]] =
      translateToType(
        planner,
        withChangeFlag = true,
        outputType)
    // Give the DataStream to the TableSink to emit it.
    sink.consumeDataStream(result)
  }

  private def writeToAppendSink[T](
      sink: AppendStreamTableSink[T],
      planner: StreamPlanner): StreamingDataStreamSink[_]= {
    // verify table is an insert-only (append-only) table
    if (!UpdatingPlanChecker.isAppendOnly(getInput)) {
      throw new TableException(
        "AppendStreamTableSink requires that Table has only insert changes.")
    }
    val outputType = TypeConversions.fromDataTypeToLegacyInfo(sink.getConsumedDataType)
      .asInstanceOf[TypeInformation[T]]
    val resultType = getTableSchema
    // translate the Table into a DataStream and provide the type that the TableSink expects.
    val result: DataStream[T] =
      translateInput(
        planner,
        resultType,
        outputType,
        withChangeFlag = false)
    // Give the DataStream to the TableSink to emit it.
    sink.consumeDataStream(result)
  }

  private def writeToUpsertSink[T](
      sink: UpsertStreamTableSink[T],
      planner: StreamPlanner): StreamingDataStreamSink[_] = {
    // optimize plan
    // check for append only table
    val isAppendOnlyTable = UpdatingPlanChecker.isAppendOnly(getInput)
    sink.setIsAppendOnly(isAppendOnlyTable)
    // extract unique key fields
    val sinkFieldNames = sink.getTableSchema.getFieldNames
    val tableKeys: Option[Array[String]] = UpdatingPlanChecker
      .getUniqueKeyFields(getInput, sinkFieldNames)
    // check that we have keys if the table has changes (is not append-only)
    tableKeys match {
      case Some(keys) => sink.setKeyFields(keys)
      case None if isAppendOnlyTable => sink.setKeyFields(null)
      case None if !isAppendOnlyTable => throw new TableException(
        "UpsertStreamTableSink requires that Table has full primary keys if it is updated.")
    }
    val outputType = TypeConversions.fromDataTypeToLegacyInfo(sink.getConsumedDataType)
      .asInstanceOf[TypeInformation[JTuple2[JBool, T]]]
    val resultType = getTableSchema
    // translate the Table into a DataStream and provide the type that the TableSink expects.
    val result: DataStream[JTuple2[JBool, T]] =
      translateInput(
        planner,
        resultType,
        outputType,
        withChangeFlag = true)
    // Give the DataStream to the TableSink to emit it.
    sink.consumeDataStream(result)
  }

  private def translateToType[A](
      planner: StreamPlanner,
      withChangeFlag: Boolean,
      tpe: TypeInformation[A]): DataStream[A] = {
    val rowType = getTableSchema

    // if no change flags are requested, verify table is an insert-only (append-only) table.
    if (!withChangeFlag && !UpdatingPlanChecker.isAppendOnly(input)) {
      throw new ValidationException(
        "Table is not an append-only table. " +
          "Use the toRetractStream() in order to handle add and retract messages.")
    }

    // get CRow plan
    translateInput(planner, rowType, tpe, withChangeFlag)
  }

  private def translateInput[A](
      planner: StreamPlanner,
      logicalSchema: TableSchema,
      tpe: TypeInformation[A],
      withChangeFlag: Boolean): DataStream[A] = {
    val dataStream = getInput().asInstanceOf[DataStreamRel].translateToPlan(planner)
    DataStreamConversions.convert(dataStream, logicalSchema, withChangeFlag, tpe, planner.getConfig)
  }

  /**
    * Returns the record type of the optimized plan with field names of the logical plan.
    */
  private def getTableSchema: TableSchema = {
    val fieldTypes = getInput.getRowType.getFieldList.asScala.map(_.getType)
      .map(FlinkTypeFactory.toTypeInfo)
      .map(TypeConversions.fromLegacyInfoToDataType)
      .toArray
    TableSchema.builder().fields(sink.getTableSchema.getFieldNames, fieldTypes).build()
  }

}
