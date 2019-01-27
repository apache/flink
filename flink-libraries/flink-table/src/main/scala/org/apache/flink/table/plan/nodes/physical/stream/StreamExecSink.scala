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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.api.common.operators.ResourceSpec
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamTableEnvironment, Table, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.SinkCodeGenerator.generateRowConverterOperator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{AccMode, AccModeTraitDef}
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.nodes.exec.{BaseStreamExecNode, RowStreamExecNode}
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.{SinkUtil, UpdatingPlanChecker}
import org.apache.flink.table.runtime.AbstractProcessStreamOperator
import org.apache.flink.table.sinks._
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.NodeResourceUtil

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
  * Flink RelNode to write data into an external sink defined by a [[TableSink]].
  */
class StreamExecSink[T](
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    sink: TableSink[T],
    sinkName: String)
  extends Sink(cluster, traitSet, input, sink, sinkName)
  with StreamPhysicalRel
  with BaseStreamExecNode[Any] {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecSink(cluster, traitSet, inputs.get(0), sink, sinkName)
  }

  override def isDeterministic: Boolean = true

  /**
    * Whether the [[FlinkRelNode]] requires retraction messages or not.
    */
  override def needsUpdatesAsRetraction(input: RelNode): Boolean =
    sink.isInstanceOf[BaseRetractStreamTableSink[_]] ||
      (isDataStreamTableSink && sink.asInstanceOf[DataStreamTableSink[T]].updatesAsRetraction)

  private val isDataStreamTableSink: Boolean = sink.isInstanceOf[DataStreamTableSink[T]]

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  /**
    * Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv The [[StreamTableEnvironment]] of the translated Table.
    * @return StreamTransformation
    */
  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[Any] = {
    val convertTransformation = sink match {

      case _: BaseRetractStreamTableSink[T] =>
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        translate(withChangeFlag = true, tableEnv)

      case upsertSink: BaseUpsertStreamTableSink[T] =>
        // check for append only table
        val isAppendOnlyTable = UpdatingPlanChecker.isAppendOnly(this)
        upsertSink.setIsAppendOnly(isAppendOnlyTable)
        // translate the Table into a DataStream and provide the type that the TableSink expects.
        translate(withChangeFlag = true, tableEnv)

      case _: AppendStreamTableSink[T] =>
        // verify table is an insert-only (append-only) table
        if (!UpdatingPlanChecker.isAppendOnly(this)) {
          throw new TableException(
            "AppendStreamTableSink requires that Table has only insert changes.")
        }

        val accMode = this.getTraitSet.getTrait(AccModeTraitDef.INSTANCE).getAccMode
        if (accMode == AccMode.AccRetract) {
          throw new TableException(
            "AppendStreamTableSink can not be used to output retraction messages.")
        }

        // translate the Table into a DataStream and provide the type that the TableSink expects.
        translate(withChangeFlag = false, tableEnv)

      case s: DataStreamTableSink[_] =>
        translate(s.withChangeFlag, tableEnv)
      case _ =>
        throw new TableException("Stream Tables can only be emitted by AppendStreamTableSink, " +
                                   "RetractStreamTableSink, or UpsertStreamTableSink.")
    }
    val resultTransformation = if (isDataStreamTableSink) {
      convertTransformation
    } else {
      val stream = new DataStream(tableEnv.execEnv, convertTransformation)
      emitDataStream(tableEnv.getConfig.getConf, stream).getTransformation
    }
    resultTransformation.asInstanceOf[StreamTransformation[Any]]
  }

  /**
    * Translates a logical [[RelNode]] into a [[StreamTransformation]].
    *
    * @param withChangeFlag Set to true to emit records with change flags.
    * @return The [[StreamTransformation]] that corresponds to the translated [[Table]].
    */
  private def translate(
    withChangeFlag: Boolean,
    tableEnv: StreamTableEnvironment): StreamTransformation[T] = {
    val inputNode = getInput
    val resultType = sink.getOutputType
    // if no change flags are requested, verify table is an insert-only (append-only) table.
    if (!withChangeFlag && !UpdatingPlanChecker.isAppendOnly(inputNode)) {
      throw new TableException(
        "Table is not an append-only table. " +
          "Use the toRetractStream() in order to handle add and retract messages.")
    }

    // get BaseRow plan
    val translateStream = inputNode match {
      // Sink's input must be RowStreamExecNode now.
      case node: RowStreamExecNode =>
        node.translateToPlan(tableEnv)
      case _ =>
        throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
                                   "This is a bug and should not happen. Please file an issue.")
    }
    val parTransformation = if (isDataStreamTableSink) {
      translateStream
    } else {
      SinkUtil.createPartitionTransformation(sink, translateStream)
    }
    val logicalType = inputNode.getRowType
    val rowtimeFields = logicalType.getFieldList
                        .filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))

    // convert the input type for the conversion mapper
    // the input will be changed in the OutputRowtimeProcessFunction later
    val convType = if (rowtimeFields.size > 1) {
      throw new TableException(
        s"Found more than one rowtime field: [${rowtimeFields.map(_.getName).mkString(", ")}] in " +
          s"the table that should be converted to a DataStream.\n" +
          s"Please select the rowtime field that should be used as event-time timestamp for the " +
          s"DataStream by casting all other fields to TIMESTAMP.")
    } else if (rowtimeFields.size == 1) {

      val origRowType = parTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo]
      val convFieldTypes = origRowType.getFieldTypes.map { t =>
        if (FlinkTypeFactory.isRowtimeIndicatorType(t)) {
          SqlTimeTypeInfo.TIMESTAMP
        } else {
          t
        }
      }
      new BaseRowTypeInfo(convFieldTypes, origRowType.getFieldNames)
    } else {
      parTransformation.getOutputType
    }
    val config = tableEnv.getConfig
    val optionRowTimeField = if (rowtimeFields.isEmpty) None else Some(rowtimeFields.head.getIndex)
    val ctx = CodeGeneratorContext(config, supportReference = true)
              .setOperatorBaseClass(classOf[AbstractProcessStreamOperator[_]])
    val (converterOperator, outputType) = generateRowConverterOperator[BaseRow, T](
      config,
      ctx,
      convType.asInstanceOf[BaseRowTypeInfo],
      logicalType,
      "DataStreamSinkConversion",
      optionRowTimeField,
      withChangeFlag,
      resultType,
      sink)

    val convertTransformation = converterOperator match {
      case None => parTransformation
      case Some(operator) => {
        val transformation = new OneInputTransformation(
          parTransformation,
          s"SinkConversion to ${TypeUtils.getExternalClassForType(resultType).getSimpleName}",
          operator,
          outputType,
          parTransformation.getParallelism
        )
        val defaultRes = NodeResourceUtil.getDefaultResourceSpec(tableEnv.getConfig.getConf)
        transformation.setResources(defaultRes, defaultRes)
        transformation
      }
    }
    convertTransformation.asInstanceOf[StreamTransformation[T]]
  }

  /**
    * emit [[DataStream]].
    *
    * @param dataStream The [[DataStream]] to emit.
    */
  private def emitDataStream(
      tableConf: Configuration,
      dataStream: DataStream[T]) : DataStreamSink[_] = {
    val boundedSink = sink match {

      case retractSink: BaseRetractStreamTableSink[T] =>
        // Give the DataStream to the TableSink to emit it.
        retractSink.emitDataStream(dataStream)

      case upsertSink: BaseUpsertStreamTableSink[T] =>
        // Give the DataStream to the TableSink to emit it.
        upsertSink.emitDataStream(dataStream)

      case appendSink: AppendStreamTableSink[T] =>
        // Give the DataStream to the TableSink to emit it.
        appendSink.emitDataStream(dataStream)

      case _ =>
        throw new TableException("Stream Tables can only be emitted by AppendStreamTableSink, " +
                                   "RetractStreamTableSink, or UpsertStreamTableSink.")
    }
    val sinkTransformation = boundedSink.getTransformation
    val resourceSpec = sinkTransformation.getMinResources
    if (resourceSpec == null || resourceSpec == ResourceSpec.DEFAULT) {
      val heapMem = NodeResourceUtil.getSinkMem(tableConf)
      val directMem = NodeResourceUtil.getSinkDirectMem(tableConf)
      val resource = NodeResourceUtil.getResourceSpec(tableConf, heapMem, directMem)
      sinkTransformation.setResources(resource, resource)
    }
    boundedSink
  }
}
