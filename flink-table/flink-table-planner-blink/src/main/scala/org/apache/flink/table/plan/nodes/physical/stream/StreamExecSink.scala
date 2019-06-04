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

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamTableEnvironment, Table, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.SinkCodeGenerator.{extractTableSinkTypeClass, generateRowConverterOperator}
import org.apache.flink.table.codegen.{CodeGenUtils, CodeGeneratorContext}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{AccMode, AccModeTraitDef}
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.table.sinks._
import org.apache.flink.table.types.logical.TimestampType
import org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeCheckUtils}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode to to write data into an external sink defined by a [[TableSink]].
  */
class StreamExecSink[T](
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sink: TableSink[T],
    sinkName: String)
  extends Sink(cluster, traitSet, inputRel, sink, sinkName)
  with StreamPhysicalRel
  with StreamExecNode[Any] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean =
    sink.isInstanceOf[RetractStreamTableSink[_]]

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecSink(cluster, traitSet, inputs.get(0), sink, sinkName)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    List(getInput.asInstanceOf[ExecNode[StreamTableEnvironment, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[Any] = {
    val resultTransformation = sink match {
      case streamTableSink: StreamTableSink[T] =>
        val transformation = streamTableSink match {
          case _: RetractStreamTableSink[T] =>
            translateToStreamTransformation(withChangeFlag = true, tableEnv)

          case upsertSink: UpsertStreamTableSink[T] =>
            // check for append only table
            val isAppendOnlyTable = UpdatingPlanChecker.isAppendOnly(this)
            upsertSink.setIsAppendOnly(isAppendOnlyTable)
            translateToStreamTransformation(withChangeFlag = true, tableEnv)

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
            translateToStreamTransformation(withChangeFlag = false, tableEnv)

          case _ =>
            throw new TableException(
              "Stream Tables can only be emitted by AppendStreamTableSink, " +
                "RetractStreamTableSink, or UpsertStreamTableSink.")
        }
        val dataStream = new DataStream(tableEnv.execEnv, transformation)
        streamTableSink.emitDataStream(dataStream).getTransformation

      case streamTableSink: DataStreamTableSink[_] =>
        // In case of table to stream through BatchTableEnvironment#translateToDataStream,
        // we insert a DataStreamTableSink then wrap it as a LogicalSink, there is no real batch
        // table sink, so we do not need to invoke TableSink#emitBoundedStream and set resource,
        // just a translation to StreamTransformation is ok.
        translateToStreamTransformation(streamTableSink.withChangeFlag, tableEnv)

      case _ =>
        throw new TableException("Only Support StreamTableSink or DataStreamTableSink!")
    }
    resultTransformation.asInstanceOf[StreamTransformation[Any]]
  }

  /**
    * Translates a logical [[RelNode]] into a [[StreamTransformation]].
    *
    * @param withChangeFlag Set to true to emit records with change flags.
    * @return The [[StreamTransformation]] that corresponds to the translated [[Table]].
    */
  private def translateToStreamTransformation(
      withChangeFlag: Boolean,
      tableEnv: StreamTableEnvironment): StreamTransformation[T] = {
    val inputNode = getInput
    // if no change flags are requested, verify table is an insert-only (append-only) table.
    if (!withChangeFlag && !UpdatingPlanChecker.isAppendOnly(inputNode)) {
      throw new TableException(
        "Table is not an append-only table. " +
          "Use the toRetractStream() in order to handle add and retract messages.")
    }

    // get BaseRow plan
    val parTransformation = inputNode match {
      // Sink's input must be StreamExecNode[BaseRow] now.
      case node: StreamExecNode[BaseRow] =>
        node.translateToPlan(tableEnv)
      case _ =>
        throw new TableException("Cannot generate DataStream due to an invalid logical plan. " +
                                   "This is a bug and should not happen. Please file an issue.")
    }
    val logicalType = inputNode.getRowType
    val rowtimeFields = logicalType.getFieldList
                        .filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))

    val convType = if (rowtimeFields.size > 1) {
      throw new TableException(
        s"Found more than one rowtime field: [${rowtimeFields.map(_.getName).mkString(", ")}] in " +
          s"the table that should be converted to a DataStream.\n" +
          s"Please select the rowtime field that should be used as event-time timestamp for the " +
          s"DataStream by casting all other fields to TIMESTAMP.")
    } else if (rowtimeFields.size == 1) {
      val origRowType = parTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo]
      val convFieldTypes = origRowType.getLogicalTypes.map { t =>
        if (TypeCheckUtils.isRowTime(t)) {
          new TimestampType(3)
        } else {
          t
        }
      }
      new BaseRowTypeInfo(convFieldTypes, origRowType.getFieldNames)
    } else {
      parTransformation.getOutputType
    }
    val resultDataType = sink.getConsumedDataType
    val resultType = fromDataTypeToLegacyInfo(resultDataType)
    val typeClass = extractTableSinkTypeClass(sink)
    if (CodeGenUtils.isInternalClass(typeClass, resultDataType)) {
      parTransformation.asInstanceOf[StreamTransformation[T]]
    } else {
      val (converterOperator, outputTypeInfo) = generateRowConverterOperator[T](
        CodeGeneratorContext(tableEnv.getConfig),
        tableEnv.getConfig,
        convType.asInstanceOf[BaseRowTypeInfo],
        "SinkConversion",
        None,
        withChangeFlag,
        resultType,
        sink
      )
      new OneInputTransformation(
        parTransformation,
        s"SinkConversionTo${resultType.getTypeClass.getSimpleName}",
        converterOperator,
        outputTypeInfo,
        parTransformation.getParallelism)
    }

  }

}
