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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.codegen.SinkCodeGenerator._
import org.apache.flink.table.planner.codegen.{CodeGenUtils, CodeGeneratorContext}
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.calcite.LegacySink
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, LegacyBatchExecNode}
import org.apache.flink.table.planner.plan.utils.UpdatingPlanChecker
import org.apache.flink.table.planner.sinks.DataStreamTableSink
import org.apache.flink.table.runtime.types.ClassLogicalTypeConverter
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.sinks.{RetractStreamTableSink, StreamTableSink, TableSink, UpsertStreamTableSink}
import org.apache.flink.table.types.DataType

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.lang.reflect.Modifier
import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode to to write data into an external sink defined by a [[TableSink]].
  */
class BatchExecLegacySink[T](
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sink: TableSink[T],
    sinkName: String)
  extends LegacySink(cluster, traitSet, inputRel, sink, sinkName)
  with BatchPhysicalRel
  with LegacyBatchExecNode[Any] {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecLegacySink(cluster, traitSet, inputs.get(0), sink, sinkName)
  }

  //~ ExecNode methods -----------------------------------------------------------

  // the input records will not trigger any output of a sink because it has no output,
  // so it's dam behavior is BLOCKING
  override def getInputEdges: util.List[ExecEdge] = List(
    ExecEdge.builder()
      .damBehavior(ExecEdge.DamBehavior.BLOCKING)
      .build())

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[Any] = {
    val resultTransformation = sink match {
      case streamTableSink: StreamTableSink[T] =>
        val transformation = streamTableSink match {
          case _: RetractStreamTableSink[T] =>
            translateToTransformation(withChangeFlag = true, planner)

          case upsertSink: UpsertStreamTableSink[T] =>
            upsertSink.setIsAppendOnly(true)
            upsertSink.setKeyFields(
              UpdatingPlanChecker.getUniqueKeyForUpsertSink(this, planner, upsertSink).orNull)
            translateToTransformation(withChangeFlag = true, planner)

          case _ =>
            translateToTransformation(withChangeFlag = false, planner)
        }
        val boundedStream = new DataStream(planner.getExecEnv, transformation)
        val dsSink = streamTableSink.consumeDataStream(boundedStream)
        if (dsSink == null) {
          throw new TableException("The StreamTableSink#consumeDataStream(DataStream) must be " +
            "implemented and return the sink transformation DataStreamSink. " +
            s"However, ${sink.getClass.getCanonicalName} doesn't implement this method.")
        }
        dsSink.getTransformation
      case dsTableSink: DataStreamTableSink[T] =>
        // In case of table to bounded stream through Batchplannerironment#toBoundedStream, we
        // insert a DataStreamTableSink then wrap it as a LogicalSink, there is no real batch table
        // sink, so we do not need to invoke TableSink#emitBoundedStream and set resource, just a
        // translation to Transformation is ok.
        translateToTransformation(withChangeFlag = dsTableSink.withChangeFlag, planner)

      case _ =>
        throw new TableException(s"Only Support StreamTableSink! " +
          s"However ${sink.getClass.getCanonicalName} is not a StreamTableSink.")
    }
    resultTransformation.asInstanceOf[Transformation[Any]]
  }

  private def translateToTransformation(
      withChangeFlag: Boolean,
      planner: BatchPlanner): Transformation[T] = {
    val config = planner.getTableConfig
    val resultDataType = sink.getConsumedDataType
    validateType(resultDataType)
    val inputNode = getInputNodes.get(0)
    inputNode match {
      // Sink's input must be LegacyBatchExecNode[RowData] or BatchExecNode[RowData] now.
      case _: LegacyBatchExecNode[RowData] | _: BatchExecNode[RowData] =>
        val plan = inputNode.translateToPlan(planner).asInstanceOf[Transformation[T]]
        if (CodeGenUtils.isInternalClass(resultDataType)) {
          plan
        } else {
          val (converterOperator, outputTypeInfo) = generateRowConverterOperator[T](
            CodeGeneratorContext(config),
            config,
            plan.getOutputType.asInstanceOf[InternalTypeInfo[RowData]].toRowType,
            sink,
            withChangeFlag,
            "SinkConversion"
          )
          ExecNodeUtil.createOneInputTransformation(
            plan,
            s"SinkConversionTo${resultDataType.getConversionClass.getSimpleName}",
            converterOperator,
            outputTypeInfo,
            plan.getParallelism,
            0)
        }
      case _ =>
        throw new TableException("Cannot generate BoundedStream due to an invalid logical plan. " +
                                   "This is a bug and should not happen. Please file an issue.")
    }
  }

  /**
    * Validate if class represented by the typeInfo is static and globally accessible
    * @param dataType type to check
    * @throws TableException if type does not meet these criteria
    */
  private def validateType(dataType: DataType): Unit = {
    var clazz = dataType.getConversionClass
    if (clazz == null) {
      clazz = ClassLogicalTypeConverter.getDefaultExternalClassForType(dataType.getLogicalType)
    }
    if ((clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) ||
      !Modifier.isPublic(clazz.getModifiers) ||
      clazz.getCanonicalName == null) {
      throw new TableException(
        s"Class '$clazz' described in type information '$dataType' must be " +
          s"static and globally accessible.")
    }
  }
}
