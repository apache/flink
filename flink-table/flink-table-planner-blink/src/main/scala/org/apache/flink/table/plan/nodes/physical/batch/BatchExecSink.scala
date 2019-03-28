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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{BatchTableEnvironment, TableEnvironment, TableException}
import org.apache.flink.table.codegen.SinkCodeGenerator._
import org.apache.flink.table.codegen.{CodeGenUtils, CodeGeneratorContext}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.sinks.{BatchTableSink, DataStreamTableSink, TableSink}
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode to to write data into an external sink defined by a [[TableSink]].
  */
class BatchExecSink[T](
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sink: TableSink[T],
    sinkName: String)
  extends Sink(cluster, traitSet, inputRel, sink, sinkName)
  with BatchPhysicalRel
  with BatchExecNode[Any] {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecSink(cluster, traitSet, inputs.get(0), sink, sinkName)
  }

  //~ ExecNode methods -----------------------------------------------------------

  /**
    * For sink operator, the records will not pass through it, so it's DamBehavior is FULL_DAM.
    *
    * @return Returns [[DamBehavior]] of Sink.
    */
  override def getDamBehavior: DamBehavior = DamBehavior.FULL_DAM

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] = {
    List(getInput.asInstanceOf[ExecNode[BatchTableEnvironment, _]])
  }

  override protected def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[Any] = {
    val resultTransformation = sink match {
      case batchTableSink: BatchTableSink[T] =>
        val transformation = translateToStreamTransformation(withChangeFlag = false, tableEnv)
        val boundedStream = new DataStream(tableEnv.streamEnv, transformation)
        batchTableSink.emitBoundedStream(
          boundedStream, tableEnv.getConfig, tableEnv.streamEnv.getConfig).getTransformation

      case streamTableSink: DataStreamTableSink[T] =>
        // In case of table to bounded stream through BatchTableEnvironment#toBoundedStream, we
        // insert a DataStreamTableSink then wrap it as a LogicalSink, there is no real batch table
        // sink, so we do not need to invoke TableSink#emitBoundedStream and set resource, just a
        // translation to StreamTransformation is ok.
        translateToStreamTransformation(withChangeFlag = streamTableSink.withChangeFlag, tableEnv)

      case _ =>
        throw new TableException("Only Support BatchTableSink or DataStreamTableSink!")
    }
    resultTransformation.asInstanceOf[StreamTransformation[Any]]
  }

  private def translateToStreamTransformation(
      withChangeFlag: Boolean,
      tableEnv: BatchTableEnvironment): StreamTransformation[T] = {
    val resultType = sink.getOutputType
    TableEnvironment.validateType(resultType)
    val inputNode = getInputNodes.get(0)
    inputNode match {
      // Sink's input must be BatchExecNode[BaseRow] now.
      case node: BatchExecNode[BaseRow] =>
        val plan = node.translateToPlan(tableEnv)
        val typeClass = extractTableSinkTypeClass(sink)
        if (CodeGenUtils.isInternalClass(typeClass, resultType)) {
          plan.asInstanceOf[StreamTransformation[T]]
        } else {
          val (converterOperator, outputTypeInfo) = generateRowConverterOperator[T](
            CodeGeneratorContext(tableEnv.getConfig),
            tableEnv.getConfig,
            plan.getOutputType.asInstanceOf[BaseRowTypeInfo],
            "SinkConversion",
            None,
            withChangeFlag,
            resultType,
            sink
          )
          new OneInputTransformation(
            plan,
            s"SinkConversionTo${resultType.getTypeClass.getSimpleName}",
            converterOperator,
            outputTypeInfo,
            plan.getParallelism)
        }
      case _ =>
        throw new TableException("Cannot generate BoundedStream due to an invalid logical plan. " +
                                   "This is a bug and should not happen. Please file an issue.")
    }
  }


}
