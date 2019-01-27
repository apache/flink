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
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import java.util

import org.apache.flink.table.api._
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.SinkCodeGenerator.generateRowConverterOperator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.exec.{BaseBatchExecNode, RowBatchExecNode}
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.SinkUtil
import org.apache.flink.table.sinks.{BatchCompatibleStreamTableSink, BatchTableSink, DataStreamTableSink, TableSink}
import org.apache.flink.table.typeutils.{BaseRowTypeInfo, TypeUtils}
import org.apache.flink.table.util.NodeResourceUtil
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType

/**
  * Flink RelNode to write data into an external sink defined by a [[TableSink]].
  */
class BatchExecSink[T](
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    sink: TableSink[T],
    sinkName: String)
  extends Sink(cluster, traitSet, input, sink, sinkName)
  with BatchPhysicalRel
  with BaseBatchExecNode[Any] {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecSink(cluster, traitSet, inputs.get(0), sink, sinkName)
  }

  override def isDeterministic: Boolean = true

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def accept(visitor: BatchExecNodeVisitor): Unit = visitor.visit(this)

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  /**
    * Internal method, translates the [[org.apache.flink.table.plan.nodes.exec.BatchExecNode]]
    * into a Batch operator.
    *
    * @param tableEnv    The [[StreamTableEnvironment]] of the translated Table.
    * @return StreamTransformation
    */
  override def translateToPlanInternal(
    tableEnv: BatchTableEnvironment): StreamTransformation[Any] = {
    val result = sink match {
      case _: BatchTableSink[T] =>
        translate(withChangeFlag = false, tableEnv)
      case _: BatchCompatibleStreamTableSink[T] =>
        translate(withChangeFlag = true, tableEnv)
      case streamTableSink: DataStreamTableSink[T] =>
        translate(withChangeFlag = streamTableSink.withChangeFlag, tableEnv)
      case _ =>
        throw new TableException("Only Support BatchTableSink or BatchCompatibleStreamTableSink")
    }
    // In case of table to bounded stream through BatchTableEnvironment#toBoundedStream, we insert a
    // DataStreamTableSink then wrap it as a LogicalSink, there is no real batch table sink, so
    // we do not need to invoke TableSink#emitBoundedStream and set resource, just a translation to
    // StreamTransformation is ok.
    if (sink.isInstanceOf[DataStreamTableSink[T]]) {
      result.getTransformation.asInstanceOf[StreamTransformation[Any]]
    } else {
      emitBoundedStreamSink(result, tableEnv).getTransformation.
        asInstanceOf[StreamTransformation[Any]]
    }
  }

  private def emitBoundedStreamSink(
    boundedStream: DataStream[T],
    tableEnv: BatchTableEnvironment): DataStreamSink[_] = {
    val config = tableEnv.getConfig
    sink match {
      case sinkBatch: BatchTableSink[T] =>
        val boundedSink = sinkBatch.emitBoundedStream(boundedStream, config,
                                                      tableEnv.streamEnv.getConfig)
        assignDefaultResourceAndParallelism(boundedStream, boundedSink, config)
        boundedSink
      case compatible: BatchCompatibleStreamTableSink[T] =>
        val boundedSink = compatible.emitBoundedStream(boundedStream)
        assignDefaultResourceAndParallelism(boundedStream, boundedSink, config)
        boundedSink
      case _ => throw new TableException("BatchTableSink or CompatibleStreamTableSink " +
                                           "required to emit batch exec Table")
    }
  }

  private def assignDefaultResourceAndParallelism(
    boundedStream: DataStream[_],
    boundedSink: DataStreamSink[_],
    tableConfig: TableConfig) {
    val sinkTransformation = boundedSink.getTransformation
    val streamTransformation = boundedStream.getTransformation
    val resourceSpec = sinkTransformation.getMinResources
    if (resourceSpec == null) {
      val heapMem = NodeResourceUtil.getSinkMem(tableConfig.getConf)
      val directMem = NodeResourceUtil.getSinkDirectMem(tableConfig.getConf)
      val resource = NodeResourceUtil.getResourceSpec(tableConfig.getConf, heapMem, directMem)
      sinkTransformation.setResources(resource, resource)
    }
    if (sinkTransformation.getMaxParallelism > 0) {
      sinkTransformation.setParallelism(sinkTransformation.getMaxParallelism)
    } else {
      val configSinkParallelism = NodeResourceUtil.getSinkParallelism(tableConfig.getConf)
      if (configSinkParallelism > 0) {
        sinkTransformation.setParallelism(configSinkParallelism)
      } else if (streamTransformation.getParallelism > 0) {
        sinkTransformation.setParallelism(streamTransformation.getParallelism)
      }
    }
  }

  /**
    * Translates a logical [[RelNode]] into a [[DataStream]].
    * Converts to target type if necessary.
    *
    * @return The [[DataStream]] that corresponds to the translated [[Table]].
    */
  private def translate(
    withChangeFlag: Boolean,
    tableEnv: BatchTableEnvironment): DataStream[T] = {
    val resultType = sink.getOutputType
    TableEnvironment.validateType(resultType)
    val inputNode = getInputNodes.get(0)
    inputNode match {
      // Sink's input must be RowBatchExecNode now.
      case node: RowBatchExecNode =>
        val plan = node.translateToPlan(tableEnv)
        val parTransformation = SinkUtil.createPartitionTransformation(sink, plan)
        val convertTransformation =
          getConversionMapper[BaseRow, T](
            parTransformation,
            parTransformation.getOutputType.asInstanceOf[BaseRowTypeInfo],
            inputNode.getFlinkPhysicalRel.getRowType,
            "BoundedStreamSinkConversion",
            withChangeFlag,
            resultType,
            tableEnv.getConfig,
            sink)
        new DataStream(tableEnv.streamEnv, convertTransformation)
      case _ =>
        throw new TableException("Cannot generate BoundedStream due to an invalid logical plan. " +
                               "This is a bug and should not happen. Please file an issue.")
    }
  }


  /**
    * If the input' outputType is incompatible with the external type, here need create a final
    * converter that maps the internal row type to external type.
    *
    * @param physicalTypeInfo the input of the sink
    * @param relType          the input relDataType with correct field names
    * @param name             name of the map operator. Must not be unique but has to be a
    *                         valid Java class identifier.
    * @param withChangeFlag   Set to true to emit records with change flags.
    * @param resultType       The [[DataType]] of the resulting [[DataStream]].
    */
  private def getConversionMapper[IN, OUT](
    input: StreamTransformation[IN],
    physicalTypeInfo: BaseRowTypeInfo,
    relType: RelDataType,
    name: String,
    withChangeFlag: Boolean,
    resultType: DataType,
    config: TableConfig,
    sink: TableSink[_]): StreamTransformation[OUT] = {
    val (converterOperator, outputTypeInfo) = generateRowConverterOperator[IN, OUT](
      config,
      CodeGeneratorContext(config, supportReference = true),
      physicalTypeInfo,
      relType,
      name,
      None,
      withChangeFlag,
      resultType,
      sink)
    converterOperator match {
      case None => input.asInstanceOf[StreamTransformation[OUT]]
      case Some(operator) =>
        val transformation = new OneInputTransformation(
          input,
          s"SinkConversion to ${TypeUtils.getExternalClassForType(resultType).getSimpleName}",
          operator,
          outputTypeInfo,
          input.getParallelism)
        val defaultResource = NodeResourceUtil.getDefaultResourceSpec(config.getConf)
        transformation.setResources(defaultResource, defaultResource)
        transformation
    }
  }

}
