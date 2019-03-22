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
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{BatchTableEnvironment, Table, TableEnvironment, TableException}
import org.apache.flink.table.codegen.CodeGenUtils
import org.apache.flink.table.codegen.SinkCodeGenerator._
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.nodes.calcite.Sink
import org.apache.flink.table.sinks.{BatchTableSink, DataStreamTableSink, TableSink}

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

  /**
    * Returns [[DamBehavior]] of this node.
    */
  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  /**
    * Returns an array of this node's inputs. If there are no inputs,
    * returns an empty list, not null.
    *
    * @return Array of this node's inputs
    */
  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] = {
    List(getInput.asInstanceOf[ExecNode[BatchTableEnvironment, _]])
  }

  /**
    * Internal method, translates this node into a Flink operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override protected def translateToPlanInternal(
    tableEnv: BatchTableEnvironment): StreamTransformation[Any] = {
    val convertTransformation = sink match {
      case _: BatchTableSink[T] =>
        translate(withChangeFlag = false, tableEnv)
      case streamTableSink: DataStreamTableSink[T] =>
        translate(withChangeFlag = streamTableSink.withChangeFlag, tableEnv)
      case _ =>
        throw new TableException("Only Support BatchTableSink or BatchCompatibleStreamTableSink")
    }
    // In case of table to bounded stream through BatchTableEnvironment#toBoundedStream, we insert a
    // DataStreamTableSink then wrap it as a LogicalSink, there is no real batch table sink, so
    // we do not need to invoke TableSink#emitBoundedStream and set resource, just a translation to
    // StreamTransformation is ok.
    val resultTransformation = if (sink.isInstanceOf[DataStreamTableSink[T]]) {
      convertTransformation
    } else {
      val stream = new DataStream(tableEnv.streamEnv, convertTransformation)
      emitBoundedStreamSink(stream, tableEnv).getTransformation
    }
    resultTransformation.asInstanceOf[StreamTransformation[Any]]
  }

  private def emitBoundedStreamSink(
    boundedStream: DataStream[T],
    tableEnv: BatchTableEnvironment): DataStreamSink[_] = {
    val config = tableEnv.getConfig
    sink match {
      case sinkBatch: BatchTableSink[T] =>
        sinkBatch.emitBoundedStream(boundedStream, config, tableEnv.streamEnv.getConfig)

      case _ => throw new TableException("BatchTableSink or CompatibleStreamTableSink " +
                                           "required to emit batch exec Table")
    }
  }

  /**
    * Translates a logical [[RelNode]] into a [[StreamTransformation]].
    * Converts to target type if necessary.
    *
    * @return The [[StreamTransformation]] that corresponds to the translated [[Table]].
    */
  private def translate(
    withChangeFlag: Boolean,
    tableEnv: BatchTableEnvironment): StreamTransformation[T] = {
    val resultType = sink.getOutputType
    TableEnvironment.validateType(resultType)
    val inputNode = getInputNodes.get(0)
    inputNode match {
      // Sink's input must be BatchExecNode[BaseRow] now.
      case node: BatchExecNode[BaseRow] =>
        val plan = node.translateToPlan(tableEnv)
        // TODO support SinkConversion after FLINK-11974 is done
        val typeClass = extractTableSinkTypeClass(sink)
        if (CodeGenUtils.isInternalClass(typeClass, resultType)) {
          plan.asInstanceOf[StreamTransformation[T]]
        } else {
          throw new TableException(
            s"Not support SinkConvention now."
          )
        }
      case _ =>
        throw new TableException("Cannot generate BoundedStream due to an invalid logical plan. " +
                                   "This is a bug and should not happen. Please file an issue.")
    }
  }


}
