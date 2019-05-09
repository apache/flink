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

import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfigOptions, TableException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{MiniBatchIntervalTraitDef, MiniBatchMode}
import org.apache.flink.table.plan.nodes.calcite.WatermarkAssigner
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.optimize.program.FlinkOptimizeContext
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter}

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for [[WatermarkAssigner]].
  */
class StreamExecWatermarkAssigner(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputRel: RelNode,
    rowtimeFieldIndex: Option[Int],
    watermarkDelay: Option[Long])
  extends WatermarkAssigner(cluster, traits, inputRel, rowtimeFieldIndex, watermarkDelay)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecWatermarkAssigner(
      cluster,
      traitSet,
      inputs.get(0),
      rowtimeFieldIndex,
      watermarkDelay)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val miniBatchInterval = traits.getTrait(MiniBatchIntervalTraitDef.INSTANCE).getMiniBatchInterval

    val value = miniBatchInterval.mode match {
      case MiniBatchMode.None =>
        // 1. operator requiring watermark, but minibatch is not enabled
        // 2. redundant watermark definition in DDL
        // 3. existing window, and window minibatch is disabled.
        "None"
      case MiniBatchMode.ProcTime =>
        val config = cluster.getPlanner.getContext.asInstanceOf[FlinkOptimizeContext].getTableConfig
        val miniBatchLatency = config.getConf.getLong(
          TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)
        Preconditions.checkArgument(miniBatchLatency > 0,
          "MiniBatch latency must be greater that 0.", null)
        s"Proctime, ${miniBatchLatency}ms"
      case MiniBatchMode.RowTime =>
        s"Rowtime, ${miniBatchInterval.interval}ms"
      case o => throw new TableException(s"Unsupported mode: $o")
    }
    super.explainTerms(pw).item("miniBatchInterval", value)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamTableEnvironment, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    throw new TableException("Implements this")
  }

}

object StreamExecWatermarkAssigner {

  def createRowTimeWatermarkAssigner(
      cluster: RelOptCluster,
      traits: RelTraitSet,
      inputRel: RelNode,
      rowtimeFieldIndex: Int,
      watermarkDelay: Long): StreamExecWatermarkAssigner = {
    new StreamExecWatermarkAssigner(
      cluster,
      traits,
      inputRel,
      Some(rowtimeFieldIndex),
      Some(watermarkDelay))
  }

  def createIngestionTimeWatermarkAssigner(
      cluster: RelOptCluster,
      traits: RelTraitSet,
      inputRel: RelNode): StreamExecWatermarkAssigner = {
    new StreamExecWatermarkAssigner(cluster, traits, inputRel, None, None)
  }
}
