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

package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.`trait`.{MiniBatchIntervalTraitDef, MiniBatchMode}
import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.utils.TableConfigUtils.getMillisecondFromConfigDuration
import org.apache.flink.table.runtime.operators.wmassigners.{MiniBatchAssignerOperator, MiniBatchedWatermarkAssignerOperator, WatermarkAssignerOperator}
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
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

    val value = if (miniBatchInterval.mode == MiniBatchMode.None ||
      miniBatchInterval.interval == 0) {
      // 1. redundant watermark definition in DDL
      // 2. existing window aggregate
      // 3. operator requiring watermark, but minibatch is not enabled
      "None"
    } else if (miniBatchInterval.mode == MiniBatchMode.ProcTime) {
      val tableConfig = cluster.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
      val miniBatchLatency = getMillisecondFromConfigDuration(tableConfig,
        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY)
      Preconditions.checkArgument(miniBatchLatency > 0,
        "MiniBatch latency must be greater that 0 ms.", null)
      s"Proctime, ${miniBatchLatency}ms"
    } else if (miniBatchInterval.mode == MiniBatchMode.RowTime) {
      s"Rowtime, ${miniBatchInterval.interval}ms"
    } else {
      throw new TableException(s"Unsupported mode: $miniBatchInterval")
    }
    super.explainTerms(pw).item("miniBatchInterval", value)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[BaseRow] = {
    val inputTransformation = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[BaseRow]]

    val config = planner.getTableConfig
    val inferredInterval = getTraitSet.getTrait(
      MiniBatchIntervalTraitDef.INSTANCE).getMiniBatchInterval
    val idleTimeout = getMillisecondFromConfigDuration(config,
      ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT)

    val operator = if (inferredInterval.mode == MiniBatchMode.None ||
      inferredInterval.interval == 0) {
      require(rowtimeFieldIndex.isDefined, "rowtimeFieldIndex should not be None")
      require(watermarkDelay.isDefined, "watermarkDelay should not be None")
      // 1. redundant watermark definition in DDL
      // 2. existing window aggregate
      // 3. operator requiring watermark, but minibatch is not enabled
      new WatermarkAssignerOperator(rowtimeFieldIndex.get, watermarkDelay.get, idleTimeout)
    } else if (inferredInterval.mode == MiniBatchMode.ProcTime) {
      new MiniBatchAssignerOperator(inferredInterval.interval)
    } else {
      require(rowtimeFieldIndex.isDefined, "rowtimeFieldIndex should not be None")
      require(watermarkDelay.isDefined, "watermarkDelay should not be None")
      new MiniBatchedWatermarkAssignerOperator(
        rowtimeFieldIndex.get,
        watermarkDelay.get,
        0,
        idleTimeout,
        inferredInterval.interval)
    }

    val outputRowTypeInfo = BaseRowTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))
    val transformation = new OneInputTransformation[BaseRow, BaseRow](
      inputTransformation,
      getRelDetailedDescription,
      operator,
      outputRowTypeInfo,
      inputTransformation.getParallelism)
    transformation
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
