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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.plan.`trait`.{MiniBatchInterval, MiniBatchIntervalTrait, MiniBatchIntervalTraitDef, MiniBatchMode}
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecDataStreamScan, StreamExecGroupWindowAggregate, StreamExecMiniBatchAssigner, StreamExecLegacyTableSourceScan, StreamExecWatermarkAssigner, StreamPhysicalRel}
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode

import scala.collection.JavaConversions._

/**
  * Planner rule that infers the mini-batch interval of minibatch asssigner.
  *
  * This rule could handle the following two kinds of operator:
  * 1. supports operators which supports mini-batch and does not require watermark, e.g.
  * group aggregate. In this case, [[StreamExecMiniBatchAssigner]] with Protime mode will be
  * created if not exist, and the interval value will be set as
  * [[ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY]].
  * 2. supports operators which requires watermark, e.g. window join, window aggregate.
  * In this case, [[StreamExecWatermarkAssigner]] already exists, and its MiniBatchIntervalTrait
  * will be updated as the merged intervals from its outputs.
  * Currently, mini-batched window aggregate is not supported, and will be introduced later.
  *
  * NOTES: This rule only supports HepPlanner with TOP_DOWN match order.
  */
class MiniBatchIntervalInferRule extends RelOptRule(
  operand(classOf[StreamPhysicalRel], any()),
  "MiniBatchIntervalInferRule") {

  /**
    * Get all children RelNodes of a RelNode.
    *
    * @param parent The parent RelNode
    * @return All child nodes
    */
  def getInputs(parent: RelNode): Seq[RelNode] = {
    parent.getInputs.map(_.asInstanceOf[HepRelVertex].getCurrentRel)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val rel: StreamPhysicalRel = call.rel(0)
    val miniBatchIntervalTrait = rel.getTraitSet.getTrait(MiniBatchIntervalTraitDef.INSTANCE)
    val inputs = getInputs(rel)
    val config = FlinkRelOptUtil.getTableConfigFromContext(rel)
    val miniBatchEnabled = config.getConfiguration.getBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)

    val updatedTrait = rel match {
      case _: StreamExecGroupWindowAggregate =>
        // TODO introduce mini-batch window aggregate later
        MiniBatchIntervalTrait.NO_MINIBATCH

      case _: StreamExecWatermarkAssigner => MiniBatchIntervalTrait.NONE

      case _: StreamExecMiniBatchAssigner => MiniBatchIntervalTrait.NONE

      case _ => if (rel.requireWatermark && miniBatchEnabled) {
        val mergedInterval = FlinkRelOptUtil.mergeMiniBatchInterval(
          miniBatchIntervalTrait.getMiniBatchInterval, MiniBatchInterval(0, MiniBatchMode.RowTime))
        new MiniBatchIntervalTrait(mergedInterval)
      } else {
        miniBatchIntervalTrait
      }
    }

    // propagate parent's MiniBatchInterval to children.
    val updatedInputs = inputs.map { input =>
      // add mini-batch watermark assigner node.
      if (shouldAppendMiniBatchAssignerNode(input)) {
        new StreamExecMiniBatchAssigner(
          input.getCluster,
          input.getTraitSet,
          // attach NONE trait for all of the inputs of MiniBatchAssigner,
          // as they are leaf nodes and don't need to do propagate
          input.copy(input.getTraitSet.plus(MiniBatchIntervalTrait.NONE), input.getInputs)
        )
      } else {
        val originTrait = input.getTraitSet.getTrait(MiniBatchIntervalTraitDef.INSTANCE)
        if (originTrait != updatedTrait) {
          // calculate new MiniBatchIntervalTrait according parent's miniBatchInterval
          // and the child's original miniBatchInterval.
          val mergedMiniBatchInterval = FlinkRelOptUtil.mergeMiniBatchInterval(
            originTrait.getMiniBatchInterval, updatedTrait.getMiniBatchInterval)
          val inferredTrait = new MiniBatchIntervalTrait(mergedMiniBatchInterval)
          input.copy(input.getTraitSet.plus(inferredTrait), input.getInputs)
        } else {
          input
        }
      }
    }
    // update parent if a child was updated
    if (inputs != updatedInputs) {
      val newRel = rel.copy(rel.getTraitSet, updatedInputs)
      call.transformTo(newRel)
    }
  }

  private def shouldAppendMiniBatchAssignerNode(node: RelNode): Boolean = {
    val mode = node.getTraitSet
      .getTrait(MiniBatchIntervalTraitDef.INSTANCE)
      .getMiniBatchInterval
      .mode
    node match {
      case _: StreamExecDataStreamScan | _: StreamExecLegacyTableSourceScan =>
        // append minibatch node if the mode is not NONE and reach a source leaf node
        mode == MiniBatchMode.RowTime || mode == MiniBatchMode.ProcTime
      case _: StreamExecWatermarkAssigner  =>
        // append minibatch node if it is rowtime mode and the child is watermark assigner
        // TODO: if it is ProcTime mode, we also append a minibatch node for now.
        //  Because the downstream can be a regular aggregate and the watermark assigner
        //  might be redundant. In FLINK-14621, we will remove redundant watermark assigner,
        //  then we can remove the ProcTime condition.
        mode == MiniBatchMode.RowTime || mode == MiniBatchMode.ProcTime
      case _ =>
        // others do not append minibatch node
        false
    }
  }
}

object MiniBatchIntervalInferRule {
  val INSTANCE: RelOptRule = new MiniBatchIntervalInferRule
}
