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
import org.apache.flink.configuration.MemorySize
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, NestedLoopJoinCodeGenerator}
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil
import org.apache.flink.table.runtime.typeutils.{BinaryRowDataSerializer, InternalTypeInfo}

import org.apache.calcite.plan._
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for nested-loop [[Join]].
  */
class BatchExecNestedLoopJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType,
    // true if LHS is build side, else RHS is build side
    val leftIsBuild: Boolean,
    // true if one side returns single row, else false
    val singleRowJoin: Boolean)
  extends BatchExecJoinBase(cluster, traitSet, leftRel, rightRel, condition, joinType) {

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new BatchExecNestedLoopJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType,
      leftIsBuild,
      singleRowJoin)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("build", if (leftIsBuild) "left" else "right")
      .itemIf("singleRowJoin", singleRowJoin, singleRowJoin)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)
    if (leftRowCnt == null || rightRowCnt == null) {
      return null
    }

    val buildRel = if (leftIsBuild) getLeft else getRight
    val buildRows = mq.getRowCount(buildRel)
    val buildRowSize = mq.getAverageRowSize(buildRel)
    val memoryCost = buildRows *
      (buildRowSize + BinaryRowDataSerializer.LENGTH_SIZE_IN_BYTES) * shuffleBuildCount(mq)
    val cpuCost = leftRowCnt * rightRowCnt
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    val cost = costFactory.makeCost(mq.getRowCount(this), cpuCost, 0, 0, memoryCost)
    if (singleRowJoin) {
      // Make single row join more preferable than non-single row join.
      cost.multiplyBy(0.99)
    } else {
      cost
    }
  }

  private def shuffleBuildCount(mq: RelMetadataQuery): Int = {
    val probeRel = if (leftIsBuild) getRight else getLeft
    val rowCount = mq.getRowCount(probeRel)
    if (rowCount == null) {
      1
    } else {
      val probeRowSize = mq.getAverageRowSize(probeRel)
      Math.max(1,
        (rowCount * probeRowSize / FlinkCost.SQL_DEFAULT_PARALLELISM_WORKER_PROCESS_SIZE).toInt)
    }
  }

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    // Assume NestedLoopJoin always broadcast data from child which smaller.
    satisfyTraitsOnBroadcastJoin(requiredTraitSet, leftIsBuild)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputEdges: util.List[ExecEdge] = {
    // this is in sync with BatchExecNestedLoopJoinRuleBase#createNestedLoopJoin
    val (buildRequiredShuffle, probeRequiredShuffle) = if (joinType == JoinRelType.FULL) {
      (ExecEdge.RequiredShuffle.singleton(), ExecEdge.RequiredShuffle.singleton())
    } else {
      (ExecEdge.RequiredShuffle.broadcast(), ExecEdge.RequiredShuffle.any())
    }
    val buildEdge = ExecEdge.builder()
      .requiredShuffle(buildRequiredShuffle)
      .damBehavior(ExecEdge.DamBehavior.BLOCKING)
      .priority(0)
      .build()
    val probeEdge = ExecEdge.builder()
      .requiredShuffle(probeRequiredShuffle)
      .damBehavior(ExecEdge.DamBehavior.PIPELINED)
      .priority(1)
      .build()

    if (leftIsBuild) {
      List(buildEdge, probeEdge)
    } else {
      List(probeEdge, buildEdge)
    }
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    val lInput = getInputNodes.get(0).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]
    val rInput = getInputNodes.get(1).translateToPlan(planner)
        .asInstanceOf[Transformation[RowData]]

    // get type
    val lType = lInput.getOutputType.asInstanceOf[InternalTypeInfo[RowData]].toRowType
    val rType = rInput.getOutputType.asInstanceOf[InternalTypeInfo[RowData]].toRowType
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)

    val op = new NestedLoopJoinCodeGenerator(
      CodeGeneratorContext(planner.getTableConfig),
      singleRowJoin,
      leftIsBuild,
      lType,
      rType,
      outputType,
      flinkJoinType,
      condition
    ).gen()

    val parallelism = if (leftIsBuild) rInput.getParallelism else lInput.getParallelism
    val manageMem = if (singleRowJoin) 0 else {
      MemorySize.parse(planner.getTableConfig.getConfiguration.getString(
        ExecutionConfigOptions.TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY)).getBytes
    }
    ExecNodeUtil.createTwoInputTransformation(
      lInput,
      rInput,
      getRelDetailedDescription,
      op,
      InternalTypeInfo.of(outputType),
      parallelism,
      manageMem)
  }
}
