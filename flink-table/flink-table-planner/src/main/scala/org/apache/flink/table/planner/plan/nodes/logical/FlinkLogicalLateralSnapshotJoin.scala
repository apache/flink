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
package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.utils.LateralSnapshotJoinUtil

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.core.{CorrelationId, Join, JoinRelType}
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode

import java.util.Collections

/**
 * Logical node for the {@code LATERAL SNAPSHOT} processing-time temporal table join.
 *
 * <p> the {@code LATERAL SNAPSHOT} join materializes the build-side row-time attribute, the
 * row-time attribute of the probe-side is forwarded. The arguments of the {@code SNAPSHOT} function
 * are persisted in fields of the logical node.
 */
class FlinkLogicalLateralSnapshotJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    condition: RexNode,
    joinType: JoinRelType,
    val loadCompletedCondition: String,
    val loadCompletedTime: java.lang.Long,
    val loadCompletedIdleTimeoutMs: java.lang.Long,
    val stateTtlMs: java.lang.Long)
  extends Join(
    cluster,
    traitSet,
    Collections.emptyList[RelHint](),
    left,
    right,
    condition,
    Collections.emptySet[CorrelationId](),
    joinType)
  with FlinkLogicalRel {

  require(loadCompletedTime != null, "loadCompletedTime must not be null.")

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new FlinkLogicalLateralSnapshotJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType,
      loadCompletedCondition,
      loadCompletedTime,
      loadCompletedIdleTimeoutMs,
      stateTtlMs)
  }

  override def deriveRowType(): RelDataType =
    LateralSnapshotJoinUtil.deriveRowType(
      getCluster.getTypeFactory,
      left.getRowType,
      right.getRowType,
      joinType,
      getSystemFieldList)

  override def explainTerms(pw: RelWriter): RelWriter = {
    val terms = super.explainTerms(pw)
    terms.item("loadCompletedCondition", loadCompletedCondition)
    terms.item("loadCompletedTime", loadCompletedTime)
    if (loadCompletedIdleTimeoutMs != null) {
      terms.item("loadCompletedIdleTimeout", s"$loadCompletedIdleTimeoutMs ms")
    }
    if (stateTtlMs != null) {
      terms.item("stateTtl", s"$stateTtlMs ms")
    }
    terms
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = mq.getRowCount(getLeft)
    val leftRowSize = mq.getAverageRowSize(getLeft)
    val rightRowCnt = mq.getRowCount(getRight)
    val cpuCost = leftRowCnt + rightRowCnt
    val ioCost = leftRowCnt * leftRowSize
    planner.getCostFactory.makeCost(leftRowCnt, cpuCost, ioCost)
  }
}

object FlinkLogicalLateralSnapshotJoin {

  def create(
      left: RelNode,
      right: RelNode,
      condition: RexNode,
      joinType: JoinRelType,
      loadCompletedCondition: String,
      loadCompletedTime: java.lang.Long,
      loadCompletedIdleTimeoutMs: java.lang.Long,
      stateTtlMs: java.lang.Long): FlinkLogicalLateralSnapshotJoin = {
    val cluster = left.getCluster
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalLateralSnapshotJoin(
      cluster,
      traitSet,
      left,
      right,
      condition,
      joinType,
      loadCompletedCondition,
      loadCompletedTime,
      loadCompletedIdleTimeoutMs,
      stateTtlMs)
  }
}
