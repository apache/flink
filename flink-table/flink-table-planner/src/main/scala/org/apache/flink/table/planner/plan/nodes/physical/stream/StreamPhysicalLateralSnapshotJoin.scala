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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLateralSnapshotJoin
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.utils.LateralSnapshotJoinUtil
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConverters._

/**
 * Stream physical node for the LATERAL SNAPSHOT processing-time temporal table join. The build side
 * is loaded into operator state during a LOAD phase; once the build-side watermark crosses the
 * configured flip point, the operator switches to a JOIN phase and processes probe-side records
 * against the loaded build state.
 */
class StreamPhysicalLateralSnapshotJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType,
    loadCompletedCondition: String,
    loadCompletedTime: java.lang.Long,
    loadCompletedIdleTimeoutMs: java.lang.Long,
    stateTtlMs: java.lang.Long)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
  with StreamPhysicalRel {

  require(loadCompletedTime != null, "loadCompletedTime must not be null.")

  override def requireWatermark: Boolean = true

  override def deriveRowType(): RelDataType =
    LateralSnapshotJoinUtil.deriveRowType(
      getCluster.getTypeFactory,
      getLeft.getRowType,
      getRight.getRowType,
      joinType,
      getSystemFieldList)

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new StreamPhysicalLateralSnapshotJoin(
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

  override def translateToExecNode(): ExecNode[_] = {
    // The build (right) side carries a watermark, so it must expose a row-time attribute whose
    // field index drives the event-time-ordered application of buffered build-side changes.
    val rightTimeAttributeIndex = getRight.getRowType.getFieldList.asScala.indexWhere(
      f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))
    if (rightTimeAttributeIndex < 0) {
      throw new TableException(
        "The build (right) side of a LATERAL SNAPSHOT join must have a row-time attribute. " +
          "This is a bug, please file an issue.")
    }

    new StreamExecLateralSnapshotJoin(
      unwrapTableConfig(this),
      joinSpec,
      rightTimeAttributeIndex,
      loadCompletedCondition,
      loadCompletedTime,
      loadCompletedIdleTimeoutMs,
      stateTtlMs,
      InputProperty.DEFAULT,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
