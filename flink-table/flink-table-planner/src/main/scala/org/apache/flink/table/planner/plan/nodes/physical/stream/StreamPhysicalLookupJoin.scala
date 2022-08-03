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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalLookupJoin
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, FlinkRexUtil, JoinTypeUtil}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rex.RexProgram

import java.util

import scala.collection.JavaConverters._

/** Stream physical RelNode for temporal table join that implemented by lookup. */
class StreamPhysicalLookupJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    temporalTable: RelOptTable,
    tableCalcProgram: Option[RexProgram],
    joinInfo: JoinInfo,
    joinType: JoinRelType,
    val upsertMaterialize: Boolean = false)
  extends CommonPhysicalLookupJoin(
    cluster,
    traitSet,
    inputRel,
    temporalTable,
    tableCalcProgram,
    joinInfo,
    joinType)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalLookupJoin(
      cluster,
      traitSet,
      inputs.get(0),
      temporalTable,
      tableCalcProgram,
      joinInfo,
      joinType,
      upsertMaterialize)
  }

  def copy(upsertMaterialize: Boolean): StreamPhysicalLookupJoin = {
    new StreamPhysicalLookupJoin(
      cluster,
      traitSet,
      getInput,
      temporalTable,
      tableCalcProgram,
      joinInfo,
      joinType,
      upsertMaterialize)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val (projectionOnTemporalTable, filterOnTemporalTable) = calcOnTemporalTable match {
      case Some(program) =>
        val (projection, filter) = FlinkRexUtil.expandRexProgram(program)
        (JavaScalaConversionUtil.toJava(projection), filter.orNull)
      case _ =>
        (null, null)
    }
    val inputChangelogMode =
      ChangelogPlanUtils.getChangelogMode(getInput.asInstanceOf[StreamPhysicalRel]).get

    new StreamExecLookupJoin(
      unwrapTableConfig(this),
      JoinTypeUtil.getFlinkJoinType(joinType),
      remainingCondition.orNull,
      new TemporalTableSourceSpec(temporalTable),
      allLookupKeys.map(item => (Int.box(item._1), item._2)).asJava,
      projectionOnTemporalTable,
      filterOnTemporalTable,
      inputChangelogMode,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      lookupKeyContainsPrimaryKey(),
      upsertMaterialize,
      getRelDetailedDescription)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super
      .explainTerms(pw)
      .itemIf("upsertMaterialize", "true", upsertMaterialize)
  }
}
