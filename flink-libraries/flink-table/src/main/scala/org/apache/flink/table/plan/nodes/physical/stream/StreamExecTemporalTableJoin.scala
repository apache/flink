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
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.common.CommonTemporalTableJoin
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.TemporalJoinUtil
import org.apache.flink.table.sources.TableSource

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rex.{RexNode, RexProgram}

import java.util

class StreamExecTemporalTableJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    tableSource: TableSource,
    tableRowType: RelDataType,
    tableCalcProgram: Option[RexProgram],
    period: RexNode,
    joinInfo: JoinInfo,
    joinType: JoinRelType)
  extends CommonTemporalTableJoin(
    cluster,
    traitSet,
    input,
    tableSource,
    tableRowType,
    tableCalcProgram,
    period,
    joinInfo,
    joinType)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecTemporalTableJoin(
      cluster,
      traitSet,
      inputs.get(0),
      tableSource,
      tableRowType,
      tableCalcProgram,
      period,
      joinInfo,
      joinType)
  }

  override def isDeterministic: Boolean = {
    TemporalJoinUtil.isDeterministic(
      tableCalcProgram,
      period,
      joinInfo.getRemaining(cluster.getRexBuilder))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val inputTransformation = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val transformation = translateToPlanInternal(
      inputTransformation,
      tableEnv.execEnv,
      tableEnv.config,
      tableEnv.getRelBuilder)
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }

}
