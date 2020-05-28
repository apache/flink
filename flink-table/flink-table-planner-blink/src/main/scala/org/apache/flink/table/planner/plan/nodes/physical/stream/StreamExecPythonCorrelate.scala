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
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPythonCorrelate
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexNode, RexProgram}
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode

/**
  * Flink RelNode which matches along with join a python user defined table function.
  */
class StreamExecPythonCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    projectProgram: Option[RexProgram],
    scan: FlinkLogicalTableFunctionScan,
    condition: Option[RexNode],
    outputRowType: RelDataType,
    joinType: JoinRelType)
  extends StreamExecCorrelateBase(
    cluster,
    traitSet,
    inputRel,
    projectProgram,
    scan,
    condition,
    outputRowType,
    joinType)
  with CommonPythonCorrelate {

  if (condition.isDefined) {
    throw new TableException("Currently Python correlate does not support conditions in left join.")
  }

  def copy(
      traitSet: RelTraitSet,
      newChild: RelNode,
      projectProgram: Option[RexProgram],
      outputType: RelDataType): RelNode = {
    new StreamExecPythonCorrelate(
      cluster,
      traitSet,
      newChild,
      projectProgram,
      scan,
      condition,
      outputType,
      joinType)
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val inputTransformation = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val ret = createPythonOneInputTransformation(
      inputTransformation,
      scan,
      "StreamExecPythonCorrelate",
      outputRowType,
      getConfig(planner.getExecEnv, planner.getTableConfig),
      joinType)
    if (isPythonWorkerUsingManagedMemory(planner.getTableConfig.getConfiguration)) {
      ExecNode.setManagedMemoryWeight(
        ret, getPythonWorkerMemory(planner.getTableConfig.getConfiguration).getBytes)
    }
    ret
  }
}
