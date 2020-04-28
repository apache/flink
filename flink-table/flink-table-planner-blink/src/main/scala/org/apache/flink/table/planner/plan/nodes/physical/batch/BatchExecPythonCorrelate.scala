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
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPythonCorrelate
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Correlate, JoinRelType}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.{RexNode, RexProgram}

/**
  * Batch physical RelNode for [[Correlate]] (Python user defined table function).
  */
class BatchExecPythonCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    scan: FlinkLogicalTableFunctionScan,
    condition: Option[RexNode],
    projectProgram: Option[RexProgram],
    outputRowType: RelDataType,
    joinType: JoinRelType)
  extends BatchExecCorrelateBase(
    cluster,
    traitSet,
    inputRel,
    scan,
    condition,
    projectProgram,
    outputRowType,
    joinType)
  with CommonPythonCorrelate {

  def copy(
      traitSet: RelTraitSet,
      child: RelNode,
      projectProgram: Option[RexProgram],
      outputType: RelDataType): RelNode = {
    new BatchExecPythonCorrelate(
      cluster,
      traitSet,
      child,
      scan,
      condition,
      projectProgram,
      outputType,
      joinType)
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[BaseRow] = {
    val inputTransformation = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[BaseRow]]
    createPythonOneInputTransformation(
      inputTransformation,
      scan,
      "BatchExecPythonCorrelate",
      outputRowType,
      getConfig(planner.getExecEnv, planner.getTableConfig),
      joinType)
  }
}
