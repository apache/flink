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
package org.apache.flink.table.planner.plan.nodes.exec.stream

import org.apache.flink.api.dag.Transformation
import org.apache.flink.core.memory.ManagedMemoryUseCase
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.plan.nodes.common.CommonPythonCorrelate
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, ExecNodeBase}
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexCall, RexNode}

import java.util.Collections

/**
 * Stream exec node which matches along with join a python user defined table function.
 *
 * <p>Note: This class can't be ported to Java,
 * because java class can't extend scala interface with default implementation.
 * FLINK-20693 will port this class to Java.
 *
 * <p>TODO change JoinRelType to FlinkJoinType
 */
class StreamExecPythonCorrelate(
    joinType: JoinRelType,
    invocation: RexCall,
    condition: Option[RexNode],
    inputEdge: ExecEdge,
    outputType: RowType,
    description: String)
  extends ExecNodeBase[RowData](Collections.singletonList(inputEdge), outputType, description)
  with StreamExecNode[RowData]
  with CommonPythonCorrelate {

  if (joinType == JoinRelType.LEFT && condition.isDefined) {
    throw new TableException("Currently Python correlate does not support conditions in left join.")
  }

  override protected def translateToPlanInternal(planner: PlannerBase): Transformation[RowData] = {
    val inputTransformation = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val ret = createPythonOneInputTransformation(
      inputTransformation,
      invocation,
      "StreamExecPythonCorrelate",
      getOutputType.asInstanceOf[RowType],
      getConfig(planner.getExecEnv, planner.getTableConfig),
      joinType)
    if (isPythonWorkerUsingManagedMemory(planner.getTableConfig.getConfiguration)) {
      ret.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON)
    }
    ret
  }
}
