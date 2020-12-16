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
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecPythonCalc
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rex.RexProgram

import java.util

/**
 * Stream ExecNode for Python ScalarFunctions.
 *
 * <p>Note: This class can't be ported to Java,
 * because java class can't extend scala interface with default implementation.
 * FLINK-20620 will port this class to Java.
 */
class StreamExecPythonCalc(
    calcProgram: RexProgram,
    inputEdge: ExecEdge,
    outputType: RowType,
    description: String)
  extends StreamExecNode[RowData](
    util.Collections.singletonList(inputEdge),
    outputType,
    description)
  with CommonExecPythonCalc {

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val ret = createPythonOneInputTransformation(
      inputTransform,
      calcProgram,
      "StreamExecPythonCalc",
      getConfig(planner.getExecEnv, planner.getTableConfig))

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }
    if (isPythonWorkerUsingManagedMemory(planner.getTableConfig.getConfiguration)) {
      ret.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON)
    }
    ret
  }
}
