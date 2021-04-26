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
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.codegen.ValuesCodeGenerator
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.StreamExecNode

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rex.RexLiteral

/**
  * Stream physical RelNode for [[Values]].
  */
class StreamExecValues(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    tuples: ImmutableList[ImmutableList[RexLiteral]],
    outputRowType: RelDataType)
  extends Values(cluster, outputRowType, tuples, traitSet)
  with StreamPhysicalRel
  with StreamExecNode[RowData] {

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecValues(cluster, traitSet, getTuples, outputRowType)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val inputFormat = ValuesCodeGenerator.generatorInputFormat(
      planner.getTableConfig,
      getRowType,
      tuples,
      getRelTypeName)
    val transformation = planner.getExecEnv.createInput(inputFormat,
      inputFormat.getProducedType).getTransformation
    transformation.setName(getRelDetailedDescription)
    transformation.setParallelism(1)
    transformation.setMaxParallelism(1)
    transformation
  }

}
