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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecUnion
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, ExecNode}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{SetOp, Union}
import org.apache.calcite.rel.{RelNode, RelWriter}

import java.util

import scala.collection.JavaConversions._

/**
 * Stream physical RelNode for [[Union]].
 */
class StreamPhysicalUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRels: util.List[RelNode],
    all: Boolean,
    outputRowType: RelDataType)
  extends Union(cluster, traitSet, inputRels, all)
  with StreamPhysicalRel {

  require(all, "Only support union all")

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode], all: Boolean): SetOp = {
    new StreamPhysicalUnion(cluster, traitSet, inputs, all, outputRowType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("union", outputRowType.getFieldNames.mkString(", "))
  }

  override def translateToExecNode(): ExecNode[_] = {
    new StreamExecUnion(
      getInputs.map(_ => ExecEdge.DEFAULT),
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
