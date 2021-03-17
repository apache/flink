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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecOverAggregate
import org.apache.flink.table.planner.plan.nodes.exec.spec.{OverSpec, PartitionSpec}
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window

import scala.collection.JavaConversions._

/**
 * Batch physical RelNode for sort-based over [[Window]] aggregate.
 */
class BatchPhysicalOverAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    windowGroups: Seq[Window.Group],
    logicWindow: Window)
  extends BatchPhysicalOverAggregateBase(
    cluster,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    windowGroups,
    logicWindow) {

  override def deriveRowType: RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchPhysicalOverAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      windowGroups,
      logicWindow)
  }

  override def translateToExecNode(): ExecNode[_] = {
    new BatchExecOverAggregate(
      new OverSpec(
        new PartitionSpec(partitionKeyIndices),
        offsetAndInsensitiveSensitiveGroups.map(OverAggregateUtil.createGroupSpec(_, logicWindow)),
        logicWindow.constants,
        OverAggregateUtil.calcOriginalInputFields(logicWindow)),
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
