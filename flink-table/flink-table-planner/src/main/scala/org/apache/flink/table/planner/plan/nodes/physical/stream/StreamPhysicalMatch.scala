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
import org.apache.flink.table.planner.plan.logical.MatchRecognize
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMatch
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalMatch
import org.apache.flink.table.planner.plan.utils.MatchUtil
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import _root_.java.util
import _root_.scala.collection.JavaConversions._
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType

/** Stream physical RelNode which matches along with MATCH_RECOGNIZE. */
class StreamPhysicalMatch(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    logicalMatch: MatchRecognize,
    outputRowType: RelDataType)
  extends CommonPhysicalMatch(cluster, traitSet, inputNode, logicalMatch, outputRowType)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = {
    val rowTimeFields = getInput.getRowType.getFieldList
      .filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))
    rowTimeFields.nonEmpty
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalMatch(cluster, traitSet, inputs.get(0), logicalMatch, outputRowType)
  }

  override def translateToExecNode(): ExecNode[_] = {
    new StreamExecMatch(
      unwrapTableConfig(this),
      MatchUtil.createMatchSpec(logicalMatch),
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
