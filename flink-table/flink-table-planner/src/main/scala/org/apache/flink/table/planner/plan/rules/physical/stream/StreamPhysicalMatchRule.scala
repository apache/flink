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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.plan.logical.MatchRecognize
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalMatch
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMatch
import org.apache.flink.table.planner.plan.rules.physical.common.CommonPhysicalMatchRule

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelNode

class StreamPhysicalMatchRule
  extends CommonPhysicalMatchRule(
    classOf[FlinkLogicalMatch],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamPhysicalMatchRule") {

  override def convert(rel: RelNode): RelNode = {
    super.convert(rel, FlinkConventions.STREAM_PHYSICAL)
  }

  override def convertToPhysicalMatch(
      cluster: RelOptCluster,
      traitSet: RelTraitSet,
      convertInput: RelNode,
      matchRecognize: MatchRecognize,
      rowType: RelDataType): RelNode = {
    new StreamPhysicalMatch(cluster, traitSet, convertInput, matchRecognize, rowType)
  }
}

object StreamPhysicalMatchRule {
  val INSTANCE: RelOptRule = new StreamPhysicalMatchRule
}
