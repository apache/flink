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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCollectionScan
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecCollectionScan
import org.apache.flink.table.planner.plan.schema.CollectionTable

/**
  * Rule that converts [[FlinkLogicalCollectionScan]] to [[StreamExecCollectionScan]]
  */
class StreamExecCollectionScanRule
  extends ConverterRule(
    classOf[FlinkLogicalCollectionScan],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecCollectionScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: FlinkLogicalCollectionScan = call.rel(0)
    val collectionTable = scan.getTable.unwrap(classOf[CollectionTable])
    collectionTable != null
  }

  override def convert(rel: RelNode): RelNode = {
    val scan: FlinkLogicalCollectionScan = rel.asInstanceOf[FlinkLogicalCollectionScan]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    new StreamExecCollectionScan(
      rel.getCluster,
      traitSet,
      scan.getTable)
  }
}

object StreamExecCollectionScanRule {
  val INSTANCE: RelOptRule = new StreamExecCollectionScanRule
}
