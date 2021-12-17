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

package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalIntermediateTableScan
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalIntermediateTableScan

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

/**
 * Rule that converts [[FlinkLogicalIntermediateTableScan]] to
 * [[BatchPhysicalIntermediateTableScan]].
 */
class BatchPhysicalIntermediateTableScanRule
  extends ConverterRule(
    classOf[FlinkLogicalIntermediateTableScan],
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCH_PHYSICAL,
    "BatchPhysicalIntermediateTableScanRule") {

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[FlinkLogicalIntermediateTableScan]
    val newTrait = rel.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    new BatchPhysicalIntermediateTableScan(rel.getCluster, newTrait, scan.getTable, rel.getRowType)
  }
}

object BatchPhysicalIntermediateTableScanRule {
  val INSTANCE: RelOptRule = new BatchPhysicalIntermediateTableScanRule
}
