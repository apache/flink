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

package org.apache.flink.table.plan.rules.physical.batch

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalDataStreamTableScan
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecBoundedStreamScan
import org.apache.flink.table.plan.schema.DataStreamTable

class BatchExecScanTableRule
  extends ConverterRule(
    classOf[FlinkLogicalDataStreamTableScan],
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCH_PHYSICAL,
    "BatchExecScanTableRule") {

  /**
    * If the input is not a DataStreamTable, we want the TableScanRule to match instead
    */
  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: FlinkLogicalDataStreamTableScan = call.rel(0)
    val dataStreamTable = scan.getTable.unwrap(classOf[DataStreamTable[Any]])
    dataStreamTable != null
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[FlinkLogicalDataStreamTableScan]
    val newTrait = rel.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    new BatchExecBoundedStreamScan(rel.getCluster, newTrait, scan.getTable, rel.getRowType)
  }
}

object BatchExecScanTableRule {
  val INSTANCE: RelOptRule = new BatchExecScanTableRule
}
