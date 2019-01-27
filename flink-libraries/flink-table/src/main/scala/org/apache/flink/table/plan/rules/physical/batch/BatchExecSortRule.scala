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

import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSort
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSort

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode

class BatchExecSortRule extends RelOptRule(
  operand(classOf[FlinkLogicalSort], operand(classOf[RelNode], any)), "BatchExecSortRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort = call.rel(0).asInstanceOf[FlinkLogicalSort]
    !sort.getCollation.getFieldCollations.isEmpty && sort.fetch == null && sort.offset == null
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val sort = call.rels(0).asInstanceOf[FlinkLogicalSort]
    val conf = sort.getCluster.getPlanner.getContext.unwrap(classOf[TableConfig])
    val enableRangeSort = conf.getConf.getBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED)
    val distribution = if (enableRangeSort) {
      FlinkRelDistribution.range(sort.getCollation.getFieldCollations)
    } else {
      FlinkRelDistribution.SINGLETON
    }
    val (requiredTraitSet, providedTraitSet) =
      (sort.getInput.getTraitSet.replace(distribution).replace(FlinkConventions.BATCH_PHYSICAL),
        sort.getTraitSet.replace(distribution).replace(FlinkConventions.BATCH_PHYSICAL))
    val newInput = RelOptRule.convert(sort.getInput, requiredTraitSet)
    call.transformTo(
      new BatchExecSort(
      sort.getCluster,
      providedTraitSet,
      newInput,
      sort.getCollation))
  }
}

object BatchExecSortRule {
  val INSTANCE: RelOptRule = new BatchExecSortRule
}
