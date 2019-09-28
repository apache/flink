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

import org.apache.flink.annotation.Experimental
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSort
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecSort

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

import java.lang.{Boolean => JBoolean}

/**
  * Rule that matches [[FlinkLogicalSort]] which sort fields is non-empty and both `fetch` and
  * `offset` are null, and converts it to [[BatchExecSort]].
  */
class BatchExecSortRule extends ConverterRule(
  classOf[FlinkLogicalSort],
  FlinkConventions.LOGICAL,
  FlinkConventions.BATCH_PHYSICAL,
  "BatchExecSortRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort: FlinkLogicalSort = call.rel(0)
    // only matches Sort without fetch and offset
    !sort.getCollation.getFieldCollations.isEmpty && sort.fetch == null && sort.offset == null
  }

  override def convert(rel: RelNode): RelNode = {
    val sort: FlinkLogicalSort = rel.asInstanceOf[FlinkLogicalSort]
    val input = sort.getInput
    val config = sort.getCluster.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
    val enableRangeSort = config.getConfiguration.getBoolean(
      BatchExecSortRule.TABLE_EXEC_SORT_RANGE_ENABLED)
    val distribution = if (enableRangeSort) {
      FlinkRelDistribution.range(sort.getCollation.getFieldCollations)
    } else {
      FlinkRelDistribution.SINGLETON
    }
    val requiredTraitSet = input.getTraitSet
      .replace(distribution)
      .replace(FlinkConventions.BATCH_PHYSICAL)
    val providedTraitSet = sort.getTraitSet
      .replace(distribution)
      .replace(FlinkConventions.BATCH_PHYSICAL)

    val newInput = RelOptRule.convert(input, requiredTraitSet)
    new BatchExecSort(
      sort.getCluster,
      providedTraitSet,
      newInput,
      sort.getCollation)
  }
}

object BatchExecSortRule {
  val INSTANCE: RelOptRule = new BatchExecSortRule

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_SORT_RANGE_ENABLED: ConfigOption[JBoolean] =
  key("table.exec.range-sort.enabled")
      .defaultValue(JBoolean.valueOf(false))
      .withDescription("Sets whether to enable range sort, use range sort to sort all data in" +
          " several partitions. When it is false, sorting in only one partition")

}
