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

import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSort
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLimit

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.RexLiteral

class BatchExecLimitRule
    extends ConverterRule(
      classOf[FlinkLogicalSort],
      FlinkConventions.LOGICAL,
      FlinkConventions.BATCH_PHYSICAL,
      "BatchExecLimitRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort = call.rel(0).asInstanceOf[FlinkLogicalSort]
    val limit = sort.fetch
    sort.getCollation.getFieldCollations.isEmpty && limit != null &&
        RexLiteral.intValue(limit) < Long.MaxValue
  }

  override def convert(rel: RelNode): RelNode = {
    val sort = rel.asInstanceOf[FlinkLogicalSort]
    val input = sort.getInput
    val traitSet = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val newLocalInput = RelOptRule.convert(input, traitSet)
    val providedLocalTraitSet = traitSet
    val localLimit = new BatchExecLimit(
      rel.getCluster,
      providedLocalTraitSet,
      newLocalInput,
      sort.offset,
      sort.fetch,
      false,
      description
    )
    val newTrait = traitSet.replace(FlinkRelDistribution.SINGLETON)
    val newInput = RelOptRule.convert(localLimit, newTrait)
    val providedGlobalTraitSet = newTrait
    val globalLimit = new BatchExecLimit(
      rel.getCluster,
      providedGlobalTraitSet,
      newInput,
      sort.offset,
      sort.fetch,
      true,
      description
    )
    globalLimit
  }
}

object BatchExecLimitRule {
  val INSTANCE: RelOptRule = new BatchExecLimitRule
}
