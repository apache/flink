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

import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSort
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortLimit
import org.apache.flink.table.planner.plan.utils.SortUtil

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.sql.`type`.SqlTypeName

/**
  * Rule that matches [[FlinkLogicalSort]] with non-empty sort fields and non-null fetch or offset,
  * and converts it to
  * {{{
  * BatchPhysicalSortLimit (global)
  * +- BatchPhysicalExchange (singleton)
  *    +- BatchPhysicalSortLimit (local)
  *       +- input of sort
  * }}}
  * when fetch is not null, or
  * {{{
  * BatchPhysicalSortLimit (global)
  * +- BatchPhysicalExchange (singleton)
  *    +- input of sort
  * }}}
  * when fetch is null
  */
class BatchPhysicalSortLimitRule
  extends ConverterRule(
    classOf[FlinkLogicalSort],
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCH_PHYSICAL,
    "BatchPhysicalSortLimitRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort: FlinkLogicalSort = call.rel(0)
    // only matches Sort with non-empty sort fields and non-null fetch
    !sort.getCollation.getFieldCollations.isEmpty && (sort.fetch != null || sort.offset != null)
  }

  override def convert(rel: RelNode): RelNode = {
    val sort = rel.asInstanceOf[FlinkLogicalSort]
    // create local BatchPhysicalSortLimit
    val localRequiredTrait = sort.getInput.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val localInput = RelOptRule.convert(sort.getInput, localRequiredTrait)

    // if fetch is null, there is no need to create local BatchPhysicalSortLimit
    val inputOfExchange = if (sort.fetch != null) {
      val limit = SortUtil.getLimitEnd(sort.offset, sort.fetch)
      val rexBuilder = sort.getCluster.getRexBuilder
      val intType = rexBuilder.getTypeFactory.createSqlType(SqlTypeName.INTEGER)
      val providedLocalTraitSet = localRequiredTrait.replace(sort.getCollation)
      // for local BatchPhysicalSortLimit, offset is always 0, and fetch is `limit`
      new BatchPhysicalSortLimit(
        rel.getCluster,
        providedLocalTraitSet,
        localInput,
        sort.getCollation,
        rexBuilder.makeLiteral(0, intType, true),
        rexBuilder.makeLiteral(limit, intType, true),
        false)
    } else {
      localInput
    }

    // require SINGLETON exchange
    val requiredTrait = rel.getCluster.getPlanner.emptyTraitSet()
      .replace(FlinkConventions.BATCH_PHYSICAL)
      .replace(FlinkRelDistribution.SINGLETON)
    val newInput = RelOptRule.convert(inputOfExchange, requiredTrait)

    // create global BatchPhysicalSortLimit
    val providedGlobalTraitSet = requiredTrait.replace(sort.getCollation)
    new BatchPhysicalSortLimit(
      rel.getCluster,
      providedGlobalTraitSet,
      newInput,
      sort.getCollation,
      sort.offset,
      sort.fetch,
      true
    )
  }
}

object BatchPhysicalSortLimitRule {
  val INSTANCE: RelOptRule = new BatchPhysicalSortLimitRule
}
