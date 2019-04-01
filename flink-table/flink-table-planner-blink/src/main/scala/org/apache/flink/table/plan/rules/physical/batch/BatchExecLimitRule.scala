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
import org.apache.flink.table.plan.util.FlinkRelOptUtil

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.sql.`type`.SqlTypeName

/**
  * Rule that matches [[FlinkLogicalSort]] with empty sort fields,
  * and converts it to
  * {{{
  * BatchExecLimit (global)
  * +- BatchExecExchange (singleton)
  *    +- BatchExecLimit (local)
  *       +- input of sort
  * }}}
  * when fetch is not null, or
  * {{{
  * BatchExecLimit
  * +- BatchExecExchange (singleton)
  *    +- input of sort
  * }}}
  * when fetch is null.
  */
class BatchExecLimitRule
  extends ConverterRule(
    classOf[FlinkLogicalSort],
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCH_PHYSICAL,
    "BatchExecLimitRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort: FlinkLogicalSort = call.rel(0)
    // only matches Sort with empty sort fields and non-null fetch
    val fetch = sort.fetch
    sort.getCollation.getFieldCollations.isEmpty &&
      (fetch == null || fetch != null && RexLiteral.intValue(fetch) < Long.MaxValue)
  }

  override def convert(rel: RelNode): RelNode = {
    val sort = rel.asInstanceOf[FlinkLogicalSort]
    val input = sort.getInput

    val traitSet = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val newLocalInput = RelOptRule.convert(input, traitSet)

    // if fetch is null, there is no need to create local BatchExecLimit
    val inputOfExchange = if (sort.fetch != null) {
      val providedLocalTraitSet = traitSet
      val limit = FlinkRelOptUtil.getLimitEnd(sort.offset, sort.fetch)
      val rexBuilder = sort.getCluster.getRexBuilder
      val intType = rexBuilder.getTypeFactory.createSqlType(SqlTypeName.INTEGER)
      // for local BatchExecLimit, offset is always 0, and fetch is `limit`
      new BatchExecLimit(
        rel.getCluster,
        providedLocalTraitSet,
        newLocalInput,
        rexBuilder.makeLiteral(0, intType, true),
        rexBuilder.makeLiteral(limit, intType, true), // TODO use Long type for limit ?
        false)
    } else {
      newLocalInput
    }

    // require SINGLETON exchange
    val newTraitSet = traitSet.replace(FlinkRelDistribution.SINGLETON)
    val newInput = RelOptRule.convert(inputOfExchange, newTraitSet)

    // create global BatchExecLimit
    val providedGlobalTraitSet = newTraitSet
    new BatchExecLimit(
      rel.getCluster,
      providedGlobalTraitSet,
      newInput,
      sort.offset,
      sort.fetch,
      true)
  }
}

object BatchExecLimitRule {
  val INSTANCE: RelOptRule = new BatchExecLimitRule
}
