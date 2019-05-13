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

import org.apache.flink.table.JDouble
import org.apache.flink.table.api.PlannerConfigOptions
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecLocalHashAggregate
import org.apache.flink.table.plan.util.{FlinkRelMdUtil, FlinkRelOptUtil}

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableBitSet

trait BatchExecJoinRuleBase {

  def addLocalDistinctAgg(
      node: RelNode,
      distinctKeys: Seq[Int],
      relBuilder: RelBuilder): RelNode = {
    val localRequiredTraitSet = node.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val newInput = RelOptRule.convert(node, localRequiredTraitSet)
    val providedTraitSet = localRequiredTraitSet

    new BatchExecLocalHashAggregate(
      node.getCluster,
      relBuilder,
      providedTraitSet,
      newInput,
      node.getRowType, // output row type
      node.getRowType, // input row type
      distinctKeys.toArray,
      Array.empty,
      Seq())
  }

  def chooseSemiBuildDistinct(
      buildRel: RelNode,
      distinctKeys: Seq[Int]): Boolean = {
    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(buildRel)
    val mq = buildRel.getCluster.getMetadataQuery
    val ratioConf = tableConfig.getConf.getDouble(
      PlannerConfigOptions.SQL_OPTIMIZER_SEMI_JOIN_BUILD_DISTINCT_NDV_RATIO)
    val inputRows = mq.getRowCount(buildRel)
    val ndvOfGroupKey = mq.getDistinctRowCount(
      buildRel, ImmutableBitSet.of(distinctKeys: _*), null)
    if (ndvOfGroupKey == null) {
      false
    } else {
      ndvOfGroupKey / inputRows < ratioConf
    }
  }

  private[flink] def binaryRowRelNodeSize(relNode: RelNode): JDouble = {
    val mq = relNode.getCluster.getMetadataQuery
    val rowCount = mq.getRowCount(relNode)
    if (rowCount == null) {
      null
    } else {
      rowCount * FlinkRelMdUtil.binaryRowAverageSize(relNode)
    }
  }
}
