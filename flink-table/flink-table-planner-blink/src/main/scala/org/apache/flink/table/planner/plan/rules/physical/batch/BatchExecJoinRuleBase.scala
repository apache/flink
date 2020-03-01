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
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecLocalHashAggregate
import org.apache.flink.table.planner.plan.utils.{FlinkRelMdUtil, FlinkRelOptUtil}

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableBitSet

import java.lang.{Boolean => JBoolean, Double => JDouble}

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
    val ratioConf = tableConfig.getConfiguration.getDouble(
      BatchExecJoinRuleBase.TABLE_OPTIMIZER_SEMI_JOIN_BUILD_DISTINCT_NDV_RATIO)
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
object BatchExecJoinRuleBase {

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_OPTIMIZER_SEMI_JOIN_BUILD_DISTINCT_NDV_RATIO: ConfigOption[JDouble] =
    key("table.optimizer.semi-anti-join.build-distinct.ndv-ratio")
      .defaultValue(JDouble.valueOf(0.8))
      .withDescription("In order to reduce the amount of data on semi/anti join's" +
          " build side, we will add distinct node before semi/anti join when" +
          "  the semi-side or semi/anti join can distinct a lot of data in advance." +
          " We add this configuration to help the optimizer to decide whether to" +
          " add the distinct.")

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED: ConfigOption[JBoolean] =
    key("table.optimizer.shuffle-by-partial-key-enabled")
        .defaultValue(JBoolean.valueOf(false))
        .withDescription("Enables shuffling by partial partition keys. " +
            "For example, A join with join condition: L.c1 = R.c1 and L.c2 = R.c2. " +
            "If this flag is enabled, there are 3 shuffle strategy:\n " +
            "1. L and R shuffle by c1 \n 2. L and R shuffle by c2\n " +
            "3. L and R shuffle by c1 and c2\n It can reduce some shuffle cost someTimes.")
}
