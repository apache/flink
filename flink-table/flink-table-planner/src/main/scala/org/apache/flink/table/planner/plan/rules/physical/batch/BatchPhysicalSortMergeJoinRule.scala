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
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.connector.source.partitioning.KeyGroupedPartitioning
import org.apache.flink.table.planner.hint.JoinStrategy
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalTableSourceScan}
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalSortMergeJoin, BatchPhysicalTableSourceScan}
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, ScanUtil}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.{RelCollations, RelNode}
import org.apache.calcite.rel.core.Join
import org.apache.calcite.util.ImmutableIntList

import java.lang.{Boolean => JBoolean}
import java.util

import scala.collection.JavaConversions._

/**
 * Rule that converts [[FlinkLogicalJoin]] to [[BatchPhysicalSortMergeJoin]] if there exists at
 * least one equal-join condition and SortMergeJoin is enabled.
 */
class BatchPhysicalSortMergeJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin], operand(classOf[RelNode], any)),
    "BatchPhysicalSortMergeJoinRule")
  with BatchPhysicalJoinRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    val tableConfig = unwrapTableConfig(join)
    canUseJoinStrategy(join, tableConfig, JoinStrategy.SHUFFLE_MERGE)
  }

  // TODO confirm this is the best practice for getting the table scan from a RelNode
  private def getTableScan(relNode: RelNode): Option[FlinkLogicalTableSourceScan] = {
    relNode match {
      // Handle RelSubset by getting the best input
      case subset: RelSubset =>
        // Get the best input or first input if no best is set
        val best = Option(subset.getBest).orElse(
          if (!subset.getRelList.isEmpty) Some(subset.getRelList.get(0))
          else None
        )
        best.flatMap(getTableScan)

      // Handle different types of table scan nodes
      case scan: FlinkLogicalTableSourceScan => Some(scan)

      // For other nodes with a single input
      case node if node.getInputs.size() == 1 && !node.getInput(0).equals(node) =>
        getTableScan(node.getInput(0))

      case _ => None
    }
  }

  private def isPartitionBy(
      partition: KeyGroupedPartitioning,
      fieldNames: util.List[String]): Boolean = {
    val partitionKeys = partition.keys()
    partitionKeys.length == fieldNames.size() &&
    partitionKeys.zip(fieldNames).forall {
      case (partitionKey, fieldName) =>
        partitionKey.getKey == fieldName
    }
  }

  private def canApplyStoragePartitionJoin(join: Join): Boolean = {
    // TODO ensure the join condition is equal join, not like col1 + 1 = col2, or func(col1) = func(col2)
    val joinInfo = join.analyzeCondition()

    // Return false if it's not an equi-join
    if (joinInfo.nonEquiConditions != null && !joinInfo.nonEquiConditions.isEmpty) {
      return false
    }

    // Return false if there are no equi-join conditions
    if (joinInfo.leftKeys.isEmpty || joinInfo.rightKeys.isEmpty) {
      return false
    }

    // Find all table scans in both branches
    val leftTableScan = getTableScan(join.getLeft)
    val rightTableScan = getTableScan(join.getRight)

    if (leftTableScan.isEmpty || rightTableScan.isEmpty) {
      return false
    }

    // Get the field names from the table scans
    val leftFieldNames = leftTableScan.get.getRowType.getFieldNames
    val rightFieldNames = rightTableScan.get.getRowType.getFieldNames

    // TODO: this won't work in case there is a projection in between join and table and adds extra columns
    // for example: in testCannotPushDownProbeSideWithCalc,
    //   "select * from dim inner join (select fact_date_sk, RAND(10) as random from fact) "
    //                        + "as factSide on dim.amount = factSide.random and dim.price < 500";
    // the join condition is dim.amount = factSide.random, but the right side table doesn't contains random

    // Map join keys to field names
    val leftJoinFields = (0 until joinInfo.leftKeys.size()).map {
      i => leftFieldNames.get(joinInfo.leftKeys.get(i))
    }

    val rightJoinFields = (0 until joinInfo.rightKeys.size()).map {
      i => rightFieldNames.get(joinInfo.rightKeys.get(i))
    }

    // conditions:
    // 1. leftPartition is partitioned by leftFieldNames
    // 2. rightPartition is partitioned by rightFieldNames
    // 3. leftPartition is compatible with rightPartition

    val leftPartition = ScanUtil.getPartition(leftTableScan.get.relOptTable)
    val rightPartition = ScanUtil.getPartition(rightTableScan.get.relOptTable)

    // ensure both leftPartition and rightPartition are KeyGroupedPartitioning class
    if (leftPartition.isEmpty || rightPartition.isEmpty) {
      return false
    }
    if (
      !leftPartition.get.isInstanceOf[KeyGroupedPartitioning] ||
      !rightPartition.get.isInstanceOf[KeyGroupedPartitioning]
    ) {
      return false
    }
    val leftKeyGroupedPartitioning = leftPartition.get.asInstanceOf[KeyGroupedPartitioning]
    val rightKeyGroupedPartitioning = rightPartition.get.asInstanceOf[KeyGroupedPartitioning]

    isPartitionBy(leftKeyGroupedPartitioning, leftJoinFields) &&
    isPartitionBy(rightKeyGroupedPartitioning, rightJoinFields) &&
    leftKeyGroupedPartitioning.isCompatible(rightKeyGroupedPartitioning)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val joinInfo = join.analyzeCondition
    val left = join.getLeft
    val right = join.getRight

    val tableConfig = unwrapTableConfig(join)
    val canApplyPartitionJoin =
      tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_STORAGE_PARTITION_JOIN_ENABLED) &&
        canApplyStoragePartitionJoin(join)

    def getTraitSetByShuffleKeys(
        shuffleKeys: ImmutableIntList,
        requireStrict: Boolean,
        requireCollation: Boolean): RelTraitSet = {
      var traitSet = if (canApplyPartitionJoin) {
        // precondition requireCollation is always false when using ANY distribution
        // this is related to TABLE_OPTIMIZER_SMJ_REMOVE_SORT_ENABLED
        // this can only be true if that is set
        assert(!requireCollation, "requireCollation should be false when using ANY distribution")
        call.getPlanner
          .emptyTraitSet()
          .replace(FlinkConventions.BATCH_PHYSICAL)
          .replace(FlinkRelDistribution.ANY)
      } else {
        call.getPlanner
          .emptyTraitSet()
          .replace(FlinkConventions.BATCH_PHYSICAL)
          .replace(FlinkRelDistribution.hash(shuffleKeys, requireStrict))
      }

      if (requireCollation) {
        val fieldCollations = shuffleKeys.map(FlinkRelOptUtil.ofRelFieldCollation(_))
        val relCollation = RelCollations.of(fieldCollations)
        traitSet = traitSet.replace(relCollation)
      }
      traitSet
    }

    def transformToEquiv(
        leftRequiredShuffleKeys: ImmutableIntList,
        rightRequiredShuffleKeys: ImmutableIntList,
        requireLeftSorted: Boolean,
        requireRightSorted: Boolean): Unit = {

      val leftRequiredTrait =
        getTraitSetByShuffleKeys(leftRequiredShuffleKeys, requireStrict = true, requireLeftSorted)
      val rightRequiredTrait =
        getTraitSetByShuffleKeys(rightRequiredShuffleKeys, requireStrict = true, requireRightSorted)

      val newLeft = RelOptRule.convert(left, leftRequiredTrait)
      val newRight = RelOptRule.convert(right, rightRequiredTrait)

      val providedTraitSet = call.getPlanner
        .emptyTraitSet()
        .replace(FlinkConventions.BATCH_PHYSICAL)
      val newJoin = new BatchPhysicalSortMergeJoin(
        join.getCluster,
        providedTraitSet,
        newLeft,
        newRight,
        join.getCondition,
        join.getJoinType,
        requireLeftSorted,
        requireRightSorted)
      call.transformTo(newJoin)
    }

    val candidates =
      if (tableConfig.get(BatchPhysicalSortMergeJoinRule.TABLE_OPTIMIZER_SMJ_REMOVE_SORT_ENABLED)) {
        // add more possibility to remove redundant sort, and longer optimization time
        Array((false, false), (true, false), (false, true), (true, true))
      } else {
        // will not try to remove redundant sort, and shorter optimization time
        Array((false, false))
      }
    candidates.foreach {
      case (requireLeftSorted, requireRightSorted) =>
        transformToEquiv(
          joinInfo.leftKeys,
          joinInfo.rightKeys,
          requireLeftSorted,
          requireRightSorted)
    }

    // add more possibility to only shuffle by partial joinKeys, now only single one
    val isShuffleByPartialKeyEnabled =
      tableConfig.get(BatchPhysicalJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED)
    if (isShuffleByPartialKeyEnabled && joinInfo.pairs().length > 1) {
      joinInfo.pairs().foreach {
        pair =>
          // sort require full key not partial key,
          // so requireLeftSorted and requireRightSorted should both be false here
          transformToEquiv(
            ImmutableIntList.of(pair.source),
            ImmutableIntList.of(pair.target),
            requireLeftSorted = false,
            requireRightSorted = false)
      }
    }
  }
}

object BatchPhysicalSortMergeJoinRule {
  val INSTANCE: RelOptRule = new BatchPhysicalSortMergeJoinRule

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_OPTIMIZER_SMJ_REMOVE_SORT_ENABLED: ConfigOption[JBoolean] =
    key("table.optimizer.smj.remove-sort-enabled")
      .booleanType()
      .defaultValue(JBoolean.FALSE)
      .withDescription(
        "When true, the optimizer will try to remove redundant sort " +
          "for sort merge join. However that will increase optimization time. " +
          "Default value is false.")
}
