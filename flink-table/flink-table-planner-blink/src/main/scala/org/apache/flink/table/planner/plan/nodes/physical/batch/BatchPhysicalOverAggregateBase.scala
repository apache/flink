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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.CalcitePair
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalJoinRuleBase
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil.splitOutOffsetOrInsensitiveGroup
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, OverAggregateUtil, RelExplainUtil}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelDistribution.Type._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.util.ImmutableIntList

import java.util

import scala.collection.JavaConversions._

/**
 * Base batch physical RelNode for sort-based over [[Window]] aggregate.
 */
abstract class BatchPhysicalOverAggregateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    windowGroups: Seq[Window.Group],
    logicWindow: Window)
  extends SingleRel(cluster, traitSet, inputRel)
  with BatchPhysicalRel {

  val partitionKeyIndices: Array[Int] = windowGroups.head.keys.toArray
  windowGroups.tail.foreach { g =>
    if (!util.Arrays.equals(partitionKeyIndices, g.keys.toArray)) {
      throw new TableException("" +
        "BatchPhysicalOverAggregateBase requires all groups should have same partition key.")
    }
  }

  protected lazy val offsetAndInsensitiveSensitiveGroups: Seq[Group] =
    splitOutOffsetOrInsensitiveGroup(windowGroups)

  override def deriveRowType: RelDataType = outputRowType

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // sort is done in the last sort operator.
    val inputRows = mq.getRowCount(getInput())
    if (inputRows == null) {
      return null
    }
    val cpu = FlinkCost.FUNC_CPU_COST * inputRows *
      offsetAndInsensitiveSensitiveGroups.flatMap(_.aggCalls).size
    val averageRowSize: Double = mq.getAverageRowSize(this)
    val memCost = averageRowSize
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpu, 0, 0, memCost)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val writer = super.explainTerms(pw)
      .itemIf("partitionBy",
        RelExplainUtil.fieldToString(partitionKeyIndices, inputRowType),
        partitionKeyIndices.nonEmpty)
      .itemIf("orderBy",
        RelExplainUtil.collationToString(windowGroups.head.orderKeys, inputRowType),
        windowGroups.head.orderKeys.getFieldCollations.nonEmpty)

    var offset = inputRowType.getFieldCount
    offsetAndInsensitiveSensitiveGroups.zipWithIndex.foreach { case (group, index) =>
      val namedAggregates = generateNamedAggregates(group)
      val select = RelExplainUtil.overAggregationToString(
        inputRowType,
        outputRowType,
        logicWindow.constants,
        namedAggregates,
        outputInputName = false,
        rowTypeOffset = offset)
      offset += namedAggregates.size
      val windowRange = RelExplainUtil.windowRangeToString(logicWindow, group)
      writer.item("window#" + index, select + windowRange)
    }
    writer.item("select", getRowType.getFieldNames.mkString(", "))
  }

  private def generateNamedAggregates(
      windowGroup: Group): Seq[CalcitePair[AggregateCall, String]] = {
    val aggregateCalls = windowGroup.getAggregateCalls(logicWindow)
    for (i <- 0 until aggregateCalls.size())
      yield new CalcitePair[AggregateCall, String](aggregateCalls.get(i), "windowAgg$" + i)
  }

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    if (requiredDistribution.getType == ANY && requiredCollation.getFieldCollations.isEmpty) {
      return None
    }

    val selfProvidedTraitSet = inferProvidedTraitSet()
    if (selfProvidedTraitSet.satisfies(requiredTraitSet)) {
      // Current node can satisfy the requiredTraitSet,return the current node with ProvidedTraitSet
      return Some(copy(selfProvidedTraitSet, Seq(getInput)))
    }

    val inputFieldCnt = getInput.getRowType.getFieldCount
    val canSatisfy = if (requiredDistribution.getType == ANY) {
      true
    } else {
      if (!partitionKeyIndices.isEmpty) {
        if (requiredDistribution.requireStrict) {
          requiredDistribution.getKeys == ImmutableIntList.of(partitionKeyIndices: _*)
        } else {
          val isAllFieldsFromInput = requiredDistribution.getKeys.forall(_ < inputFieldCnt)
          if (isAllFieldsFromInput) {
            val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(this)
            if (tableConfig.getConfiguration.getBoolean(
              BatchPhysicalJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED)) {
              ImmutableIntList.of(partitionKeyIndices: _*).containsAll(requiredDistribution.getKeys)
            } else {
              requiredDistribution.getKeys == ImmutableIntList.of(partitionKeyIndices: _*)
            }
          } else {
            // If requirement distribution keys are not all comes from input directly,
            // cannot satisfy requirement distribution and collations.
            false
          }
        }
      } else {
        requiredDistribution.getType == SINGLETON
      }
    }
    // If OverAggregate can provide distribution, but it's traits cannot satisfy required
    // distribution, cannot push down distribution and collation requirement (because later
    // shuffle will destroy previous collation.
    if (!canSatisfy) {
      return None
    }

    var inputRequiredTraits = getInput.getTraitSet
    var providedTraits = selfProvidedTraitSet
    val providedCollation = selfProvidedTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    if (!requiredDistribution.isTop) {
      inputRequiredTraits = inputRequiredTraits.replace(requiredDistribution)
      providedTraits = providedTraits.replace(requiredDistribution)
    }

    if (providedCollation.satisfies(requiredCollation)) {
      // the providedCollation can satisfy the requirement,
      // so don't push down the sort into it's input.
    } else if (providedCollation.getFieldCollations.isEmpty &&
      requiredCollation.getFieldCollations.nonEmpty) {
      // If OverAgg cannot provide collation itself, try to push down collation requirements into
      // it's input if collation fields all come from input node.
      val canPushDownCollation = requiredCollation.getFieldCollations
        .forall(_.getFieldIndex < inputFieldCnt)
      if (canPushDownCollation) {
        inputRequiredTraits = inputRequiredTraits.replace(requiredCollation)
        providedTraits = providedTraits.replace(requiredCollation)
      }
    } else {
      // Don't push down the sort into it's input,
      // due to the provided collation will destroy the input's provided collation.
    }
    val newInput = RelOptRule.convert(getInput, inputRequiredTraits)
    Some(copy(providedTraits, Seq(newInput)))
  }

  private def inferProvidedTraitSet(): RelTraitSet = {
    var selfProvidedTraitSet = getTraitSet
    // provided distribution
    val providedDistribution = if (partitionKeyIndices.nonEmpty) {
      FlinkRelDistribution.hash(
        partitionKeyIndices.map(Integer.valueOf).toList, requireStrict = false)
    } else {
      FlinkRelDistribution.SINGLETON
    }
    selfProvidedTraitSet = selfProvidedTraitSet.replace(providedDistribution)
    // provided collation
    val firstGroup = offsetAndInsensitiveSensitiveGroups.head
    if (OverAggregateUtil.needCollationTrait(logicWindow, firstGroup)) {
      val collation = OverAggregateUtil.createCollation(firstGroup)
      if (!collation.equals(RelCollations.EMPTY)) {
        selfProvidedTraitSet = selfProvidedTraitSet.replace(collation)
      }
    }
    selfProvidedTraitSet
  }

}
