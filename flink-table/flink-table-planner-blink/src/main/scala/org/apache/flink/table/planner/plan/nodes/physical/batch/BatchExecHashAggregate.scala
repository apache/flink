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

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchExecJoinRuleBase
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, RelExplainUtil}

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelDistribution.Type.{HASH_DISTRIBUTED, SINGLETON}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.{ImmutableIntList, Util}

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for (global) hash-based aggregate operator.
  *
  * @see [[BatchExecGroupAggregateBase]] for more info.
  */
class BatchExecHashAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    val aggInputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    isMerge: Boolean)
  extends BatchExecHashAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputRel,
    outputRowType,
    inputRowType,
    aggInputRowType,
    grouping,
    auxGrouping,
    aggCallToAggFunction,
    isMerge = isMerge,
    isFinal = true) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecHashAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      aggInputRowType,
      grouping,
      auxGrouping,
      aggCallToAggFunction,
      isMerge)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("isMerge", isMerge)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .itemIf("auxGrouping", RelExplainUtil.fieldToString(auxGrouping, inputRowType),
        auxGrouping.nonEmpty)
      .item("select", RelExplainUtil.groupAggregationToString(
        inputRowType,
        getRowType,
        grouping,
        auxGrouping,
        aggCallToAggFunction,
        isMerge,
        isGlobal = true))
  }

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val canSatisfy = requiredDistribution.getType match {
      case SINGLETON => grouping.length == 0
      case HASH_DISTRIBUTED =>
        val shuffleKeys = requiredDistribution.getKeys
        val groupKeysList = ImmutableIntList.of(grouping.indices.toArray: _*)
        if (requiredDistribution.requireStrict) {
          shuffleKeys == groupKeysList
        } else if (Util.startsWith(shuffleKeys, groupKeysList)) {
          // If required distribution is not strict, Hash[a] can satisfy Hash[a, b].
          // so return true if shuffleKeys(Hash[a, b]) start with groupKeys(Hash[a])
          true
        } else {
          // If partialKey is enabled, try to use partial key to satisfy the required distribution
          val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(this)
          val partialKeyEnabled = tableConfig.getConfiguration.getBoolean(
            BatchExecJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED)
          partialKeyEnabled && groupKeysList.containsAll(shuffleKeys)
        }
      case _ => false
    }
    if (!canSatisfy) {
      return None
    }

    val inputRequiredDistribution = requiredDistribution.getType match {
      case SINGLETON => requiredDistribution
      case HASH_DISTRIBUTED =>
        val shuffleKeys = requiredDistribution.getKeys
        val groupKeysList = ImmutableIntList.of(grouping.indices.toArray: _*)
        if (requiredDistribution.requireStrict) {
          FlinkRelDistribution.hash(grouping, requireStrict = true)
        } else if (Util.startsWith(shuffleKeys, groupKeysList)) {
          // Hash[a] can satisfy Hash[a, b]
          FlinkRelDistribution.hash(grouping, requireStrict = false)
        } else {
          // use partial key to satisfy the required distribution
          FlinkRelDistribution.hash(shuffleKeys.map(grouping(_)).toArray, requireStrict = false)
        }
    }

    val newInput = RelOptRule.convert(getInput, inputRequiredDistribution)
    val newProvidedTraitSet = getTraitSet.replace(requiredDistribution)
    Some(copy(newProvidedTraitSet, Seq(newInput)))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior = DamBehavior.FULL_DAM
}
