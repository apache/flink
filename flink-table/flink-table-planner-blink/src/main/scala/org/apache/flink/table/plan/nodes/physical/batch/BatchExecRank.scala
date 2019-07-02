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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.{BatchTableEnvironment, PlannerConfigOptions, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.sort.ComparatorCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.plan.nodes.calcite.Rank
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.util.{FlinkRelOptUtil, RelExplainUtil}
import org.apache.flink.table.runtime.rank.{ConstantRankRange, RankRange, RankType}
import org.apache.flink.table.runtime.sort.RankOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelDistribution.Type
import org.apache.calcite.rel.RelDistribution.Type.{HASH_DISTRIBUTED, SINGLETON}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.util.{ImmutableBitSet, ImmutableIntList, Util}
import java.util

import org.apache.flink.api.dag.Transformation

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for [[Rank]].
  *
  * This node supports two-stage(local and global) rank to reduce data-shuffling.
  */
class BatchExecRank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    partitionKey: ImmutableBitSet,
    orderKey: RelCollation,
    rankType: RankType,
    rankRange: RankRange,
    rankNumberType: RelDataTypeField,
    outputRankNumber: Boolean,
    val isGlobal: Boolean)
  extends Rank(
    cluster,
    traitSet,
    inputRel,
    partitionKey,
    orderKey,
    rankType,
    rankRange,
    rankNumberType,
    outputRankNumber)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow] {

  require(rankType == RankType.RANK, "Only RANK is supported now")
  val (rankStart, rankEnd) = rankRange match {
    case r: ConstantRankRange => (r.getRankStart, r.getRankEnd)
    case o => throw new TableException(s"$o is not supported now")
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecRank(
      cluster,
      traitSet,
      inputs.get(0),
      partitionKey,
      orderKey,
      rankType,
      rankRange,
      rankNumberType,
      outputRankNumber,
      isGlobal
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = inputRel.getRowType
    pw.item("input", getInput)
      .item("rankType", rankType)
      .item("rankRange", rankRange.toString(inputRowType.getFieldNames))
      .item("partitionBy", RelExplainUtil.fieldToString(partitionKey.toArray, inputRowType))
      .item("orderBy", RelExplainUtil.collationToString(orderKey, inputRowType))
      .item("global", isGlobal)
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // sort is done in the last sort operator, only need to compare between agg column.
    val inputRowCnt = mq.getRowCount(getInput())
    val cpuCost = FlinkCost.FUNC_CPU_COST * inputRowCnt
    val memCost: Double = mq.getAverageRowSize(this)
    val rowCount = mq.getRowCount(this)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCount, cpuCost, 0, 0, memCost)
  }

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    if (isGlobal) {
      satisfyTraitsOnGlobalRank(requiredTraitSet)
    } else {
      satisfyTraitsOnLocalRank(requiredTraitSet)
    }
  }

  private def satisfyTraitsOnGlobalRank(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val canSatisfy = requiredDistribution.getType match {
      case SINGLETON => partitionKey.cardinality() == 0
      case HASH_DISTRIBUTED =>
        val shuffleKeys = requiredDistribution.getKeys
        val partitionKeyList = ImmutableIntList.of(partitionKey.toArray: _*)
        if (requiredDistribution.requireStrict) {
          shuffleKeys == partitionKeyList
        } else if (Util.startsWith(shuffleKeys, partitionKeyList)) {
          // If required distribution is not strict, Hash[a] can satisfy Hash[a, b].
          // so return true if shuffleKeys(Hash[a, b]) start with partitionKeyList(Hash[a])
          true
        } else {
          // If partialKey is enabled, try to use partial key to satisfy the required distribution
          val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(this)
          val partialKeyEnabled = tableConfig.getConf.getBoolean(
            PlannerConfigOptions.SQL_OPTIMIZER_SHUFFLE_PARTIAL_KEY_ENABLED)
          partialKeyEnabled && partitionKeyList.containsAll(shuffleKeys)
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
        val partitionKeyList = ImmutableIntList.of(partitionKey.toArray: _*)
        if (requiredDistribution.requireStrict) {
          FlinkRelDistribution.hash(partitionKeyList)
        } else if (Util.startsWith(shuffleKeys, partitionKeyList)) {
          // Hash[a] can satisfy Hash[a, b]
          FlinkRelDistribution.hash(partitionKeyList, requireStrict = false)
        } else {
          // use partial key to satisfy the required distribution
          FlinkRelDistribution.hash(shuffleKeys.map(partitionKeyList(_)), requireStrict = false)
        }
    }

    // sort by partition keys + orderby keys
    val providedFieldCollations = partitionKey.toArray.map {
      k => FlinkRelOptUtil.ofRelFieldCollation(k)
    }.toList ++ orderKey.getFieldCollations
    val providedCollation = RelCollations.of(providedFieldCollations)
    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    val newProvidedTraitSet = if (providedCollation.satisfies(requiredCollation)) {
      getTraitSet.replace(requiredDistribution).replace(requiredCollation)
    } else {
      getTraitSet.replace(requiredDistribution)
    }
    val newInput = RelOptRule.convert(getInput, inputRequiredDistribution)
    Some(copy(newProvidedTraitSet, Seq(newInput)))
  }

  private def satisfyTraitsOnLocalRank(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    requiredDistribution.getType match {
      case Type.SINGLETON =>
        val inputRequiredDistribution = requiredDistribution
        // sort by orderby keys
        val providedCollation = orderKey
        val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
        val newProvidedTraitSet = if (providedCollation.satisfies(requiredCollation)) {
          getTraitSet.replace(requiredDistribution).replace(requiredCollation)
        } else {
          getTraitSet.replace(requiredDistribution)
        }

        val inputRequiredTraits = getInput.getTraitSet.replace(inputRequiredDistribution)
        val newInput = RelOptRule.convert(getInput, inputRequiredTraits)
        Some(copy(newProvidedTraitSet, Seq(newInput)))
      case Type.HASH_DISTRIBUTED =>
        val shuffleKeys = requiredDistribution.getKeys
        if (outputRankNumber) {
          // rank function column is the last one
          val rankColumnIndex = getRowType.getFieldCount - 1
          if (!shuffleKeys.contains(rankColumnIndex)) {
            // Cannot satisfy required distribution if some keys are not from input
            return None
          }
        }

        val inputRequiredDistributionKeys = shuffleKeys
        val inputRequiredDistribution = FlinkRelDistribution.hash(
          inputRequiredDistributionKeys, requiredDistribution.requireStrict)

        // sort by partition keys + orderby keys
        val providedFieldCollations = partitionKey.toArray.map {
          k => FlinkRelOptUtil.ofRelFieldCollation(k)
        }.toList ++ orderKey.getFieldCollations
        val providedCollation = RelCollations.of(providedFieldCollations)
        val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
        val newProvidedTraitSet = if (providedCollation.satisfies(requiredCollation)) {
          getTraitSet.replace(requiredDistribution).replace(requiredCollation)
        } else {
          getTraitSet.replace(requiredDistribution)
        }

        val inputRequiredTraits = getInput.getTraitSet.replace(inputRequiredDistribution)
        val newInput = RelOptRule.convert(getInput, inputRequiredTraits)
        Some(copy(newProvidedTraitSet, Seq(newInput)))
      case _ => None
    }
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] =
    List(getInput.asInstanceOf[ExecNode[BatchTableEnvironment, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): Transformation[BaseRow] = {
    val input = getInputNodes.get(0).translateToPlan(tableEnv)
        .asInstanceOf[Transformation[BaseRow]]
    val outputType = FlinkTypeFactory.toLogicalRowType(getRowType)
    val partitionBySortingKeys = partitionKey.toArray
    // The collation for the partition-by fields is inessential here, we only use the
    // comparator to distinguish different groups.
    // (order[is_asc], null_is_last)
    val partitionBySortCollation = partitionBySortingKeys.map(_ => (true, true))

    // The collation for the order-by fields is inessential here, we only use the
    // comparator to distinguish order-by fields change.
    // (order[is_asc], null_is_last)
    val orderByCollation = orderKey.getFieldCollations.map(_ => (true, true)).toArray
    val orderByKeys = orderKey.getFieldCollations.map(_.getFieldIndex).toArray

    val inputType = FlinkTypeFactory.toLogicalRowType(getInput.getRowType)
    //operator needn't cache data
    val operator = new RankOperator(
      ComparatorCodeGenerator.gen(
        tableEnv.getConfig,
        "PartitionByComparator",
        partitionBySortingKeys,
        partitionBySortingKeys.map(inputType.getTypeAt),
        partitionBySortCollation.map(_._1),
        partitionBySortCollation.map(_._2)),
      ComparatorCodeGenerator.gen(
        tableEnv.getConfig,
        "OrderByComparator",
        orderByKeys,
        orderByKeys.map(inputType.getTypeAt),
        orderByCollation.map(_._1),
        orderByCollation.map(_._2)),
      rankStart,
      rankEnd,
      outputRankNumber)

    new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      BaseRowTypeInfo.of(outputType),
      getResource.getParallelism)
  }

  private def getOperatorName: String = {
    if (isGlobal) {
      "GlobalRank"
    } else {
      "LocalRank"
    }
  }
}
