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
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfigOptions, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{GeneratedSorter, SortCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.cost.FlinkBatchCost.FUNC_CPU_COST
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.nodes.calcite.Rank
import org.apache.flink.table.plan.nodes.exec.RowBatchExecNode
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.{ConstantRankRange, FlinkRelOptUtil, FlinkRexUtil, RankRange}
import org.apache.flink.table.runtime.aggregate.RelFieldCollations
import org.apache.flink.table.runtime.rank.RankOperator
import org.apache.flink.table.typeutils.TypeUtils

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelDistribution.Type
import org.apache.calcite.rel.RelDistribution.Type.{HASH_DISTRIBUTED, SINGLETON}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.sql.{SqlKind, SqlRankFunction}
import org.apache.calcite.util.{ImmutableBitSet, ImmutableIntList, Util}

import java.util

import scala.collection.JavaConversions._

class BatchExecRank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    rankFunction: SqlRankFunction,
    partitionKey: ImmutableBitSet,
    sortCollation: RelCollation,
    rankRange: RankRange,
    val outputRankFunColumn: Boolean,
    val isGlobal: Boolean)
  extends Rank(
    cluster,
    traitSet,
    input,
    rankFunction,
    partitionKey,
    sortCollation,
    rankRange)
  with BatchPhysicalRel
  with RowBatchExecNode {

  require(rankFunction.kind == SqlKind.RANK, "Only RANK is supported now")
  val (rankStart, rankEnd) = rankRange match {
    case r: ConstantRankRange => (r.rankStart, r.rankEnd)
    case o => throw new TableException(s"$o is not supported now")
  }

  override def deriveRowType(): RelDataType = {
    if (outputRankFunColumn) {
      super.deriveRowType()
    } else {
      input.getRowType
    }
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecRank(
      cluster,
      traitSet,
      inputs.head,
      rankFunction,
      partitionKey,
      sortCollation,
      rankRange,
      outputRankFunColumn,
      isGlobal
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputFieldNames = input.getRowType.getFieldNames
    pw.item("input", getInput)
      .item("rankFunction", rankFunction)
      .item("partitionBy", partitionKey.map(inputFieldNames.get(_)).mkString(","))
      .item("orderBy", Rank.sortFieldsToString(sortCollation, input.getRowType))
      .item("rankRange", rankRange.toString(inputFieldNames))
      .item("global", isGlobal)
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  override def isDeterministic: Boolean = FlinkRexUtil.isDeterministicOperator(rankFunction)

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // sort is done in the last sort operator, only need to compare between agg column.
    val inputRows = mq.getRowCount(getInput())
    val cpuCost = FUNC_CPU_COST * inputRows
    val memCost: Double = mq.getAverageRowSize(this)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpuCost, 0, 0, memCost)
  }

  override def satisfyTraitsByInput(requiredTraitSet: RelTraitSet): RelNode = {
    if (isGlobal) {
      satisfyTraitsByInputForGlobal(requiredTraitSet)
    } else {
      satisfyTraitsByInputForLocal(requiredTraitSet)
    }
  }

  private def satisfyTraitsByInputForGlobal(requiredTraitSet: RelTraitSet): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val pushDownDistribution = requiredDistribution.getType match {
      case SINGLETON => if (partitionKey.cardinality() == 0) requiredDistribution else null
      case HASH_DISTRIBUTED =>
        val shuffleKeys = requiredDistribution.getKeys
        val partitionKeyList = ImmutableIntList.of(partitionKey.toArray: _*)
        if (requiredDistribution.requireStrict) {
          if (shuffleKeys == partitionKeyList) {
            FlinkRelDistribution.hash(partitionKeyList)
          } else {
            null
          }
        } else if (Util.startsWith(shuffleKeys, partitionKeyList)) {
          // If required distribution is not strict, Hash[a] can satisfy Hash[a, b].
          // If partitionKeys satisfies shuffleKeys (the shuffle between this node and
          // its output is not necessary), just push down partitionKeys into input.
          FlinkRelDistribution.hash(partitionKeyList, requireStrict = false)
        } else {
          val tableConfig = FlinkRelOptUtil.getTableConfig(this)
          if (tableConfig.getConf.getBoolean(
            TableConfigOptions.SQL_OPTIMIZER_SHUFFLE_PARTIAL_KEY_ENABLED) &&
            partitionKeyList.containsAll(shuffleKeys)) {
            // If partialKey is enabled, push down partialKey requirement into input.
            FlinkRelDistribution.hash(shuffleKeys.map(partitionKeyList(_)), requireStrict = false)
          } else {
            null
          }
        }
      case _ => null
    }
    if (pushDownDistribution == null) {
      return null
    }
    // sort by partition keys + orderby keys
    val providedFieldCollations = partitionKey.toArray.map(RelFieldCollations.of).toList ++
      sortCollation.getFieldCollations
    val providedCollation = RelCollations.of(providedFieldCollations)
    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    val newProvidedTraitSet = if (providedCollation.satisfies(requiredCollation)) {
      getTraitSet.replace(requiredDistribution).replace(requiredCollation)
    } else {
      getTraitSet.replace(requiredDistribution)
    }
    val newInput = RelOptRule.convert(getInput, pushDownDistribution)
    copy(newProvidedTraitSet, Seq(newInput))
  }

  private def satisfyTraitsByInputForLocal(requiredTraitSet: RelTraitSet): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    requiredDistribution.getType match {
      case Type.SINGLETON =>
        val pushDownDistribution = requiredDistribution
        // sort by orderby keys
        val providedCollation = sortCollation
        val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
        val newProvidedTraitSet = if (providedCollation.satisfies(requiredCollation)) {
          getTraitSet.replace(requiredDistribution).replace(requiredCollation)
        } else {
          getTraitSet.replace(requiredDistribution)
        }

        val pushDownRelTraits = input.getTraitSet.replace(pushDownDistribution)
        val newInput = RelOptRule.convert(getInput, pushDownRelTraits)
        copy(newProvidedTraitSet, Seq(newInput))
      case Type.HASH_DISTRIBUTED =>
        val shuffleKeys = requiredDistribution.getKeys
        if (outputRankFunColumn) {
          // rank function column is the last one
          val rankColumnIndex = getRowType.getFieldCount - 1
          if (!shuffleKeys.contains(rankColumnIndex)) {
            // Cannot push down distribution if some keys are not from input
            return null
          }
        }

        val pushDownDistributionKeys = shuffleKeys
        val pushDownDistribution = FlinkRelDistribution.hash(
          pushDownDistributionKeys, requiredDistribution.requireStrict)

        // sort by partition keys + orderby keys
        val providedFieldCollations = partitionKey.toArray.map(RelFieldCollations.of).toList ++
          sortCollation.getFieldCollations
        val providedCollation = RelCollations.of(providedFieldCollations)
        val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
        val newProvidedTraitSet = if (providedCollation.satisfies(requiredCollation)) {
          getTraitSet.replace(requiredDistribution).replace(requiredCollation)
        } else {
          getTraitSet.replace(requiredDistribution)
        }

        val pushDownRelTraits = input.getTraitSet.replace(pushDownDistribution)
        val newInput = RelOptRule.convert(getInput, pushDownRelTraits)
        copy(newProvidedTraitSet, Seq(newInput))
      case _ => null
    }
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def accept(visitor: BatchExecNodeVisitor): Unit = visitor.visit(this)

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  /**
    * Internal method, translates the [[org.apache.flink.table.plan.nodes.exec.BatchExecNode]]
    * into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {

    val input = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val outputType = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType)
    val partitionBySortingKeys = partitionKey.toArray
    // The collation for the partition-by fields is inessential here, we only use the
    // comparator to distinguish different groups.
    // (order[is_asc], null_is_last)
    val partitionBySortCollation = partitionBySortingKeys.map(_ => (true, true))

    val inputRowType = FlinkTypeFactory.toInternalRowType(getInput.getRowType)
    val (partitionByComparators, partitionBySerializers) = TypeUtils.flattenComparatorAndSerializer(
      inputRowType.getArity,
      partitionBySortingKeys,
      partitionBySortCollation.map(_._1),
      inputRowType.getFieldInternalTypes)
    val partitionByCodeGen = new SortCodeGenerator(
      partitionBySortingKeys,
      partitionBySortingKeys.map(inputRowType.getInternalTypeAt),
      partitionByComparators,
      partitionBySortCollation.map(_._1),
      partitionBySortCollation.map(_._2))
    val partitionBySorter = GeneratedSorter(
      null,
      partitionByCodeGen.generateRecordComparator("PartitionByComparator"),
      partitionBySerializers,
      partitionByComparators)

    // The collation for the order-by fields is inessential here, we only use the
    // comparator to distinguish order-by fields change.
    // (order[is_asc], null_is_last)
    val orderByCollation = sortCollation.getFieldCollations.map(_ => (true, true)).toArray
    val orderByKeys = sortCollation.getFieldCollations.map(_.getFieldIndex).toArray

    val (orderByComparators, orderBySerializers) = TypeUtils.flattenComparatorAndSerializer(
      inputRowType.getArity,
      orderByKeys,
      orderByCollation.map(_._1),
      inputRowType.getFieldInternalTypes)
    val orderBySortCodeGen = new SortCodeGenerator(
      orderByKeys,
      orderByKeys.map(inputRowType.getInternalTypeAt),
      orderByComparators,
      orderByCollation.map(_._1),
      orderByCollation.map(_._2))
    val orderBySorter = GeneratedSorter(
      null,
      orderBySortCodeGen.generateRecordComparator("OrderByComparator"),
      orderBySerializers,
      orderByComparators)

    //operator needn't cache data
    val operator = new RankOperator(
      partitionBySorter, orderBySorter, rankStart, rankEnd, outputRankFunColumn)
    val transformation = new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      outputType,
      getResource.getParallelism)
    tableEnv.getRUKeeper.addTransformation(this, transformation)
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }

  private def getOperatorName: String = {
    if (isGlobal) {
      "GlobalRank"
    } else {
      "LocalRank"
    }
  }
}
