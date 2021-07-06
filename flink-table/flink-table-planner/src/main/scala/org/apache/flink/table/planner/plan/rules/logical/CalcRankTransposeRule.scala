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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.logical._
import org.apache.flink.table.planner.plan.utils.{FlinkRexUtil, RankUtil}
import org.apache.flink.table.runtime.operators.rank.VariableRankRange

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.{RexBuilder, RexInputRef, RexProgram}
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.JavaConversions._

/**
  * Planner rule that transposes [[FlinkLogicalCalc]] past [[FlinkLogicalRank]]
  * to reduce rank input fields.
  */
class CalcRankTransposeRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalCalc],
      operand(classOf[FlinkLogicalRank], any())),
    "CalcRankTransposeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0)
    val rank: FlinkLogicalRank = call.rel(1)

    val totalColumnCount = rank.getInput.getRowType.getFieldCount
    // apply the rule only when calc could prune some columns
    val pushableColumns = getPushableColumns(calc, rank)
    pushableColumns.length < totalColumnCount
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: FlinkLogicalCalc = call.rel(0)
    val rank: FlinkLogicalRank = call.rel(1)

    val pushableColumns = getPushableColumns(calc, rank)

    val rexBuilder = calc.getCluster.getRexBuilder
    // create a new Calc to project columns of Rank's input
    val innerProgram = createNewInnerCalcProgram(
      pushableColumns,
      rank.getInput.getRowType,
      rexBuilder)
    val newInnerCalc = calc.copy(calc.getTraitSet, rank.getInput, innerProgram)

    // create a new Rank on top of new Calc
    var fieldMapping = pushableColumns.zipWithIndex.toMap
    val newRank = createNewRankOnCalc(fieldMapping, newInnerCalc, rank)

    // create a new Calc on top of newRank if needed
    if (rank.outputRankNumber) {
      // append RankNumber field mapping
      val oldRankFunFieldIdx = RankUtil.getRankNumberColumnIndex(rank)
        .getOrElse(throw new TableException("This should not happen"))
      val newRankFunFieldIdx = RankUtil.getRankNumberColumnIndex(newRank)
        .getOrElse(throw new TableException("This should not happen"))
      fieldMapping += (oldRankFunFieldIdx -> newRankFunFieldIdx)
    }
    val topProgram = createNewTopCalcProgram(
      calc.getProgram,
      fieldMapping,
      newRank.getRowType,
      rexBuilder)

    val equiv = if (topProgram.isTrivial) {
      // Ignore newTopCac if it's program is trivial
      newRank
    } else {
      calc.copy(calc.getTraitSet, newRank, topProgram)
    }
    call.transformTo(equiv)
  }

  private def getPushableColumns(calc: Calc, rank: FlinkLogicalRank): Array[Int] = {
    val usedFields = getUsedFields(calc.getProgram)
    val rankFunFieldIndex = RankUtil.getRankNumberColumnIndex(rank).getOrElse(-1)
    val usedFieldsExcludeRankNumber = usedFields.filter(_ != rankFunFieldIndex)

    val requiredFields = getKeyFields(rank)
    usedFieldsExcludeRankNumber.union(requiredFields).distinct.sorted
  }

  private def getUsedFields(program: RexProgram): Array[Int] = {
    val projects = program.getProjectList.map(program.expandLocalRef)
    val condition = if (program.getCondition != null) {
      program.expandLocalRef(program.getCondition)
    } else {
      null
    }
    RelOptUtil.InputFinder.bits(projects, condition).toArray
  }

  private def getKeyFields(rank: FlinkLogicalRank): Array[Int] = {
    val partitionKey = rank.partitionKey.toArray
    val orderKey = rank.orderKey.getFieldCollations.map(_.getFieldIndex).toArray
    val upsertKeys = FlinkRelMetadataQuery.reuseOrCreate(rank.getCluster.getMetadataQuery)
        .getUpsertKeysInKeyGroupRange(rank.getInput, partitionKey)
    val keysInUniqueKeys = if (upsertKeys == null || upsertKeys.isEmpty) {
      Array[Int]()
    } else {
      upsertKeys.flatMap(_.toArray).toArray
    }
    val rankRangeKey = rank.rankRange match {
      case v: VariableRankRange => Array(v.getRankEndIndex)
      case _ => Array[Int]()
    }
    // All key including partition key, order key, unique keys, VariableRankRange rankEndIndex
    Set(partitionKey, orderKey, keysInUniqueKeys, rankRangeKey).flatten.toArray
  }

  private def createNewInnerCalcProgram(
      projectedFields: Array[Int],
      inputRowType: RelDataType,
      rexBuilder: RexBuilder): RexProgram = {
    val projects = projectedFields.map(RexInputRef.of(_, inputRowType))
    val inputColNames = inputRowType.getFieldNames
    val colNames = projectedFields.map(inputColNames.get)
    RexProgram.create(inputRowType, projects.toList, null, colNames.toList, rexBuilder)
  }

  private def createNewTopCalcProgram(
      oldTopProgram: RexProgram,
      fieldMapping: Map[Int, Int],
      inputRowType: RelDataType,
      rexBuilder: RexBuilder): RexProgram = {
    val oldProjects = oldTopProgram.getProjectList
    val newProjects = oldProjects.map(oldTopProgram.expandLocalRef).map {
      p => FlinkRexUtil.adjustInputRef(p, fieldMapping)
    }
    val oldCondition = oldTopProgram.getCondition
    val newCondition = if (oldCondition != null) {
      FlinkRexUtil.adjustInputRef(oldTopProgram.expandLocalRef(oldCondition), fieldMapping)
    } else {
      null
    }
    val colNames = oldTopProgram.getOutputRowType.getFieldNames
    RexProgram.create(
      inputRowType,
      newProjects,
      newCondition,
      colNames,
      rexBuilder)
  }

  private def createNewRankOnCalc(
      fieldMapping: Map[Int, Int],
      input: Calc,
      rank: FlinkLogicalRank): FlinkLogicalRank = {
    val newPartitionKey = rank.partitionKey.toArray.map(fieldMapping(_))
    val oldOrderKey = rank.orderKey
    val oldFieldCollations = oldOrderKey.getFieldCollations
    val newFieldCollations = oldFieldCollations.map {
      fc => fc.copy(fieldMapping(fc.getFieldIndex))
    }
    val newOrderKey = if (newFieldCollations.eq(oldFieldCollations)) {
      oldOrderKey
    } else {
      RelCollations.of(newFieldCollations)
    }
    new FlinkLogicalRank(
      rank.getCluster,
      rank.getTraitSet,
      input,
      ImmutableBitSet.of(newPartitionKey: _*),
      newOrderKey,
      rank.rankType,
      rank.rankRange,
      rank.rankNumberType,
      rank.outputRankNumber)
  }
}

object CalcRankTransposeRule {
  val INSTANCE = new CalcRankTransposeRule
}
