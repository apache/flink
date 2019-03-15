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
package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.table.plan.`trait`.TraitUtil
import org.apache.flink.table.plan.nodes.calcite.{Rank, RankRange}
import org.apache.flink.table.plan.util.{RelExplainUtil, UpdatingPlanChecker}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.SqlRankFunction
import org.apache.calcite.util.ImmutableBitSet

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for [[Rank]].
  *
  * @see [[StreamExecTemporalSort]] which must be time-ascending-order sort without `limit`.
  * @see [[StreamExecSort]] which can be used for testing now, its sort key can be any type.
  */
class StreamExecRank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    rankFunction: SqlRankFunction,
    partitionKey: ImmutableBitSet,
    sortCollation: RelCollation,
    rankRange: RankRange,
    val outputRankFunColumn: Boolean)
  extends Rank(
    cluster,
    traitSet,
    inputRel,
    rankFunction,
    partitionKey,
    sortCollation,
    rankRange)
  with StreamPhysicalRel {

  /** please uses [[getStrategy]] instead of this field */
  private var strategy: RankStrategy = _

  def getStrategy(forceRecompute: Boolean = false): RankStrategy = {
    if (strategy == null || forceRecompute) {
      strategy = analyzeRankStrategy
    }
    strategy
  }

  override def producesUpdates = true

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = {
    getStrategy(forceRecompute = true) == RetractRank
  }

  override def consumesRetractions = true

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = {
    if (outputRankFunColumn) {
      super.deriveRowType()
    } else {
      inputRel.getRowType
    }
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecRank(
      cluster,
      traitSet,
      inputs.get(0),
      rankFunction,
      partitionKey,
      sortCollation,
      rankRange,
      outputRankFunColumn)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = inputRel.getRowType
    pw.input("input", getInput)
      .item("strategy", getStrategy())
      .item("rankFunction", rankFunction.getKind)
      .item("rankRange", rankRange.toString(inputRowType.getFieldNames))
      .item("partitionBy", RelExplainUtil.fieldToString(partitionKey.toArray, inputRowType))
      .item("orderBy", RelExplainUtil.collationToString(sortCollation, inputRowType))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  private def analyzeRankStrategy: RankStrategy = {
    val rankInput = getInput
    val mq = cluster.getMetadataQuery
    val isUpdateStream = !UpdatingPlanChecker.isAppendOnly(rankInput)

    if (isUpdateStream) {
      val inputIsAccRetract = TraitUtil.isAccRetract(rankInput)
      val uniqueKeys = mq.getUniqueKeys(rankInput)
      if (inputIsAccRetract || uniqueKeys == null || uniqueKeys.isEmpty
        // unique key should contains partition key
        || !uniqueKeys.exists(k => k.contains(partitionKey))) {
        // input is AccRetract or extract the unique keys failed,
        // and we fall back to using retract rank
        RetractRank
      } else {
        // TODO get `isMonotonic` value by RelModifiedMonotonicity handler
        val isMonotonic = false

        if (isMonotonic) {
          //FIXME choose a set of primary key
          UpdateFastRank(uniqueKeys.iterator().next().toArray)
        } else {
          val fieldCollations = sortCollation.getFieldCollations
          if (fieldCollations.length == 1) {
            // single sort key in update stream scenario (no monotonic)
            // we can utilize unary rank function to speed up processing
            UnaryUpdateRank(uniqueKeys.iterator().next().toArray)
          } else {
            // no other choices, have to use retract rank
            RetractRank
          }
        }
      }
    } else {
      AppendFastRank
    }
  }
}

/**
  * Base class of Strategy to choose different process function.
  */
sealed trait RankStrategy

case object AppendFastRank extends RankStrategy

case object RetractRank extends RankStrategy

case class UpdateFastRank(primaryKeys: Array[Int]) extends RankStrategy {
  override def toString: String = "UpdateFastRank" + primaryKeys.mkString("[", ",", "]")
}

case class UnaryUpdateRank(primaryKeys: Array[Int]) extends RankStrategy {
  override def toString: String = "UnaryUpdateRank" + primaryKeys.mkString("[", ",", "]")
}
