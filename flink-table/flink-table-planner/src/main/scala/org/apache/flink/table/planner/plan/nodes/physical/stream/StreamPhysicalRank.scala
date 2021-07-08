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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.calcite.Rank
import org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.utils._
import org.apache.flink.table.runtime.operators.rank._

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.util.ImmutableBitSet

import java.util

import scala.collection.JavaConversions._


/**
 * Stream physical RelNode for [[Rank]].
 */
class StreamPhysicalRank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    partitionKey: ImmutableBitSet,
    orderKey: RelCollation,
    rankType: RankType,
    rankRange: RankRange,
    rankNumberType: RelDataTypeField,
    outputRankNumber: Boolean,
    rankStrategy: RankProcessStrategy)
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
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalRank(
      cluster,
      traitSet,
      inputs.get(0),
      partitionKey,
      orderKey,
      rankType,
      rankRange,
      rankNumberType,
      outputRankNumber,
      rankStrategy)
  }

  def copy(newStrategy: RankProcessStrategy): StreamPhysicalRank = {
    new StreamPhysicalRank(
      cluster,
      traitSet,
      inputRel,
      partitionKey,
      orderKey,
      rankType,
      rankRange,
      rankNumberType,
      outputRankNumber,
      newStrategy)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = inputRel.getRowType
    pw.input("input", getInput)
      .item("strategy", rankStrategy)
      .item("rankType", rankType)
      .item("rankRange", rankRange.toString(inputRowType.getFieldNames))
      .item("partitionBy", RelExplainUtil.fieldToString(partitionKey.toArray, inputRowType))
      .item("orderBy", RelExplainUtil.collationToString(orderKey, inputRowType))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  override def translateToExecNode(): ExecNode[_] = {
    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)
    val fieldCollations = orderKey.getFieldCollations
    new StreamExecRank(
      rankType,
      new PartitionSpec(partitionKey.toArray),
      SortUtil.getSortSpec(fieldCollations),
      rankRange,
      rankStrategy,
      outputRankNumber,
      generateUpdateBefore,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
