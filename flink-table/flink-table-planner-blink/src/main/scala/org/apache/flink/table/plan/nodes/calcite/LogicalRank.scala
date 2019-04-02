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

package org.apache.flink.table.plan.nodes.calcite

import org.apache.flink.table.plan.nodes.calcite.RankType.RankType

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.rel.{RelCollation, RelNode}
import org.apache.calcite.util.ImmutableBitSet

import java.util

import scala.collection.JavaConversions._

/**
  * Sub-class of [[Rank]] that is a relational expression which returns
  * the rows in which the rank function value of each row is in the given range.
  * This class corresponds to Calcite logical rel.
  */
final class LogicalRank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    partitionKey: ImmutableBitSet,
    orderKey: RelCollation,
    rankType: RankType,
    rankRange: RankRange,
    rankNumberType: RelDataTypeField,
    outputRankNumber: Boolean)
  extends Rank(
    cluster,
    traitSet,
    input,
    partitionKey,
    orderKey,
    rankType,
    rankRange,
    rankNumberType,
    outputRankNumber) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new LogicalRank(
      cluster,
      traitSet,
      inputs.head,
      partitionKey,
      orderKey,
      rankType,
      rankRange,
      rankNumberType,
      outputRankNumber
    )
  }
}

object LogicalRank {

  def create(
      input: RelNode,
      partitionKey: ImmutableBitSet,
      orderKey: RelCollation,
      rankType: RankType,
      rankRange: RankRange,
      rankNumberType: RelDataTypeField,
      outputRankNumber: Boolean): LogicalRank = {
    val traits = input.getCluster.traitSetOf(Convention.NONE)
    new LogicalRank(
      input.getCluster,
      traits,
      input,
      partitionKey,
      orderKey,
      rankType,
      rankRange,
      rankNumberType,
      outputRankNumber
    )
  }
}
