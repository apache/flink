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

package org.apache.flink.table.plan.nodes.logical

import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.util.MatchUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Match
import org.apache.calcite.rel.logical.LogicalMatch
import org.apache.calcite.rel.{RelCollation, RelNode}
import org.apache.calcite.rex.RexNode

import java.util

class FlinkLogicalMatch(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    pattern: RexNode,
    strictStart: Boolean,
    strictEnd: Boolean,
    patternDefinitions: util.Map[String, RexNode],
    measures: util.Map[String, RexNode],
    after: RexNode,
    subsets: util.Map[String, _ <: util.SortedSet[String]],
    rowsPerMatch: RexNode,
    partitionKeys: util.List[RexNode],
    orderKeys: RelCollation,
    interval: RexNode,
    rowType: RelDataType)
  extends Match(
    cluster,
    traitSet,
    input,
    rowType,
    pattern,
    strictStart,
    strictEnd,
    patternDefinitions,
    measures,
    after,
    subsets,
    rowsPerMatch,
    partitionKeys,
    orderKeys,
    interval)
  with FlinkLogicalRel {

  def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      rowType: RelDataType,
      pattern: RexNode,
      strictStart: Boolean,
      strictEnd: Boolean,
      patternDefinitions: util.Map[String, RexNode],
      measures: util.Map[String, RexNode],
      after: RexNode,
      subsets: util.Map[String, _ <: util.SortedSet[String]],
      rowsPerMatch: RexNode,
      partitionKeys: util.List[RexNode],
      orderKeys: RelCollation,
      interval: RexNode): Match = {
    new FlinkLogicalMatch(
      cluster,
      traitSet,
      input,
      pattern,
      strictStart,
      strictEnd,
      patternDefinitions,
      measures,
      after,
      subsets,
      rowsPerMatch,
      partitionKeys,
      orderKeys,
      interval,
      rowType)
  }

  override def copy(
      input: RelNode,
      rowType: RelDataType,
      pattern: RexNode,
      strictStart: Boolean,
      strictEnd: Boolean,
      patternDefinitions: util.Map[String, RexNode],
      measures: util.Map[String, RexNode],
      after: RexNode,
      subsets: util.Map[String, _ <: util.SortedSet[String]],
      rowsPerMatch: RexNode,
      partitionKeys: util.List[RexNode],
      orderKeys: RelCollation,
      interval: RexNode): Match = {
    new FlinkLogicalMatch(
      cluster,
      traitSet,
      input,
      pattern,
      strictStart,
      strictEnd,
      patternDefinitions,
      measures,
      after,
      subsets,
      rowsPerMatch,
      partitionKeys,
      orderKeys,
      interval,
      rowType)
  }

  override def isDeterministic: Boolean = MatchUtil.isDeterministic(this)
}

private class FlinkLogicalMatchConverter
  extends ConverterRule(
    classOf[LogicalMatch],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalMatchConverter") {

  override def convert(rel: RelNode): RelNode = {
    val logicalMatch = rel.asInstanceOf[LogicalMatch]
    val newInput = RelOptRule.convert(logicalMatch.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalMatch.create(
      newInput,
      logicalMatch.getPattern,
      logicalMatch.isStrictStart,
      logicalMatch.isStrictEnd,
      logicalMatch.getPatternDefinitions,
      logicalMatch.getMeasures,
      logicalMatch.getAfter,
      logicalMatch.getSubsets,
      logicalMatch.getRowsPerMatch,
      logicalMatch.getPartitionKeys,
      logicalMatch.getOrderKeys,
      logicalMatch.getInterval,
      logicalMatch.getRowType)
  }
}

object FlinkLogicalMatch {
  val CONVERTER: ConverterRule = new FlinkLogicalMatchConverter()

  def create(
      input: RelNode,
      pattern: RexNode,
      strictStart: Boolean,
      strictEnd: Boolean,
      patternDefinitions: util.Map[String, RexNode],
      measures: util.Map[String, RexNode],
      after: RexNode,
      subsets: util.Map[String, _ <: util.SortedSet[String]],
      rowsPerMatch: RexNode,
      partitionKeys: util.List[RexNode],
      orderKeys: RelCollation,
      interval: RexNode,
      rowType: RelDataType): FlinkLogicalMatch = {
    val cluster = input.getCluster
    val traitSet = cluster.traitSetOf(Convention.NONE)
    // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
    // the distribution trait, so we have to create FlinkLogicalMatch to
    // calculate the distribution trait
    val logicalMatch = new FlinkLogicalMatch(
      cluster,
      traitSet,
      input,
      pattern,
      strictStart,
      strictEnd,
      patternDefinitions,
      measures,
      after,
      subsets,
      rowsPerMatch,
      partitionKeys,
      orderKeys,
      interval,
      rowType)
    val newTraitSet = FlinkRelMetadataQuery.traitSet(logicalMatch)
      .replace(FlinkConventions.LOGICAL).simplify()
    logicalMatch.copy(
      newTraitSet,
      input,
      rowType,
      pattern,
      strictStart,
      strictEnd,
      patternDefinitions,
      measures,
      after,
      subsets,
      rowsPerMatch,
      partitionKeys,
      orderKeys,
      interval).asInstanceOf[FlinkLogicalMatch]
  }
}
