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

package org.apache.flink.api.table.plan.rules.dataSet

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableBitSet
import com.google.common.collect.ImmutableList

import scala.collection.JavaConversions._

/**
  * This rule is a (fixed) copy of the FilterAggregateTransposeRule of Apache Calcite.
  *
  * A fix for this rule is contained Calcite's master branch.
  * This custom rule can be removed once Calcite 1.7 is released and our dependency adjusted.
  *
  * Planner rule that pushes a `org.apache.calcite.rel.core.Filter`
  * past a `org.apache.calcite.rel.core.Aggregate`.
  *
  * @see org.apache.calcite.rel.rules.AggregateFilterTransposeRule
  */
class FlinkFilterAggregateTransposeRule(
    filterClass: Class[_ <: Filter],
    builderFactory: RelBuilderFactory,
    aggregateClass: Class[_ <: Aggregate])
  extends RelOptRule(
    RelOptRule.operand(filterClass, RelOptRule.operand(aggregateClass, RelOptRule.any)),
    builderFactory,
    null)
{

  def onMatch(call: RelOptRuleCall) {
    val filterRel: Filter = call.rel(0)
    val aggRel: Aggregate = call.rel(1)
    val conditions = RelOptUtil.conjunctions(filterRel.getCondition).toList
    val rexBuilder = filterRel.getCluster.getRexBuilder
    val origFields = aggRel.getRowType.getFieldList.toList

    // Fixed computation of adjustments
    val adjustments = aggRel.getGroupSet.asList().zipWithIndex.map {
      case (g, i) => g - i
    }.toArray

    var pushedConditions: List[RexNode] = Nil
    var remainingConditions: List[RexNode] = Nil

    for (condition <- conditions) {
      val rCols: ImmutableBitSet = RelOptUtil.InputFinder.bits(condition)
      if (canPush(aggRel, rCols)) {
        pushedConditions = condition.accept(
            new RelOptUtil.RexInputConverter(
              rexBuilder,
              origFields,
              aggRel.getInput(0).getRowType.getFieldList,
              adjustments)) :: pushedConditions
      }
      else {
        remainingConditions = condition :: remainingConditions
      }
    }
    val builder: RelBuilder = call.builder
    var rel: RelNode = builder.push(aggRel.getInput).filter(pushedConditions).build
    if (rel eq aggRel.getInput(0)) {
      return
    }
    rel = aggRel.copy(aggRel.getTraitSet, ImmutableList.of(rel))
    rel = builder.push(rel).filter(remainingConditions).build
    call.transformTo(rel)
  }

  private def canPush(aggregate: Aggregate, rCols: ImmutableBitSet): Boolean = {
    val groupKeys: ImmutableBitSet = ImmutableBitSet.range(0, aggregate.getGroupSet.cardinality)
    if (!groupKeys.contains(rCols)) {
      return false
    }
    if (aggregate.indicator) {
      import scala.collection.JavaConversions._
      for (groupingSet <- aggregate.getGroupSets) {
        if (!groupingSet.contains(rCols)) {
          return false
        }
      }
    }
    true
  }
}

object FlinkFilterAggregateTransposeRule {
  /** The default instance of `FilterAggregateTransposeRule`.
    *
    * It matches any kind of agg. or filter
    */
  val INSTANCE: FlinkFilterAggregateTransposeRule =
    new FlinkFilterAggregateTransposeRule(
      classOf[Filter],
      RelFactories.LOGICAL_BUILDER,
      classOf[Aggregate])
}
