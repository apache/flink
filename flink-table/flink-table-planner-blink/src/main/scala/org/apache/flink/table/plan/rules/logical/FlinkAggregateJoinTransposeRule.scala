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

package org.apache.flink.table.plan.rules.logical

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rel.core.{Aggregate, Join, RelFactories}
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalJoin, LogicalSnapshot}
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule
import org.apache.calcite.tools.RelBuilderFactory

/**
  * Flink's [[AggregateJoinTransposeRule]] which does not match temporal join
  * since lookup table source doesn't support aggregate.
  */
class FlinkAggregateJoinTransposeRule(
    aggregateClass: Class[_ <: Aggregate],
    joinClass: Class[_ <: Join],
    factory: RelBuilderFactory,
    allowFunctions: Boolean)
  extends AggregateJoinTransposeRule(aggregateClass, joinClass, factory, allowFunctions) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(1)
    if (containsSnapshot(join.getRight)) {
      // avoid push aggregates through temporal join
      false
    } else {
      super.matches(call)
    }
  }

  private def containsSnapshot(relNode: RelNode): Boolean = {
    val original = relNode match {
      case r: RelSubset => r.getOriginal
      case r: HepRelVertex => r.getCurrentRel
      case _ => relNode
    }
    original match {
      case _: LogicalSnapshot => true
      case r: SingleRel => containsSnapshot(r.getInput)
      case _ => false
    }
  }
}

object FlinkAggregateJoinTransposeRule {

  /** Extended instance of the rule that can push down aggregate functions. */
  val EXTENDED = new FlinkAggregateJoinTransposeRule(
    classOf[LogicalAggregate],
    classOf[LogicalJoin],
    RelFactories.LOGICAL_BUILDER,
    true)
}
