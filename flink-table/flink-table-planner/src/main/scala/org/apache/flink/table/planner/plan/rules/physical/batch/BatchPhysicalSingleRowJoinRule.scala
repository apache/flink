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

package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalNestedLoopJoin

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core._

/**
  * Rule that converts [[FlinkLogicalJoin]] to [[BatchPhysicalNestedLoopJoin]]
  * if one of join input sides returns at most a single row.
  */
class BatchPhysicalSingleRowJoinRule
  extends ConverterRule(
    classOf[FlinkLogicalJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCH_PHYSICAL,
    "BatchPhysicalSingleRowJoinRule")
  with BatchPhysicalJoinRuleBase
  with BatchPhysicalNestedLoopJoinRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    join.getJoinType match {
      case JoinRelType.INNER | JoinRelType.FULL =>
        isSingleRow(join.getLeft) || isSingleRow(join.getRight)
      case JoinRelType.LEFT => isSingleRow(join.getRight)
      case JoinRelType.RIGHT => isSingleRow(join.getLeft)
      case JoinRelType.SEMI | JoinRelType.ANTI => isSingleRow(join.getRight)
      case _ => false
    }
  }

  /**
    * Recursively checks if a [[RelNode]] returns at most a single row.
    * Input must be a global aggregation possibly followed by projections or filters.
    */
  private def isSingleRow(node: RelNode): Boolean = {
    node match {
      case ss: RelSubset => isSingleRow(ss.getOriginal)
      case lp: Project => isSingleRow(lp.getInput)
      case lf: Filter => isSingleRow(lf.getInput)
      case lc: Calc => isSingleRow(lc.getInput)
      case la: Aggregate => la.getGroupSet.isEmpty
      case _ => false
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val join = rel.asInstanceOf[Join]
    val left = join.getLeft
    val leftIsBuild = isSingleRow(left)
    createNestedLoopJoin(
      join,
      left,
      join.getRight,
      leftIsBuild,
      singleRowJoin = true)
  }
}

object BatchPhysicalSingleRowJoinRule {
  val INSTANCE: RelOptRule = new BatchPhysicalSingleRowJoinRule
}
