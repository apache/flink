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

package org.apache.flink.table.plan.rules.physical.batch

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core._
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalSemiJoin}
import org.apache.flink.table.plan.rules.physical.batch.BatchExecNestedLoopJoinRule.transformToNestedLoopJoin

class BatchExecSingleRowJoinRule(joinClass: Class[_ <: Join])
  extends ConverterRule(
    joinClass,
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCH_PHYSICAL,
    s"BatchExecSingleRowJoinRule_${joinClass.getSimpleName}")
    with BatchExecJoinRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel(0).asInstanceOf[Join]
    getFlinkJoinRelType(join) match {
      case FlinkJoinRelType.INNER | FlinkJoinRelType.FULL =>
        isSingleRow(join.getLeft) || isSingleRow(join.getRight)
      case FlinkJoinRelType.LEFT if isSingleRow(join.getRight) => true
      case FlinkJoinRelType.RIGHT if isSingleRow(join.getLeft) => true
      case FlinkJoinRelType.SEMI if isSingleRow(join.getRight) => true
      case FlinkJoinRelType.ANTI if isSingleRow(join.getRight) => true
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
    transformToNestedLoopJoin(
      join,
      isSingleRow(left),
      left,
      join.getRight,
      description,
      singleRowJoin = true)
  }
}

object BatchExecSingleRowJoinRule {
  val INSTANCE: RelOptRule = new BatchExecSingleRowJoinRule(classOf[FlinkLogicalJoin])
  val SEMI_JOIN: RelOptRule = new BatchExecSingleRowJoinRule(classOf[FlinkLogicalSemiJoin])
}
