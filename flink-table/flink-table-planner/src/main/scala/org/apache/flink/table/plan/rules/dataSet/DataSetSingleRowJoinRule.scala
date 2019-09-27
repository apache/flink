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

package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core._
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetSingleRowJoin
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalJoin

class DataSetSingleRowJoinRule
  extends ConverterRule(
    classOf[FlinkLogicalJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetSingleRowJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel(0).asInstanceOf[FlinkLogicalJoin]

    join.getJoinType match {
      case JoinRelType.INNER if isSingleRow(join.getLeft) || isSingleRow(join.getRight) => true
      case JoinRelType.LEFT if isSingleRow(join.getRight) => true
      case JoinRelType.RIGHT if isSingleRow(join.getLeft) => true
      case _ => false
    }
  }

  private def isInnerJoin(join: FlinkLogicalJoin) = {
    join.getJoinType == JoinRelType.INNER
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
    val join = rel.asInstanceOf[FlinkLogicalJoin]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val dataSetLeftNode = RelOptRule.convert(join.getLeft, FlinkConventions.DATASET)
    val dataSetRightNode = RelOptRule.convert(join.getRight, FlinkConventions.DATASET)
    val leftIsSingle = isSingleRow(join.getLeft)

    new DataSetSingleRowJoin(
      rel.getCluster,
      traitSet,
      dataSetLeftNode,
      dataSetRightNode,
      leftIsSingle,
      rel.getRowType,
      join.getCondition,
      join.getRowType,
      join.getJoinType,
      "DataSetSingleRowJoinRule")
  }
}

object DataSetSingleRowJoinRule {
  val INSTANCE: RelOptRule = new DataSetSingleRowJoinRule
}
