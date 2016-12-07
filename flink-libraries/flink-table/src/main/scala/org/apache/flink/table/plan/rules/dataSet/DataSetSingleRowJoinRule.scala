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
import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalJoin}
import org.apache.flink.table.plan.nodes.dataset.{DataSetConvention, DataSetSingleRowJoin}

class DataSetSingleRowJoinRule
  extends ConverterRule(
      classOf[LogicalJoin],
      Convention.NONE,
      DataSetConvention.INSTANCE,
      "DataSetSingleRowCrossRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel(0).asInstanceOf[LogicalJoin]

    if (isInnerJoin(join)) {
      isGlobalAggregation(join.getRight.asInstanceOf[RelSubset].getOriginal) ||
        isGlobalAggregation(join.getLeft.asInstanceOf[RelSubset].getOriginal)
    } else {
      false
    }
  }

  private def isInnerJoin(join: LogicalJoin) = {
    join.getJoinType == JoinRelType.INNER
  }

  private def isGlobalAggregation(node: RelNode) = {
    node.isInstanceOf[LogicalAggregate] &&
      isSingleRow(node.asInstanceOf[LogicalAggregate])
  }

  private def isSingleRow(agg: LogicalAggregate) = {
    agg.getGroupSet.isEmpty
  }

  override def convert(rel: RelNode): RelNode = {
    val join = rel.asInstanceOf[LogicalJoin]
    val traitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)
    val dataSetLeftNode = RelOptRule.convert(join.getLeft, DataSetConvention.INSTANCE)
    val dataSetRightNode = RelOptRule.convert(join.getRight, DataSetConvention.INSTANCE)
    val leftIsSingle = isGlobalAggregation(join.getLeft.asInstanceOf[RelSubset].getOriginal)

    new DataSetSingleRowJoin(
      rel.getCluster,
      traitSet,
      dataSetLeftNode,
      dataSetRightNode,
      leftIsSingle,
      rel.getRowType,
      join.getCondition,
      join.getRowType,
      description)
  }
}

object DataSetSingleRowJoinRule {
  val INSTANCE: RelOptRule = new DataSetSingleRowJoinRule
}
