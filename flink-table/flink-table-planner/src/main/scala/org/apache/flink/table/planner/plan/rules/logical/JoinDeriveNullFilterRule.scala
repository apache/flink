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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.rules.logical.JoinDeriveNullFilterRule.JOIN_NULL_FILTER_THRESHOLD

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.ImmutableIntList

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Planner rule that filters null values before join if the count null value from join input is
 * greater than null filter threshold.
 *
 * Since the key of the Null value is impossible to match in the inner join, and there is a single
 * point skew due to too many Null values. We should push down a not-null filter into the child node
 * of join.
 */
class JoinDeriveNullFilterRule
  extends RelOptRule(operand(classOf[LogicalJoin], any()), "JoinDeriveNullFilterRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    join.getJoinType == JoinRelType.INNER && join.analyzeCondition.pairs().nonEmpty
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: LogicalJoin = call.rel(0)

    val rexBuilder = join.getCluster.getRexBuilder
    val mq = FlinkRelMetadataQuery.reuseOrCreate(join.getCluster.getMetadataQuery)

    def createIsNotNullFilter(input: RelNode, keys: ImmutableIntList): RelNode = {
      val relBuilder = call.builder()
      val filters = new mutable.ArrayBuffer[RexNode]
      keys.foreach {
        key =>
          val nullCount = mq.getColumnNullCount(input, key)
          if (nullCount != null && nullCount > JOIN_NULL_FILTER_THRESHOLD) {
            filters += relBuilder.call(
              SqlStdOperatorTable.IS_NOT_NULL,
              rexBuilder.makeInputRef(input, key))
          }
      }
      if (filters.nonEmpty) {
        relBuilder.push(input).filter(filters).build()
      } else {
        input
      }
    }

    val joinInfo = join.analyzeCondition
    val newLeft = createIsNotNullFilter(join.getLeft, joinInfo.leftKeys)
    val newRight = createIsNotNullFilter(join.getRight, joinInfo.rightKeys)

    if ((newLeft ne join.getLeft) || (newRight ne join.getRight)) {
      val newJoin = join.copy(join.getTraitSet, Seq(newLeft, newRight))
      call.transformTo(newJoin)
    }
  }
}

object JoinDeriveNullFilterRule {
  val INSTANCE = new JoinDeriveNullFilterRule

  // To avoid the impact of null values on the single join node,
  // We will add a null filter (possibly be pushed down) before the join to filter
  // null values when the source of InnerJoin has nullCount more than this value.
  val JOIN_NULL_FILTER_THRESHOLD = 2000000L
}
