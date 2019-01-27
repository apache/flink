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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalRel, FlinkLogicalSnapshot}
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecJoin
import org.apache.flink.table.runtime.join.WindowJoinUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinInfo
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import java.util

import org.apache.flink.table.plan.util.TemporalJoinUtil

import scala.collection.JavaConversions._

class StreamExecJoinRule
  extends RelOptRule(
    operand(
      classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalRel], any())),
    "StreamExecJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0).asInstanceOf[FlinkLogicalJoin]
    val left: FlinkLogicalRel = call.rel(1).asInstanceOf[FlinkLogicalRel]
    val right: FlinkLogicalRel = call.rel(2).asInstanceOf[FlinkLogicalRel]

    if (left.isInstanceOf[FlinkLogicalSnapshot]) {
      throw new TableException(
        "Temporal table join only support apply FOR SYSTEM_TIME AS OF on the right table.")
    }

    // this rule shouldn't match temporal table join
    if (right.isInstanceOf[FlinkLogicalSnapshot] ||
      TemporalJoinUtil.containsTemporalJoinCondition(join.getCondition)) {
      return false
    }

    val (windowBounds, remainingPreds) = WindowJoinUtil.extractWindowBoundsFromPredicate(
      join.getCondition,
      join.getLeft.getRowType.getFieldCount,
      join.getRowType,
      join.getCluster.getRexBuilder,
      TableConfig.DEFAULT)

    if (windowBounds.isDefined) {
      return false
    }

    // remaining predicate must not access time attributes
    val remainingPredsAccessTime = remainingPreds.isDefined &&
      WindowJoinUtil.accessesTimeAttribute(remainingPreds.get, join.getRowType)

    val rowTimeAttrInOutput = join.getRowType.getFieldList
      .exists(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))
    if (rowTimeAttrInOutput) {
      throw new TableException(
        "Rowtime attributes must not be in the input rows of a regular join. " +
          "As a workaround you can cast the time attributes of input tables to TIMESTAMP before.")
    }

    // joins require an equality condition
    // or a conjunctive predicate with at least one equality condition
    // and disable outer joins with non-equality predicates(see FLINK-5520)
    // And do not accept a FlinkLogicalTemporalTableSourceScan as right input
    !remainingPredsAccessTime
  }


  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: FlinkLogicalJoin = call.rel(0).asInstanceOf[FlinkLogicalJoin]
    lazy val (joinInfo, filterNulls) = {
      val filterNulls = new util.ArrayList[java.lang.Boolean]
      val joinInfo = JoinInfo.of(join.getLeft, join.getRight, join.getCondition, filterNulls)
      (joinInfo, filterNulls.map(_.booleanValue()).toArray)
    }
    def toHashTraitByColumns(columns: util.Collection[_ <: Number], inputTraitSets: RelTraitSet) = {
      val distribution = if (columns.size() == 0) {
        FlinkRelDistribution.SINGLETON
      } else {
        FlinkRelDistribution.hash(columns)
      }
      inputTraitSets.
        replace(FlinkConventions.STREAM_PHYSICAL).
        replace(distribution)
    }
    val (leftRequiredTrait, rightRequiredTrait) = (
      toHashTraitByColumns(joinInfo.leftKeys, join.getLeft.getTraitSet),
      toHashTraitByColumns(joinInfo.rightKeys, join.getRight.getTraitSet))

    val providedTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), leftRequiredTrait)
    val convRight: RelNode = RelOptRule.convert(join.getInput(1), rightRequiredTrait)

    val newJoin = new StreamExecJoin(
      join.getCluster,
      providedTraitSet,
      convLeft,
      convRight,
      join.getRowType,
      join.getCondition,
      join.getRowType,
      joinInfo,
      filterNulls,
      joinInfo.pairs.toList,
      FlinkJoinRelType.toFlinkJoinRelType(join.getJoinType),
      null,
      description)
    call.transformTo(newJoin)
  }
}

object StreamExecJoinRule {
  val INSTANCE: RelOptRule = new StreamExecJoinRule
}
