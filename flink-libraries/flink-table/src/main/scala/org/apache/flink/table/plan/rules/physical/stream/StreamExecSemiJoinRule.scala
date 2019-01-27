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

import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSemiJoin
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecJoin
import org.apache.flink.table.plan.rules.physical.batch.BatchExecJoinRuleBase

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{Join, JoinInfo}

import java.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StreamExecSemiJoinRule
  extends ConverterRule(
    classOf[FlinkLogicalSemiJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecSemiJoinRule")
  with BatchExecJoinRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rels(0).asInstanceOf[Join]
    getFlinkJoinRelType(join) match {
      case FlinkJoinRelType.ANTI | FlinkJoinRelType.SEMI => true
      case _ => false
    }
  }

  override def convert(relNode: RelNode): RelNode = {
    val join: FlinkLogicalSemiJoin = relNode.asInstanceOf[FlinkLogicalSemiJoin]
    lazy val (joinInfo, filterNulls) = {
      val filterNulls = new util.ArrayList[java.lang.Boolean]
      val joinInfo = JoinInfo.of(join.getLeft, join.getRight, join.getCondition, filterNulls)
      (joinInfo, filterNulls.map(_.booleanValue()).toArray)
    }
    def toHashTraitByColumns(columns: util.Collection[_ <: Number], inputTraitSet: RelTraitSet) = {
      val distribution = if (columns.size() == 0) {
        FlinkRelDistribution.SINGLETON
      } else {
        FlinkRelDistribution.hash(columns)
      }
      inputTraitSet.
        replace(FlinkConventions.STREAM_PHYSICAL).
        replace(distribution)
    }
    val (leftRequiredTrait, rightRequiredTrait) = (
      toHashTraitByColumns(joinInfo.leftKeys, join.getLeft.getTraitSet),
      toHashTraitByColumns(joinInfo.rightKeys, join.getRight.getTraitSet))

    val providedTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), leftRequiredTrait)
    val convRight: RelNode = RelOptRule.convert(join.getInput(1), rightRequiredTrait)
    val joinType = if (join.isAnti) {
      FlinkJoinRelType.ANTI
    } else {
      FlinkJoinRelType.SEMI
    }
    new StreamExecJoin(
      relNode.getCluster,
      providedTraitSet,
      convLeft,
      convRight,
      relNode.getRowType,
      join.getCondition,
      getInputRowType(join),
      joinInfo,
      filterNulls,
      joinInfo.pairs().asScala.toList,
      joinType,
      null,
      description
    )
  }
}

object StreamExecSemiJoinRule {
  val INSTANCE = new StreamExecSemiJoinRule
}
