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

package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamTemporalTableJoin
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTemporalTableJoin
import org.apache.flink.table.plan.schema.RowSchema

class DataStreamTemporalTableJoinRule
  extends ConverterRule(
    classOf[FlinkLogicalTemporalTableJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamTemporalTableJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalTemporalTableJoin = call.rel(0)
    join.getJoinType == JoinRelType.INNER
  }

  override def convert(rel: RelNode): RelNode = {
    val temporalJoin = rel.asInstanceOf[FlinkLogicalTemporalTableJoin]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val left: RelNode = RelOptRule.convert(temporalJoin.getInput(0), FlinkConventions.DATASTREAM)
    val right: RelNode = RelOptRule.convert(temporalJoin.getInput(1), FlinkConventions.DATASTREAM)
    val joinInfo = temporalJoin.analyzeCondition
    val leftRowSchema = new RowSchema(left.getRowType)
    val rightRowSchema = new RowSchema(right.getRowType)

    new DataStreamTemporalTableJoin(
      rel.getCluster,
      traitSet,
      left,
      right,
      temporalJoin.getCondition,
      joinInfo,
      leftRowSchema,
      rightRowSchema,
      new RowSchema(rel.getRowType),
      description)
  }
}

object DataStreamTemporalTableJoinRule {
  val INSTANCE: RelOptRule = new DataStreamTemporalTableJoinRule
}
