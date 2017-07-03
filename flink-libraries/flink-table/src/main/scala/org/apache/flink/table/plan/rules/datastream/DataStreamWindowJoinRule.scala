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
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamWindowJoin
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.join.WindowJoinUtil

class DataStreamWindowJoinRule
  extends ConverterRule(
      classOf[FlinkLogicalJoin],
      FlinkConventions.LOGICAL,
      FlinkConventions.DATASTREAM,
      "DataStreamJoinRule") {

  /** Time indicator type **/
  private var timeType: RelDataType = _

  /** left input lower boudary **/
  private var leftLowerBoundary: Long = _

  /** left input upper boudary **/
  private var leftUpperBoundary: Long = _

  /** remain join condition exclude equal condition and time condition **/
  private var remainCondition: Option[RexNode] = _

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0).asInstanceOf[FlinkLogicalJoin]

    val joinInfo = join.analyzeCondition

    try {
      val leftRowSchema = new RowSchema(join.getLeft.getRowType)

      val result =
        WindowJoinUtil.analyzeTimeBoundary(
          joinInfo.getRemaining(join.getCluster.getRexBuilder),
          leftRowSchema.logicalType.getFieldCount,
          leftRowSchema.physicalType.getFieldCount,
          join.getRowType,
          join.getCluster.getRexBuilder,
          TableConfig.DEFAULT)
      timeType = result._1
      leftLowerBoundary = result._2
      leftUpperBoundary = result._3
      remainCondition = result._4
      true
    } catch {
      case _: TableException =>
        false
    }
  }

  override def convert(rel: RelNode): RelNode = {

    val join: FlinkLogicalJoin = rel.asInstanceOf[FlinkLogicalJoin]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), FlinkConventions.DATASTREAM)
    val convRight: RelNode = RelOptRule.convert(join.getInput(1), FlinkConventions.DATASTREAM)

    new DataStreamWindowJoin(
      rel.getCluster,
      traitSet,
      convLeft,
      convRight,
      join.getCondition,
      join.getJoinType,
      new RowSchema(convLeft.getRowType),
      new RowSchema(convRight.getRowType),
      new RowSchema(rel.getRowType),
      timeType,
      leftLowerBoundary,
      leftUpperBoundary,
      remainCondition,
      description)
  }
}

object DataStreamWindowJoinRule {
  val INSTANCE: RelOptRule = new DataStreamWindowJoinRule
}
