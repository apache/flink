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
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamJoin
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.join.WindowJoinUtil

import scala.collection.JavaConverters._

class DataStreamJoinRule
  extends ConverterRule(
    classOf[FlinkLogicalJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0).asInstanceOf[FlinkLogicalJoin]
    val joinInfo = join.analyzeCondition

    val (windowBounds, _) = WindowJoinUtil.extractWindowBoundsFromPredicate(
      joinInfo.getRemaining(join.getCluster.getRexBuilder),
      join.getLeft.getRowType.getFieldCount,
      join.getRowType,
      join.getCluster.getRexBuilder,
      TableConfig.getDefault)

    if (windowBounds.isDefined) {
      return false
    }

    // Check that no event-time attributes are in the outputs (composed of two inputs)
    // because non-window join is unbounded and we don't know how much to hold back watermarks.
    val rowTimeAttrInOutput = join.getRowType.getFieldList.asScala
      .exists(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))

    if (rowTimeAttrInOutput) {
      throw new TableException(
        "Rowtime attributes must not be in the input rows of a regular join. " +
        "As a workaround you can cast the time attributes of input tables to TIMESTAMP before.")
    }
    true
  }

  override def convert(rel: RelNode): RelNode = {

    val join: FlinkLogicalJoin = rel.asInstanceOf[FlinkLogicalJoin]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), FlinkConventions.DATASTREAM)
    val convRight: RelNode = RelOptRule.convert(join.getInput(1), FlinkConventions.DATASTREAM)
    val joinInfo = join.analyzeCondition
    val leftRowSchema = new RowSchema(convLeft.getRowType)
    val rightRowSchema = new RowSchema(convRight.getRowType)

    new DataStreamJoin(
      rel.getCluster,
      traitSet,
      convLeft,
      convRight,
      join.getCondition,
      joinInfo,
      join.getJoinType,
      leftRowSchema,
      rightRowSchema,
      new RowSchema(rel.getRowType),
      "DataStreamJoinRule")
  }
}

object DataStreamJoinRule {
  val INSTANCE: RelOptRule = new DataStreamJoinRule
}
