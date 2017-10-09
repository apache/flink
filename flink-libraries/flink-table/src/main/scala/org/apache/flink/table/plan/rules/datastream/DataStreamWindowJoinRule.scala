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
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamWindowJoin
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.join.WindowJoinUtil

import scala.collection.JavaConverters._

class DataStreamWindowJoinRule
  extends ConverterRule(
    classOf[FlinkLogicalJoin],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0).asInstanceOf[FlinkLogicalJoin]

    val (windowBounds, remainingPreds) = WindowJoinUtil.extractWindowBoundsFromPredicate(
      join.getCondition,
      join.getLeft.getRowType.getFieldCount,
      join.getRowType,
      join.getCluster.getRexBuilder,
      TableConfig.DEFAULT)

    // remaining predicate must not access time attributes
    val remainingPredsAccessTime = remainingPreds.isDefined &&
      WindowJoinUtil.accessesTimeAttribute(remainingPreds.get, join.getRowType)

    if (windowBounds.isDefined) {
      if (windowBounds.get.isEventTime) {
        !remainingPredsAccessTime
      } else {
        // Check that no event-time attributes are in the input.
        // The proc-time join implementation does ensure that record timestamp are correctly set.
        // It is always the timestamp of the later arriving record.
        // We rely on projection pushdown to remove unused attributes before the join.
        val rowTimeAttrInOutput = join.getRowType.getFieldList.asScala
          .exists(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))

        !remainingPredsAccessTime && !rowTimeAttrInOutput
      }
    } else {
      // the given join does not have valid window bounds. We cannot translate it.
      false
    }

  }

  override def convert(rel: RelNode): RelNode = {

    val join: FlinkLogicalJoin = rel.asInstanceOf[FlinkLogicalJoin]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), FlinkConventions.DATASTREAM)
    val convRight: RelNode = RelOptRule.convert(join.getInput(1), FlinkConventions.DATASTREAM)
    val leftRowSchema = new RowSchema(convLeft.getRowType)
    val rightRowSchema = new RowSchema(convRight.getRowType)

    val (windowBounds, remainCondition) =
      WindowJoinUtil.extractWindowBoundsFromPredicate(
        join.getCondition,
        leftRowSchema.arity,
        join.getRowType,
        join.getCluster.getRexBuilder,
        TableConfig.DEFAULT)

    new DataStreamWindowJoin(
      rel.getCluster,
      traitSet,
      convLeft,
      convRight,
      join.getCondition,
      join.getJoinType,
      leftRowSchema,
      rightRowSchema,
      new RowSchema(rel.getRowType),
      windowBounds.get.isEventTime,
      windowBounds.get.leftLowerBound,
      windowBounds.get.leftUpperBound,
      windowBounds.get.leftTimeIdx,
      windowBounds.get.rightTimeIdx,
      remainCondition,
      description)
  }
}

object DataStreamWindowJoinRule {
  val INSTANCE: RelOptRule = new DataStreamWindowJoinRule
}
