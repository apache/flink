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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSort
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecTemporalSort

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

/**
  * Rule that matches [[FlinkLogicalSort]] which is sorted by time attribute in ascending order
  * and its `fetch` and `offset` are null, and converts it to [[StreamExecTemporalSort]].
  */
class StreamExecTemporalSortRule
  extends ConverterRule(
    classOf[FlinkLogicalSort],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecTemporalSortRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort: FlinkLogicalSort = call.rel(0)
    StreamExecTemporalSortRule.canConvertToTemporalSort(sort)
  }

  override def convert(rel: RelNode): RelNode = {
    val sort: FlinkLogicalSort = rel.asInstanceOf[FlinkLogicalSort]
    val input = sort.getInput()
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val convInput: RelNode = RelOptRule.convert(input, FlinkConventions.STREAM_PHYSICAL)

    new StreamExecTemporalSort(
      rel.getCluster,
      traitSet,
      convInput,
      sort.collation)
  }

}

object StreamExecTemporalSortRule {
  val INSTANCE: RelOptRule = new StreamExecTemporalSortRule

  /**
    * Whether the given sort could be converted to [[StreamExecTemporalSort]].
    *
    * Return true if the given sort is sorted by time attribute in ascending order
    * and its `fetch` and `offset` are null, else false.
    *
    * @param sort the [[FlinkLogicalSort]] node
    * @return True if the input sort could be converted to [[StreamExecTemporalSort]]
    */
  def canConvertToTemporalSort(sort: FlinkLogicalSort): Boolean = {
    val fieldCollations = sort.collation.getFieldCollations
    if (sort.fetch != null || sort.offset != null) {
      return false
    }

    if (fieldCollations.isEmpty) {
      false
    } else {
      // get type of first sort field
      val firstSortField = fieldCollations.get(0)
      val inputRowType = sort.getInput.getRowType
      val firstSortFieldType = inputRowType.getFieldList.get(firstSortField.getFieldIndex).getType
      // checks if first sort attribute is time attribute type and order is ascending
      FlinkTypeFactory.isTimeIndicatorType(firstSortFieldType) &&
        firstSortField.direction == Direction.ASCENDING
    }
  }
}
