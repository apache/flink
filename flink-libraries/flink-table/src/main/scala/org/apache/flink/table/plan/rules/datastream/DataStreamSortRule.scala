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

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.datastream.DataStreamSort
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSort
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.flink.table.runtime.aggregate.SortUtil

/**
 * Rule to convert a LogicalSort into a DataStreamSort.
 */
class DataStreamSortRule
    extends ConverterRule(
      classOf[FlinkLogicalSort],
      FlinkConventions.LOGICAL,
      FlinkConventions.DATASTREAM,
      "DataStreamSortRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort: FlinkLogicalSort = call.rel(0).asInstanceOf[FlinkLogicalSort]
    // Check if first sort attribute is time attribute and order is ascending
    checkTimeOrder(sort)
  }

  override def convert(rel: RelNode): RelNode = {
    val sort: FlinkLogicalSort = rel.asInstanceOf[FlinkLogicalSort]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convInput: RelNode = RelOptRule.convert(sort.getInput(0), FlinkConventions.DATASTREAM)
    
    val inputRowType = convInput.asInstanceOf[RelSubset].getOriginal.getRowType

    new DataStreamSort(
      rel.getCluster,
      traitSet,
      convInput,
      new RowSchema(inputRowType),
      new RowSchema(rel.getRowType),
      sort.collation,
      sort.offset,
      sort.fetch,
      description)
  }
  
   
  /**
   * Checks if first sort attribute is time attribute and order is ascending.
   */
  def checkTimeOrder(sort: FlinkLogicalSort): Boolean = {
    
    val sortCollation = sort.collation
    if (sortCollation.getFieldCollations.size() == 0) {
      // no sort fields defined
      return false
    }
    // get type of first sort field
    val firstSortType = SortUtil.getFirstSortField(sortCollation, sort.getRowType).getType
    // get direction of first sort field
    val firstSortDirection = SortUtil.getFirstSortDirection(sortCollation)

    FlinkTypeFactory.isTimeIndicatorType(firstSortType) && firstSortDirection == Direction.ASCENDING
  }
}

object DataStreamSortRule {
  val INSTANCE: RelOptRule = new DataStreamSortRule
}
