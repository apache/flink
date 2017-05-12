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
import org.apache.calcite.plan.{ Convention, RelOptRule, RelOptRuleCall, RelTraitSet }
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.{ LogicalFilter, LogicalCorrelate, LogicalTableFunctionScan }
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.plan.nodes.datastream.DataStreamCorrelate
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.flink.table.plan.nodes.datastream.DataStreamSort
import org.apache.calcite.rel.RelCollation
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSort

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
    super.matches(call)
  }

  override def convert(rel: RelNode): RelNode = {
    val calcSort: FlinkLogicalSort = rel.asInstanceOf[FlinkLogicalSort]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convInput: RelNode = RelOptRule.convert(calcSort.getInput(0), FlinkConventions.DATASTREAM)
    
    val inputRowType = convInput.asInstanceOf[RelSubset].getOriginal.getRowType

    new DataStreamSort(
      calcSort.collation,
      calcSort.offset,
      calcSort.fetch,
      rel.getCluster,
      traitSet,
      convInput,
      new RowSchema(rel.getRowType),
      new RowSchema(inputRowType),
      description)
    
  }

}

object DataStreamSortRule {
  val INSTANCE: RelOptRule = new DataStreamSortRule
}
