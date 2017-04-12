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
import org.apache.flink.table.plan.nodes.datastream.DataStreamConvention
import org.apache.flink.table.plan.nodes.datastream.DataStreamCorrelate
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.flink.table.plan.nodes.datastream.DataStreamSort

/**
 * Rule to convert a LogicalSort into a DataStreamSort.
 */
class DataStreamSortRule
    extends ConverterRule(
      classOf[LogicalSort],
      Convention.NONE,
      DataStreamConvention.INSTANCE,
      "DataStreamSortRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    super.matches(call)
  }

  override def convert(rel: RelNode): RelNode = {
    val calc: LogicalSort = rel.asInstanceOf[LogicalSort]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataStreamConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(calc.getInput(0), DataStreamConvention.INSTANCE)

    val inputRowType = convInput.asInstanceOf[RelSubset].getOriginal.getRowType
    
    new DataStreamSort(
      calc,
      rel.getCluster,
      traitSet,
      convInput,
      rel.getRowType,
      inputRowType,
      description + calc.getId())
    
  }

}

object DataStreamSortRule {
  val INSTANCE: RelOptRule = new DataStreamSortRule
}
