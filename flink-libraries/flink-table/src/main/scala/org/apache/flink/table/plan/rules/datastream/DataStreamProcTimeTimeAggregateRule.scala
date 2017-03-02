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

import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.logical.LogicalWindow
import org.apache.calcite.sql.`type`.IntervalSqlType
import org.apache.flink.table.plan.nodes.datastream.DataStreamConvention
import org.apache.flink.table.plan.nodes.datastream.DataStreamProcTimeTimeAggregate

class DataStreamProcTimeTimeAggregateRule extends ConverterRule(
  classOf[LogicalWindow],
  Convention.NONE,
  DataStreamConvention.INSTANCE,
  "DataStreamProcTimeTimeAggregateRule") {

  override def convert(rel: RelNode): RelNode = {
    val windowAggregate: LogicalWindow = rel.asInstanceOf[LogicalWindow]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataStreamConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(windowAggregate.getInput,
      DataStreamConvention.INSTANCE)

    new DataStreamProcTimeTimeAggregate(
      rel.getCluster,
      traitSet,
      convInput,
      rel.getRowType,
      description + windowAggregate.getId,
      windowAggregate);

  }

  override def matches(call: RelOptRuleCall): Boolean = {

    val rl0: RelNode = call.rels(0)
    val calc: LogicalWindow = rl0.asInstanceOf[LogicalWindow]

    /*
     * if bounded and time range
     * return true
     */

    val boundaries: Group = calc.groups.iterator.next

    if (boundaries.lowerBound.getOffset.getType.isInstanceOf[IntervalSqlType]) {
      if (boundaries.upperBound.isPreceding) {
        return true
      }

      if (boundaries.upperBound.isCurrentRow) {
        return true
      }

    }

    return false

  }

}

object DataStreamProcTimeTimeAggregateRule {
  val INSTANCE: RelOptRule = new DataStreamProcTimeTimeAggregateRule
}
