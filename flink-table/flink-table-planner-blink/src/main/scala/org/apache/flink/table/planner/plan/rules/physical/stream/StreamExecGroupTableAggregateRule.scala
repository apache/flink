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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableAggregate
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecGroupTableAggregate
import org.apache.flink.table.planner.plan.utils.PythonUtil.isPythonAggregate

import scala.collection.JavaConversions._

class StreamExecGroupTableAggregateRule extends ConverterRule(
    classOf[FlinkLogicalTableAggregate],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecGroupTableAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: FlinkLogicalTableAggregate = call.rel(0)
    !agg.getAggCallList.exists(isPythonAggregate(_))
  }

  def convert(rel: RelNode): RelNode = {
    val agg: FlinkLogicalTableAggregate = rel.asInstanceOf[FlinkLogicalTableAggregate]
    val requiredDistribution = if (agg.getGroupSet.cardinality() != 0) {
      FlinkRelDistribution.hash(agg.getGroupSet.asList)
    } else {
      FlinkRelDistribution.SINGLETON
    }
    val requiredTraitSet = rel.getCluster.getPlanner.emptyTraitSet()
      .replace(requiredDistribution)
      .replace(FlinkConventions.STREAM_PHYSICAL)
    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput: RelNode = RelOptRule.convert(agg.getInput, requiredTraitSet)

    new StreamExecGroupTableAggregate(
      rel.getCluster,
      providedTraitSet,
      newInput,
      agg.getRowType,
      agg.getGroupSet.toArray,
      agg.getAggCallList
    )
  }
}

object StreamExecGroupTableAggregateRule {
  val INSTANCE: StreamExecGroupTableAggregateRule = new StreamExecGroupTableAggregateRule()
}

