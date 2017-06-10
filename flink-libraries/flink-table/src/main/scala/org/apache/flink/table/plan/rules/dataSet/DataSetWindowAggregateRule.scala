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
package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetWindowAggregate
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalWindowAggregate

import scala.collection.JavaConversions._

class DataSetWindowAggregateRule
  extends ConverterRule(
    classOf[FlinkLogicalWindowAggregate],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetWindowAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: FlinkLogicalWindowAggregate = call.rel(0).asInstanceOf[FlinkLogicalWindowAggregate]

    // check if we have distinct aggregates
    val distinctAggs = agg.getAggCallList.exists(_.isDistinct)
    if (distinctAggs) {
      throw TableException("DISTINCT aggregates are currently not supported.")
    }

    // check if we have grouping sets
    val groupSets = agg.getGroupSets.size() != 1 || agg.getGroupSets.get(0) != agg.getGroupSet
    if (groupSets || agg.indicator) {
      throw TableException("GROUPING SETS are currently not supported.")
    }

    !distinctAggs && !groupSets && !agg.indicator
  }

  override def convert(rel: RelNode): RelNode = {
    val agg: FlinkLogicalWindowAggregate = rel.asInstanceOf[FlinkLogicalWindowAggregate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val convInput: RelNode = RelOptRule.convert(agg.getInput, FlinkConventions.DATASET)

    new DataSetWindowAggregate(
      agg.getWindow,
      agg.getNamedProperties,
      rel.getCluster,
      traitSet,
      convInput,
      agg.getNamedAggCalls,
      rel.getRowType,
      agg.getInput.getRowType,
      agg.getGroupSet.toArray)
  }
}

object DataSetWindowAggregateRule {
  val INSTANCE: RelOptRule = new DataSetWindowAggregateRule
}
