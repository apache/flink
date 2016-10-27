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

package org.apache.flink.api.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRuleCall, Convention, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.flink.api.table.TableException
import org.apache.flink.api.table.plan.nodes.dataset.{DataSetAggregate, DataSetConvention}
import scala.collection.JavaConversions._

class DataSetAggregateRule
  extends ConverterRule(
      classOf[LogicalAggregate],
      Convention.NONE,
      DataSetConvention.INSTANCE,
      "DataSetAggregateRule")
  {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: LogicalAggregate = call.rel(0).asInstanceOf[LogicalAggregate]

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
    val agg: LogicalAggregate = rel.asInstanceOf[LogicalAggregate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(agg.getInput, DataSetConvention.INSTANCE)

    new DataSetAggregate(
      rel.getCluster,
      traitSet,
      convInput,
      agg.getNamedAggCalls,
      rel.getRowType,
      agg.getInput.getRowType,
      agg.getGroupSet.toArray)
    }
  }

object DataSetAggregateRule {
  val INSTANCE: RelOptRule = new DataSetAggregateRule
}
