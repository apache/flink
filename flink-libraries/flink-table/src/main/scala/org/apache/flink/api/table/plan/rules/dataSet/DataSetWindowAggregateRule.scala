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

import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.api.table.TableException
import org.apache.flink.api.table.plan.logical.rel.LogicalWindowAggregate
import org.apache.flink.api.table.plan.nodes.dataset.{DataSetConvention,DataSetWindowAggregate}

import scala.collection.JavaConversions._

class DataSetWindowAggregateRule
  extends ConverterRule(
    classOf[LogicalWindowAggregate],
    Convention.NONE,
    DataSetConvention.INSTANCE,
    "DataSetWindowAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: LogicalWindowAggregate = call.rel(0).asInstanceOf[LogicalWindowAggregate]

    // check if it have distinct aggregates
    val distinctAggs = agg.getAggCallList.exists(_.isDistinct)
    if (distinctAggs) {
      throw TableException("DISTINCT aggregates are currently not supported.")
    }

    // check if it have grouping sets
    val groupSets = agg.getGroupSets.size() != 1 || agg.getGroupSets.get(0) != agg.getGroupSet
    if (groupSets || agg.indicator) {
      throw TableException("GROUPING SETS are currently not supported.")
    }

    !distinctAggs && !groupSets && !agg.indicator
  }

  override def convert(rel: RelNode): RelNode = {
    val agg: LogicalWindowAggregate = rel.asInstanceOf[LogicalWindowAggregate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(agg.getInput, DataSetConvention.INSTANCE)

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
