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
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.flink.table.plan.nodes.dataset.{DataSetAggregate, DataSetConvention, DataSetUnion}
import scala.collection.JavaConversions._

class DataSetAggregateRule
  extends ConverterRule(
    classOf[LogicalAggregate],
    Convention.NONE,
    DataSetConvention.INSTANCE,
    "DataSetAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: LogicalAggregate = call.rel(0).asInstanceOf[LogicalAggregate]

    // for non-grouped agg sets we attach null row to source data
    // we need to apply DataSetAggregateWithNullValuesRule
    if (agg.getGroupSet.isEmpty) {
      return false
    }

    // distinct is translated into dedicated operator
    if (agg.getAggCallList.isEmpty &&
      agg.getGroupCount == agg.getRowType.getFieldCount &&
      agg.getRowType.equals(agg.getInput.getRowType) &&
      agg.getGroupSets.size() == 1) {
      return false
    }

    // check if we have distinct aggregates
    val distinctAggs = agg.getAggCallList.exists(_.isDistinct)

    !distinctAggs
  }

  override def convert(rel: RelNode): RelNode = {
    val agg: LogicalAggregate = rel.asInstanceOf[LogicalAggregate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(agg.getInput, DataSetConvention.INSTANCE)

    if (agg.indicator) {
        agg.groupSets.map(set =>
          new DataSetAggregate(
            rel.getCluster,
            traitSet,
            convInput,
            agg.getNamedAggCalls,
            rel.getRowType,
            agg.getInput.getRowType,
            set.toArray,
            inGroupingSet = true
          ).asInstanceOf[RelNode]
        ).reduce(
          (rel1, rel2) => {
            new DataSetUnion(
              rel.getCluster,
              traitSet,
              rel1,
              rel2,
              rel.getRowType
            )
          }
        )
    } else {
      new DataSetAggregate(
        rel.getCluster,
        traitSet,
        convInput,
        agg.getNamedAggCalls,
        rel.getRowType,
        agg.getInput.getRowType,
        agg.getGroupSet.toArray,
        inGroupingSet = false
      )
    }
  }
}

object DataSetAggregateRule {
  val INSTANCE: RelOptRule = new DataSetAggregateRule
}
