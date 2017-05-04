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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.{DataSetAggregate, DataSetExpand}
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalAggregate

import scala.collection.JavaConversions._

/**
  * Rule for converting [[FlinkLogicalAggregate]] to [[DataSetAggregate]].
  *
  * When the indicator of [[FlinkLogicalAggregate]] is true, we covert it to [[DataSetAggregate]]
  * with [[org.apache.flink.table.plan.nodes.dataset.DataSetExpand]]. For Example:
  *
  * MyTable:
  * a: INT, b: BIGINT, c: VARCHAR(32)
  *
  * SQL:
  * SELECT a, c, SUM(b) as b FROM MyTable GROUP BY GROUPING SETS (a, c)
  *
  * logical plan:
  * {{{
  *   LogicalProject(a=[$0], c=[$1], b=[$4])
  *    LogicalProject(a=[CASE($2, null, $0)], f2=[CASE($3, null, $1)], i$a=[$2], i$c=[$3], b=[$4])
  *     LogicalAggregate(group=[{0, 2}], groups=[[{0}, {2}]], indicator=[true], b=[SUM($1)])
  *      LogicalProject(a=[$0], c=[$2], b=[$1])
  *       LogicalTableScan(table=[[MyTable]])
  * }}}
  *
  * physical plan with [[DataSetExpand]]:
  * {{{
  *   DataSetCalc(select=[CASE(i$a, null, a) AS a, CASE(i$c, null, c) AS c, b])
  *    DataSetAggregate(groupBy=[a, c, i$a, i$c], select=[a, c, i$a, i$c, SUM(b) AS b])
  *     DataSetExpand(expand=[a, b, c, i$a, i$c])
  *      DataSetScan(table=[[MyTable]])
  * }}}
  */
class DataSetAggregateRule
  extends ConverterRule(
    classOf[FlinkLogicalAggregate],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: FlinkLogicalAggregate = call.rel(0).asInstanceOf[FlinkLogicalAggregate]

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
    val agg: FlinkLogicalAggregate = rel.asInstanceOf[FlinkLogicalAggregate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val convInput: RelNode = RelOptRule.convert(agg.getInput, FlinkConventions.DATASET)

    if (agg.indicator) {

      val expand = new DataSetExpand(
        rel.getCluster,
        traitSet,
        convInput,
        agg.groupSets,
        agg.getRowType,
        description
      )

      val originFieldCount = convInput.getRowType.getFieldCount
      val expandFieldCount = expand.getRowType.getFieldCount
      val newGroupSet = agg.getGroupSet.toArray ++ (originFieldCount until expandFieldCount)

      new DataSetAggregate(
        rel.getCluster,
        traitSet,
        expand,
        agg.getNamedAggCalls,
        rel.getRowType,
        expand.getRowType,
        newGroupSet,
        inGroupingSet = false
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
