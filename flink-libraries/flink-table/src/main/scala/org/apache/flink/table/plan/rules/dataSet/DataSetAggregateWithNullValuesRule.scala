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


import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.DataSetAggregate
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalAggregate, FlinkLogicalUnion, FlinkLogicalValues}

import scala.collection.JavaConversions._

/**
  * Rule for insert [[org.apache.flink.types.Row]] with null records into a [[DataSetAggregate]].
  * Rule apply for non grouped aggregate query.
  */
class DataSetAggregateWithNullValuesRule
  extends ConverterRule(
    classOf[FlinkLogicalAggregate],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASET,
    "DataSetAggregateWithNullValuesRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: FlinkLogicalAggregate = call.rel(0).asInstanceOf[FlinkLogicalAggregate]

    // group sets shouldn't attach a null row
    // we need to apply other rules. i.e. DataSetAggregateRule
    if (!agg.getGroupSet.isEmpty) {
      return false
    }

    // check if we have distinct aggregates
    val distinctAggs = agg.getAggCallList.exists(_.isDistinct)

    !distinctAggs
  }

  override def convert(rel: RelNode): RelNode = {
    val agg: FlinkLogicalAggregate = rel.asInstanceOf[FlinkLogicalAggregate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASET)
    val cluster: RelOptCluster = rel.getCluster

    val fieldTypes = agg.getInput.getRowType.getFieldList.map(_.getType)
    val nullLiterals: ImmutableList[ImmutableList[RexLiteral]] =
      ImmutableList.of(ImmutableList.copyOf[RexLiteral](
        for (fieldType <- fieldTypes)
          yield {
            cluster.getRexBuilder.
              makeLiteral(null, fieldType, false).asInstanceOf[RexLiteral]
          }))

    val logicalValues = FlinkLogicalValues.create(cluster, agg.getInput.getRowType, nullLiterals)
    val logicalUnion = FlinkLogicalUnion.create(List(logicalValues, agg.getInput), all = true)

    new DataSetAggregate(
      cluster,
      traitSet,
      RelOptRule.convert(logicalUnion, FlinkConventions.DATASET),
      agg.getNamedAggCalls,
      rel.getRowType,
      agg.getInput.getRowType,
      agg.getGroupSet.toArray
    )
  }
}

object DataSetAggregateWithNullValuesRule {
  val INSTANCE: RelOptRule = new DataSetAggregateWithNullValuesRule
}
