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

import org.apache.calcite.plan._

import scala.collection.JavaConversions._
import com.google.common.collect.ImmutableList
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalUnion, LogicalValues}
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.nodes.dataset.{DataSetAggregate, DataSetConvention}

/**
  * Rule for insert [[org.apache.flink.types.Row]] with null records into a [[DataSetAggregate]].
  * Rule apply for non grouped aggregate query.
  */
class DataSetAggregateWithNullValuesRule
  extends ConverterRule(
    classOf[LogicalAggregate],
    Convention.NONE,
    DataSetConvention.INSTANCE,
    "DataSetAggregateWithNullValuesRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: LogicalAggregate = call.rel(0).asInstanceOf[LogicalAggregate]

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
    val agg: LogicalAggregate = rel.asInstanceOf[LogicalAggregate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataSetConvention.INSTANCE)
    val cluster: RelOptCluster = rel.getCluster

    val fieldTypes = agg.getInput.getRowType.getFieldList.map(_.getType)
    val nullLiterals: ImmutableList[ImmutableList[RexLiteral]] =
      ImmutableList.of(ImmutableList.copyOf[RexLiteral](
        for (fieldType <- fieldTypes)
          yield {
            cluster.getRexBuilder.
              makeLiteral(null, fieldType, false).asInstanceOf[RexLiteral]
          }))

    val logicalValues = LogicalValues.create(cluster, agg.getInput.getRowType, nullLiterals)
    val logicalUnion = LogicalUnion.create(List(logicalValues, agg.getInput), true)

    new DataSetAggregate(
      cluster,
      traitSet,
      RelOptRule.convert(logicalUnion, DataSetConvention.INSTANCE),
      agg.getNamedAggCalls,
      rel.getRowType,
      agg.getInput.getRowType,
      agg.getGroupSet.toArray,
      inGroupingSet = false
    )
  }
}

object DataSetAggregateWithNullValuesRule {
  val INSTANCE: RelOptRule = new DataSetAggregateWithNullValuesRule
}
