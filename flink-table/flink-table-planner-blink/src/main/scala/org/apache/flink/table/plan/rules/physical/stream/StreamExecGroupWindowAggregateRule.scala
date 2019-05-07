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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkContext
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalWindowAggregate
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecGroupWindowAggregate
import org.apache.flink.table.plan.util.AggregateUtil.{isRowtimeIndicatorType, timeFieldIndex}
import org.apache.flink.table.plan.util.WindowEmitStrategy

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Aggregate.Group

import scala.collection.JavaConversions._

/**
  * Rule to convert a [[FlinkLogicalWindowAggregate]] into a [[StreamExecGroupWindowAggregate]].
  */
class StreamExecGroupWindowAggregateRule
  extends ConverterRule(
    classOf[FlinkLogicalWindowAggregate],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecGroupWindowAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: FlinkLogicalWindowAggregate = call.rel(0)

    // check if we have grouping sets
    val groupSets = agg.getGroupType != Group.SIMPLE
    if (groupSets || agg.indicator) {
      throw new TableException("GROUPING SETS are currently not supported.")
    }

    true
  }

  override def convert(rel: RelNode): RelNode = {
    val agg = rel.asInstanceOf[FlinkLogicalWindowAggregate]
    val input = agg.getInput
    val inputRowType = input.getRowType
    val cluster = rel.getCluster
    val requiredDistribution = if (agg.getGroupCount != 0) {
      FlinkRelDistribution.hash(agg.getGroupSet.asList)
    } else {
      FlinkRelDistribution.SINGLETON
    }
    val requiredTraitSet = input.getTraitSet
      .replace(FlinkConventions.STREAM_PHYSICAL)
      .replace(requiredDistribution)
    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput: RelNode = RelOptRule.convert(input, requiredTraitSet)

    val config = cluster.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
    val emitStrategy = WindowEmitStrategy(config, agg.getWindow)

    val timeField = agg.getWindow.timeAttribute
    val inputTimestampIndex = if (isRowtimeIndicatorType(timeField.getResultType)) {
      timeFieldIndex(inputRowType, relBuilderFactory.create(cluster, null), timeField)
    } else {
      -1
    }

    new StreamExecGroupWindowAggregate(
      cluster,
      providedTraitSet,
      newInput,
      rel.getRowType,
      inputRowType,
      agg.getGroupSet.toArray,
      agg.getAggCallList,
      agg.getWindow,
      agg.getNamedProperties,
      inputTimestampIndex,
      emitStrategy)
  }
}

object StreamExecGroupWindowAggregateRule {
  val INSTANCE: RelOptRule = new StreamExecGroupWindowAggregateRule
}
