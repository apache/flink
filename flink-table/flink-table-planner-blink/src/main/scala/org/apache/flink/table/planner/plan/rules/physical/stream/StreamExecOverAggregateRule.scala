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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalOverAggregate
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecOverAggregate

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

/**
  * Rule that converts [[FlinkLogicalOverAggregate]] to [[StreamExecOverAggregate]].
  * NOTES: StreamExecOverAggregate only supports one [[org.apache.calcite.rel.core.Window.Group]],
  * else throw exception now
  */
class StreamExecOverAggregateRule
  extends ConverterRule(
    classOf[FlinkLogicalOverAggregate],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecOverAggregateRule") {

  override def convert(rel: RelNode): RelNode = {
    val logicWindow: FlinkLogicalOverAggregate = rel.asInstanceOf[FlinkLogicalOverAggregate]

    if (logicWindow.groups.size > 1) {
      throw new TableException(
        "Over Agg: Unsupported use of OVER windows. " +
          "All aggregates must be computed on the same window. " +
          "please re-check the over window statement.")
    }

    val keys = logicWindow.groups.get(0).keys

    val requiredDistribution = if (!keys.isEmpty) {
      FlinkRelDistribution.hash(keys.asList())
    } else {
      FlinkRelDistribution.SINGLETON
    }
    val requiredTraitSet = logicWindow.getInput.getTraitSet
      .replace(FlinkConventions.STREAM_PHYSICAL)
      .replace(requiredDistribution)
    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput = RelOptRule.convert(logicWindow.getInput, requiredTraitSet)

    new StreamExecOverAggregate(
      rel.getCluster,
      providedTraitSet,
      newInput,
      rel.getRowType,
      newInput.getRowType,
      logicWindow)
  }
}

object StreamExecOverAggregateRule {
  val INSTANCE: RelOptRule = new StreamExecOverAggregateRule
}

