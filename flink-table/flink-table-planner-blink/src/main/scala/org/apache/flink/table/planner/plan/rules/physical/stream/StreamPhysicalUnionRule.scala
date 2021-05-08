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

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalUnion
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

import scala.collection.JavaConversions._

/**
  * Rule that converts [[FlinkLogicalUnion]] to [[StreamPhysicalUnion]].
  */
class StreamPhysicalUnionRule
  extends ConverterRule(
    classOf[FlinkLogicalUnion],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamPhysicalUnionRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    call.rel(0).asInstanceOf[FlinkLogicalUnion].all
  }

  def convert(rel: RelNode): RelNode = {
    val union: FlinkLogicalUnion = rel.asInstanceOf[FlinkLogicalUnion]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInputs = union.getInputs.map(RelOptRule.convert(_, FlinkConventions.STREAM_PHYSICAL))

    new StreamPhysicalUnion(
      rel.getCluster,
      traitSet,
      newInputs,
      union.all,
      rel.getRowType)
  }
}

object StreamPhysicalUnionRule {
  val INSTANCE: RelOptRule = new StreamPhysicalUnionRule
}
