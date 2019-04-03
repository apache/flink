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

package org.apache.flink.table.plan.rules.physical.batch

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalUnion

import scala.collection.JavaConversions._

/**
  * Rule that converts [[FlinkLogicalUnion]] to [[BatchExecUnion]].
  */
class BatchExecUnionRule
  extends ConverterRule(
    classOf[FlinkLogicalUnion],
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCH_PHYSICAL,
    "BatchExecUnionRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    call.rel(0).asInstanceOf[FlinkLogicalUnion].all
  }

  def convert(rel: RelNode): RelNode = {
    val union = rel.asInstanceOf[FlinkLogicalUnion]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val newInputs = union.getInputs.map(RelOptRule.convert(_, FlinkConventions.BATCH_PHYSICAL))

    new BatchExecUnion(
      rel.getCluster,
      traitSet,
      newInputs,
      union.all,
      rel.getRowType)
  }
}

object BatchExecUnionRule {
  val INSTANCE: RelOptRule = new BatchExecUnionRule
}
