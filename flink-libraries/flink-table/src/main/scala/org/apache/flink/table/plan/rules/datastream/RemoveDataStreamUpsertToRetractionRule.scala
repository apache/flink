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
package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.flink.table.plan.nodes.datastream.{AccMode, DataStreamUpsertToRetraction}

/**
  * Rule to remove [[DataStreamUpsertToRetraction]] under [[AccMode.Acc]]. In this case, it is a
  * no-op node.
  */
class RemoveDataStreamUpsertToRetractionRule extends RelOptRule(
  operand(
    classOf[DataStreamUpsertToRetraction], none()),
  "RemoveDataStreamUpsertToRetractionRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val rel = call.rel(0).asInstanceOf[DataStreamUpsertToRetraction]
    val input = rel.getInput.asInstanceOf[HepRelVertex].getCurrentRel

    if (!DataStreamRetractionRules.isAccRetract(rel)) {
      call.transformTo(input)
    }
  }
}

object RemoveDataStreamUpsertToRetractionRule {
  val INSTANCE: RemoveDataStreamUpsertToRetractionRule = new RemoveDataStreamUpsertToRetractionRule
}
