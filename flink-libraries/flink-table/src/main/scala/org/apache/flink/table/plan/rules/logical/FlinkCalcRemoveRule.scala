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

package org.apache.flink.table.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCalc

class FlinkCalcRemoveRule extends RelOptRule(
  operand(classOf[FlinkLogicalCalc],
    operand(classOf[RelNode], any)),
  "FlinkCalcRemoveRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: FlinkLogicalCalc = call.rel(0)
    if (calc.getProgram.isTrivial) {
      val input = call.getPlanner.register(calc.getInput, calc)
      call.transformTo(RelOptRule.convert(input, calc.getTraitSet))
    }
  }
}

object FlinkCalcRemoveRule {
  val INSTANCE = new FlinkCalcRemoveRule
}
