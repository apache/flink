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
package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.functions.FunctionKind
import org.apache.flink.table.functions.python.PythonFunction
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalPythonAsyncCalc
import org.apache.flink.table.planner.utils.ShortcutUtils

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rex.{RexCall, RexNode}

import scala.collection.JavaConverters._

/**
 * Rule that converts [[FlinkLogicalCalc]] to [[BatchPhysicalPythonAsyncCalc]].
 *
 * <p>This rule identifies async Python scalar functions and converts them to a dedicated async
 * execution node for better performance.
 */
class BatchPhysicalPythonAsyncCalcRule(config: Config) extends ConverterRule(config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0)
    val program = calc.getProgram
    program.getExprList.asScala.exists(containsPythonAsyncCall(_))
  }

  def convert(rel: RelNode): RelNode = {
    val calc = rel.asInstanceOf[FlinkLogicalCalc]
    val newTrait = rel.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val newInput = RelOptRule.convert(calc.getInput, FlinkConventions.BATCH_PHYSICAL)

    new BatchPhysicalPythonAsyncCalc(
      rel.getCluster,
      newTrait,
      newInput,
      calc.getProgram,
      rel.getRowType)
  }

  /**
   * Checks if a RexNode contains a Python async scalar function call.
   *
   * @param node
   *   The RexNode to check
   * @return
   *   true if the node contains a Python async scalar function, false otherwise
   */
  private def containsPythonAsyncCall(node: RexNode): Boolean = {
    node match {
      case call: RexCall =>
        // Check if this is a function call with async scalar kind
        call.getOperator match {
          case function: BridgingSqlFunction =>
            val definition = ShortcutUtils.unwrapFunctionDefinition(call)
            // Must be a Python function with ASYNC_SCALAR kind
            if (
              definition.isInstanceOf[
                PythonFunction] && (function.getDefinition.getKind eq FunctionKind.ASYNC_SCALAR)
            ) return true
          case _ =>
        }
        // Recursively check operands
        call.getOperands.stream.anyMatch(this.containsPythonAsyncCall)

      case _ => false
    }
  }
}

object BatchPhysicalPythonAsyncCalcRule {
  val INSTANCE: RelOptRule = new BatchPhysicalPythonAsyncCalcRule(
    Config.INSTANCE.withConversion(
      classOf[FlinkLogicalCalc],
      FlinkConventions.LOGICAL,
      FlinkConventions.BATCH_PHYSICAL,
      "BatchPhysicalPythonAsyncCalcRule"))
}
