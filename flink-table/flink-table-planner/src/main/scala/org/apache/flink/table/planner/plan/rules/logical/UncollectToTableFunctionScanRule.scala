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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.functions.BuiltInFunctionDefinitions
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.plan.utils.UnnestConversionUtil
import org.apache.flink.table.planner.utils.ShortcutUtils
import org.apache.flink.table.runtime.functions.table.UnnestRowsFunction
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toRowType

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.logical._

import java.util.Collections

/** Planner rule that converts Uncollect values to TableFunctionScan. */
class UncollectToTableFunctionScanRule(operand: RelOptRuleOperand, description: String)
  extends RelOptRule(operand, description)
  with UnnestConversionUtil {

  private def isProjectFilterValues(relNode: RelNode): Boolean = {
    relNode match {
      case p: LogicalProject => isProjectFilterValues(p.getInput())
      case f: LogicalFilter => isProjectFilterValues(f.getInput())
      case h: HepRelVertex => isProjectFilterValues(h.getCurrentRel)
      case _: LogicalValues => true
      case _ => false
    }
  }
  override def matches(call: RelOptRuleCall): Boolean = {
    val array: Uncollect = call.rel(0)
    isProjectFilterValues(array.getInput)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val array: Uncollect = call.rel(0)
    val tableFunctionScan = convert(array, array.getCluster, array.getTraitSet)
    call.transformTo(tableFunctionScan)
  }
}

object UncollectToTableFunctionScanRule {
  val INSTANCE = new UncollectToTableFunctionScanRule(
    operand(classOf[Uncollect], any()),
    "UncollectToTableFunctionScanRule")
}
