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

package org.apache.flink.table.plan.rules.physical.batch.runtimefilter

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.functions.sql.internal.SqlRuntimeFilterFunction
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecCalc, BatchExecExchange}
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.BaseRuntimeFilterPushDownRule.findRuntimeFilters

/**
  * Planner rule that pushes a [[SqlRuntimeFilterFunction]] past a [[BatchExecExchange]].
  */
class RuntimeFilterExchangeTransposeRule extends SingleRelRfPushDownRule(
  classOf[BatchExecExchange],
  "RuntimeFilterExchangeTransposeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: BatchExecCalc = call.rel(0)
    findRuntimeFilters(calc.getProgram).nonEmpty
  }

  override def canPush(
      rel: BatchExecExchange,
      rCols: ImmutableBitSet,
      cond: RexNode): Boolean = true

  override def getFieldAdjustments(rel: BatchExecExchange): Array[Int] =
    new Array[Int](rel.getRowType.getFieldCount)
}

object RuntimeFilterExchangeTransposeRule {
  val INSTANCE = new RuntimeFilterExchangeTransposeRule
}
