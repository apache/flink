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

import java.util.Collections

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex._
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.functions.sql.internal.SqlRuntimeFilterBuilderFunction
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecCalc, BatchExecExchange}
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.BaseRuntimeFilterPushDownRule.findRfBuilders

/**
  * Planner rule that pushes a [[SqlRuntimeFilterBuilderFunction]] past a [[BatchExecExchange]].
  */
class RfBuilderExchangeTransposeRule extends BaseRuntimeFilterPushDownRule(
  classOf[BatchExecExchange],
  "RfBuilderExchangeTransposeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: BatchExecCalc = call.rel(0)
    findRfBuilders(calc.getProgram).nonEmpty
  }

  override def canPush(
      rel: BatchExecExchange,
      rCols: ImmutableBitSet,
      cond: RexNode): Boolean = true

  override def getFieldAdjustments(rel: BatchExecExchange): Array[Int] =
    new Array[Int](rel.getRowType.getFieldCount)

  override def updateRfFunction(filterInput: RelNode, program: RexProgram): Unit = {
    // exchange does not modify NDV
  }

  override def getInputOfInput(input: BatchExecExchange): RelNode = input.getInput

  override def replaceInput(input: BatchExecExchange, filter: BatchExecCalc): RelNode =
    input.copy(input.getTraitSet, Collections.singletonList(filter))
}

object RfBuilderExchangeTransposeRule {
  val INSTANCE = new RfBuilderExchangeTransposeRule
}
