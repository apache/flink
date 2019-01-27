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
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.functions.sql.internal.SqlRuntimeFilterFunction
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.BaseRuntimeFilterPushDownRule.findRuntimeFilters
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.RfBuilderJoinTransposeRule.getIndexFromCall

/**
  * Planner rule that pushes a [[SqlRuntimeFilterFunction]] past a [[SingleRel]].
  */
abstract class SingleRelRfPushDownRule[T <: SingleRel](
    inputClass: Class[T],
    description: String) extends BaseRuntimeFilterPushDownRule(inputClass, description) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: BatchExecCalc = call.rel(0)
    findRuntimeFilters(calc.getProgram).nonEmpty
  }

  override def getInputOfInput(input: T): RelNode = input.getInput

  override def replaceInput(input: T, filter: BatchExecCalc): RelNode =
    input.copy(input.getTraitSet, Collections.singletonList(filter))

  override def updateRfFunction(filterInput: RelNode, program: RexProgram): Unit = {
    // update ndv and rowCount
    val rfCalls = findRuntimeFilters(program)
    rfCalls.foreach { call =>
      val fieldIndex = getIndexFromCall(call)
      val rf = call.getOperator.asInstanceOf[SqlRuntimeFilterFunction]
      val query = filterInput.getCluster.getMetadataQuery
      val ndv = query.getDistinctRowCount(filterInput, ImmutableBitSet.of(fieldIndex), null)
      if (ndv != null) {
        rf.ndv = ndv
      }
      val rowCount = query.getRowCount(filterInput)
      if (rowCount != null) {
        rf.rowCount = rowCount
      }
    }
  }
}
