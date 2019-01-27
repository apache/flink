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

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlOperator
import org.apache.flink.table.functions.sql.internal.{SqlRuntimeFilterBuilderFunction, SqlRuntimeFilterFunction}
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.UselessRfBuilderRemoveRule._
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.UselessRuntimeFilterRemoveRule.removeFilters

import scala.collection.mutable

/**
  * Planner rule that removes a useless [[SqlRuntimeFilterBuilderFunction]] without
  * [[SqlRuntimeFilterFunction]] reference.
  */
class UselessRfBuilderRemoveRule extends RelOptRule(
  operand(classOf[BatchExecCalc], operand(classOf[RelNode], any)),
  "UselessRfBuilderRemoveRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: BatchExecCalc = call.rel(0)
    findRfBuilders(calc.getProgram).nonEmpty
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: BatchExecCalc = call.rel(0)
    val rfs = findRfBuilders(calc.getProgram)
        .map(_.getOperator.asInstanceOf[SqlRuntimeFilterBuilderFunction])
    val toBeRemove = new mutable.ArrayBuffer[SqlOperator]
    rfs.foreach { rf =>
      if (rf.filters.isEmpty) {
        toBeRemove += rf
      }
    }

    if (toBeRemove.nonEmpty) {
      call.transformTo(removeFilters(calc, toBeRemove.toArray))
    }
  }
}

object UselessRfBuilderRemoveRule {

  val INSTANCE = new UselessRfBuilderRemoveRule

  def findRfBuilders(program: RexProgram): Array[RexCall] = {
    val rfs = new mutable.ArrayBuffer[RexCall]
    RexUtil.apply(new RexVisitorImpl[Void](true) {
      override def visitCall(call: RexCall): Void = {
        call.getOperator match {
          case _: SqlRuntimeFilterBuilderFunction => rfs += call
          case _ =>
        }
        super.visitCall(call)
      }
    }, program.getExprList, null)
    rfs.toArray
  }
}
