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
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.{RexCall, RexNode, RexProgramBuilder}
import org.apache.flink.table.functions.sql.internal.SqlRuntimeFilterBuilderFunction
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecCalc
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.BaseRuntimeFilterPushDownRule.findRfBuilders

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Planner rule that combines two [[SqlRuntimeFilterBuilderFunction]]s.
  */
class RuntimeFilterBuilderMerger extends RelOptRule(
  operand(classOf[BatchExecCalc],
    operand(classOf[RelNode], any)),
  "RuntimeFilterBuilderMerger") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: BatchExecCalc = call.rel(0)
    findRfBuilders(calc.getProgram).size > 1
  }

  override def onMatch(ruleCall: RelOptRuleCall): Unit = {
    val calc: BatchExecCalc = ruleCall.rel(0)
    val conditions = RelOptUtil.conjunctions(
      calc.getProgram.expandLocalRef(calc.getProgram.getCondition))

    val newConds = new mutable.ArrayBuffer[RexNode]()
    val rfBuilderMap = new mutable.HashMap[String, SqlRuntimeFilterBuilderFunction]()
    conditions.foreach {
      case call: RexCall => call.getOperator match {
        case builder: SqlRuntimeFilterBuilderFunction =>
          // hashCode toString => id
          val id = call.getOperands.head.toString
          if (!rfBuilderMap.contains(id)) {
            rfBuilderMap += id -> builder
            newConds += call
          } else {
            val existBuilder = rfBuilderMap(id)
            existBuilder.filters ++= builder.filters
            builder.filters.foreach(rf => rf.builder = existBuilder)
          }
        case _ => newConds += call
      }
      case cond => newConds += cond
    }

    if (newConds.size != conditions.size) {
      val rexBuilder = calc.getCluster.getRexBuilder
      val pBuilder = RexProgramBuilder.forProgram(calc.getProgram, rexBuilder, true)
      pBuilder.clearCondition()
      newConds.foreach(pBuilder.addCondition)
      ruleCall.transformTo(calc.copy(calc.getTraitSet, calc.getInput, pBuilder.getProgram()))
    }
  }
}

object RuntimeFilterBuilderMerger {
  val INSTANCE = new RuntimeFilterBuilderMerger
}
