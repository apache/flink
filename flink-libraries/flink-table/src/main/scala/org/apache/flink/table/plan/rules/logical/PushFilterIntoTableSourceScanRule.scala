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

import java.util

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rex.RexProgram
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalTableSourceScan}
import org.apache.flink.table.plan.util.RexProgramExtractor
import org.apache.flink.table.sources.FilterableTableSource
import org.apache.flink.table.validate.FunctionCatalog
import org.apache.flink.util.Preconditions

import scala.collection.JavaConverters._

class PushFilterIntoTableSourceScanRule extends RelOptRule(
  operand(classOf[FlinkLogicalCalc],
    operand(classOf[FlinkLogicalTableSourceScan], none)),
  "PushFilterIntoTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val scan: FlinkLogicalTableSourceScan = call.rel(1).asInstanceOf[FlinkLogicalTableSourceScan]
    scan.tableSource match {
      case source: FilterableTableSource[_] =>
        calc.getProgram.getCondition != null && !source.isFilterPushedDown
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: FlinkLogicalCalc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val scan: FlinkLogicalTableSourceScan = call.rel(1).asInstanceOf[FlinkLogicalTableSourceScan]
    val filterableSource = scan.tableSource.asInstanceOf[FilterableTableSource[_]]
    pushFilterIntoScan(call, calc, scan, filterableSource, description)
  }

  private def pushFilterIntoScan(
      call: RelOptRuleCall,
      calc: FlinkLogicalCalc,
      scan: FlinkLogicalTableSourceScan,
      filterableSource: FilterableTableSource[_],
      description: String): Unit = {

    Preconditions.checkArgument(!filterableSource.isFilterPushedDown)

    val program = calc.getProgram
    val functionCatalog = FunctionCatalog.withBuiltIns
    val (predicates, unconvertedRexNodes) =
      RexProgramExtractor.extractConjunctiveConditions(
        program,
        call.builder().getRexBuilder,
        functionCatalog)
    if (predicates.isEmpty) {
      // no condition can be translated to expression
      return
    }

    val remainingPredicates = new util.LinkedList[Expression]()
    predicates.foreach(e => remainingPredicates.add(e))

    val newTableSource = filterableSource.applyPredicate(remainingPredicates)

    // check whether framework still need to do a filter
    val relBuilder = call.builder()
    val remainingCondition = {
      if (!remainingPredicates.isEmpty || unconvertedRexNodes.nonEmpty) {
        relBuilder.push(scan)
        val remainingConditions =
          (remainingPredicates.asScala.map(expr => expr.toRexNode(relBuilder))
              ++ unconvertedRexNodes)
        remainingConditions.reduce((l, r) => relBuilder.and(l, r))
      } else {
        null
      }
    }

    // check whether we still need a RexProgram. An RexProgram is needed when either
    // projection or filter exists.
    val newScan = scan.copy(scan.getTraitSet, newTableSource, scan.selectedFields)
    val newRexProgram = {
      if (remainingCondition != null || !program.projectsOnlyIdentity) {
        val expandedProjectList = program.getProjectList.asScala
            .map(ref => program.expandLocalRef(ref)).asJava
        RexProgram.create(
          program.getInputRowType,
          expandedProjectList,
          remainingCondition,
          program.getOutputRowType,
          relBuilder.getRexBuilder)
      } else {
        null
      }
    }

    if (newRexProgram != null) {
      val newCalc = calc.copy(calc.getTraitSet, newScan, newRexProgram)
      call.transformTo(newCalc)
    } else {
      call.transformTo(newScan)
    }
  }
}

object PushFilterIntoTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushFilterIntoTableSourceScanRule
}
