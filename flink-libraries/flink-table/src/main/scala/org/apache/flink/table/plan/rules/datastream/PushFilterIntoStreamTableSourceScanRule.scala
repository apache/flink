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

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.flink.table.plan.nodes.datastream.{DataStreamCalc, StreamTableSourceScan}
import org.apache.flink.table.plan.util.RexProgramExpressionExtractor._
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.FilterableTableSource

class PushFilterIntoStreamTableSourceScanRule extends RelOptRule(
  operand(classOf[DataStreamCalc],
    operand(classOf[StreamTableSourceScan], none)),
  "PushFilterIntoStreamTableSourceScanRule") {

  override def matches(call: RelOptRuleCall) = {
    val calc: DataStreamCalc = call.rel(0).asInstanceOf[DataStreamCalc]
    val scan: StreamTableSourceScan = call.rel(1).asInstanceOf[StreamTableSourceScan]
    scan.tableSource match {
      case _: FilterableTableSource =>
        calc.calcProgram.getCondition != null
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: DataStreamCalc = call.rel(0).asInstanceOf[DataStreamCalc]
    val scan: StreamTableSourceScan = call.rel(1).asInstanceOf[StreamTableSourceScan]

    val filterableSource = scan.tableSource.asInstanceOf[FilterableTableSource]

    val program = calc.calcProgram
    val tst = scan.getTable.unwrap(classOf[TableSourceTable[_]])
    val predicates = extractPredicateExpressions(
      program,
      call.builder().getRexBuilder,
      tst.tableEnv.getFunctionCatalog)

    if (predicates.length != 0) {
      val remainingPredicate = filterableSource.setPredicate(predicates)

      if (verifyExpressions(predicates, remainingPredicate)) {

        val filterRexNode = getFilterExpressionAsRexNode(
          program.getInputRowType,
          scan,
          predicates.diff(remainingPredicate))(call.builder())

        val newScan = new StreamTableSourceScan(
          scan.getCluster,
          scan.getTraitSet,
          scan.getTable,
          scan.tableSource,
          filterRexNode)

        val newCalcProgram = rewriteRexProgram(
          program,
          newScan,
          remainingPredicate)(call.builder())

        val newCalc = new DataStreamCalc(
          calc.getCluster,
          calc.getTraitSet,
          newScan,
          calc.getRowType,
          newCalcProgram,
          description)

        call.transformTo(newCalc)
      }
    }
  }

}

object PushFilterIntoStreamTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushFilterIntoStreamTableSourceScanRule
}
