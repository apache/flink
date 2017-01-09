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

package org.apache.flink.table.plan.rules.dataSet

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.plan.nodes.dataset.{BatchTableSourceScan, DataSetCalc}
import org.apache.flink.table.plan.rules.util.RexProgramProjectExtractor._
import org.apache.flink.table.sources.{BatchTableSource, ProjectableTableSource}

/**
  * This rule tries to push projections into a BatchTableSourceScan.
  */
class PushProjectIntoBatchTableSourceScanRule extends RelOptRule(
  operand(classOf[DataSetCalc],
          operand(classOf[BatchTableSourceScan], none)),
  "PushProjectIntoBatchTableSourceScanRule") {

  override def matches(call: RelOptRuleCall) = {
    val scan: BatchTableSourceScan = call.rel(1).asInstanceOf[BatchTableSourceScan]
    scan.tableSource match {
      case _: ProjectableTableSource[_] => true
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall) {
    val calc: DataSetCalc = call.rel(0).asInstanceOf[DataSetCalc]
    val scan: BatchTableSourceScan = call.rel(1).asInstanceOf[BatchTableSourceScan]

    val usedFields: Array[Int] = extractRefInputFields(calc.calcProgram)

    // if no fields can be projected, we keep the original plan.
    if (TableEnvironment.getFieldNames(scan.tableSource).length != usedFields.length) {
      val originTableSource = scan.tableSource.asInstanceOf[ProjectableTableSource[_]]
      val newTableSource = originTableSource.projectFields(usedFields)
      val newScan = new BatchTableSourceScan(
        scan.getCluster,
        scan.getTraitSet,
        scan.getTable,
        newTableSource.asInstanceOf[BatchTableSource[_]])

      val newCalcProgram = rewriteRexProgram(
        calc.calcProgram,
        newScan.getRowType,
        usedFields,
        calc.getCluster.getRexBuilder)

      if (newCalcProgram.isTrivial) {
        // drop calc if the transformed program merely returns its input and doesn't exist filter
        call.transformTo(newScan)
      } else {
        val newCalc = new DataSetCalc(
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

object PushProjectIntoBatchTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushProjectIntoBatchTableSourceScanRule
}
