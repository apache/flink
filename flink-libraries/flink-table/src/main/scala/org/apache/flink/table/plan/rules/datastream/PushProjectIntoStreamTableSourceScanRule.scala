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
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.plan.nodes.datastream.{DataStreamCalc, StreamTableSourceScan}
import org.apache.flink.table.plan.rules.util.RexProgramProjectExtractor._
import org.apache.flink.table.sources.{ProjectableTableSource, StreamTableSource}

/**
  * The rule is responsible for push project into a [[StreamTableSourceScan]]
  */
class PushProjectIntoStreamTableSourceScanRule extends RelOptRule(
  operand(classOf[DataStreamCalc],
    operand(classOf[StreamTableSourceScan], none())),
  "PushProjectIntoStreamTableSourceScanRule") {

  /** Rule must only match if [[StreamTableSource]] targets a [[ProjectableTableSource]] */
  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: StreamTableSourceScan = call.rel(1).asInstanceOf[StreamTableSourceScan]
    scan.tableSource match {
      case _: ProjectableTableSource[_] => true
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc = call.rel(0).asInstanceOf[DataStreamCalc]
    val scan = call.rel(1).asInstanceOf[StreamTableSourceScan]

    val usedFields = extractRefInputFields(calc.calcProgram)

    // if no fields can be projected, we keep the original plan
    if (TableEnvironment.getFieldNames(scan.tableSource).length != usedFields.length) {
      val originTableSource = scan.tableSource.asInstanceOf[ProjectableTableSource[_]]
      val newTableSource = originTableSource.projectFields(usedFields)
      val newScan = new StreamTableSourceScan(
        scan.getCluster,
        scan.getTraitSet,
        scan.getTable,
        newTableSource.asInstanceOf[StreamTableSource[_]])

      val newProgram = rewriteRexProgram(
        calc.calcProgram,
        newScan.getRowType,
        usedFields,
        calc.getCluster.getRexBuilder)

      if (newProgram.isTrivial) {
        // drop calc if the transformed program merely returns its input and doesn't exist filter
        call.transformTo(newScan)
      } else {
        val newCalc = new DataStreamCalc(
          calc.getCluster,
          calc.getTraitSet,
          newScan,
          calc.getRowType,
          newProgram,
          description)
        call.transformTo(newCalc)
      }
    }
  }
}

object PushProjectIntoStreamTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushProjectIntoStreamTableSourceScanRule
}
