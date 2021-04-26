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

package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalCorrelate, FlinkLogicalTableFunctionScan}
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCorrelate
import org.apache.flink.table.planner.plan.utils.PythonUtil

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.RexNode

class BatchPhysicalCorrelateRule extends ConverterRule(
  classOf[FlinkLogicalCorrelate],
  FlinkConventions.LOGICAL,
  FlinkConventions.BATCH_PHYSICAL,
  "BatchPhysicalCorrelateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel(0).asInstanceOf[FlinkLogicalCorrelate]
    val right = join.getRight.asInstanceOf[RelSubset].getOriginal

    right match {
      // right node is a table function
      // return true if it is a non python table function
      case scan: FlinkLogicalTableFunctionScan => PythonUtil.isNonPythonCall(scan.getCall)
      // a filter is pushed above the table function
      // return true if it is a non python table function
      case calc: FlinkLogicalCalc =>
        calc.getInput.asInstanceOf[RelSubset].getOriginal match {
          case scan: FlinkLogicalTableFunctionScan => PythonUtil.isNonPythonCall(scan.getCall)
          case _ => false
        }
      case _ => false
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val join = rel.asInstanceOf[FlinkLogicalCorrelate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val convInput: RelNode = RelOptRule.convert(join.getInput(0), FlinkConventions.BATCH_PHYSICAL)
    val right: RelNode = join.getInput(1)

    def convertToCorrelate(relNode: RelNode, condition: Option[RexNode]): BatchPhysicalCorrelate = {
      relNode match {
        case rel: RelSubset =>
          convertToCorrelate(rel.getRelList.get(0), condition)

        case calc: FlinkLogicalCalc =>
          convertToCorrelate(
            calc.getInput.asInstanceOf[RelSubset].getOriginal,
            Some(calc.getProgram.expandLocalRef(calc.getProgram.getCondition)))

        case scan: FlinkLogicalTableFunctionScan =>
          new BatchPhysicalCorrelate(
            rel.getCluster,
            traitSet,
            convInput,
            scan,
            condition,
            None,
            rel.getRowType,
            join.getJoinType)
      }
    }
    convertToCorrelate(right, None)
  }
}

object BatchPhysicalCorrelateRule {
  val INSTANCE: RelOptRule = new BatchPhysicalCorrelateRule
}
