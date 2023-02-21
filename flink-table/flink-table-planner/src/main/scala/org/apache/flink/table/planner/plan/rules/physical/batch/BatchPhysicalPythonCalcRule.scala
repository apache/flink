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
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalPythonCalc
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config

import scala.collection.JavaConverters._

/** Rule that converts [[FlinkLogicalCalc]] to [[BatchPhysicalPythonCalc]]. */
class BatchPhysicalPythonCalcRule(config: Config) extends ConverterRule(config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel(0)
    val program = calc.getProgram
    program.getExprList.asScala.exists(containsPythonCall(_))
  }

  def convert(rel: RelNode): RelNode = {
    val calc = rel.asInstanceOf[FlinkLogicalCalc]
    val newTrait = rel.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val newInput = RelOptRule.convert(calc.getInput, FlinkConventions.BATCH_PHYSICAL)

    new BatchPhysicalPythonCalc(rel.getCluster, newTrait, newInput, calc.getProgram, rel.getRowType)
  }
}

object BatchPhysicalPythonCalcRule {
  val INSTANCE: RelOptRule = new BatchPhysicalPythonCalcRule(
    Config.INSTANCE.withConversion(
      classOf[FlinkLogicalCalc],
      FlinkConventions.LOGICAL,
      FlinkConventions.BATCH_PHYSICAL,
      "BatchPhysicalPythonCalcRule"))
}
