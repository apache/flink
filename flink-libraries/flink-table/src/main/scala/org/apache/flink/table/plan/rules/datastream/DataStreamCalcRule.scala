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

import org.apache.calcite.plan.{ Convention, RelOptRule, RelTraitSet }
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.LogicalCalc
import org.apache.flink.table.plan.nodes.datastream.DataStreamCalc
import org.apache.flink.table.plan.nodes.datastream.DataStreamConvention
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rel.logical.LogicalCorrelate
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexOver

class DataStreamCalcRule
    extends ConverterRule(
      classOf[LogicalCalc],
      Convention.NONE,
      DataStreamConvention.INSTANCE,
      "DataStreamCalcRule") {

  def convert(rel: RelNode): RelNode = {
    val calc: LogicalCalc = rel.asInstanceOf[LogicalCalc]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataStreamConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(calc.getInput, DataStreamConvention.INSTANCE)

    new DataStreamCalc(
      rel.getCluster,
      traitSet,
      convInput,
      rel.getRowType,
      calc.getProgram,
      description)
  }

  override def matches(call: RelOptRuleCall): Boolean = {

    val rl0: RelNode = call.rels(0)
    val calc: LogicalCalc = rl0.asInstanceOf[LogicalCalc]

    val expr = calc.getProgram.getExprList.toArray()

    expr.foreach { x => if (x.isInstanceOf[RexOver]) return false }

    super.matches(call);

  }

}

object DataStreamCalcRule {
  val INSTANCE: RelOptRule = new DataStreamCalcRule
}
