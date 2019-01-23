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

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalUpsertToRetraction}

import scala.collection.JavaConversions._

/**
  * Use this rule to transpose Calc through UpsertToRetraction relnode. It is beneficial if we get
  * smaller state size in upsertToRetraction.
  */
class CalcUpsertToRetractionTransposeRule extends RelOptRule(
  operand(classOf[FlinkLogicalCalc],
    operand(classOf[FlinkLogicalUpsertToRetraction], none)),
  "CalcUpsertToRetractionTransposeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val upsertToRetraction = call.rel(1).asInstanceOf[FlinkLogicalUpsertToRetraction]

    fieldsRemainAfterCalc(upsertToRetraction.keyNames, calc)
  }

  override def onMatch(call: RelOptRuleCall) {
    val calc = call.rel(0).asInstanceOf[FlinkLogicalCalc]
    val upsertToRetraction = call.rel(1).asInstanceOf[FlinkLogicalUpsertToRetraction]
    call.transformTo(getNewUpsertToRetraction(calc, upsertToRetraction))
  }

  private def getNewUpsertToRetraction(
    calc: FlinkLogicalCalc,
    upsertToRetraction: FlinkLogicalUpsertToRetraction): FlinkLogicalUpsertToRetraction = {

    val newCalc = calc.copy(calc.getTraitSet, upsertToRetraction.getInput, calc.getProgram)

    val oldKeyNames = upsertToRetraction.keyNames
    val newKeyNames = getNamesAfterCalc(oldKeyNames, calc)
    new FlinkLogicalUpsertToRetraction(
      upsertToRetraction.getCluster,
      upsertToRetraction.getTraitSet,
      newCalc,
      newKeyNames)
  }

  private def fieldsRemainAfterCalc(fields: Seq[String], calc: FlinkLogicalCalc): Boolean = {
    calc.getRowType.getFieldNames
      .flatMap(calc.getInputFromOutputName(calc, _))
      .containsAll(fields)
  }

  private def getNamesAfterCalc(names: Seq[String], calc: FlinkLogicalCalc): Seq[String] = {
    calc.getRowType.getFieldNames
      .filter{ outputName =>
        val inputName = calc.getInputFromOutputName(calc, outputName)
        inputName.isDefined && names.contains(inputName.get)
      }
  }
}

object CalcUpsertToRetractionTransposeRule {
  val INSTANCE = new CalcUpsertToRetractionTransposeRule()
}

