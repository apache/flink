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

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexInputRef
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.logical._

import scala.collection.JavaConverters._

/**
  * This rule is used to transpose FlinkLogicalCalc past FlinkLogicalLastRow to reduce state size.
  */
class CalcLastRowTransposeRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalCalc],
            operand(classOf[FlinkLogicalLastRow], any())),
    "LastRowCalcTransposeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: FlinkLogicalCalc = call.rel[FlinkLogicalCalc](0)
    val appToUpdate: FlinkLogicalLastRow = call.rel[FlinkLogicalLastRow](1)
    val origUniqueKeys = appToUpdate.getUniqueKeys
    // 1. can smaller state size  2. calc is simple project 3. unique key fields haven't lost
    calc.getRowType.getFieldNames.size() < appToUpdate.getRowType.getFieldNames.size() &&
      simpleProject(calc) &&
      getUnqueKeysAfterTranspose(origUniqueKeys, calc).size == origUniqueKeys.size &&
      countRowTimeField(calc) == countRowTimeField(appToUpdate)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: FlinkLogicalCalc = call.rel[FlinkLogicalCalc](0)
    val appToUpdate: FlinkLogicalLastRow = call.rel[FlinkLogicalLastRow](1)

    val newCalc = calc.copy(calc.getTraitSet, appToUpdate.getInputs)
    val newAppToUpdate = new FlinkLogicalLastRow(
      appToUpdate.getCluster,
      appToUpdate.getTraitSet,
      newCalc,
      getUnqueKeysAfterTranspose(appToUpdate.getUniqueKeys, calc),
      newCalc.getRowType)
    call.transformTo(newAppToUpdate)
  }

  def simpleProject(rel: FlinkLogicalCalc): Boolean = {
    rel.getProgram.getExprList.asScala.forall(_.isInstanceOf[RexInputRef])
  }

  def getUnqueKeysAfterTranspose(
    origUniqueKeys: Array[Int], calc: FlinkLogicalCalc): Array[Int] = {

    val newUniqueKeyIndex = calc.getProgram.getProjectList.asScala.zipWithIndex
      .filter(e => origUniqueKeys.contains(e._1.getIndex))
      .map(e => e._2)
      .toArray

    newUniqueKeyIndex
  }

  def countRowTimeField(rel: RelNode): Int = {
    rel.getRowType.getFieldList.asScala
      .count(e => FlinkTypeFactory.isRowtimeIndicatorType(e.getType))
  }
}

object CalcLastRowTransposeRule {
  val INSTANCE = new CalcLastRowTransposeRule
}

