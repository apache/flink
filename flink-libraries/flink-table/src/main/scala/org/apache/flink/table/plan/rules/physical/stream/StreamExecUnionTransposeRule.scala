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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.plan.`trait`.{AccModeTraitDef, UpdateAsRetractionTraitDef}
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecUnion, _}

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}

import scala.collection.JavaConversions._

/**
  * Planner rule that transpose a stream RelNode with specified type into a [[StreamExecUnion]].
  */
class StreamExecUnionTransposeRule[T <: StreamPhysicalRel](
    outputClass: Class[T],
    description: String)
  extends RelOptRule(
    operand(outputClass,
      operand(classOf[StreamExecUnion], any)),
    description) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val outputRel: StreamPhysicalRel = call.rel(0)
    val union: StreamExecUnion = call.rel(1)
    val outputTraitSet = outputRel.getTraitSet
    val newInputsOfUnion = union.getInputs.map(input => outputRel.copy(outputTraitSet, Seq(input)))

    // union should extends original output trait set
    val accMode = outputRel.getTraitSet.getTrait(AccModeTraitDef.INSTANCE)
    val updateAsRetraction = outputRel.getTraitSet.getTrait(UpdateAsRetractionTraitDef.INSTANCE)
    val newUnionTraitSet = union.getTraitSet.replace(accMode).replace(updateAsRetraction)

    val newUnion = new StreamExecUnion(
      union.getCluster,
      newUnionTraitSet,
      newInputsOfUnion,
      outputRel.getRowType,
      union.all
    )

    call.transformTo(newUnion)
  }

}

object StreamExecUnionTransposeRule {

  val CALC_INSTANCE = new StreamExecUnionTransposeRule(
    classOf[StreamExecCalc],
    "StreamExecUnionCalcTransposeRule")

  val EXPAND_INSTANCE = new StreamExecUnionTransposeRule(
    classOf[StreamExecExpand],
    "StreamExecUnionExpandTransposeRule")

  val LOCAL_GROUP_AGG_INSTANCE = new StreamExecUnionTransposeRule(
    classOf[StreamExecLocalGroupAggregate],
    "StreamExecUnionLocalGroupAggTransposeRule")

}
