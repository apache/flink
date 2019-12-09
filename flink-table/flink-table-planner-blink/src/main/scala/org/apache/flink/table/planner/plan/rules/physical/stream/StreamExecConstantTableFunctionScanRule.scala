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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecCorrelate, StreamExecValues}

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexLiteral, RexUtil}

/**
  * Converts [[FlinkLogicalTableFunctionScan]] with constant RexCall to
  * {{{
  *                    [[StreamExecCorrelate]]
  *                          /       \
  * empty [[StreamExecValues]]  [[FlinkLogicalTableFunctionScan]]
  * }}}
  *
  * Add the rule to support select from a UDF directly, such as the following SQL:
  * SELECT * FROM LATERAL TABLE(func()) as T(c)
  *
  * Note: [[StreamExecCorrelateRule]] is responsible for converting a reasonable physical plan for
  * the normal correlate query, such as the following SQL:
  * example1: SELECT * FROM T, LATERAL TABLE(func()) as T(c)
  * example2: SELECT a, c FROM T, LATERAL TABLE(func(a)) as T(c)
  */
class StreamExecConstantTableFunctionScanRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalTableFunctionScan], any),
    "StreamExecConstantTableFunctionScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: FlinkLogicalTableFunctionScan = call.rel(0)
    RexUtil.isConstant(scan.getCall) && scan.getInputs.isEmpty
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val scan: FlinkLogicalTableFunctionScan = call.rel(0)

    // create correlate left
    val cluster = scan.getCluster
    val traitSet = call.getPlanner.emptyTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val values = new StreamExecValues(
      cluster,
      traitSet,
      ImmutableList.of(ImmutableList.of[RexLiteral]()),
      cluster.getTypeFactory.createStructType(ImmutableList.of(), ImmutableList.of()))

    val correlate = new StreamExecCorrelate(
      cluster,
      traitSet,
      values,
      None,
      scan,
      None,
      scan.getRowType,
      JoinRelType.INNER)
    call.transformTo(correlate)
  }

}

object StreamExecConstantTableFunctionScanRule {
  val INSTANCE = new StreamExecConstantTableFunctionScanRule
}
