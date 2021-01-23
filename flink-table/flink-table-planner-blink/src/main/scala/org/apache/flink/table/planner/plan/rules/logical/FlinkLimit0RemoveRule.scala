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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexLiteral

/**
  * Planner rule that rewrites `limit 0` to empty [[org.apache.calcite.rel.core.Values]].
  */
class FlinkLimit0RemoveRule extends RelOptRule(
  operand(classOf[Sort], any()),
  "FlinkLimit0RemoveRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort: Sort = call.rel(0)
    sort.fetch != null && RexLiteral.intValue(sort.fetch) == 0
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val sort: Sort = call.rel(0)
    val emptyValues = call.builder().values(sort.getRowType).build()
    call.transformTo(emptyValues)

    // New plan is absolutely better than old plan.
    call.getPlanner.prune(sort)
  }
}

object FlinkLimit0RemoveRule {
  val INSTANCE = new FlinkLimit0RemoveRule
}
