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

package org.apache.flink.api.table.plan.rules.dataSet

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rel.rules.JoinCommuteRule

class CrossCommuteRule(swapOuter: Boolean) extends JoinCommuteRule(
    classOf[LogicalJoin],
    RelFactories.LOGICAL_BUILDER,
    swapOuter) {

  override def matches(call: RelOptRuleCall): Boolean = {

    val join = call.rel(0).asInstanceOf[LogicalJoin]

    // check if Join is a Cartesian product
    join.getCondition.isAlwaysTrue
  }
}

object CrossCommuteRule {
  val INSTANCE = new CrossCommuteRule(true)
}
