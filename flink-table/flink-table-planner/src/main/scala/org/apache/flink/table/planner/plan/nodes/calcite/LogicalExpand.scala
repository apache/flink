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

package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexNode

import java.util

/**
  * Sub-class of [[Expand]] that is a relational expression
  * which returns multiple rows expanded from one input row.
  * This class corresponds to Calcite logical rel.
  */
final class LogicalExpand(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode,
    projects: util.List[util.List[RexNode]],
    expandIdIndex: Int)
  extends Expand(cluster, traits, input, projects, expandIdIndex) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new LogicalExpand(cluster, traitSet, inputs.get(0), projects, expandIdIndex)
  }

}

object LogicalExpand {
  def create(
      input: RelNode,
      projects: util.List[util.List[RexNode]],
      expandIdIndex: Int): LogicalExpand = {
    val traits = input.getCluster.traitSetOf(Convention.NONE)
    new LogicalExpand(input.getCluster, traits, input, projects, expandIdIndex)
  }
}

