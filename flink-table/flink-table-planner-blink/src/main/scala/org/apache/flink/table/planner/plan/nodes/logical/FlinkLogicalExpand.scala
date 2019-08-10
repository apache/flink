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

package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, LogicalExpand}

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.RexNode

import java.util

/**
  * Sub-class of [[Expand]] that is a relational expression
  * which returns multiple rows expanded from one input row.
  */
class FlinkLogicalExpand(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode,
    outputRowType: RelDataType,
    projects: util.List[util.List[RexNode]],
    expandIdIndex: Int)
  extends Expand(cluster, traits, input, outputRowType, projects, expandIdIndex)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalExpand(cluster, traitSet, inputs.get(0), outputRowType, projects, expandIdIndex)
  }

}

private class FlinkLogicalExpandConverter
  extends ConverterRule(
    classOf[LogicalExpand],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalExpandConverter") {

  override def convert(rel: RelNode): RelNode = {
    val expand = rel.asInstanceOf[LogicalExpand]
    val newInput = RelOptRule.convert(expand.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalExpand.create(
      newInput,
      expand.getRowType,
      expand.projects,
      expand.expandIdIndex)
  }
}

object FlinkLogicalExpand {
  val CONVERTER: ConverterRule = new FlinkLogicalExpandConverter()

  def create(
      input: RelNode,
      outputRowType: RelDataType,
      projects: util.List[util.List[RexNode]],
      expandIdIndex: Int): FlinkLogicalExpand = {
    val cluster = input.getCluster
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalExpand(cluster, traitSet, input, outputRowType, projects, expandIdIndex)
  }
}
