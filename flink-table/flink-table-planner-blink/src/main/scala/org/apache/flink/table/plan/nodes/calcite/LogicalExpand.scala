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

package org.apache.flink.table.plan.nodes.calcite

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.{RexInputRef, RexLiteral, RexNode}

import java.util

import scala.collection.JavaConversions._

/**
  * Sub-class of [[Expand]] that is a relational expression
  * which returns multiple rows expanded from one input row.
  * This class corresponds to Calcite logical rel.
  */
final class LogicalExpand(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode,
    outputRowType: RelDataType,
    projects: util.List[util.List[RexNode]],
    expandIdIndex: Int)
  extends Expand(cluster, traits, input, outputRowType, projects, expandIdIndex) {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new LogicalExpand(cluster, traitSet, inputs.get(0), outputRowType, projects, expandIdIndex)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val names = outputRowType.getFieldNames
    val terms = projects.map {
      project =>
        project.zipWithIndex.map {
          case (r: RexInputRef, i: Int) => s"${names.get(i)}=[${r.getName}]"
          case (l: RexLiteral, i: Int) => s"${names.get(i)}=[${l.getValue3}]"
          case (o, _) => s"$o"
        }.mkString("{", ", ", "}")
    }.mkString(", ")
    super.explainTerms(pw).item("projects", terms)
  }
}

object LogicalExpand {
  def create(
      input: RelNode,
      outputRowType: RelDataType,
      projects: util.List[util.List[RexNode]],
      expandIdIndex: Int): LogicalExpand = {
    val traits = input.getCluster.traitSetOf(Convention.NONE)
    new LogicalExpand(input.getCluster, traits, input, outputRowType, projects, expandIdIndex)
  }
}

