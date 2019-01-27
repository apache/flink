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

package org.apache.flink.table.plan.nodes.logical

import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.calcite.{Expand, LogicalExpand}
import org.apache.flink.table.plan.util.ExpandUtil

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.{RexInputRef, RexLiteral, RexNode}

import java.util

import scala.collection.JavaConversions._

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

  override def isDeterministic: Boolean = ExpandUtil.isDeterministic(projects)
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
    val traitSet = cluster.traitSetOf(Convention.NONE)
    // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
    // the distribution trait, so we have to create FlinkLogicalExpand to
    // calculate the distribution trait
    val expand = new FlinkLogicalExpand(
      cluster,
      traitSet,
      input,
      outputRowType,
      projects,
      expandIdIndex)
    val newTraitSet = FlinkRelMetadataQuery.traitSet(expand)
      .replace(FlinkConventions.LOGICAL).simplify()
    expand.copy(newTraitSet, expand.getInputs).asInstanceOf[FlinkLogicalExpand]
  }
}
