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

import java.util.{List => JList}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{SetOp, Union}
import org.apache.calcite.rel.logical.LogicalUnion
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions

import scala.collection.JavaConverters._

class FlinkLogicalUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputs: JList[RelNode],
    all: Boolean)
  extends Union(cluster, traitSet, inputs, all)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode], all: Boolean): SetOp = {
    new FlinkLogicalUnion(cluster, traitSet, inputs, all)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val children = this.getInputs.asScala
    val rowCnt = children.foldLeft(0D) { (rows, child) =>
      rows + metadata.getRowCount(child)
    }
    planner.getCostFactory.makeCost(rowCnt, 0, 0)
  }
}

private class FlinkLogicalUnionConverter
  extends ConverterRule(
    classOf[LogicalUnion],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalUnionConverter") {

  /**
    * Only translate UNION ALL.
    */
  override def matches(call: RelOptRuleCall): Boolean = {
    val union: LogicalUnion = call.rel(0).asInstanceOf[LogicalUnion]
    union.all
  }

  override def convert(rel: RelNode): RelNode = {
    val union = rel.asInstanceOf[LogicalUnion]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newInputs = union.getInputs.asScala
        .map(input => RelOptRule.convert(input, FlinkConventions.LOGICAL)).asJava

    new FlinkLogicalUnion(rel.getCluster, traitSet, newInputs, union.all)
  }
}

object FlinkLogicalUnion {

  val CONVERTER: ConverterRule = new FlinkLogicalUnionConverter()

  def create(inputs: JList[RelNode], all: Boolean): FlinkLogicalUnion = {
    val cluster: RelOptCluster = inputs.get(0).getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(FlinkConventions.LOGICAL)
    new FlinkLogicalUnion(cluster, traitSet, inputs, all)
  }
}
