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
import org.apache.calcite.rel.core.{Minus, SetOp}
import org.apache.calcite.rel.logical.LogicalMinus
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions

import scala.collection.JavaConverters._

class FlinkLogicalMinus(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputs: JList[RelNode],
    all: Boolean)
  extends Minus(cluster, traitSet, inputs, all)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode], all: Boolean): SetOp = {
    new FlinkLogicalMinus(cluster, traitSet, inputs, all)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val children = this.getInputs.asScala
    children.foldLeft(planner.getCostFactory.makeCost(0, 0, 0)) { (cost, child) =>
      val rowCnt = metadata.getRowCount(child)
      val rowSize = this.estimateRowSize(child.getRowType)
      cost.plus(planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize))
    }
  }
}

private class FlinkLogicalMinusConverter
  extends ConverterRule(
    classOf[LogicalMinus],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalMinusConverter") {

  override def convert(rel: RelNode): RelNode = {
    val minus = rel.asInstanceOf[LogicalMinus]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newInputs = minus.getInputs.asScala
        .map(input => RelOptRule.convert(input, FlinkConventions.LOGICAL)).asJava

    new FlinkLogicalMinus(rel.getCluster, traitSet, newInputs, minus.all)
  }
}

object FlinkLogicalMinus {
  val CONVERTER: ConverterRule = new FlinkLogicalMinusConverter()
}
