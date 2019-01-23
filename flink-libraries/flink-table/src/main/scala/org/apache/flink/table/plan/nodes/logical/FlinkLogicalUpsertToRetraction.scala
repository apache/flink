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
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.table.plan.logical.rel.LogicalUpsertToRetraction
import org.apache.flink.table.plan.nodes.FlinkConventions

class FlinkLogicalUpsertToRetraction(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    val keyNames: Seq[String])
  extends SingleRel(cluster, traitSet, child)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode]): RelNode = {
    new FlinkLogicalUpsertToRetraction(cluster, traitSet, inputs.get(0), keyNames)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val child = this.getInput
    val rowCnt = mq.getRowCount(child)
    // take rowCnt and fieldCnt into account, so that cost will be smaller when generate
    // UpsertToRetractionConverter after Calc.
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(child.getRowType))
  }
}

private class FlinkLogicalUpsertToRetractionConverter
  extends ConverterRule(
    classOf[LogicalUpsertToRetraction],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalUpsertToRetractionConverter") {

  override def convert(rel: RelNode): RelNode = {
    val upsertToRetraction = rel.asInstanceOf[LogicalUpsertToRetraction]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newInput = RelOptRule.convert(upsertToRetraction.getInput, FlinkConventions.LOGICAL)

    new FlinkLogicalUpsertToRetraction(
      rel.getCluster,
      traitSet,
      newInput,
      upsertToRetraction.upsertKeyNames
    )
  }
}

object FlinkLogicalUpsertToRetraction {
  val CONVERTER: ConverterRule = new FlinkLogicalUpsertToRetractionConverter()
}
