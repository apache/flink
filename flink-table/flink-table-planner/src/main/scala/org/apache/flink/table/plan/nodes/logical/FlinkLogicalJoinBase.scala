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

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConverters._

abstract class FlinkLogicalJoinBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends Join(
    cluster,
    traitSet,
    left,
    right,
    condition,
    Set.empty[CorrelationId].asJava,
    joinType)
  with FlinkLogicalRel {

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val leftRowCnt = metadata.getRowCount(getLeft)
    val leftRowSize = estimateRowSize(getLeft.getRowType)

    val rightRowCnt = metadata.getRowCount(getRight)
    val rightRowSize = estimateRowSize(getRight.getRowType)

    val ioCost = (leftRowCnt * leftRowSize) + (rightRowCnt * rightRowSize)
    val cpuCost = leftRowCnt + rightRowCnt
    val rowCnt = leftRowCnt + rightRowCnt

    planner.getCostFactory.makeCost(rowCnt, cpuCost, ioCost)
  }
}
