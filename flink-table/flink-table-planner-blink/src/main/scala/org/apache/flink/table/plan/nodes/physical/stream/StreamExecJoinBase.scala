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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.nodes.common.CommonPhysicalJoin

import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{CorrelationId, Join, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for regular [[Join]].
  *
  * Regular joins are the most generic type of join in which any new records or changes to
  * either side of the join input are visible and are affecting the whole join result.
  */
trait StreamExecJoinBase extends CommonPhysicalJoin with StreamPhysicalRel {

  override def producesUpdates: Boolean = {
    flinkJoinType != FlinkJoinRelType.INNER && flinkJoinType != FlinkJoinRelType.SEMI
  }

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = {
    def getCurrentRel(rel: RelNode): RelNode = {
      rel match {
        case _: HepRelVertex => rel.asInstanceOf[HepRelVertex].getCurrentRel
        case _ => rel
      }
    }

    val realInput = getCurrentRel(input)
    val inputUniqueKeys = getCluster.getMetadataQuery.getUniqueKeys(realInput)
    if (inputUniqueKeys != null) {
      val joinKeys = if (input == getCurrentRel(getLeft)) {
        keyPairs.map(_.source).toArray
      } else {
        keyPairs.map(_.target).toArray
      }
      val pkContainJoinKey = inputUniqueKeys.exists {
        uniqueKey => joinKeys.forall(uniqueKey.toArray.contains(_))
      }
      if (pkContainJoinKey) false else true
    } else {
      true
    }
  }

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = {
    flinkJoinType match {
      case FlinkJoinRelType.FULL | FlinkJoinRelType.RIGHT | FlinkJoinRelType.LEFT => true
      case FlinkJoinRelType.ANTI => true
      case _ => false
    }
  }

  override def requireWatermark: Boolean = false

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val elementRate = 100.0d * 2 // two input stream
    planner.getCostFactory.makeCost(elementRate, elementRate, 0)
  }

}

class StreamExecJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends Join(cluster, traitSet, leftRel, rightRel, condition, Set.empty[CorrelationId], joinType)
  with StreamExecJoinBase {

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new StreamExecJoin(cluster, traitSet, left, right, conditionExpr, joinType)
  }
}
