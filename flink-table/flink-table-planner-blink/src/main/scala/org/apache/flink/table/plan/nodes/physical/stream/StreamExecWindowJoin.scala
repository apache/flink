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
import org.apache.flink.table.plan.util.RelExplainUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for a time windowed stream join.
  */
class StreamExecWindowJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    val joinCondition: RexNode,
    val joinType: JoinRelType,
    leftInputRowType: RelDataType,
    rightInputRowType: RelDataType,
    outputRowType: RelDataType,
    val isRowTime: Boolean,
    leftLowerBound: Long,
    leftUpperBound: Long,
    leftTimeIndex: Int,
    rightTimeIndex: Int,
    remainCondition: Option[RexNode])
  extends BiRel(cluster, traitSet, leftRel, rightRel)
  with StreamPhysicalRel {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = isRowTime

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecWindowJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinCondition,
      joinType,
      leftInputRowType,
      rightInputRowType,
      outputRowType,
      isRowTime,
      leftLowerBound,
      leftUpperBound,
      leftTimeIndex,
      rightTimeIndex,
      remainCondition)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val windowBounds = s"isRowTime=$isRowTime, leftLowerBound=$leftLowerBound, " +
      s"leftUpperBound=$leftUpperBound, leftTimeIndex=$leftTimeIndex, " +
      s"rightTimeIndex=$rightTimeIndex"
    super.explainTerms(pw)
      .item("where",
        RelExplainUtil.expressionToString(joinCondition, outputRowType, getExpressionString))
      .item("join", getRowType.getFieldNames.mkString(", "))
      .item("joinType",
        RelExplainUtil.joinTypeToString(FlinkJoinRelType.toFlinkJoinRelType(joinType)))
      .item("windowBounds", windowBounds)
  }

}
