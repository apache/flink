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

package org.apache.flink.table.plan.nodes.common

import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.{JoinTypeUtil, JoinUtil, RelExplainUtil}
import org.apache.flink.table.runtime.join.FlinkJoinType

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{CorrelationId, Join, JoinInfo, JoinRelType}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.mapping.IntPair

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

/**
  * Base physical class for flink [[Join]].
  */
abstract class CommonPhysicalJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends Join(cluster, traitSet, leftRel, rightRel, condition, Set.empty[CorrelationId], joinType)
  with FlinkPhysicalRel {

  def getJoinInfo: JoinInfo = joinInfo

  lazy val filterNulls: Array[Boolean] = {
    val filterNulls = new util.ArrayList[java.lang.Boolean]
    JoinUtil.createJoinInfo(getLeft, getRight, getCondition, filterNulls)
    filterNulls.map(_.booleanValue()).toArray
  }

  lazy val keyPairs: List[IntPair] = getJoinInfo.pairs.toList

  // TODO remove FlinkJoinType
  lazy val flinkJoinType: FlinkJoinType = JoinTypeUtil.getFlinkJoinType(this.getJoinType)

  lazy val inputRowType: RelDataType = joinType match {
    case JoinRelType.SEMI | JoinRelType.ANTI =>
      // Combines inputs' RowType, the result is different from SEMI/ANTI Join's RowType.
      SqlValidatorUtil.deriveJoinRowType(
        getLeft.getRowType,
        getRight.getRowType,
        getJoinType,
        getCluster.getTypeFactory,
        null,
        Collections.emptyList[RelDataTypeField]
      )
    case _ =>
      getRowType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("left", getLeft).input("right", getRight)
      .item("joinType", flinkJoinType.toString)
      .item("where",
        RelExplainUtil.expressionToString(getCondition, inputRowType, getExpressionString))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

}
