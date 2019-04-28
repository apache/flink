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

import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{Join, SemiJoin}
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.mapping.IntPair

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

/**
  * Base physical class for flink [[Join]].
  */
trait CommonPhysicalJoin extends Join with FlinkPhysicalRel {

  lazy val (joinInfo, filterNulls) = {
    val filterNulls = new util.ArrayList[java.lang.Boolean]
    val joinInfo = JoinUtil.createJoinInfo(getLeft, getRight, getCondition, filterNulls)
    (joinInfo, filterNulls.map(_.booleanValue()).toArray)
  }

  lazy val keyPairs: List[IntPair] = joinInfo.pairs.toList

  lazy val flinkJoinType: FlinkJoinType = JoinTypeUtil.getFlinkJoinType(this)

  lazy val inputRowType: RelDataType = this match {
    case sj: SemiJoin =>
      // Combines inputs' RowType, the result is different from SemiJoin's RowType.
      SqlValidatorUtil.deriveJoinRowType(
        sj.getLeft.getRowType,
        sj.getRight.getRowType,
        getJoinType,
        sj.getCluster.getTypeFactory,
        null,
        Collections.emptyList[RelDataTypeField]
      )
    case j: Join => getRowType
    case _ => throw new IllegalArgumentException(s"Illegal join node: ${this.getRelTypeName}")
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("left", getLeft).input("right", getRight)
      .item("joinType", flinkJoinType.toString)
      .item("where",
        RelExplainUtil.expressionToString(getCondition, inputRowType, getExpressionString))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

}
