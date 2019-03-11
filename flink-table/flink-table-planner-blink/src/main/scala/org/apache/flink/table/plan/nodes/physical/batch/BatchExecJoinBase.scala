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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.util.RelExplainUtil

import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core.{Join, JoinInfo, SemiJoin}
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.mapping.IntPair

import java.util.Collections

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for [[Join]]
  */
trait BatchExecJoinBase extends Join with BatchPhysicalRel {

  lazy val joinInfo: JoinInfo = JoinInfo.of(getLeft, getRight, getCondition)

  lazy val keyPairs: List[IntPair] = joinInfo.pairs.toList

  // TODO supports FlinkJoinRelType.ANTI
  lazy val flinkJoinType: FlinkJoinRelType = this match {
    case sj: SemiJoin => FlinkJoinRelType.SEMI
    case j: Join => FlinkJoinRelType.toFlinkJoinRelType(getJoinType)
    case _ => throw new IllegalArgumentException(s"Illegal join node: ${this.getRelTypeName}")
  }

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
      .item("where",
        RelExplainUtil.expressionToString(getCondition, inputRowType, getExpressionString))
      .item("join", getRowType.getFieldNames.mkString(", "))
      .item("joinType", RelExplainUtil.joinTypeToString(flinkJoinType))
  }

}
