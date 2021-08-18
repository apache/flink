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

package org.apache.flink.table.planner.plan.nodes.physical.common

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.plan.utils.JoinUtil
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{CorrelationId, Join, JoinRelType}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

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
  extends Join(cluster, traitSet, Collections.emptyList(), leftRel, rightRel, condition,
    Set.empty[CorrelationId], joinType)
  with FlinkPhysicalRel {

  if (containsPythonCall(condition)) {
    throw new TableException("Only inner join condition with equality predicates supports the " +
      "Python UDF taking the inputs from the left table and the right table at the same time, " +
      "e.g., ON T1.id = T2.id && pythonUdf(T1.a, T2.b)")
  }

  lazy val joinSpec: JoinSpec = JoinUtil.createJoinSpec(this)

  lazy val inputRowType: RelDataType = JoinUtil.combineJoinInputsRowType(this)

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("left", getLeft).input("right", getRight)
      .item("joinType", joinSpec.getJoinType.toString)
      .item("where", getExpressionString(
        getCondition, inputRowType.getFieldNames.toList, None, preferExpressionFormat(pw)))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

}
