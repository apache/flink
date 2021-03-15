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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.logical.WindowingStrategy
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.Litmus

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * The window join requires the join condition contains window starts equality of
 * input tables and window ends equality of input tables.
 * The semantic of window join is the same to the DataStream window join.
 */
class StreamPhysicalWindowJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    joinType: JoinRelType,
    // remaining join condition contains all of join condition except window starts equality
    // and window end equality
    remainingCondition: RexNode,
    val leftWindowing: WindowingStrategy,
    val rightWindowing: WindowingStrategy)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, remainingCondition, joinType)
  with StreamPhysicalRel {

  if (joinSpec.getNonEquiCondition.isPresent
    && containsPythonCall(joinSpec.getNonEquiCondition.get)) {
    throw new TableException("Only inner join condition with equality predicates supports the " +
      "Python UDF taking the inputs from the left table and the right table at the same time, " +
      "e.g., ON T1.id = T2.id && pythonUdf(T1.a, T2.b)")
  }

  isValid(Litmus.THROW, null)

  override def isValid(litmus: Litmus, context: RelNode.Context): Boolean = {
    if (leftWindowing.getTimeAttributeType != rightWindowing.getTimeAttributeType) {
      return litmus.fail(
        "Currently, window join doesn't support different time attribute type of left and " +
          "right inputs.")
    }
    if (leftWindowing.getWindow != rightWindowing.getWindow) {
      return litmus.fail(
        "Currently, window join doesn't support different window table function of left and " +
          "right inputs.")
    }
    super.isValid(litmus, context)
  }
  override def requireWatermark: Boolean = leftWindowing.isRowtime || rightWindowing.isRowtime

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new StreamPhysicalWindowJoin(
      cluster, traitSet, left, right, joinType, remainingCondition, leftWindowing, rightWindowing)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val leftInputFieldNames = left.getRowType.getFieldNames.asScala.toArray
    val rightInputFieldNames = right.getRowType.getFieldNames.asScala.toArray
    pw.input("left", left)
      .input("right", right)
      .item("leftWindow", leftWindowing.toSummaryString(leftInputFieldNames))
      .item("rightWindow", rightWindowing.toSummaryString(rightInputFieldNames))
      .item("joinType", joinSpec.getJoinType)
      .item("where",
        getExpressionString(
          remainingCondition, getRowType.getFieldNames.toList, None, preferExpressionFormat(pw)))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  override def translateToExecNode(): ExecNode[_] = {
    ???
  }
}
