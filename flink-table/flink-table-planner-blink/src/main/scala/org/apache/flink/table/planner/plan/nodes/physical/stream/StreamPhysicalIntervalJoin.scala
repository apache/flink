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
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec.WindowBounds
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIntervalJoin
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat

import org.apache.calcite.plan._
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._

/**
 * Stream physical RelNode for a time interval stream join.
 */
class StreamPhysicalIntervalJoin(
      cluster: RelOptCluster,
      traitSet: RelTraitSet,
      leftRel: RelNode,
      rightRel: RelNode,
      joinType: JoinRelType,
      val originalCondition: RexNode,
      // remaining join condition contains all of join condition except window bounds
      remainingCondition: RexNode,
      windowBounds: WindowBounds)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, remainingCondition, joinType)
  with StreamPhysicalRel {

  if (joinSpec.getNonEquiCondition.isPresent
    && containsPythonCall(joinSpec.getNonEquiCondition.get)) {
    throw new TableException("Only inner join condition with equality predicates supports the " +
      "Python UDF taking the inputs from the left table and the right table at the same time, " +
      "e.g., ON T1.id = T2.id && pythonUdf(T1.a, T2.b)")
  }

  override def requireWatermark: Boolean = windowBounds.isEventTime

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new StreamPhysicalIntervalJoin(
        cluster, traitSet, left, right, joinType, originalCondition, conditionExpr, windowBounds)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val windowBoundsDesc = s"isRowTime=${windowBounds.isEventTime}, " +
      s"leftLowerBound=${windowBounds.getLeftLowerBound}, " +
      s"leftUpperBound=${windowBounds.getLeftUpperBound}, " +
      s"leftTimeIndex=${windowBounds.getLeftTimeIdx}, " +
      s"rightTimeIndex=${windowBounds.getRightTimeIdx}"
    pw.input("left", left)
      .input("right", right)
      .item("joinType", joinSpec.getJoinType)
      .item("windowBounds", windowBoundsDesc)
      .item("where", getExpressionString(
          originalCondition, getRowType.getFieldNames.toList, None, preferExpressionFormat(pw)))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  override def translateToExecNode(): ExecNode[_] = {
    new StreamExecIntervalJoin(
        new IntervalJoinSpec(joinSpec, windowBounds),
        InputProperty.DEFAULT,
        InputProperty.DEFAULT,
        FlinkTypeFactory.toLogicalRowType(getRowType),
        getRelDetailedDescription)
  }
}
