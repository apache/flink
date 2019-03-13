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

import org.apache.flink.table.CalcitePair
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.plan.nodes.physical.batch.OverWindowMode.OverWindowMode
import org.apache.flink.table.plan.util.RelExplainUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction
import org.apache.calcite.tools.RelBuilder

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Batch physical RelNode for sort-based over [[Window]].
  */
class BatchExecOverAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    windowGroupToAggCallToAggFunction: Seq[
      (Window.Group, Seq[(AggregateCall, UserDefinedFunction)])],
    grouping: Array[Int],
    orderKeyIndices: Array[Int],
    orders: Array[Boolean],
    nullIsLasts: Array[Boolean],
    logicWindow: Window)
  extends SingleRel(cluster, traitSet, inputRel)
  with BatchPhysicalRel {

  private lazy val modeToGroupToAggCallToAggFunction:
    Seq[(OverWindowMode, Window.Group, Seq[(AggregateCall, UserDefinedFunction)])] =
    splitOutOffsetOrInsensitiveGroup()

  lazy val aggregateCalls: Seq[AggregateCall] =
    windowGroupToAggCallToAggFunction.flatMap(_._2).map(_._1)

  def getGrouping: Array[Int] = grouping

  override def deriveRowType: RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecOverAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      windowGroupToAggCallToAggFunction,
      grouping,
      orderKeyIndices,
      orders,
      nullIsLasts,
      logicWindow)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // sort is done in the last sort operator.
    val inputRows = mq.getRowCount(getInput())
    if (inputRows == null) {
      return null
    }
    val cpu = FlinkCost.FUNC_CPU_COST * inputRows *
      modeToGroupToAggCallToAggFunction.flatMap(_._3).size
    val averageRowSize: Double = mq.getAverageRowSize(this)
    val memCost = averageRowSize
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpu, 0, 0, memCost)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val partitionKeys: Array[Int] = grouping
    val groups = modeToGroupToAggCallToAggFunction.map(_._2)
    val constants: Seq[RexLiteral] = logicWindow.constants

    val writer = super.explainTerms(pw)
      .itemIf("partitionBy", RelExplainUtil.fieldToString(partitionKeys, inputRowType),
        partitionKeys.nonEmpty)
      .itemIf("orderBy",
        RelExplainUtil.collationToString(groups.head.orderKeys, inputRowType),
        orderKeyIndices.nonEmpty)

    var offset = inputRowType.getFieldCount
    groups.zipWithIndex.foreach { case (group, index) =>
      val namedAggregates = generateNamedAggregates(group)
      val select = RelExplainUtil.overAggregationToString(
        inputRowType,
        outputRowType,
        constants,
        namedAggregates,
        outputInputName = false,
        rowTypeOffset = offset)
      offset += namedAggregates.size
      val windowRange = RelExplainUtil.windowRangeToString(logicWindow, group)
      writer.item("window#" + index, select + windowRange)
    }
    writer.item("select", getRowType.getFieldNames.mkString(", "))
  }

  private def generateNamedAggregates(
      groupWindow: Group): Seq[CalcitePair[AggregateCall, String]] = {
    val aggregateCalls = groupWindow.getAggregateCalls(logicWindow)
    for (i <- 0 until aggregateCalls.size())
      yield new CalcitePair[AggregateCall, String](aggregateCalls.get(i), "windowAgg$" + i)
  }

  private[flink] def splitOutOffsetOrInsensitiveGroup()
  : Seq[(OverWindowMode, Window.Group, Seq[(AggregateCall, UserDefinedFunction)])] = {

    def compareTo(o1: Window.RexWinAggCall, o2: Window.RexWinAggCall): Boolean = {
      val allowsFraming1 = o1.getOperator.allowsFraming
      val allowsFraming2 = o2.getOperator.allowsFraming
      if (!allowsFraming1 && !allowsFraming2) {
        o1.getOperator.getClass == o2.getOperator.getClass
      } else {
        allowsFraming1 == allowsFraming2
      }
    }

    def inferGroupMode(group: Window.Group): OverWindowMode = {
      val aggCall = group.aggCalls(0)
      if (aggCall.getOperator.allowsFraming()) {
        if (group.isRows) OverWindowMode.Row else OverWindowMode.Range
      } else {
        if (aggCall.getOperator.isInstanceOf[SqlLeadLagAggFunction]) {
          OverWindowMode.Offset
        } else {
          OverWindowMode.Insensitive
        }
      }
    }

    def createNewGroup(
        group: Window.Group,
        aggCallsBuffer: Seq[(Window.RexWinAggCall, (AggregateCall, UserDefinedFunction))])
    : (OverWindowMode, Window.Group, Seq[(AggregateCall, UserDefinedFunction)]) = {
      val newGroup = new Window.Group(
        group.keys,
        group.isRows,
        group.lowerBound,
        group.upperBound,
        group.orderKeys,
        aggCallsBuffer.map(_._1))
      val mode = inferGroupMode(newGroup)
      (mode, group, aggCallsBuffer.map(_._2))
    }

    val windowGroupInfo =
      ArrayBuffer[(OverWindowMode, Window.Group, Seq[(AggregateCall, UserDefinedFunction)])]()
    windowGroupToAggCallToAggFunction.foreach { case (group, aggCallToAggFunction) =>
      var lastAggCall: Window.RexWinAggCall = null
      val aggCallsBuffer =
        ArrayBuffer[(Window.RexWinAggCall, (AggregateCall, UserDefinedFunction))]()
      group.aggCalls.zip(aggCallToAggFunction).foreach { case (aggCall, aggFunction) =>
        if (lastAggCall != null && !compareTo(lastAggCall, aggCall)) {
          windowGroupInfo.add(createNewGroup(group, aggCallsBuffer))
          aggCallsBuffer.clear()
        }
        aggCallsBuffer.add((aggCall, aggFunction))
        lastAggCall = aggCall
      }
      if (aggCallsBuffer.nonEmpty) {
        windowGroupInfo.add(createNewGroup(group, aggCallsBuffer))
        aggCallsBuffer.clear()
      }
    }
    windowGroupInfo
  }

}

object OverWindowMode extends Enumeration {
  type OverWindowMode = Value
  val Row: OverWindowMode = Value
  val Range: OverWindowMode = Value
  //Then it is a special kind of Window when the agg is LEAD&LAG.
  val Offset: OverWindowMode = Value
  val Insensitive: OverWindowMode = Value
}
