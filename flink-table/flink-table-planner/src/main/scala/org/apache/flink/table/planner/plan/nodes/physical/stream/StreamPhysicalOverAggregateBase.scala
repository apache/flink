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

import org.apache.flink.table.planner.CalcitePair
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.utils.RelExplainUtil

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexLiteral

import scala.collection.JavaConverters._

/** Base Stream physical RelNode for time-based over [[Window]]. */
abstract class StreamPhysicalOverAggregateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    val logicWindow: Window)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = {
    // several windows are only supported with the same order key
    if (logicWindow.groups.get(0).orderKeys.getFieldCollations.size() != 1) {
      return false
    }
    val orderKey = logicWindow.groups.get(0).orderKeys.getFieldCollations.get(0)
    val timeType = outputRowType.getFieldList.get(orderKey.getFieldIndex).getType
    FlinkTypeFactory.isRowtimeIndicatorType(timeType)
  }

  override def deriveRowType(): RelDataType = outputRowType

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    // over window: one input at least one output (do not introduce retract amplification)
    mq.getRowCount(getInput)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // by default, assume cost is proportional to number of rows
    val rowCnt: Double = mq.getRowCount(this)
    val count = (getRowType.getFieldCount - 1) * 1.0 / inputRel.getRowType.getFieldCount
    planner.getCostFactory.makeCost(rowCnt, rowCnt * count, 0)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val firstWindow: Group = logicWindow.groups.get(0)
    val constants: Seq[RexLiteral] = logicWindow.constants.asScala
    val partitionKeys: Array[Int] = firstWindow.keys.toArray

    val inputRowType = getInput.getRowType
    val writer = super
      .explainTerms(pw)
      .itemIf(
        "partitionBy",
        RelExplainUtil.fieldToString(partitionKeys, inputRowType),
        partitionKeys.nonEmpty)
      .item("orderBy", RelExplainUtil.collationToString(firstWindow.orderKeys, inputRowType))

    if (logicWindow.groups.size() == 1) {
      writer
        .item("window", RelExplainUtil.windowRangeToString(logicWindow, firstWindow))
        .item(
          "select",
          RelExplainUtil.overAggregationToString(
            inputRowType,
            outputRowType,
            constants,
            generateNamedAggregates(firstWindow, 0)))
    } else {
      var offset = inputRowType.getFieldCount
      logicWindow.groups.asScala.zipWithIndex.foreach {
        case (group, index) =>
          val namedAggregates = generateNamedAggregates(group, index)
          val select = RelExplainUtil.overAggregationToString(
            inputRowType,
            outputRowType,
            constants,
            namedAggregates,
            outputInputName = false,
            rowTypeOffset = offset)
          offset += namedAggregates.size
          writer.item(
            "window#" + index,
            select + RelExplainUtil.windowRangeToString(logicWindow, group))
      }
      writer.item("select", getRowType.getFieldNames.asScala.mkString(", "))
    }
  }

  private def generateNamedAggregates(
      overWindow: Group,
      windowIndex: Int): Seq[CalcitePair[AggregateCall, String]] = {
    val aggregateCalls = overWindow.getAggregateCalls(logicWindow)
    for (i <- 0 until aggregateCalls.size())
      yield new CalcitePair[AggregateCall, String](aggregateCalls.get(i), s"w$windowIndex$$o$i")
  }

}
