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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import java.util

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.planner.CalcitePair
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.batch.OverWindowMode.OverWindowMode
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchExecJoinRuleBase
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, OverAggregateUtil, RelExplainUtil}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelDistribution.Type._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Window.Group
import org.apache.calcite.rel.core.{AggregateCall, Window}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableIntList

import com.google.common.collect.ImmutableList

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Batch physical RelNode for sort-based over [[Window]] aggregate.
  */
abstract class BatchExecOverAggregateBase(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    grouping: Array[Int],
    orderKeyIndices: Array[Int],
    orders: Array[Boolean],
    nullIsLasts: Array[Boolean],
    windowGroupToAggCallToAggFunction: Seq[
      (Window.Group, Seq[(AggregateCall, UserDefinedFunction)])],
    logicWindow: Window)
  extends SingleRel(cluster, traitSet, inputRel)
  with BatchPhysicalRel
  with BatchExecNode[RowData] {

  protected lazy val modeToGroupToAggCallToAggFunction:
    Seq[(OverWindowMode, Window.Group, Seq[(AggregateCall, UserDefinedFunction)])] =
    splitOutOffsetOrInsensitiveGroup()

  protected val constants: ImmutableList[RexLiteral] = logicWindow.constants
  protected val inputTypeWithConstants: RelDataType = {
    val constantTypes = constants.map(c => FlinkTypeFactory.toLogicalType(c.getType))
    val inputTypeNamesWithConstants =
      inputType.getFieldNames ++ constants.indices.map(i => "TMP" + i)
    val inputTypesWithConstants = inputType.getChildren ++ constantTypes
    cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
      .buildRelNodeRowType(inputTypeNamesWithConstants, inputTypesWithConstants)
  }

  lazy val aggregateCalls: Seq[AggregateCall] =
    windowGroupToAggCallToAggFunction.flatMap(_._2).map(_._1)

  protected lazy val inputType = FlinkTypeFactory.toLogicalRowType(inputRowType)

  protected def isUnboundedWindow(group: Window.Group) =
    group.lowerBound.isUnbounded && group.upperBound.isUnbounded

  protected def isUnboundedPrecedingWindow(group: Window.Group) =
    group.lowerBound.isUnbounded && !group.upperBound.isUnbounded

  protected def isUnboundedFollowingWindow(group: Window.Group) =
    !group.lowerBound.isUnbounded && group.upperBound.isUnbounded

  protected def isSlidingWindow(group: Window.Group) =
    !group.lowerBound.isUnbounded && !group.upperBound.isUnbounded

  def getGrouping: Array[Int] = grouping

  override def deriveRowType: RelDataType = outputRowType

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

  private def splitOutOffsetOrInsensitiveGroup()
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

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    if (requiredDistribution.getType == ANY && requiredCollation.getFieldCollations.isEmpty) {
      return None
    }

    val selfProvidedTraitSet = inferProvidedTraitSet()
    if (selfProvidedTraitSet.satisfies(requiredTraitSet)) {
      // Current node can satisfy the requiredTraitSet,return the current node with ProvidedTraitSet
      return Some(copy(selfProvidedTraitSet, Seq(getInput)))
    }

    val inputFieldCnt = getInput.getRowType.getFieldCount
    val canSatisfy = if (requiredDistribution.getType == ANY) {
      true
    } else {
      if (!grouping.isEmpty) {
        if (requiredDistribution.requireStrict) {
          requiredDistribution.getKeys == ImmutableIntList.of(grouping: _*)
        } else {
          val isAllFieldsFromInput = requiredDistribution.getKeys.forall(_ < inputFieldCnt)
          if (isAllFieldsFromInput) {
            val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(this)
            if (tableConfig.getConfiguration.getBoolean(
              BatchExecJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED)) {
              ImmutableIntList.of(grouping: _*).containsAll(requiredDistribution.getKeys)
            } else {
              requiredDistribution.getKeys == ImmutableIntList.of(grouping: _*)
            }
          } else {
            // If requirement distribution keys are not all comes from input directly,
            // cannot satisfy requirement distribution and collations.
            false
          }
        }
      } else {
        requiredDistribution.getType == SINGLETON
      }
    }
    // If OverAggregate can provide distribution, but it's traits cannot satisfy required
    // distribution, cannot push down distribution and collation requirement (because later
    // shuffle will destroy previous collation.
    if (!canSatisfy) {
      return None
    }

    var inputRequiredTraits = getInput.getTraitSet
    var providedTraits = selfProvidedTraitSet
    val providedCollation = selfProvidedTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    if (!requiredDistribution.isTop) {
      inputRequiredTraits = inputRequiredTraits.replace(requiredDistribution)
      providedTraits = providedTraits.replace(requiredDistribution)
    }

    if (providedCollation.satisfies(requiredCollation)) {
      // the providedCollation can satisfy the requirement,
      // so don't push down the sort into it's input.
    } else if (providedCollation.getFieldCollations.isEmpty &&
      requiredCollation.getFieldCollations.nonEmpty) {
      // If OverAgg cannot provide collation itself, try to push down collation requirements into
      // it's input if collation fields all come from input node.
      val canPushDownCollation = requiredCollation.getFieldCollations
        .forall(_.getFieldIndex < inputFieldCnt)
      if (canPushDownCollation) {
        inputRequiredTraits = inputRequiredTraits.replace(requiredCollation)
        providedTraits = providedTraits.replace(requiredCollation)
      }
    } else {
      // Don't push down the sort into it's input,
      // due to the provided collation will destroy the input's provided collation.
    }
    val newInput = RelOptRule.convert(getInput, inputRequiredTraits)
    Some(copy(providedTraits, Seq(newInput)))
  }

  private def inferProvidedTraitSet(): RelTraitSet = {
    var selfProvidedTraitSet = getTraitSet
    // provided distribution
    val providedDistribution = if (grouping.nonEmpty) {
      FlinkRelDistribution.hash(grouping.map(Integer.valueOf).toList, requireStrict = false)
    } else {
      FlinkRelDistribution.SINGLETON
    }
    selfProvidedTraitSet = selfProvidedTraitSet.replace(providedDistribution)
    // provided collation
    val firstGroup = windowGroupToAggCallToAggFunction.head._1
    if (OverAggregateUtil.needCollationTrait(logicWindow, firstGroup)) {
      val collation = OverAggregateUtil.createCollation(firstGroup)
      if (!collation.equals(RelCollations.EMPTY)) {
        selfProvidedTraitSet = selfProvidedTraitSet.replace(collation)
      }
    }
    selfProvidedTraitSet
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior = DamBehavior.PIPELINED

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] =
    List(getInput.asInstanceOf[ExecNode[BatchPlanner, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
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
