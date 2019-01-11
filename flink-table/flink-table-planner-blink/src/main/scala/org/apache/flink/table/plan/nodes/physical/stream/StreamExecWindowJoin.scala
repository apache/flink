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

import org.apache.flink.api.common.functions.{FlatJoinFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.operators.co.LegacyKeyedCoProcessOperator
import org.apache.flink.streaming.api.operators.{StreamFlatMap, StreamMap, TwoInputStreamOperator}
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation, TwoInputTransformation, UnionTransformation}
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.generated.GeneratedFunction
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.util.{JoinTypeUtil, KeySelectorUtil, RelExplainUtil, UpdatingPlanChecker, WindowJoinUtil}
import org.apache.flink.table.runtime.join.{FlinkJoinType, KeyedCoProcessOperatorWithWatermarkDelay, OuterJoinPaddingUtil, ProcTimeBoundedStreamJoin, RowTimeBoundedStreamJoin}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.Collector
import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import java.util

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
    outputRowType: RelDataType,
    val isRowTime: Boolean,
    leftLowerBound: Long,
    leftUpperBound: Long,
    leftTimeIndex: Int,
    rightTimeIndex: Int,
    remainCondition: Option[RexNode])
  extends BiRel(cluster, traitSet, leftRel, rightRel)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  // TODO remove FlinkJoinType
  private lazy val flinkJoinType: FlinkJoinType = JoinTypeUtil.getFlinkJoinType(joinType)

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
      .item("joinType", flinkJoinType.toString)
      .item("windowBounds", windowBounds)
      .item("where",
        RelExplainUtil.expressionToString(joinCondition, outputRowType, getExpressionString))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamTableEnvironment, _]])
  }
  
  override def replaceInputNode(
      ordinalInParent: Int, newInputNode: ExecNode[StreamTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val isLeftAppendOnly = UpdatingPlanChecker.isAppendOnly(left)
    val isRightAppendOnly = UpdatingPlanChecker.isAppendOnly(right)
    if (!isLeftAppendOnly || !isRightAppendOnly) {
      throw new TableException(
        "Window Join: Windowed stream join does not support updates.\n" +
          "please re-check window join statement according to description above.")
    }

    val leftPlan = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val rightPlan = getInputNodes.get(1).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    flinkJoinType match {
      case FlinkJoinType.INNER |
           FlinkJoinType.LEFT |
           FlinkJoinType.RIGHT |
           FlinkJoinType.FULL =>
        val leftRowType = FlinkTypeFactory.toInternalRowType(getLeft.getRowType)
        val rightRowType = FlinkTypeFactory.toInternalRowType(getRight.getRowType)
        val returnType = FlinkTypeFactory.toInternalRowType(getRowType).toTypeInfo

        val relativeWindowSize = leftUpperBound - leftLowerBound
        if (relativeWindowSize < 0) {
          LOG.warn(s"The relative window size $relativeWindowSize is negative," +
            " please check the join conditions.")
          createNegativeWindowSizeJoin(
            leftPlan,
            rightPlan,
            leftRowType.getArity,
            rightRowType.getArity,
            returnType)
        } else {
          // get the equi-keys and other conditions
          val joinInfo = JoinInfo.of(left, right, joinCondition)
          val leftKeys = joinInfo.leftKeys.toIntArray
          val rightKeys = joinInfo.rightKeys.toIntArray

          // generate join function
          val joinFunction = WindowJoinUtil.generateJoinFunction(
            tableEnv.getConfig,
            joinType,
            leftRowType,
            rightRowType,
            getRowType,
            remainCondition,
            "WindowJoinFunction")

          if (isRowTime) {
            createRowTimeJoin(
              leftPlan,
              rightPlan,
              returnType,
              joinFunction,
              leftKeys,
              rightKeys)
          } else {
            createProcTimeJoin(
              leftPlan,
              rightPlan,
              returnType,
              joinFunction,
              leftKeys,
              rightKeys)
          }
        }
      case FlinkJoinType.ANTI =>
        throw new TableException(
          "Window Join: {Anti Join} between stream and stream is not supported yet.\n" +
            "please re-check window join statement according to description above.")
      case FlinkJoinType.SEMI =>
        throw new TableException(
          "Window Join: {Semi Join} between stream and stream is not supported yet.\n" +
            "please re-check window join statement according to description above.")
    }
  }

  private def createNegativeWindowSizeJoin(
      leftPlan: StreamTransformation[BaseRow],
      rightPlan: StreamTransformation[BaseRow],
      leftArity: Int,
      rightArity: Int,
      returnTypeInfo: BaseRowTypeInfo): StreamTransformation[BaseRow] = {
    // We filter all records instead of adding an empty source to preserve the watermarks.
    val allFilter = new FlatMapFunction[BaseRow, BaseRow] with ResultTypeQueryable[BaseRow] {
      override def flatMap(value: BaseRow, out: Collector[BaseRow]): Unit = {}

      override def getProducedType: TypeInformation[BaseRow] = returnTypeInfo
    }

    val leftPadder = new MapFunction[BaseRow, BaseRow] with ResultTypeQueryable[BaseRow] {
      val paddingUtil = new OuterJoinPaddingUtil(leftArity, rightArity)

      override def map(value: BaseRow): BaseRow = paddingUtil.padLeft(value)

      override def getProducedType: TypeInformation[BaseRow] = returnTypeInfo
    }

    val rightPadder = new MapFunction[BaseRow, BaseRow] with ResultTypeQueryable[BaseRow] {
      val paddingUtil = new OuterJoinPaddingUtil(leftArity, rightArity)

      override def map(value: BaseRow): BaseRow = paddingUtil.padRight(value)

      override def getProducedType: TypeInformation[BaseRow] = returnTypeInfo
    }

    val leftParallelism = leftPlan.getParallelism
    val rightParallelism = rightPlan.getParallelism

    val filterAllLeftStream = new OneInputTransformation[BaseRow, BaseRow](
      leftPlan,
      "filter all left input transformation",
      new StreamFlatMap[BaseRow, BaseRow](allFilter),
      returnTypeInfo,
      leftParallelism)

    val filterAllRightStream = new OneInputTransformation[BaseRow, BaseRow](
      rightPlan,
      "filter all right input transformation",
      new StreamFlatMap[BaseRow, BaseRow](allFilter),
      returnTypeInfo,
      rightParallelism)

    val padLeftStream = new OneInputTransformation[BaseRow, BaseRow](
      leftPlan,
      "pad left input transformation",
      new StreamMap[BaseRow, BaseRow](leftPadder),
      returnTypeInfo,
      leftParallelism
    )

    val padRightStream = new OneInputTransformation[BaseRow, BaseRow](
      rightPlan,
      "pad right input transformation",
      new StreamMap[BaseRow, BaseRow](rightPadder),
      returnTypeInfo,
      rightParallelism
    )
    flinkJoinType match {
      case FlinkJoinType.INNER =>
        new UnionTransformation(List(filterAllLeftStream, filterAllRightStream))
      case FlinkJoinType.LEFT =>
        new UnionTransformation(List(padLeftStream, filterAllRightStream))
      case FlinkJoinType.RIGHT =>
        new UnionTransformation(List(filterAllLeftStream, padRightStream))
      case FlinkJoinType.FULL =>
        new UnionTransformation(List(padLeftStream, padRightStream))
    }
  }

  private def createProcTimeJoin(
      leftPlan: StreamTransformation[BaseRow],
      rightPlan: StreamTransformation[BaseRow],
      returnTypeInfo: BaseRowTypeInfo,
      joinFunction: GeneratedFunction[FlatJoinFunction[BaseRow, BaseRow, BaseRow]],
      leftKeys: Array[Int],
      rightKeys: Array[Int]): StreamTransformation[BaseRow] = {
    val leftTypeInfo = leftPlan.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val rightTypeInfo = rightPlan.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val procJoinFunc = new ProcTimeBoundedStreamJoin(
      flinkJoinType,
      leftLowerBound,
      leftUpperBound,
      leftTypeInfo,
      rightTypeInfo,
      joinFunction)

    val ret = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftPlan,
      rightPlan,
      "Co-Process",
      new LegacyKeyedCoProcessOperator(procJoinFunc).
        asInstanceOf[TwoInputStreamOperator[BaseRow,BaseRow,BaseRow]],
      returnTypeInfo,
      leftPlan.getParallelism
    )

    if (leftKeys.isEmpty) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    val leftSelect = KeySelectorUtil.getBaseRowSelector(leftKeys, leftTypeInfo)
    val rightSelect = KeySelectorUtil.getBaseRowSelector(rightKeys, rightTypeInfo)
    ret.setStateKeySelectors(leftSelect, rightSelect)
    ret.setStateKeyType(leftSelect.getProducedType)
    ret
  }

  private def createRowTimeJoin(
      leftPlan: StreamTransformation[BaseRow],
      rightPlan: StreamTransformation[BaseRow],
      returnTypeInfo: BaseRowTypeInfo,
      joinFunction: GeneratedFunction[FlatJoinFunction[BaseRow, BaseRow, BaseRow]],
      leftKeys: Array[Int],
      rightKeys: Array[Int]
  ): StreamTransformation[BaseRow] = {
    val leftTypeInfo = leftPlan.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val rightTypeInfo = rightPlan.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val rowJoinFunc = new RowTimeBoundedStreamJoin(
      flinkJoinType,
      leftLowerBound,
      leftUpperBound,
      0L,
      leftTypeInfo,
      rightTypeInfo,
      joinFunction,
      leftTimeIndex,
      rightTimeIndex)

    val ret = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftPlan,
      rightPlan,
      "Co-Process",
      new KeyedCoProcessOperatorWithWatermarkDelay(rowJoinFunc, rowJoinFunc.getMaxOutputDelay)
        .asInstanceOf[TwoInputStreamOperator[BaseRow,BaseRow,BaseRow]],
      returnTypeInfo,
      leftPlan.getParallelism
    )

    if (leftKeys.isEmpty) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    val leftSelector = KeySelectorUtil.getBaseRowSelector(leftKeys, leftTypeInfo)
    val rightSelector = KeySelectorUtil.getBaseRowSelector(rightKeys, rightTypeInfo)
    ret.setStateKeySelectors(leftSelector, rightSelector)
    ret.setStateKeyType(leftSelector.getProducedType)
    ret
  }

}
