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

import org.apache.flink.api.common.functions.{FlatJoinFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.streaming.api.operators.{StreamFlatMap, StreamMap, TwoInputStreamOperator}
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, TwoInputTransformation, UnionTransformation}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil.preferExpressionFormat
import org.apache.flink.table.planner.plan.utils.{IntervalJoinUtil, JoinTypeUtil, KeySelectorUtil}
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.operators.join.interval.{ProcTimeIntervalJoin, RowTimeIntervalJoin}
import org.apache.flink.table.runtime.operators.join.{FlinkJoinType, KeyedCoProcessOperatorWithWatermarkDelay, OuterJoinPaddingUtil}
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.flink.util.Collector

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for a time interval stream join.
  */
class StreamExecIntervalJoin(
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
  with StreamExecNode[RowData] {

  if (containsPythonCall(remainCondition.get)) {
    throw new TableException("Only inner join condition with equality predicates supports the " +
      "Python UDF taking the inputs from the left table and the right table at the same time, " +
      "e.g., ON T1.id = T2.id && pythonUdf(T1.a, T2.b)")
  }

  // TODO remove FlinkJoinType
  private lazy val flinkJoinType: FlinkJoinType = JoinTypeUtil.getFlinkJoinType(joinType)

  override def requireWatermark: Boolean = isRowTime

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecIntervalJoin(
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
      .item("where", getExpressionString(
        joinCondition, outputRowType.getFieldNames.toList, None, preferExpressionFormat(pw)))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }
  
  override def replaceInputNode(
      ordinalInParent: Int, newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val leftPlan = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val rightPlan = getInputNodes.get(1).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    flinkJoinType match {
      case FlinkJoinType.INNER |
           FlinkJoinType.LEFT |
           FlinkJoinType.RIGHT |
           FlinkJoinType.FULL =>
        val leftRowType = FlinkTypeFactory.toLogicalRowType(getLeft.getRowType)
        val rightRowType = FlinkTypeFactory.toLogicalRowType(getRight.getRowType)
        val returnType = RowDataTypeInfo.of(
          FlinkTypeFactory.toLogicalRowType(getRowType))

        val relativeWindowSize = leftUpperBound - leftLowerBound
        if (relativeWindowSize < 0) {
          LOG.warn(s"The relative time interval size $relativeWindowSize is negative," +
            " please check the join conditions.")
          createNegativeWindowSizeJoin(
            leftPlan,
            rightPlan,
            leftRowType.getFieldCount,
            rightRowType.getFieldCount,
            returnType)
        } else {
          // get the equi-keys and other conditions
          val joinInfo = JoinInfo.of(left, right, joinCondition)
          val leftKeys = joinInfo.leftKeys.toIntArray
          val rightKeys = joinInfo.rightKeys.toIntArray

          // generate join function
          val joinFunction = IntervalJoinUtil.generateJoinFunction(
            planner.getTableConfig,
            joinType,
            leftRowType,
            rightRowType,
            getRowType,
            remainCondition,
            "IntervalJoinFunction")

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
          "Interval Join: {Anti Join} between stream and stream is not supported yet.\n" +
            "please re-check interval join statement according to description above.")
      case FlinkJoinType.SEMI =>
        throw new TableException(
          "Interval Join: {Semi Join} between stream and stream is not supported yet.\n" +
            "please re-check interval join statement according to description above.")
    }
  }

  private def createNegativeWindowSizeJoin(
      leftPlan: Transformation[RowData],
      rightPlan: Transformation[RowData],
      leftArity: Int,
      rightArity: Int,
      returnTypeInfo: RowDataTypeInfo): Transformation[RowData] = {
    // We filter all records instead of adding an empty source to preserve the watermarks.
    val allFilter = new FlatMapFunction[RowData, RowData] with ResultTypeQueryable[RowData] {
      override def flatMap(value: RowData, out: Collector[RowData]): Unit = {}

      override def getProducedType: RowDataTypeInfo = returnTypeInfo
    }

    val leftPadder = new MapFunction[RowData, RowData] with ResultTypeQueryable[RowData] {
      val paddingUtil = new OuterJoinPaddingUtil(leftArity, rightArity)

      override def map(value: RowData): RowData = paddingUtil.padLeft(value)

      override def getProducedType: RowDataTypeInfo = returnTypeInfo
    }

    val rightPadder = new MapFunction[RowData, RowData] with ResultTypeQueryable[RowData] {
      val paddingUtil = new OuterJoinPaddingUtil(leftArity, rightArity)

      override def map(value: RowData): RowData = paddingUtil.padRight(value)

      override def getProducedType: RowDataTypeInfo = returnTypeInfo
    }

    val leftParallelism = leftPlan.getParallelism
    val rightParallelism = rightPlan.getParallelism

    val filterAllLeftStream = new OneInputTransformation[RowData, RowData](
      leftPlan,
      "filter all left input transformation",
      new StreamFlatMap[RowData, RowData](allFilter),
      returnTypeInfo,
      leftParallelism)

    val filterAllRightStream = new OneInputTransformation[RowData, RowData](
      rightPlan,
      "filter all right input transformation",
      new StreamFlatMap[RowData, RowData](allFilter),
      returnTypeInfo,
      rightParallelism)

    val padLeftStream = new OneInputTransformation[RowData, RowData](
      leftPlan,
      "pad left input transformation",
      new StreamMap[RowData, RowData](leftPadder),
      returnTypeInfo,
      leftParallelism
    )

    val padRightStream = new OneInputTransformation[RowData, RowData](
      rightPlan,
      "pad right input transformation",
      new StreamMap[RowData, RowData](rightPadder),
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
      leftPlan: Transformation[RowData],
      rightPlan: Transformation[RowData],
      returnTypeInfo: RowDataTypeInfo,
      joinFunction: GeneratedFunction[FlatJoinFunction[RowData, RowData, RowData]],
      leftKeys: Array[Int],
      rightKeys: Array[Int]): Transformation[RowData] = {
    val leftTypeInfo = leftPlan.getOutputType.asInstanceOf[RowDataTypeInfo]
    val rightTypeInfo = rightPlan.getOutputType.asInstanceOf[RowDataTypeInfo]
    val procJoinFunc = new ProcTimeIntervalJoin(
      flinkJoinType,
      leftLowerBound,
      leftUpperBound,
      leftTypeInfo,
      rightTypeInfo,
      joinFunction)

    val ret = new TwoInputTransformation[RowData, RowData, RowData](
      leftPlan,
      rightPlan,
      getRelDetailedDescription,
      new KeyedCoProcessOperator(procJoinFunc).
        asInstanceOf[TwoInputStreamOperator[RowData,RowData,RowData]],
      returnTypeInfo,
      leftPlan.getParallelism
    )

    if (leftKeys.isEmpty) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    val leftSelect = KeySelectorUtil.getRowDataSelector(leftKeys, leftTypeInfo)
    val rightSelect = KeySelectorUtil.getRowDataSelector(rightKeys, rightTypeInfo)
    ret.setStateKeySelectors(leftSelect, rightSelect)
    ret.setStateKeyType(leftSelect.getProducedType)
    ret
  }

  private def createRowTimeJoin(
      leftPlan: Transformation[RowData],
      rightPlan: Transformation[RowData],
      returnTypeInfo: RowDataTypeInfo,
      joinFunction: GeneratedFunction[FlatJoinFunction[RowData, RowData, RowData]],
      leftKeys: Array[Int],
      rightKeys: Array[Int]
  ): Transformation[RowData] = {
    val leftTypeInfo = leftPlan.getOutputType.asInstanceOf[RowDataTypeInfo]
    val rightTypeInfo = rightPlan.getOutputType.asInstanceOf[RowDataTypeInfo]
    val rowJoinFunc = new RowTimeIntervalJoin(
      flinkJoinType,
      leftLowerBound,
      leftUpperBound,
      0L,
      leftTypeInfo,
      rightTypeInfo,
      joinFunction,
      leftTimeIndex,
      rightTimeIndex)

    val ret = new TwoInputTransformation[RowData, RowData, RowData](
      leftPlan,
      rightPlan,
      getRelDetailedDescription,
      new KeyedCoProcessOperatorWithWatermarkDelay(rowJoinFunc, rowJoinFunc.getMaxOutputDelay)
        .asInstanceOf[TwoInputStreamOperator[RowData,RowData,RowData]],
      returnTypeInfo,
      leftPlan.getParallelism
    )

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    val leftSelector = KeySelectorUtil.getRowDataSelector(leftKeys, leftTypeInfo)
    val rightSelector = KeySelectorUtil.getRowDataSelector(rightKeys, rightTypeInfo)
    ret.setStateKeySelectors(leftSelector, rightSelector)
    ret.setStateKeyType(leftSelector.getProducedType)
    ret
  }

}
