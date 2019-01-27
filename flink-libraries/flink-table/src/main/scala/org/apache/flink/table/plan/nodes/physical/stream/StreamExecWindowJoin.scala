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

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.streaming.api.operators.{StreamFlatMap, StreamMap, TwoInputStreamOperator}
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation, TwoInputTransformation, UnionTransformation}
import org.apache.flink.table.api.types.TypeConverters
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.{FlinkRexUtil, JoinUtil, StreamExecUtil, UpdatingPlanChecker}
import org.apache.flink.table.runtime.KeyedCoProcessOperatorWithWatermarkDelay
import org.apache.flink.table.runtime.join._
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.Collector

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._

/**
  * RelNode for a time windowed stream join.
  */
class StreamExecWindowJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    val joinCondition: RexNode,
    val joinType: JoinRelType,
    leftRowSchema: BaseRowSchema,
    rightRowSchema: BaseRowSchema,
    outputRowSchema: BaseRowSchema,
    val isRowTime: Boolean,
    leftLowerBound: Long,
    leftUpperBound: Long,
    leftTimeIndex: Int,
    rightTimeIndex: Int,
    remainCondition: Option[RexNode],
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def deriveRowType(): RelDataType = outputRowSchema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecWindowJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinCondition,
      joinType,
      leftRowSchema,
      rightRowSchema,
      outputRowSchema,
      isRowTime,
      leftLowerBound,
      leftUpperBound,
      leftTimeIndex,
      rightTimeIndex,
      remainCondition,
      ruleDescription)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val windowBounds = s"isRowTime=$isRowTime, leftLowerBound=$leftLowerBound, " +
      s"leftUpperBound=$leftUpperBound, leftTimeIndex=$leftTimeIndex, " +
      s"rightTimeIndex=$rightTimeIndex"
    val writer = super.explainTerms(pw)
    JoinUtil.joinExplainTerms(
      writer,
      outputRowSchema.relDataType,
      joinCondition,
      FlinkJoinRelType.toFlinkJoinRelType(joinType),
      outputRowSchema.relDataType,
      getExpressionString)
    writer.item("windowBounds", windowBounds)
  }

  override def isDeterministic: Boolean = {
    remainCondition match {
      case Some(condition) =>
        if (!FlinkRexUtil.isDeterministicOperator(condition)) {
          return false
        }
      case _ => // do nothing
    }
    FlinkRexUtil.isDeterministicOperator(joinCondition)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    val config = tableEnv.getConfig

    val isLeftAppendOnly = UpdatingPlanChecker.isAppendOnly(left)
    val isRightAppendOnly = UpdatingPlanChecker.isAppendOnly(right)
    if (!isLeftAppendOnly || !isRightAppendOnly) {
      throw new TableException(
        TableErrors.INST.sqlWindowJoinUpdateNotSupported())
    }

    val leftDataStream = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val rightDataStream = getInputNodes.get(1).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    // get the equi-keys and other conditions
    val joinInfo = JoinInfo.of(left, right, joinCondition)
    val leftKeys = joinInfo.leftKeys.toIntArray
    val rightKeys = joinInfo.rightKeys.toIntArray
    val relativeWindowSize = leftUpperBound - leftLowerBound

    val leftType = leftRowSchema.internalType()
    val rightType = rightRowSchema.internalType()
    val returnType = outputRowSchema.internalType()

    // generate join function
    val joinFunction = WindowJoinUtil.generateJoinFunction(
      config,
      joinType,
      leftType,
      rightType,
      outputRowSchema.relDataType,
      remainCondition,
      ruleDescription)

    val flinkJoinType = FlinkJoinRelType.toFlinkJoinRelType(joinType)
     flinkJoinType match {
      case FlinkJoinRelType.INNER |
           FlinkJoinRelType.LEFT |
           FlinkJoinRelType.RIGHT |
           FlinkJoinRelType.FULL =>
        if (relativeWindowSize < 0) {
          LOG.warn(s"The relative window size $relativeWindowSize is negative," +
            " please check the join conditions.")
          createNegativeWindowSizeJoin(flinkJoinType,
            leftDataStream,
            rightDataStream,
            leftRowSchema.arity,
            rightRowSchema.arity,
            TypeConverters.toBaseRowTypeInfo(returnType))
        } else {
          if (isRowTime) {
            createRowTimeJoin(
              tableEnv,
              flinkJoinType,
              leftDataStream,
              rightDataStream,
              TypeConverters.toBaseRowTypeInfo(returnType),
              joinFunction.name,
              joinFunction.code,
              leftKeys,
              rightKeys)
          } else {
            createProcTimeJoin(
              tableEnv,
              flinkJoinType,
              leftDataStream,
              rightDataStream,
              TypeConverters.toBaseRowTypeInfo(returnType),
              joinFunction.name,
              joinFunction.code,
              leftKeys,
              rightKeys)
          }
        }
      case FlinkJoinRelType.ANTI =>
        throw new TableException(
          TableErrors.INST.sqlWindowJoinBetweenStreamNotSupported("Anti Join")
        )
      case FlinkJoinRelType.SEMI =>
        throw new TableException(
          TableErrors.INST.sqlWindowJoinBetweenStreamNotSupported("Semi Join")
        )
    }
  }

  private def setTransformationRes(streamTransformation: StreamTransformation[_]): Unit = {
    streamTransformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
  }

  def createNegativeWindowSizeJoin(
    joinType: FlinkJoinRelType,
    leftDataStream: StreamTransformation[BaseRow],
    rightDataStream: StreamTransformation[BaseRow],
    leftArity: Int,
    rightArity: Int,
    returnTypeInfo: BaseRowTypeInfo): StreamTransformation[BaseRow] = {

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

    val leftP = leftDataStream.getParallelism
    val rightP = rightDataStream.getParallelism

    val filterAllLeftStream = new OneInputTransformation[BaseRow, BaseRow](
      leftDataStream,
      "filter all left input transformation",
      new StreamFlatMap[BaseRow, BaseRow](allFilter),
      returnTypeInfo,
      leftP)
    setTransformationRes(filterAllLeftStream)

    val filterAllRightStream = new OneInputTransformation[BaseRow, BaseRow](
      rightDataStream,
      "filter all right input transformation",
      new StreamFlatMap[BaseRow, BaseRow](allFilter),
      returnTypeInfo,
      rightP)
    setTransformationRes(filterAllRightStream)

    val padLeftStream = new OneInputTransformation[BaseRow, BaseRow](
      leftDataStream,
      "pad left input transformation",
      new StreamMap[BaseRow, BaseRow](leftPadder),
      returnTypeInfo,
      leftP
    )
    setTransformationRes(padLeftStream)

    val padRightStream = new OneInputTransformation[BaseRow, BaseRow](
      rightDataStream,
      "pad right input transformation",
      new StreamMap[BaseRow, BaseRow](rightPadder),
      returnTypeInfo,
      rightP
    )
    setTransformationRes(padRightStream)
    joinType match {
      case FlinkJoinRelType.INNER =>
        new UnionTransformation(
          Array(filterAllLeftStream, filterAllRightStream).toList)
      case FlinkJoinRelType.LEFT =>
        new UnionTransformation(
          Array(padLeftStream, filterAllRightStream).toList)
      case FlinkJoinRelType.RIGHT =>
        new UnionTransformation(
          Array(filterAllLeftStream, padRightStream).toList)
      case FlinkJoinRelType.FULL =>
        new UnionTransformation(
          Array(padLeftStream, padRightStream).toList)
    }
  }

  def createProcTimeJoin(
    tableEnv: StreamTableEnvironment,
    joinType: FlinkJoinRelType,
    leftDataStream: StreamTransformation[BaseRow],
    rightDataStream: StreamTransformation[BaseRow],
    returnTypeInfo: BaseRowTypeInfo,
    joinFunctionName: String,
    joinFunctionCode: String,
    leftKeys: Array[Int],
    rightKeys: Array[Int]
  ): StreamTransformation[BaseRow] = {

    val leftType = leftRowSchema.internalType()
    val rightType = rightRowSchema.internalType()
    val leftTypeInfo = TypeConverters.toBaseRowTypeInfo(leftType)
    val rightTypeInfo = TypeConverters.toBaseRowTypeInfo(rightType)
    val procJoinFunc = new ProcTimeBoundedStreamJoin(
      joinType,
      leftLowerBound,
      leftUpperBound,
      leftTypeInfo,
      rightTypeInfo,
      joinFunctionName,
      joinFunctionCode
    )

    val leftSelect = StreamExecUtil.getKeySelector(leftKeys, leftTypeInfo)
    val rightSelect = StreamExecUtil.getKeySelector(rightKeys, rightTypeInfo)

    val ret = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftDataStream,
      rightDataStream,
      "Co-Process",
      new KeyedCoProcessOperator(procJoinFunc).
        asInstanceOf[TwoInputStreamOperator[BaseRow,BaseRow,BaseRow]],
      returnTypeInfo,
      leftDataStream.getParallelism
    )

    if (leftKeys.isEmpty) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }
    setTransformationRes(ret)
    // set KeyType and Selector for state
    ret.setStateKeySelectors(leftSelect, rightSelect)
    ret.setStateKeyType(leftSelect.getProducedType)
    ret
  }

  def createRowTimeJoin(
    tableEnv: StreamTableEnvironment,
    joinType: FlinkJoinRelType,
    leftDataStream: StreamTransformation[BaseRow],
    rightDataStream: StreamTransformation[BaseRow],
    returnTypeInfo: BaseRowTypeInfo,
    joinFunctionName: String,
    joinFunctionCode: String,
    leftKeys: Array[Int],
    rightKeys: Array[Int]
  ): StreamTransformation[BaseRow] = {
    val leftType = leftRowSchema.internalType()
    val rightType = rightRowSchema.internalType()
    val leftTypeInfo = TypeConverters.toBaseRowTypeInfo(leftType)
    val rightTypeInfo = TypeConverters.toBaseRowTypeInfo(rightType)
    val rowJoinFunc = new RowTimeBoundedStreamJoin(
      joinType,
      leftLowerBound,
      leftUpperBound,
      allowedLateness = 0L,
      leftTypeInfo,
      rightTypeInfo,
      joinFunctionName,
      joinFunctionCode,
      leftTimeIndex,
      rightTimeIndex)

    val leftSelect = StreamExecUtil.getKeySelector(leftKeys, leftTypeInfo)
    val rightSelect = StreamExecUtil.getKeySelector(rightKeys, rightTypeInfo)

    val ret = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftDataStream,
      rightDataStream,
      "Co-Process",
      new KeyedCoProcessOperatorWithWatermarkDelay(rowJoinFunc, rowJoinFunc.getMaxOutputDelay)
        .asInstanceOf[TwoInputStreamOperator[BaseRow,BaseRow,BaseRow]],
      returnTypeInfo,
      leftDataStream.getParallelism
    )

    setTransformationRes(ret)

    if (leftKeys.isEmpty) {
      ret.setMaxParallelism(1)
    }

    ret.setStateKeySelectors(leftSelect, rightSelect)
    ret.setStateKeyType(leftSelect.getProducedType)
    ret
  }
}
