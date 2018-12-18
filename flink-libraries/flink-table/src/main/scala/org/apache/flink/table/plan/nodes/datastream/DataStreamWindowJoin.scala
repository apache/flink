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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.plan.nodes.CommonJoin
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.table.runtime.{CRowKeySelector, CRowWrappingCollector}
import org.apache.flink.table.runtime.join.{OuterJoinPaddingUtil, ProcTimeBoundedStreamJoin, RowTimeBoundedStreamJoin, WindowJoinUtil}
import org.apache.flink.table.runtime.operators.KeyedCoProcessOperatorWithWatermarkDelay
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

/**
  * RelNode for a time windowed stream join.
  */
class DataStreamWindowJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    joinCondition: RexNode,
    joinType: JoinRelType,
    leftSchema: RowSchema,
    rightSchema: RowSchema,
    schema: RowSchema,
    isRowTime: Boolean,
    leftLowerBound: Long,
    leftUpperBound: Long,
    leftTimeIdx: Int,
    rightTimeIdx: Int,
    remainCondition: Option[RexNode],
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
    with CommonJoin
    with DataStreamRel
    with Logging {

  override def deriveRowType(): RelDataType = schema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamWindowJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinCondition,
      joinType,
      leftSchema,
      rightSchema,
      schema,
      isRowTime,
      leftLowerBound,
      leftUpperBound,
      leftTimeIdx,
      rightTimeIdx,
      remainCondition,
      ruleDescription)
  }

  override def toString: String = {
    joinToString(
      schema.relDataType,
      joinCondition,
      joinType,
      getExpressionString)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    joinExplainTerms(
      super.explainTerms(pw),
      schema.relDataType,
      joinCondition,
      joinType,
      getExpressionString)
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val config = tableEnv.getConfig

    val isLeftAppendOnly = UpdatingPlanChecker.isAppendOnly(left)
    val isRightAppendOnly = UpdatingPlanChecker.isAppendOnly(right)
    if (!isLeftAppendOnly || !isRightAppendOnly) {
      throw new TableException(
        "Windowed stream join does not support updates.")
    }

    val leftDataStream = left.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)
    val rightDataStream = right.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)

    // get the equi-keys and other conditions
    val joinInfo = JoinInfo.of(leftNode, rightNode, joinCondition)
    val leftKeys = joinInfo.leftKeys.toIntArray
    val rightKeys = joinInfo.rightKeys.toIntArray
    val relativeWindowSize = leftUpperBound - leftLowerBound
    val returnTypeInfo = CRowTypeInfo(schema.typeInfo)

    // generate join function
    val joinFunction =
    WindowJoinUtil.generateJoinFunction(
      config,
      joinType,
      leftSchema.typeInfo,
      rightSchema.typeInfo,
      schema,
      remainCondition,
      ruleDescription)

    val joinOpName =
      s"where: (" +
        s"${joinConditionToString(schema.relDataType, joinCondition, getExpressionString)}), " +
        s"join: (${joinSelectionToString(schema.relDataType)})"

    val flinkJoinType = joinType match {
      case JoinRelType.INNER => JoinType.INNER
      case JoinRelType.FULL => JoinType.FULL_OUTER
      case JoinRelType.LEFT => JoinType.LEFT_OUTER
      case JoinRelType.RIGHT => JoinType.RIGHT_OUTER
    }

    if (relativeWindowSize < 0) {
      LOG.warn(s"The relative window size $relativeWindowSize is negative," +
        " please check the join conditions.")
      createNegativeWindowSizeJoin(
        flinkJoinType,
        leftDataStream,
        rightDataStream,
        leftSchema.arity,
        rightSchema.arity,
        returnTypeInfo)
    } else {
      if (isRowTime) {
        createRowTimeJoin(
          flinkJoinType,
          leftDataStream,
          rightDataStream,
          returnTypeInfo,
          joinOpName,
          joinFunction.name,
          joinFunction.code,
          leftKeys,
          rightKeys
        )
      } else {
        createProcTimeJoin(
          flinkJoinType,
          leftDataStream,
          rightDataStream,
          returnTypeInfo,
          joinOpName,
          joinFunction.name,
          joinFunction.code,
          leftKeys,
          rightKeys
        )
      }
    }
  }

  def createNegativeWindowSizeJoin(
      joinType: JoinType,
      leftDataStream: DataStream[CRow],
      rightDataStream: DataStream[CRow],
      leftArity: Int,
      rightArity: Int,
      returnTypeInfo: TypeInformation[CRow]): DataStream[CRow] = {
    // We filter all records instead of adding an empty source to preserve the watermarks.
    val allFilter = new FlatMapFunction[CRow, CRow] with ResultTypeQueryable[CRow] {
      override def flatMap(value: CRow, out: Collector[CRow]): Unit = { }
      override def getProducedType: TypeInformation[CRow] = returnTypeInfo
    }

    val leftPadder = new MapFunction[CRow, CRow] with ResultTypeQueryable[CRow] {
      val paddingUtil = new OuterJoinPaddingUtil(leftArity, rightArity)
      override def map(value: CRow): CRow = new CRow(paddingUtil.padLeft(value.row), true)
      override def getProducedType: TypeInformation[CRow] = returnTypeInfo
    }

    val rightPadder = new MapFunction[CRow, CRow] with ResultTypeQueryable[CRow] {
      val paddingUtil = new OuterJoinPaddingUtil(leftArity, rightArity)
      override def map(value: CRow): CRow = new CRow(paddingUtil.padRight(value.row), true)
      override def getProducedType: TypeInformation[CRow] = returnTypeInfo
    }

    val leftP = leftDataStream.getParallelism
    val rightP = rightDataStream.getParallelism

    joinType match {
      case JoinType.INNER =>
        leftDataStream.flatMap(allFilter).name("Empty Inner Join").setParallelism(leftP)
          .union(rightDataStream.flatMap(allFilter).name("Empty Inner Join").setParallelism(rightP))
      case JoinType.LEFT_OUTER =>
        leftDataStream.map(leftPadder).name("Left Outer Join").setParallelism(leftP)
          .union(rightDataStream.flatMap(allFilter).name("Left Outer Join").setParallelism(rightP))
      case JoinType.RIGHT_OUTER =>
        leftDataStream.flatMap(allFilter).name("Right Outer Join").setParallelism(leftP)
          .union(rightDataStream.map(rightPadder).name("Right Outer Join").setParallelism(rightP))
      case JoinType.FULL_OUTER =>
        leftDataStream.map(leftPadder).name("Full Outer Join").setParallelism(leftP)
          .union(rightDataStream.map(rightPadder).name("Full Outer Join").setParallelism(rightP))
    }
  }

  def createProcTimeJoin(
      joinType: JoinType,
      leftDataStream: DataStream[CRow],
      rightDataStream: DataStream[CRow],
      returnTypeInfo: TypeInformation[CRow],
      operatorName: String,
      joinFunctionName: String,
      joinFunctionCode: String,
      leftKeys: Array[Int],
      rightKeys: Array[Int]): DataStream[CRow] = {

    val procJoinFunc = new ProcTimeBoundedStreamJoin(
      joinType,
      leftLowerBound,
      leftUpperBound,
      leftSchema.typeInfo,
      rightSchema.typeInfo,
      joinFunctionName,
      joinFunctionCode)

    if (!leftKeys.isEmpty) {
      leftDataStream.connect(rightDataStream)
        .keyBy(
          new CRowKeySelector(leftKeys, leftSchema.projectedTypeInfo(leftKeys)),
          new CRowKeySelector(rightKeys, rightSchema.projectedTypeInfo(rightKeys)))
        .process(procJoinFunc)
        .name(operatorName)
        .returns(returnTypeInfo)
    } else {
      leftDataStream.connect(rightDataStream)
        .keyBy(new NullByteKeySelector[CRow](), new NullByteKeySelector[CRow]())
        .process(procJoinFunc)
        .setParallelism(1)
        .setMaxParallelism(1)
        .name(operatorName)
        .returns(returnTypeInfo)
    }
  }

  def createRowTimeJoin(
      joinType: JoinType,
      leftDataStream: DataStream[CRow],
      rightDataStream: DataStream[CRow],
      returnTypeInfo: TypeInformation[CRow],
      operatorName: String,
      joinFunctionName: String,
      joinFunctionCode: String,
      leftKeys: Array[Int],
      rightKeys: Array[Int]): DataStream[CRow] = {

    val rowTimeJoinFunc = new RowTimeBoundedStreamJoin(
      joinType,
      leftLowerBound,
      leftUpperBound,
      allowedLateness = 0L,
      leftSchema.typeInfo,
      rightSchema.typeInfo,
      joinFunctionName,
      joinFunctionCode,
      leftTimeIdx,
      rightTimeIdx)

    if (!leftKeys.isEmpty) {
      leftDataStream
        .connect(rightDataStream)
        .keyBy(
          new CRowKeySelector(leftKeys, leftSchema.projectedTypeInfo(leftKeys)),
          new CRowKeySelector(rightKeys, rightSchema.projectedTypeInfo(rightKeys)))
        .transform(
          operatorName,
          returnTypeInfo,
          new KeyedCoProcessOperatorWithWatermarkDelay[Tuple, CRow, CRow, CRow](
            rowTimeJoinFunc,
            rowTimeJoinFunc.getMaxOutputDelay)
        )
    } else {
      leftDataStream.connect(rightDataStream)
        .keyBy(new NullByteKeySelector[CRow](), new NullByteKeySelector[CRow])
        .transform(
          operatorName,
          returnTypeInfo,
          new KeyedCoProcessOperatorWithWatermarkDelay[java.lang.Byte, CRow, CRow, CRow](
            rowTimeJoinFunc,
            rowTimeJoinFunc.getMaxOutputDelay)
        )
        .setParallelism(1)
        .setMaxParallelism(1)
    }
  }
}
