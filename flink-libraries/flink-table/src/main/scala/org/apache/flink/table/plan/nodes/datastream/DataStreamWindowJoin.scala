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
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.plan.nodes.CommonJoin
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.table.runtime.join.{ProcTimeWindowInnerJoin, WindowJoinUtil}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

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
    remainCondition: Option[RexNode],
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
    with CommonJoin
    with DataStreamRel {

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

    // get the equality keys and other condition
    val joinInfo = JoinInfo.of(leftNode, rightNode, joinCondition)
    val leftKeys = joinInfo.leftKeys.toIntArray
    val rightKeys = joinInfo.rightKeys.toIntArray

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

    joinType match {
      case JoinRelType.INNER =>
        if (isRowTime) {
          // RowTime JoinCoProcessFunction
          throw new TableException(
            "RowTime inner join between stream and stream is not supported yet.")
        } else {
          // Proctime JoinCoProcessFunction
          createProcTimeInnerJoinFunction(
            leftDataStream,
            rightDataStream,
            joinFunction.name,
            joinFunction.code,
            leftKeys,
            rightKeys
          )
        }
      case JoinRelType.FULL =>
        throw new TableException(
          "Full join between stream and stream is not supported yet.")
      case JoinRelType.LEFT =>
        throw new TableException(
          "Left join between stream and stream is not supported yet.")
      case JoinRelType.RIGHT =>
        throw new TableException(
          "Right join between stream and stream is not supported yet.")
    }
  }

  def createProcTimeInnerJoinFunction(
      leftDataStream: DataStream[CRow],
      rightDataStream: DataStream[CRow],
      joinFunctionName: String,
      joinFunctionCode: String,
      leftKeys: Array[Int],
      rightKeys: Array[Int]): DataStream[CRow] = {

    val returnTypeInfo = CRowTypeInfo(schema.typeInfo)

    val procInnerJoinFunc = new ProcTimeWindowInnerJoin(
      leftLowerBound,
      leftUpperBound,
      leftSchema.typeInfo,
      rightSchema.typeInfo,
      joinFunctionName,
      joinFunctionCode)

    if (!leftKeys.isEmpty) {
      leftDataStream.connect(rightDataStream)
        .keyBy(leftKeys, rightKeys)
        .process(procInnerJoinFunc)
        .returns(returnTypeInfo)
    } else {
      leftDataStream.connect(rightDataStream)
        .keyBy(new NullByteKeySelector[CRow](), new NullByteKeySelector[CRow]())
        .process(procInnerJoinFunc)
        .setParallelism(1)
        .setMaxParallelism(1)
        .returns(returnTypeInfo)
    }
  }
}
