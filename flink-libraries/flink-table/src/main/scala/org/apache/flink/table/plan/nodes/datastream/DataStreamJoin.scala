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
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.CommonJoin
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.join.{JoinUtil, ProcTimeInnerJoin}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

/**
  * Flink RelNode which matches along with JoinOperator and its related operations.
  */
class DataStreamJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    joinCondition: RexNode,
    joinType: JoinRelType,
    leftSchema: RowSchema,
    rightSchema: RowSchema,
    schema: RowSchema,
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with CommonJoin
  with DataStreamRel {

  override def deriveRowType() = schema.logicalType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinCondition,
      joinType,
      leftSchema,
      rightSchema,
      schema,
      ruleDescription)
  }

  override def toString: String = {

    s"${joinTypeToString(joinType)}" +
      s"(condition: (${joinConditionToString(schema.logicalType,
        joinCondition, getExpressionString)}), " +
      s"select: (${joinSelectionToString(schema.logicalType)}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("condition", joinConditionToString(schema.logicalType,
        joinCondition, getExpressionString))
      .item("select", joinSelectionToString(schema.logicalType))
      .item("joinType", joinTypeToString(joinType))
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[CRow] = {

    val config = tableEnv.getConfig

    // get the equality keys and other condition
    val joinInfo = JoinInfo.of(leftNode, rightNode, joinCondition)
    val leftKeys = joinInfo.leftKeys.toIntArray
    val rightKeys = joinInfo.rightKeys.toIntArray
    val otherCondition = joinInfo.getRemaining(cluster.getRexBuilder)

    if (left.isInstanceOf[StreamTableSourceScan]
        || right.isInstanceOf[StreamTableSourceScan]) {
      throw new TableException(
        "Join between stream and table is not supported yet.")
    }
    // analyze time boundary and time predicate type(proctime/rowtime)
    val (timeType, leftStreamWindowSize, rightStreamWindowSize, conditionWithoutTime) =
      JoinUtil.analyzeTimeBoundary(
        otherCondition,
        leftSchema.logicalType.getFieldCount,
        leftSchema.physicalType.getFieldCount,
        schema.logicalType,
        cluster.getRexBuilder,
        config)

    val leftDataStream = left.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)
    val rightDataStream = right.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)

    // generate join function
    val joinFunction =
      JoinUtil.generateJoinFunction(
        config,
        joinType,
        leftSchema.physicalTypeInfo,
        rightSchema.physicalTypeInfo,
        schema,
        conditionWithoutTime,
        ruleDescription)

    joinType match {
      case JoinRelType.INNER =>
        timeType match {
          case _ if FlinkTypeFactory.isProctimeIndicatorType(timeType) =>
            // Proctime JoinCoProcessFunction
            createProcTimeInnerJoinFunction(
              leftStreamWindowSize,
              rightStreamWindowSize,
              leftDataStream,
              rightDataStream,
              joinFunction.name,
              joinFunction.code,
              leftKeys,
              rightKeys
            )
          case _ if FlinkTypeFactory.isRowtimeIndicatorType(timeType) =>
            // RowTime JoinCoProcessFunction
            throw new TableException(
              "RowTime inner join between stream and stream is not supported yet.")
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
      leftStreamWindowSize: Long,
      rightStreamWindowSize: Long,
      leftDataStream: DataStream[CRow],
      rightDataStream: DataStream[CRow],
      joinFunctionName: String,
      joinFunctionCode: String,
      leftKeys: Array[Int],
      rightKeys: Array[Int]): DataStream[CRow] = {

    val returnTypeInfo = CRowTypeInfo(schema.physicalTypeInfo)

    val procInnerJoinFunc = new ProcTimeInnerJoin(
      leftStreamWindowSize,
      rightStreamWindowSize,
      leftSchema.physicalTypeInfo,
      rightSchema.physicalTypeInfo,
      joinFunctionName,
      joinFunctionCode)

    leftDataStream.connect(rightDataStream)
      .keyBy(leftKeys, rightKeys)
      .process(procInnerJoinFunc)
      .returns(returnTypeInfo)
  }
}
