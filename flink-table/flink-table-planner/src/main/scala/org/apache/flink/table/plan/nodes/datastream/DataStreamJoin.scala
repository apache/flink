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
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, TableException}
import org.apache.flink.table.plan.nodes.CommonJoin
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

import scala.collection.JavaConversions._

/**
  * RelNode for a non-windowed stream join.
  */
class DataStreamJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    joinCondition: RexNode,
    joinInfo: JoinInfo,
    joinType: JoinRelType,
    leftSchema: RowSchema,
    rightSchema: RowSchema,
    schema: RowSchema,
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with CommonJoin
  with DataStreamRel {

  validatePythonFunctionInJoinCondition(joinCondition)

  override def deriveRowType(): RelDataType = schema.relDataType

  override def needsUpdatesAsRetraction: Boolean = true

  // outer join will generate retractions
  override def producesRetractions: Boolean = joinType != JoinRelType.INNER

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinCondition,
      joinInfo,
      joinType,
      leftSchema,
      rightSchema,
      schema,
      ruleDescription)
  }

  def getJoinInfo: JoinInfo = joinInfo

  def getJoinType: JoinRelType = joinType

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
      planner: StreamPlanner,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    validateKeyTypes()

    val leftDataStream =
      left.asInstanceOf[DataStreamRel].translateToPlan(planner, queryConfig)
    val rightDataStream =
      right.asInstanceOf[DataStreamRel].translateToPlan(planner, queryConfig)

    val connectOperator = leftDataStream.connect(rightDataStream)

    val joinTranslator = createTranslator(planner)

    val joinOpName = joinToString(getRowType, joinCondition, joinType, getExpressionString)
    val joinOperator = joinTranslator.getJoinOperator(
      joinType,
      schema.fieldNames,
      ruleDescription,
      queryConfig)
    connectOperator
      .keyBy(
        joinTranslator.getLeftKeySelector(),
        joinTranslator.getRightKeySelector())
      .transform(
        joinOpName,
        CRowTypeInfo(schema.typeInfo),
        joinOperator)
  }

  private def validateKeyTypes(): Unit = {
    // at least one equality expression
    val leftFields = left.getRowType.getFieldList
    val rightFields = right.getRowType.getFieldList

    joinInfo.pairs().toList.foreach(pair => {
      val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
      val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName
      // check if keys are compatible
      if (leftKeyType != rightKeyType) {
        throw new TableException(
          "Equality join predicate on incompatible types.\n" +
            s"\tLeft: $left,\n" +
            s"\tRight: $right,\n" +
            s"\tCondition: (${joinConditionToString(schema.relDataType,
              joinCondition, getExpressionString)})"
        )
      }
    })
  }

  protected def createTranslator(
      planner: StreamPlanner): DataStreamJoinToCoProcessTranslator = {
    new DataStreamJoinToCoProcessTranslator(
      planner.getConfig,
      schema.typeInfo,
      leftSchema,
      rightSchema,
      joinInfo,
      cluster.getRexBuilder)
  }
}
