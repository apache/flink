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
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.codegen.FunctionCodeGenerator
import org.apache.flink.table.plan.nodes.CommonJoin
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.join.DataStreamInnerJoin
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

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

  override def deriveRowType(): RelDataType = schema.relDataType

  override def needsUpdatesAsRetraction: Boolean = true

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
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val config = tableEnv.getConfig
    val returnType = schema.typeInfo
    val keyPairs = joinInfo.pairs().toList

    // get the equality keys
    val leftKeys = ArrayBuffer.empty[Int]
    val rightKeys = ArrayBuffer.empty[Int]
    if (keyPairs.isEmpty) {
      // if no equality keys => not supported
      throw TableException(
        "Joins should have at least one equality condition.\n" +
          s"\tLeft: ${left.toString},\n" +
          s"\tRight: ${right.toString},\n" +
          s"\tCondition: (${joinConditionToString(schema.relDataType,
             joinCondition, getExpressionString)})"
      )
    }
    else {
      // at least one equality expression
      val leftFields = left.getRowType.getFieldList
      val rightFields = right.getRowType.getFieldList

      keyPairs.foreach(pair => {
        val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
        val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName

        // check if keys are compatible
        if (leftKeyType == rightKeyType) {
          // add key pair
          leftKeys.add(pair.source)
          rightKeys.add(pair.target)
        } else {
          throw TableException(
            "Equality join predicate on incompatible types.\n" +
              s"\tLeft: ${left.toString},\n" +
              s"\tRight: ${right.toString},\n" +
              s"\tCondition: (${joinConditionToString(schema.relDataType,
                joinCondition, getExpressionString)})"
          )
        }
      })
    }

    val leftDataStream =
      left.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)
    val rightDataStream =
      right.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)

    val (connectOperator, nullCheck) = joinType match {
      case JoinRelType.INNER => (leftDataStream.connect(rightDataStream), false)
      case _ => throw new UnsupportedOperationException(s"An Unsupported JoinType [ $joinType ]")
    }

    val generator = new FunctionCodeGenerator(
      config,
      nullCheck,
      leftSchema.typeInfo,
      Some(rightSchema.typeInfo))
    val conversion = generator.generateConverterResultExpression(
      schema.typeInfo,
      schema.fieldNames)


    val body = if (joinInfo.isEqui) {
      // only equality condition
      s"""
         |${conversion.code}
         |${generator.collectorTerm}.collect(${conversion.resultTerm});
         |""".stripMargin
    } else {
      val nonEquiPredicates = joinInfo.getRemaining(this.cluster.getRexBuilder)
      val condition = generator.generateExpression(nonEquiPredicates)
      s"""
         |${condition.code}
         |if (${condition.resultTerm}) {
         |  ${conversion.code}
         |  ${generator.collectorTerm}.collect(${conversion.resultTerm});
         |}
         |""".stripMargin
    }

    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatJoinFunction[Row, Row, Row]],
      body,
      returnType)


    val joinOpName = joinToString(getRowType, joinCondition, joinType, getExpressionString)

    val coMapFun =
      new DataStreamInnerJoin(
        leftSchema.typeInfo,
        rightSchema.typeInfo,
        CRowTypeInfo(returnType),
        genFunction.name,
        genFunction.code,
        queryConfig)

    connectOperator
      .keyBy(leftKeys.toArray, rightKeys.toArray)
      .process(coMapFun)
      .name(joinOpName)
      .returns(CRowTypeInfo(returnType))
  }
}
