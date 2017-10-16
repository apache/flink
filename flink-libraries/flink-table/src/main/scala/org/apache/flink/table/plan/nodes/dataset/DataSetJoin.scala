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

package org.apache.flink.table.plan.nodes.dataset

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.{BatchTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.FunctionCodeGenerator
import org.apache.flink.table.plan.nodes.CommonJoin
import org.apache.flink.table.runtime.FlatJoinRunner
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Flink RelNode which matches along with JoinOperator and its related operations.
  */
class DataSetJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    rowRelDataType: RelDataType,
    joinCondition: RexNode,
    joinRowType: RelDataType,
    joinInfo: JoinInfo,
    keyPairs: List[IntPair],
    joinType: JoinRelType,
    joinHint: JoinHint,
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with CommonJoin
  with DataSetRel {

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      getRowType,
      joinCondition,
      joinRowType,
      joinInfo,
      keyPairs,
      joinType,
      joinHint,
      ruleDescription)
  }

  override def toString: String = {
    joinToString(
      joinRowType,
      joinCondition,
      joinType,
      getExpressionString)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    joinExplainTerms(
      super.explainTerms(pw),
      joinRowType,
      joinCondition,
      joinType,
      getExpressionString)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val leftRowCnt = metadata.getRowCount(getLeft)
    val leftRowSize = estimateRowSize(getLeft.getRowType)

    val rightRowCnt = metadata.getRowCount(getRight)
    val rightRowSize = estimateRowSize(getRight.getRowType)

    val ioCost = (leftRowCnt * leftRowSize) + (rightRowCnt * rightRowSize)
    val cpuCost = leftRowCnt + rightRowCnt
    val rowCnt = leftRowCnt + rightRowCnt

    planner.getCostFactory.makeCost(rowCnt, cpuCost, ioCost)
  }

  override def translateToPlan(tableEnv: BatchTableEnvironment): DataSet[Row] = {

    val config = tableEnv.getConfig

    val returnType = FlinkTypeFactory.toInternalRowTypeInfo(getRowType)

    // get the equality keys
    val leftKeys = ArrayBuffer.empty[Int]
    val rightKeys = ArrayBuffer.empty[Int]
    if (keyPairs.isEmpty) {
      // if no equality keys => not supported
      throw TableException(
        "Joins should have at least one equality condition.\n" +
          s"\tLeft: ${left.toString},\n" +
          s"\tRight: ${right.toString},\n" +
          s"\tCondition: (${joinConditionToString(joinRowType,
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
              s"\tCondition: (${joinConditionToString(joinRowType,
                joinCondition, getExpressionString)})"
          )
        }
      })
    }

    val leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(tableEnv)
    val rightDataSet = right.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val (joinOperator, nullCheck) = joinType match {
      case JoinRelType.INNER => (leftDataSet.join(rightDataSet), false)
      case JoinRelType.LEFT => (leftDataSet.leftOuterJoin(rightDataSet), true)
      case JoinRelType.RIGHT => (leftDataSet.rightOuterJoin(rightDataSet), true)
      case JoinRelType.FULL => (leftDataSet.fullOuterJoin(rightDataSet), true)
    }

    if (nullCheck && !config.getNullCheck) {
      throw TableException("Null check in TableConfig must be enabled for outer joins.")
    }

    val generator = new FunctionCodeGenerator(
      config,
      nullCheck,
      leftDataSet.getType,
      Some(rightDataSet.getType))
    val conversion = generator.generateConverterResultExpression(
      returnType,
      joinRowType.getFieldNames)

    var body = ""

    if (joinInfo.isEqui) {
      // only equality condition
      body = s"""
           |${conversion.code}
           |${generator.collectorTerm}.collect(${conversion.resultTerm});
           |""".stripMargin
    }
    else {
      val nonEquiPredicates = joinInfo.getRemaining(this.cluster.getRexBuilder)
      val condition = generator.generateExpression(nonEquiPredicates)
      body = s"""
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

    val joinFun = new FlatJoinRunner[Row, Row, Row](
      genFunction.name,
      genFunction.code,
      genFunction.returnType)

    val joinOpName =
      s"where: (${joinConditionToString(joinRowType, joinCondition, getExpressionString)}), " +
        s"join: (${joinSelectionToString(joinRowType)})"

    joinOperator
      .where(leftKeys.toArray: _*)
      .equalTo(rightKeys.toArray: _*)
      .`with`(joinFun)
      .name(joinOpName)
  }
}
