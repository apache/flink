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

package org.apache.flink.api.table.plan.nodes.dataset

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.runtime.FlatJoinRunner
import org.apache.flink.api.table.typeutils.TypeConverter.determineReturnType
import org.apache.flink.api.table.{BatchTableEnvironment, TableException}
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Flink RelNode which matches along with JoinOperator and its related operations.
  */
class DataSetJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    rowType: RelDataType,
    joinCondition: RexNode,
    joinRowType: RelDataType,
    joinInfo: JoinInfo,
    keyPairs: List[IntPair],
    joinType: JoinRelType,
    joinHint: JoinHint,
    ruleDescription: String)
  extends BiRel(cluster, traitSet, left, right)
  with DataSetRel {

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      rowType,
      joinCondition,
      joinRowType,
      joinInfo,
      keyPairs,
      joinType,
      joinHint,
      ruleDescription)
  }

  override def toString: String = {
    s"$joinTypeToString(where: ($joinConditionToString), join: ($joinSelectionToString))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("where", joinConditionToString)
      .item("join", joinSelectionToString)
      .item("joinType", joinTypeToString)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val children = this.getInputs
    children.foldLeft(planner.getCostFactory.makeCost(0, 0, 0)) { (cost, child) =>
      val rowCnt = metadata.getRowCount(child)
      val rowSize = this.estimateRowSize(child.getRowType)
      cost.plus(planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize))
    }
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val config = tableEnv.getConfig

    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage)

    // get the equality keys
    val leftKeys = ArrayBuffer.empty[Int]
    val rightKeys = ArrayBuffer.empty[Int]
    if (keyPairs.isEmpty) {
      // if no equality keys => not supported
      throw new TableException(
        "Joins should have at least one equality condition.\n" +
          s"\tLeft: ${left.toString},\n" +
          s"\tRight: ${right.toString},\n" +
          s"\tCondition: ($joinConditionToString)"
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
          throw new TableException(
            "Equality join predicate on incompatible types.\n" +
              s"\tLeft: ${left.toString},\n" +
              s"\tRight: ${right.toString},\n" +
              s"\tCondition: ($joinConditionToString)"
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
      throw new TableException("Null check in TableConfig must be enabled for outer joins.")
    }

    val generator = new CodeGenerator(
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
      val condition = generator.generateExpression(joinCondition)
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
      classOf[FlatJoinFunction[Any, Any, Any]],
      body,
      returnType)

    val joinFun = new FlatJoinRunner[Any, Any, Any](
      genFunction.name,
      genFunction.code,
      genFunction.returnType)

    val joinOpName = s"where: ($joinConditionToString), join: ($joinSelectionToString)"

    joinOperator.where(leftKeys.toArray: _*).equalTo(rightKeys.toArray: _*)
      .`with`(joinFun).name(joinOpName).asInstanceOf[DataSet[Any]]
  }

  private def joinSelectionToString: String = {
    rowType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString: String = {

    val inFields = joinRowType.getFieldNames.asScala.toList
    getExpressionString(joinCondition, inFields, None)
  }

  private def joinTypeToString = joinType match {
    case JoinRelType.INNER => "Join"
    case JoinRelType.LEFT=> "LeftOuterJoin"
    case JoinRelType.RIGHT => "RightOuterJoin"
    case JoinRelType.FULL => "FullOuterJoin"
  }

}
