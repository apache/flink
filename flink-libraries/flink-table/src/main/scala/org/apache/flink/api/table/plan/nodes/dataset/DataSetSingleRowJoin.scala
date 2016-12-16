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
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.functions.{FlatJoinFunction, FlatMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.runtime.{MapJoinLeftRunner, MapJoinRightRunner}
import org.apache.flink.api.table.typeutils.TypeConverter.determineReturnType
import org.apache.flink.api.table.{BatchTableEnvironment, TableConfig}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Flink RelNode that executes a Join where one of inputs is a single row.
  */
class DataSetSingleRowJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    leftIsSingle: Boolean,
    rowRelDataType: RelDataType,
    joinCondition: RexNode,
    joinRowType: RelDataType,
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with DataSetRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetSingleRowJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      leftIsSingle,
      getRowType,
      joinCondition,
      joinRowType,
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
    val child = if (leftIsSingle) {
      this.getRight
    } else {
      this.getLeft
    }
    val rowCnt = metadata.getRowCount(child)
    val rowSize = this.estimateRowSize(child.getRowType)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(tableEnv)
    val rightDataSet = right.asInstanceOf[DataSetRel].translateToPlan(tableEnv)
    val broadcastSetName = "joinSet"
    val mapSideJoin = generateMapFunction(
      tableEnv.getConfig,
      leftDataSet.getType,
      rightDataSet.getType,
      leftIsSingle,
      joinCondition,
      broadcastSetName,
      expectedType)

    val (multiRowDataSet, singleRowDataSet) =
      if (leftIsSingle) {
        (rightDataSet, leftDataSet)
      } else {
        (leftDataSet, rightDataSet)
      }

    multiRowDataSet
      .flatMap(mapSideJoin)
      .withBroadcastSet(singleRowDataSet, broadcastSetName)
      .name(getMapOperatorName)
      .asInstanceOf[DataSet[Any]]
  }

  private def generateMapFunction(
      config: TableConfig,
      inputType1: TypeInformation[Any],
      inputType2: TypeInformation[Any],
      firstIsSingle: Boolean,
      joinCondition: RexNode,
      broadcastInputSetName: String,
      expectedType: Option[TypeInformation[Any]]): FlatMapFunction[Any, Any] = {

    val codeGenerator = new CodeGenerator(
      config,
      false,
      inputType1,
      Some(inputType2))

    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage)

    val conversion = codeGenerator.generateConverterResultExpression(
      returnType,
      joinRowType.getFieldNames)

    val condition = codeGenerator.generateExpression(joinCondition)

    val joinMethodBody = s"""
                  |${condition.code}
                  |if (${condition.resultTerm}) {
                  |  ${conversion.code}
                  |  ${codeGenerator.collectorTerm}.collect(${conversion.resultTerm});
                  |}
                  |""".stripMargin

    val genFunction = codeGenerator.generateFunction(
      ruleDescription,
      classOf[FlatJoinFunction[Any, Any, Any]],
      joinMethodBody,
      returnType)

    if (firstIsSingle) {
      new MapJoinRightRunner[Any, Any, Any](
        genFunction.name,
        genFunction.code,
        genFunction.returnType,
        broadcastInputSetName)
    } else {
      new MapJoinLeftRunner[Any, Any, Any](
        genFunction.name,
        genFunction.code,
        genFunction.returnType,
        broadcastInputSetName)
    }
  }

  private def getMapOperatorName: String = {
    s"where: ($joinConditionToString), join: ($joinSelectionToString)"
  }

  private def joinSelectionToString: String = {
    getRowType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString: String = {
    val inFields = joinRowType.getFieldNames.asScala.toList
    getExpressionString(joinCondition, inFields, None)
  }

  private def joinTypeToString: String = {
    "NestedLoopJoin"
  }

}
