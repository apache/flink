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
import org.apache.flink.api.common.functions.CrossFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.runtime.CrossJoinRunner
import org.apache.flink.api.table.typeutils.TypeConverter.determineReturnType
import org.apache.flink.api.table.{BatchTableEnvironment, TableConfig}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with CrossOperator.
  */
class DataSetSingleRowCross(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    multiRowNode: RelNode,
    singleRowNode: RelNode,
    rowRelDataType: RelDataType,
    joinRowType: RelDataType,
    ruleDescription: String)
  extends BiRel(cluster, traitSet, multiRowNode, singleRowNode)
  with DataSetRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetSingleRowCross(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      getRowType,
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
    val children = this.getInputs
    children.foldLeft(planner.getCostFactory.makeZeroCost()) { (cost, child) =>
      val rowCnt = metadata.getRowCount(child)
      val rowSize = this.estimateRowSize(child.getRowType)
      cost.plus(planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize))
    }
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val multiRowDataSet = left.asInstanceOf[DataSetRel].translateToPlan(tableEnv)
    val singleRowDataSet = right.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val crossFunction = generateCrossFunction(
      tableEnv.getConfig,
      multiRowDataSet.getType,
      singleRowDataSet.getType,
      expectedType)

    multiRowDataSet
      .cross(singleRowDataSet)
      .`with`(crossFunction)
      .name(getCrossOperatorName)
      .asInstanceOf[DataSet[Any]]
  }

  private def generateCrossFunction(
      config: TableConfig,
      leftInputType: TypeInformation[Any],
      rightInputType: TypeInformation[Any],
      expectedType: Option[TypeInformation[Any]]): CrossFunction[Any, Any, Any] = {

    val codeGenerator = new CodeGenerator(
      config,
      false,
      leftInputType,
      Some(rightInputType))

    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage)

    val conversion = codeGenerator.generateConverterResultExpression(
      returnType,
      joinRowType.getFieldNames)

    val bodyCode = s"""
                  |${conversion.code}
                  |return ${conversion.resultTerm};
                  |""".stripMargin

    val genFunction = codeGenerator.generateFunction(
      ruleDescription,
      classOf[CrossFunction[Any, Any, Any]],
      bodyCode,
      returnType)

    new CrossJoinRunner[Any, Any, Any](
      genFunction.name,
      genFunction.code,
      genFunction.returnType)
  }

  private def getCrossOperatorName: String = {
    s"where: ($joinConditionToString), join: ($joinSelectionToString)"
  }

  private def joinSelectionToString: String = {
    getRowType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString: String = {
    "true"
  }

  private def joinTypeToString: String = {
    "CrossJoin"
  }

}
