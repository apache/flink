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

import org.apache.calcite.plan.{RelTraitSet, RelOptCluster}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinInfo
import org.apache.calcite.rel.{RelWriter, BiRel, RelNode}
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.runtime.FlatJoinRunner
import org.apache.flink.api.table.{TableException, TableConfig}
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.table.plan.TypeConverter._
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import org.apache.calcite.rex.RexNode

/**
  * Flink RelNode which matches along with JoinOperator and its related operations.
  */
class DataSetJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    rowType: RelDataType,
    opName: String,
    joinCondition: RexNode,
    joinRowType: RelDataType,
    joinInfo: JoinInfo,
    keyPairs: List[IntPair],
    joinType: JoinType,
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
      opName,
      joinCondition,
      joinRowType,
      joinInfo,
      keyPairs,
      joinType,
      joinHint,
      ruleDescription)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("name", opName)
  }

  override def translateToPlan(
      config: TableConfig,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(config)
    val rightDataSet = right.asInstanceOf[DataSetRel].translateToPlan(config)

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
      throw new TableException("Joins should have at least one equality condition")
    }
    else {
      // at least one equality expression => generate a join function
      keyPairs.foreach(pair => {
        leftKeys.add(pair.source)
        rightKeys.add(pair.target)
      })
    }

    val generator = new CodeGenerator(config, leftDataSet.getType, Some(rightDataSet.getType))
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

    leftDataSet.join(rightDataSet).where(leftKeys.toArray: _*).equalTo(rightKeys.toArray: _*)
      .`with`(joinFun).asInstanceOf[DataSet[Any]]
  }

}
