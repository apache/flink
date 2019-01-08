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

import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator
import org.apache.flink.table.api.{StreamQueryConfig, TableConfig}
import org.apache.flink.table.codegen.{FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.CRowKeySelector
import org.apache.flink.table.runtime.join._
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row

class DataStreamJoinToCoProcessTranslator(
    config: TableConfig,
    returnType: TypeInformation[Row],
    leftSchema: RowSchema,
    rightSchema: RowSchema,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder) {

  val nonEquiJoinPredicates: Option[RexNode] = if (joinInfo.isEqui) {
    None
  }
  else {
    Some(joinInfo.getRemaining(rexBuilder))
  }

  def getLeftKeySelector(): CRowKeySelector = {
    new CRowKeySelector(
      joinInfo.leftKeys.toIntArray,
      leftSchema.projectedTypeInfo(joinInfo.leftKeys.toIntArray))
  }

  def getRightKeySelector(): CRowKeySelector = {
    new CRowKeySelector(
      joinInfo.rightKeys.toIntArray,
      rightSchema.projectedTypeInfo(joinInfo.rightKeys.toIntArray))
  }

  def getJoinOperator(
      joinType: JoinRelType,
      returnFieldNames: Seq[String],
      ruleDescription: String,
      queryConfig: StreamQueryConfig): TwoInputStreamOperator[CRow, CRow, CRow] = {
    // input must not be nullable, because the runtime join function will make sure
    // the code-generated function won't process null inputs
    val generator = new FunctionCodeGenerator(
      config,
      nullableInput = false,
      leftSchema.typeInfo,
      Some(rightSchema.typeInfo))
    val conversion = generator.generateConverterResultExpression(
      returnType,
      returnFieldNames)

    val body = if (nonEquiJoinPredicates.isEmpty) {
      // only equality condition
      s"""
         |${conversion.code}
         |${generator.collectorTerm}.collect(${conversion.resultTerm});
         |""".stripMargin
    } else {
      val condition = generator.generateExpression(nonEquiJoinPredicates.get)
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

    createJoinOperator(joinType, queryConfig, genFunction)
  }

  protected def createJoinOperator(
    joinType: JoinRelType,
    queryConfig: StreamQueryConfig,
    genFunction: GeneratedFunction[FlatJoinFunction[Row, Row, Row], Row])
    : TwoInputStreamOperator[CRow, CRow, CRow] = {

    val joinFunction = joinType match {
      case JoinRelType.INNER =>
        new NonWindowInnerJoin(
          leftSchema.typeInfo,
          rightSchema.typeInfo,
          genFunction.name,
          genFunction.code,
          queryConfig)
      case JoinRelType.LEFT | JoinRelType.RIGHT if joinInfo.isEqui =>
        new NonWindowLeftRightJoin(
          leftSchema.typeInfo,
          rightSchema.typeInfo,
          genFunction.name,
          genFunction.code,
          joinType == JoinRelType.LEFT,
          queryConfig)
      case JoinRelType.LEFT | JoinRelType.RIGHT =>
        new NonWindowLeftRightJoinWithNonEquiPredicates(
          leftSchema.typeInfo,
          rightSchema.typeInfo,
          genFunction.name,
          genFunction.code,
          joinType == JoinRelType.LEFT,
          queryConfig)
      case JoinRelType.FULL if joinInfo.isEqui =>
        new NonWindowFullJoin(
          leftSchema.typeInfo,
          rightSchema.typeInfo,
          genFunction.name,
          genFunction.code,
          queryConfig)
      case JoinRelType.FULL =>
        new NonWindowFullJoinWithNonEquiPredicates(
          leftSchema.typeInfo,
          rightSchema.typeInfo,
          genFunction.name,
          genFunction.code,
          queryConfig)
    }
    new KeyedCoProcessOperator(joinFunction)
  }
}
