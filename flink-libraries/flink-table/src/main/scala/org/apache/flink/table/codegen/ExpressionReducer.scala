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

package org.apache.flink.table.codegen

import java.util

import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

/**
  * Evaluates constant expressions using Flink's [[FunctionCodeGenerator]].
  */
class ExpressionReducer(config: TableConfig)
  extends RelOptPlanner.Executor with Compiler[MapFunction[Row, Row]] {

  private val EMPTY_ROW_INFO = new RowTypeInfo()
  private val EMPTY_ROW = new Row(0)

  override def reduce(
    rexBuilder: RexBuilder,
    constExprs: util.List[RexNode],
    reducedValues: util.List[RexNode]): Unit = {

    val typeFactory = rexBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    val literals = constExprs.asScala.map(e => (e.getType.getSqlTypeName, e)).flatMap {

      // we need to cast here for RexBuilder.makeLiteral
      case (SqlTypeName.DATE, e) =>
        Some(
          rexBuilder.makeCast(
            typeFactory.createTypeFromTypeInfo(BasicTypeInfo.INT_TYPE_INFO, e.getType.isNullable),
            e)
        )
      case (SqlTypeName.TIME, e) =>
        Some(
          rexBuilder.makeCast(
            typeFactory.createTypeFromTypeInfo(BasicTypeInfo.INT_TYPE_INFO, e.getType.isNullable),
            e)
        )
      case (SqlTypeName.TIMESTAMP, e) =>
        Some(
          rexBuilder.makeCast(
            typeFactory.createTypeFromTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, e.getType.isNullable),
            e)
        )

      // we don't support object literals yet, we skip those constant expressions
      case (SqlTypeName.ANY, _) |
           (SqlTypeName.ROW, _) |
           (SqlTypeName.ARRAY, _) |
           (SqlTypeName.MAP, _) |
           (SqlTypeName.MULTISET, _) => None

      case (_, e) => Some(e)
    }

    val literalTypes = literals.map(e => FlinkTypeFactory.toTypeInfo(e.getType))
    val resultType = new RowTypeInfo(literalTypes: _*)

    // generate MapFunction
    val generator = new FunctionCodeGenerator(config, false, EMPTY_ROW_INFO)

    val result = generator.generateResultExpression(
      resultType,
      resultType.getFieldNames,
      literals)

    val generatedFunction = generator.generateFunction[MapFunction[Row, Row], Row](
      "ExpressionReducer",
      classOf[MapFunction[Row, Row]],
      s"""
        |${result.code}
        |return ${result.resultTerm};
        |""".stripMargin,
      resultType)

    val clazz = compile(
      Thread.currentThread().getContextClassLoader,
      generatedFunction.name,
      generatedFunction.code)
    val function = clazz.newInstance()

    // execute
    val reduced = function.map(EMPTY_ROW)

    // add the reduced results or keep them unreduced
    var i = 0
    var reducedIdx = 0
    while (i < constExprs.size()) {
      val unreduced = constExprs.get(i)
      unreduced.getType.getSqlTypeName match {
        // we insert the original expression for object literals
        case SqlTypeName.ANY |
             SqlTypeName.ROW |
             SqlTypeName.ARRAY |
             SqlTypeName.MAP |
             SqlTypeName.MULTISET =>
          reducedValues.add(unreduced)

        case _ =>
          val reducedValue = reduced.getField(reducedIdx)
          // RexBuilder handle double literal incorrectly, convert it into BigDecimal manually
          val value = if (unreduced.getType.getSqlTypeName == SqlTypeName.DOUBLE) {
            new java.math.BigDecimal(reducedValue.asInstanceOf[Number].doubleValue())
          } else {
            reducedValue
          }

          val literal = rexBuilder.makeLiteral(
            value,
            unreduced.getType,
            true)
          reducedValues.add(literal)
          reducedIdx += 1
      }
      i += 1
    }
  }
}
