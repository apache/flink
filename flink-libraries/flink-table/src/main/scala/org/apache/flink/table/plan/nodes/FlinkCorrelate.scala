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
package org.apache.flink.table.plan.nodes

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.calcite.sql.SemiJoinType
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression, GeneratedFunction}
import org.apache.flink.table.codegen.CodeGenUtils.primitiveDefaultValue
import org.apache.flink.table.codegen.GeneratedExpression.{ALWAYS_NULL, NO_CODE}
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.runtime.{CorrelateFlatMapRunner, TableFunctionCollector}
import org.apache.flink.table.typeutils.TypeConverter._
import org.apache.flink.table.api.{TableConfig, TableException}

import scala.collection.JavaConverters._

/**
  * Join a user-defined table function
  */
trait FlinkCorrelate {

  private[flink] def generateFunction(
      generator: CodeGenerator,
      udtfTypeInfo: TypeInformation[Any],
      rowType: RelDataType,
      rexCall: RexCall,
      condition: Option[RexNode],
      config: TableConfig,
      joinType: SemiJoinType,
      expectedType: Option[TypeInformation[Any]],
      ruleDescription: String)
  : (GeneratedFunction[FlatMapFunction[Any, Any]],
    GeneratedFunction[TableFunctionCollector[Any]]) = {

    val returnType = determineReturnType(
      rowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage)

    val (input1AccessExprs, input2AccessExprs) = generator.generateCorrelateAccessExprs

    val collectorTerm = generator.addReusableConstructor(classOf[TableFunctionCollector[_]]).head

    val call = generator.generateExpression(rexCall)
    var body =
      s"""
         |${call.resultTerm}.setCollector($collectorTerm);
         |${call.code}
         |boolean hasOutput = $collectorTerm.isCollected();
       """.stripMargin

    if (joinType == SemiJoinType.INNER) {
      // cross join
      body +=
        s"""
           |if (!hasOutput) {
           |  return;
           |}
        """.stripMargin
    } else if (joinType == SemiJoinType.LEFT) {
      // left outer join

      // in case of left outer join and the returned row of table function is empty,
      // fill all fields of row with null
      val input2NullExprs = input2AccessExprs.map { x =>
        GeneratedExpression(
          primitiveDefaultValue(x.resultType),
          ALWAYS_NULL,
          NO_CODE,
          x.resultType)
      }
      val outerResultExpr = generator.generateResultExpression(
        input1AccessExprs ++ input2NullExprs, returnType, rowType.getFieldNames.asScala)
      body +=
        s"""
           |if (!hasOutput) {
           |  ${outerResultExpr.code}
           |  ${generator.collectorTerm}.collect(${outerResultExpr.resultTerm});
           |  return;
           |}
        """.stripMargin
    } else {
      throw TableException(s"Unsupported SemiJoinType: $joinType for correlate join.")
    }

    val flatMapFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatMapFunction[Any, Any]],
      body,
      returnType)

    // -------------------------- generate table function collector -----------------------

    val crossResultExpr = generator.generateResultExpression(
      input1AccessExprs ++ input2AccessExprs,
      returnType,
      rowType.getFieldNames.asScala)

    val collectorCode = if (condition.isEmpty) {
      s"""
         |${crossResultExpr.code}
         |getCollector().collect(${crossResultExpr.resultTerm});
       """.stripMargin
    } else {
      val filterGenerator = new CodeGenerator(config, false, udtfTypeInfo)
      filterGenerator.input1Term = filterGenerator.input2Term
      val filterCondition = filterGenerator.generateExpression(condition.get)
      s"""
         |${filterGenerator.reuseInputUnboxingCode()}
         |${filterCondition.code}
         |if (${filterCondition.resultTerm}) {
         |  ${crossResultExpr.code}
         |  getCollector().collect(${crossResultExpr.resultTerm});
         |}
         |""".stripMargin
    }

    val collectorFunction = generator.generateTableFunctionCollector(
      "TableFunctionCollector",
      collectorCode,
      udtfTypeInfo)

    (flatMapFunction, collectorFunction)
  }

  private[flink] def correlateMapFunction(
      flatMap: GeneratedFunction[FlatMapFunction[Any, Any]],
      collector: GeneratedFunction[TableFunctionCollector[Any]])
    : CorrelateFlatMapRunner[Any, Any] = {

    new CorrelateFlatMapRunner[Any, Any](
      flatMap.name,
      flatMap.code,
      collector.name,
      collector.code,
      flatMap.returnType)
  }

  private[flink] def selectToString(rowType: RelDataType): String = {
    rowType.getFieldNames.asScala.mkString(",")
  }

  private[flink] def correlateOpName(
      rexCall: RexCall,
      sqlFunction: TableSqlFunction,
      rowType: RelDataType)
    : String = {

    s"correlate: ${correlateToString(rexCall, sqlFunction)}, select: ${selectToString(rowType)}"
  }

  private[flink] def correlateToString(rexCall: RexCall, sqlFunction: TableSqlFunction): String = {
    val udtfName = sqlFunction.getName
    val operands = rexCall.getOperands.asScala.map(_.toString).mkString(",")
    s"table($udtfName($operands))"
  }

}
