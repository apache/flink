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
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedCollector, GeneratedExpression, GeneratedFunction}
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

  /**
    * Creates the [[CorrelateFlatMapRunner]] to execute the join of input table
    * and user-defined table function.
    */
  private[flink] def correlateMapFunction(
      config: TableConfig,
      inputTypeInfo: TypeInformation[Any],
      udtfTypeInfo: TypeInformation[Any],
      rowType: RelDataType,
      joinType: SemiJoinType,
      rexCall: RexCall,
      condition: Option[RexNode],
      expectedType: Option[TypeInformation[Any]],
      pojoFieldMapping: Option[Array[Int]], // udtf return type pojo field mapping
      ruleDescription: String)
    : CorrelateFlatMapRunner[Any, Any] = {

    val returnType = determineReturnType(
      rowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage)

    val flatMap = generateFunction(
      config,
      inputTypeInfo,
      udtfTypeInfo,
      returnType,
      rowType,
      joinType,
      rexCall,
      pojoFieldMapping,
      ruleDescription)

    val collector = generateCollector(
      config,
      inputTypeInfo,
      udtfTypeInfo,
      returnType,
      rowType,
      condition,
      pojoFieldMapping)

    new CorrelateFlatMapRunner[Any, Any](
      flatMap.name,
      flatMap.code,
      collector.name,
      collector.code,
      flatMap.returnType)

  }

  /**
    * Generates the flat map function to run the user-defined table function.
    */
  private def generateFunction(
      config: TableConfig,
      inputTypeInfo: TypeInformation[Any],
      udtfTypeInfo: TypeInformation[Any],
      returnType: TypeInformation[Any],
      rowType: RelDataType,
      joinType: SemiJoinType,
      rexCall: RexCall,
      pojoFieldMapping: Option[Array[Int]],
      ruleDescription: String)
    : GeneratedFunction[FlatMapFunction[Any, Any]] = {

    val functionGenerator = new CodeGenerator(
      config,
      false,
      inputTypeInfo,
      Some(udtfTypeInfo),
      None,
      pojoFieldMapping)

    val (input1AccessExprs, input2AccessExprs) = functionGenerator.generateCorrelateAccessExprs

    val collectorTerm = functionGenerator
      .addReusableConstructor(classOf[TableFunctionCollector[_]])
      .head

    val call = functionGenerator.generateExpression(rexCall)
    var body =
      s"""
        |${call.resultTerm}.setCollector($collectorTerm);
        |${call.code}
        |""".stripMargin

    if (joinType == SemiJoinType.LEFT) {
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
      val outerResultExpr = functionGenerator.generateResultExpression(
        input1AccessExprs ++ input2NullExprs, returnType, rowType.getFieldNames.asScala)
      body +=
        s"""
          |boolean hasOutput = $collectorTerm.isCollected();
          |if (!hasOutput) {
          |  ${outerResultExpr.code}
          |  ${functionGenerator.collectorTerm}.collect(${outerResultExpr.resultTerm});
          |}
          |""".stripMargin
    } else if (joinType != SemiJoinType.INNER) {
      throw TableException(s"Unsupported SemiJoinType: $joinType for correlate join.")
    }

    functionGenerator.generateFunction(
      ruleDescription,
      classOf[FlatMapFunction[Any, Any]],
      body,
      returnType)
  }

  /**
    * Generates table function collector.
    */
  private[flink] def generateCollector(
      config: TableConfig,
      inputTypeInfo: TypeInformation[Any],
      udtfTypeInfo: TypeInformation[Any],
      returnType: TypeInformation[Any],
      rowType: RelDataType,
      condition: Option[RexNode],
      pojoFieldMapping: Option[Array[Int]])
    : GeneratedCollector = {

    val generator = new CodeGenerator(
      config,
      false,
      inputTypeInfo,
      Some(udtfTypeInfo),
      None,
      pojoFieldMapping)

    val (input1AccessExprs, input2AccessExprs) = generator.generateCorrelateAccessExprs

    val crossResultExpr = generator.generateResultExpression(
      input1AccessExprs ++ input2AccessExprs,
      returnType,
      rowType.getFieldNames.asScala)

    val collectorCode = if (condition.isEmpty) {
      s"""
        |${crossResultExpr.code}
        |getCollector().collect(${crossResultExpr.resultTerm});
        |""".stripMargin
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

    generator.generateTableFunctionCollector(
      "TableFunctionCollector",
      collectorCode,
      udtfTypeInfo)
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
