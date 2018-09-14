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
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexShuttle}
import org.apache.calcite.sql.SemiJoinType
import org.apache.flink.api.common.functions.Function
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.codegen.CodeGenUtils.primitiveDefaultValue
import org.apache.flink.table.codegen.GeneratedExpression.{ALWAYS_NULL, NO_CODE}
import org.apache.flink.table.codegen._
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.TableFunctionCollector
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

/**
  * Join a user-defined table function
  */
trait CommonCorrelate {

  /**
    * Generates the flat map function to run the user-defined table function.
    */
  private[flink] def generateFunction[T <: Function](
    config: TableConfig,
    inputSchema: RowSchema,
    udtfTypeInfo: TypeInformation[Any],
    returnSchema: RowSchema,
    joinType: SemiJoinType,
    rexCall: RexCall,
    pojoFieldMapping: Option[Array[Int]],
    ruleDescription: String,
    functionClass: Class[T]):
  GeneratedFunction[T, Row] = {

    val functionGenerator = new FunctionCodeGenerator(
      config,
      false,
      inputSchema.typeInfo,
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
        input1AccessExprs ++ input2NullExprs,
        returnSchema.typeInfo,
        returnSchema.fieldNames)
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
      functionClass,
      body,
      returnSchema.typeInfo)
  }

  /**
    * Generates table function collector.
    */
  private[flink] def generateCollector(
    config: TableConfig,
    inputSchema: RowSchema,
    udtfTypeInfo: TypeInformation[Any],
    returnSchema: RowSchema,
    condition: Option[RexNode],
    pojoFieldMapping: Option[Array[Int]])
  : GeneratedCollector = {

    val generator = new CollectorCodeGenerator(
      config,
      false,
      inputSchema.typeInfo,
      Some(udtfTypeInfo),
      None,
      pojoFieldMapping)

    val (input1AccessExprs, input2AccessExprs) = generator.generateCorrelateAccessExprs

    val crossResultExpr = generator.generateResultExpression(
      input1AccessExprs ++ input2AccessExprs,
      returnSchema.typeInfo,
      returnSchema.fieldNames)

    val collectorCode = if (condition.isEmpty) {
      s"""
         |${crossResultExpr.code}
         |getCollector().collect(${crossResultExpr.resultTerm});
         |""".stripMargin
    } else {

      // adjust indices of InputRefs to adhere to schema expected by generator
      val changeInputRefIndexShuttle = new RexShuttle {
        override def visitInputRef(inputRef: RexInputRef): RexNode = {
          new RexInputRef(inputSchema.arity + inputRef.getIndex, inputRef.getType)
        }
      }
      // Run generateExpression to add init statements (ScalarFunctions) of condition to generator.
      //   The generated expression is discarded.
      generator.generateExpression(condition.get.accept(changeInputRefIndexShuttle))

      val filterGenerator = new FunctionCodeGenerator(
        config,
        false,
        udtfTypeInfo,
        None,
        pojoFieldMapping)

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
    rowType.getFieldNames.asScala.mkString(", ")
  }

  private[flink] def correlateOpName(
      inputType: RelDataType,
      rexCall: RexCall,
      sqlFunction: TableSqlFunction,
      rowType: RelDataType,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String)
    : String = {

    s"correlate: ${correlateToString(inputType, rexCall, sqlFunction, expression)}," +
      s" select: ${selectToString(rowType)}"
  }

  private[flink] def correlateToString(
      inputType: RelDataType,
      rexCall: RexCall,
      sqlFunction: TableSqlFunction,
      expression: (RexNode, List[String], Option[List[RexNode]]) => String): String = {
    val inFields = inputType.getFieldNames.asScala.toList
    val udtfName = sqlFunction.toString
    val operands = rexCall.getOperands.asScala.map(expression(_, inFields, None)).mkString(", ")
    s"table($udtfName($operands))"
  }

}
