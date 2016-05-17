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

package org.apache.flink.api.table.expressions.utils

import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.functions.{Function, MapFunction}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{DataSet => JDataSet}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.table._
import org.apache.flink.api.table.codegen.{CodeGenerator, GeneratedFunction}
import org.apache.flink.api.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.api.table.runtime.FunctionCompiler
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.junit.Assert._
import org.junit.{After, Before}
import org.mockito.Mockito._

import scala.collection.mutable

/**
  * Base test class for expression tests.
  */
abstract class ExpressionTestBase {

  private val testExprs = mutable.LinkedHashSet[(RexNode, String)]()

  // setup test utils
  private val tableName = "testTable"
  private val context = prepareContext(typeInfo)
  private val planner = new FlinkPlannerImpl(
    context._2.getFrameworkConfig,
    context._2.getPlanner,
    context._2.getTypeFactory)

  private def prepareContext(typeInfo: TypeInformation[Any]): (RelBuilder, TableEnvironment) = {
    // create DataSetTable
    val dataSetMock = mock(classOf[DataSet[Any]])
    val jDataSetMock = mock(classOf[JDataSet[Any]])
    when(dataSetMock.javaSet).thenReturn(jDataSetMock)
    when(jDataSetMock.getType).thenReturn(typeInfo)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerDataSet(tableName, dataSetMock)

    // prepare RelBuilder
    val relBuilder = tEnv.getRelBuilder
    relBuilder.scan(tableName)

    (relBuilder, tEnv)
  }

  def testData: Any

  def typeInfo: TypeInformation[Any]

  @Before
  def resetTestExprs() = {
    testExprs.clear()
  }

  @After
  def evaluateExprs() = {
    val relBuilder = context._1
    val config = new TableConfig()
    val generator = new CodeGenerator(config, false, typeInfo)

    // cast expressions to String
    val stringTestExprs = testExprs.map(expr => relBuilder.cast(expr._1, VARCHAR)).toSeq

    // generate code
    val resultType = new RowTypeInfo(Seq.fill(testExprs.size)(STRING_TYPE_INFO))
    val genExpr = generator.generateResultExpression(
      resultType,
      resultType.getFieldNames,
      stringTestExprs)

    val bodyCode =
      s"""
        |${genExpr.code}
        |return ${genExpr.resultTerm};
        |""".stripMargin

    val genFunc = generator.generateFunction[MapFunction[Any, String]](
      "TestFunction",
      classOf[MapFunction[Any, String]],
      bodyCode,
      resultType.asInstanceOf[TypeInformation[Any]])

    // compile and evaluate
    val clazz = new TestCompiler[MapFunction[Any, String]]().compile(genFunc)
    val mapper = clazz.newInstance()
    val result = mapper.map(testData).asInstanceOf[Row]

    // compare
    testExprs
      .zipWithIndex
      .foreach {
        case ((expr, expected), index) =>
          assertEquals(s"Wrong result for: $expr", expected, result.productElement(index))
      }
  }

  private def addSqlTestExpr(sqlExpr: String, expected: String): Unit = {
    // create RelNode from SQL expression
    val parsed = planner.parse(s"SELECT $sqlExpr FROM $tableName")
    val validated = planner.validate(parsed)
    val converted = planner.rel(validated)

    // extract RexNode
    val expr: RexNode = converted.rel.asInstanceOf[LogicalProject].getChildExps.get(0)
    testExprs.add((expr, expected))
  }

  private def addTableApiTestExpr(tableApiExpr: Expression, expected: String): Unit = {
    val env = context._2
    val expr = env
      .asInstanceOf[BatchTableEnvironment]
      .scan(tableName)
      .select(tableApiExpr)
      .getRelNode
      .asInstanceOf[LogicalProject]
      .getChildExps
      .get(0)
    testExprs.add((expr, expected))
  }

  private def addTableApiTestExpr(tableApiString: String, expected: String): Unit = {
    addTableApiTestExpr(ExpressionParser.parseExpression(tableApiString), expected)
  }

  def testAllApis(
      expr: Expression,
      exprString: String,
      sqlExpr: String,
      expected: String)
    : Unit = {
    addTableApiTestExpr(expr, expected)
    addTableApiTestExpr(exprString, expected)
    addSqlTestExpr(sqlExpr, expected)
  }

  def testTableApi(
      expr: Expression,
      exprString: String,
      expected: String)
    : Unit = {
    addTableApiTestExpr(expr, expected)
    addTableApiTestExpr(exprString, expected)
  }

  def testSqlApi(
      sqlExpr: String,
      expected: String)
    : Unit = {
    addSqlTestExpr(sqlExpr, expected)
  }

  // ----------------------------------------------------------------------------------------------

  // TestCompiler that uses current class loader
  class TestCompiler[T <: Function] extends FunctionCompiler[T] {
    def compile(genFunc: GeneratedFunction[T]): Class[T] =
      compile(getClass.getClassLoader, genFunc.name, genFunc.code)
  }
}
