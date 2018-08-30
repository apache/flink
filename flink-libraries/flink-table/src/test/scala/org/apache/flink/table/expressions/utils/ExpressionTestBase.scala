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

package org.apache.flink.table.expressions.utils

import java.util
import java.util.concurrent.Future

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgramBuilder}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.{Programs, RelBuilder}
import org.apache.flink.api.common.TaskInfo
import org.apache.flink.api.common.accumulators.Accumulator
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.functions.util.RuntimeUDFContext
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet => JDataSet}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig, TableEnvironment}
import org.apache.flink.table.calcite.FlinkPlannerImpl
import org.apache.flink.table.codegen.{Compiler, FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.dataset.{DataSetCalc, DataSetScan}
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.{After, Before}
import org.mockito.Mockito._

import scala.collection.mutable

/**
  * Base test class for expression tests.
  */
abstract class ExpressionTestBase {

  private val testExprs = mutable.ArrayBuffer[(String, RexNode, String)]()

  // setup test utils
  private val tableName = "testTable"
  private val context = prepareContext(typeInfo)
  private val planner = new FlinkPlannerImpl(
    context._2.getFrameworkConfig,
    context._2.getPlanner,
    context._2.getTypeFactory)
  private val logicalOptProgram = Programs.ofRules(FlinkRuleSets.LOGICAL_OPT_RULES)
  private val dataSetOptProgram = Programs.ofRules(FlinkRuleSets.DATASET_OPT_RULES)

  private def hepPlanner = {
    val builder = new HepProgramBuilder
    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP)
    val it = FlinkRuleSets.DATASET_NORM_RULES.iterator()
    while (it.hasNext) {
      builder.addRuleInstance(it.next())
    }
    new HepPlanner(builder.build, context._2.getFrameworkConfig.getContext)
  }

  private def prepareContext(typeInfo: TypeInformation[Any])
    : (RelBuilder, TableEnvironment, ExecutionEnvironment) = {
    // create DataSetTable
    val dataSetMock = mock(classOf[DataSet[Any]])
    val jDataSetMock = mock(classOf[JDataSet[Any]])
    when(dataSetMock.javaSet).thenReturn(jDataSetMock)
    when(jDataSetMock.getType).thenReturn(typeInfo)

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerDataSet(tableName, dataSetMock)
    functions.foreach(f => tEnv.registerFunction(f._1, f._2))

    // prepare RelBuilder
    val relBuilder = tEnv.getRelBuilder
    relBuilder.scan(tableName)

    (relBuilder, tEnv, env)
  }

  def testData: Any

  def typeInfo: TypeInformation[Any]

  def functions: Map[String, ScalarFunction] = Map()

  @Before
  def resetTestExprs() = {
    testExprs.clear()
  }

  @After
  def evaluateExprs() = {
    val relBuilder = context._1
    val config = new TableConfig()
    val generator = new FunctionCodeGenerator(config, false, typeInfo)

    // cast expressions to String
    val stringTestExprs = testExprs.map(expr => relBuilder.cast(expr._2, VARCHAR))

    // generate code
    val resultType = new RowTypeInfo(Seq.fill(testExprs.size)(STRING_TYPE_INFO): _*)
    val genExpr = generator.generateResultExpression(
      resultType,
      resultType.getFieldNames,
      stringTestExprs)

    val bodyCode =
      s"""
        |${genExpr.code}
        |return ${genExpr.resultTerm};
        |""".stripMargin

    val genFunc = generator.generateFunction[MapFunction[Any, Row], Row](
      "TestFunction",
      classOf[MapFunction[Any, Row]],
      bodyCode,
      resultType)

    // compile and evaluate
    val clazz = new TestCompiler[MapFunction[Any, Row], Row]().compile(genFunc)
    val mapper = clazz.newInstance()

    val isRichFunction = mapper.isInstanceOf[RichFunction]

    // call setRuntimeContext method and open method for RichFunction
    if (isRichFunction) {
      val richMapper = mapper.asInstanceOf[RichMapFunction[_, _]]
      val t = new RuntimeUDFContext(
        new TaskInfo("ExpressionTest", 1, 0, 1, 1),
        null,
        context._3.getConfig,
        new util.HashMap[String, Future[Path]](),
        new util.HashMap[String, Accumulator[_, _]](),
        null)
      richMapper.setRuntimeContext(t)
      richMapper.open(new Configuration())
    }

    val result = mapper.map(testData)

    // call close method for RichFunction
    if (isRichFunction) {
      mapper.asInstanceOf[RichMapFunction[_, _]].close()
    }

    // compare
    testExprs
      .zipWithIndex
      .foreach {
        case ((originalExpr, optimizedExpr, expected), index) =>
          val actual = result.getField(index)
          assertEquals(
            s"Wrong result for: [$originalExpr] optimized to: [$optimizedExpr]",
            expected,
            if (actual == null) "null" else actual)
      }
  }

  private def addSqlTestExpr(sqlExpr: String, expected: String): Unit = {
    // create RelNode from SQL expression
    val parsed = planner.parse(s"SELECT $sqlExpr FROM $tableName")
    val validated = planner.validate(parsed)
    val converted = planner.rel(validated).rel

    val env = context._2.asInstanceOf[BatchTableEnvironment]
    val optimized = env.optimize(converted)

    // throw exception if plan contains more than a calc
    if (!optimized.getInput(0).isInstanceOf[DataSetScan]) {
      fail("Expression is converted into more than a Calc operation. Use a different test method.")
    }

    testExprs += ((sqlExpr, extractRexNode(optimized), expected))
  }

  private def addTableApiTestExpr(tableApiExpr: Expression, expected: String): Unit = {
    // create RelNode from Table API expression
    val env = context._2.asInstanceOf[BatchTableEnvironment]
    val converted = env
      .scan(tableName)
      .select(tableApiExpr)
      .getRelNode

    val optimized = env.optimize(converted)

    testExprs += ((tableApiExpr.toString, extractRexNode(optimized), expected))
  }

  private def extractRexNode(node: RelNode): RexNode = {
    val calcProgram = node
      .asInstanceOf[DataSetCalc]
      .getProgram
    calcProgram.expandLocalRef(calcProgram.getProjectList.get(0))
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
  class TestCompiler[F <: Function, T <: Any] extends Compiler[F] {
    def compile(genFunc: GeneratedFunction[F, T]): Class[F] =
      compile(getClass.getClassLoader, genFunc.name, genFunc.code)
  }
}
