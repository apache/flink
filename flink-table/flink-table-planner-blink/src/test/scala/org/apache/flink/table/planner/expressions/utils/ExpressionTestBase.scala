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

package org.apache.flink.table.planner.expressions.utils

import org.apache.flink.api.common.TaskInfo
import org.apache.flink.api.common.functions.util.RuntimeUDFContext
import org.apache.flink.api.common.functions.{MapFunction, RichFunction, RichMapFunction}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableConfig}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, DataFormatConverters}
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.expressions.ExpressionBuilder
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{RowType, VarCharType}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

import org.apache.calcite.plan.hep.{HepPlanner, HepProgramBuilder}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.{LogicalCalc, LogicalTableScan}
import org.apache.calcite.rel.rules._
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.`type`.SqlTypeName.VARCHAR
import org.junit.Assert.{assertEquals, fail}
import org.junit.rules.ExpectedException
import org.junit.{After, Before, Rule}

import java.util.Collections

import scala.collection.mutable

abstract class ExpressionTestBase {

  val config = new TableConfig()

  // (originalExpr, optimizedExpr, expectedResult)
  private val testExprs = mutable.ArrayBuffer[(String, RexNode, String)]()
  private val env = StreamExecutionEnvironment.createLocalEnvironment(4)
  private val setting = EnvironmentSettings.newInstance()
    .useBlinkPlanner().inStreamingMode().build()
  // use impl class instead of interface class to avoid
  // "Static methods in interface require -target:jvm-1.8"
  private val tEnv = StreamTableEnvironmentImpl.create(env, setting, config)
  private val planner = tEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[PlannerBase]
  private val relBuilder = planner.getRelBuilder
  private val calcitePlanner = planner.createFlinkPlanner

  // setup test utils
  private val tableName = "testTable"
  protected val nullable = "null"
  protected val notNullable = "not null"

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  def functions: Map[String, ScalarFunction] = Map()

  @Before
  def prepare(): Unit = {
    val ds = env.fromCollection(Collections.emptyList[Row](), typeInfo)
    tEnv.registerDataStream(tableName, ds)
    functions.foreach(f => tEnv.registerFunction(f._1, f._2))

    // prepare RelBuilder
    relBuilder.scan(tableName)

    // reset test exprs
    testExprs.clear()
  }

  @After
  def evaluateExprs(): Unit = {
    val ctx = CodeGeneratorContext(config)
    val inputType = fromTypeInfoToLogicalType(typeInfo)
    val exprGenerator = new ExprCodeGenerator(ctx, nullableInput = false).bindInput(inputType)

    // cast expressions to String
    val stringTestExprs = testExprs.map(expr => relBuilder.cast(expr._2, VARCHAR))

    // generate code
    val resultType = RowType.of(Seq.fill(testExprs.size)(
      new VarCharType(VarCharType.MAX_LENGTH)): _*)

    val exprs = stringTestExprs.map(exprGenerator.generateExpression)
    val genExpr = exprGenerator.generateResultExpression(exprs, resultType, classOf[BinaryRow])

    val bodyCode =
      s"""
         |${genExpr.code}
         |return ${genExpr.resultTerm};
        """.stripMargin

    val genFunc = FunctionCodeGenerator.generateFunction[MapFunction[BaseRow, BinaryRow]](
      ctx,
      "TestFunction",
      classOf[MapFunction[BaseRow, BinaryRow]],
      bodyCode,
      resultType,
      inputType)

    val mapper = genFunc.newInstance(getClass.getClassLoader)

    val isRichFunction = mapper.isInstanceOf[RichFunction]

    // call setRuntimeContext method and open method for RichFunction
    if (isRichFunction) {
      val richMapper = mapper.asInstanceOf[RichMapFunction[_, _]]
      val t = new RuntimeUDFContext(
        new TaskInfo("ExpressionTest", 1, 0, 1, 1),
        null,
        env.getConfig,
        Collections.emptyMap(),
        Collections.emptyMap(),
        null)
      richMapper.setRuntimeContext(t)
      richMapper.open(new Configuration())
    }

    val converter = DataFormatConverters
      .getConverterForDataType(dataType)
      .asInstanceOf[DataFormatConverters.DataFormatConverter[BaseRow, Row]]
    val testRow = converter.toInternal(testData)
    val result = mapper.map(testRow)

    // call close method for RichFunction
    if (isRichFunction) {
      mapper.asInstanceOf[RichMapFunction[_, _]].close()
    }

    // compare
    testExprs
      .zipWithIndex
      .foreach {
        case ((originalExpr, optimizedExpr, expected), index) =>

          // adapt string result
          val actual = if(!result.asInstanceOf[BinaryRow].isNullAt(index)) {
            result.asInstanceOf[BinaryRow].getString(index).toString
          } else {
            null
          }

          val original = if (originalExpr == null) "" else s"for: [$originalExpr]"

          assertEquals(
            s"Wrong result $original optimized to: [$optimizedExpr]",
            expected,
            if (actual == null) "null" else actual)
      }
  }

  private def addSqlTestExpr(sqlExpr: String, expected: String): Unit = {
    // create RelNode from SQL expression
    val parsed = calcitePlanner.parse(s"SELECT $sqlExpr FROM $tableName")
    val validated = calcitePlanner.validate(parsed)
    val converted = calcitePlanner.rel(validated).rel
    addTestExpr(converted, expected, sqlExpr)
  }

  private def addTestExpr(relNode: RelNode, expected: String, summaryString: String): Unit = {
    val builder = new HepProgramBuilder()
    builder.addRuleInstance(ProjectToCalcRule.INSTANCE)
    val hep = new HepPlanner(builder.build())
    hep.setRoot(relNode)
    val optimized = hep.findBestExp()

    // throw exception if plan contains more than a calc
    if (!optimized.getInput(0).isInstanceOf[LogicalTableScan]) {
      fail("Expression is converted into more than a Calc operation. Use a different test method.")
    }

    testExprs += ((summaryString, extractRexNode(optimized), expected))
  }

  private def extractRexNode(node: RelNode): RexNode = {
    val calcProgram = node
      .asInstanceOf[LogicalCalc]
      .getProgram
    calcProgram.expandLocalRef(calcProgram.getProjectList.get(0))
  }

  def testAllApis(
      expr: Expression,
      exprString: String,
      sqlExpr: String,
      expected: String): Unit = {
    addTableApiTestExpr(expr, expected)
    addTableApiTestExpr(exprString, expected)
    addSqlTestExpr(sqlExpr, expected)
  }

  def testTableApi(
      expr: Expression,
      exprString: String,
      expected: String): Unit = {
    addTableApiTestExpr(expr, expected)
    addTableApiTestExpr(exprString, expected)
  }

  private def addTableApiTestExpr(tableApiString: String, expected: String): Unit = {
    addTableApiTestExpr(ExpressionParser.parseExpression(tableApiString), expected)
  }

  private def addTableApiTestExpr(tableApiExpr: Expression, expected: String): Unit = {
    // create RelNode from Table API expression
    val relNode = relBuilder
        .queryOperation(tEnv.scan(tableName).select(tableApiExpr).getQueryOperation).build()

    addTestExpr(relNode, expected, tableApiExpr.asSummaryString())
  }

  def testSqlApi(
      sqlExpr: String,
      expected: String): Unit = {
    addSqlTestExpr(sqlExpr, expected)
  }

  def testData: Row

  def typeInfo: RowTypeInfo

  def dataType: DataType = TypeConversions.fromLegacyInfoToDataType(typeInfo)

}
