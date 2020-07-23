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

import java.util.Collections

import org.apache.calcite.plan.hep.{HepPlanner, HepProgramBuilder}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalCalc
import org.apache.calcite.rel.rules._
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.`type`.SqlTypeName.VARCHAR
import org.apache.flink.api.common.TaskInfo
import org.apache.flink.api.common.functions.util.RuntimeUDFContext
import org.apache.flink.api.common.functions.{MapFunction, RichFunction, RichMapFunction}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.{EnvironmentSettings, TableConfig}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.data.conversion.{DataStructureConverter, DataStructureConverters}
import org.apache.flink.table.data.util.DataFormatConverters
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.types.AbstractDataType
import org.apache.flink.table.types.logical.{RowType, VarCharType}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row
import org.junit.Assert.{assertEquals, fail}
import org.junit.rules.ExpectedException
import org.junit.{After, Before, Rule}

import scala.collection.mutable
import scala.collection.JavaConverters._

abstract class ExpressionTestBase {

  val config = new TableConfig()

  // (originalExpr, optimizedExpr, expectedResult)
  private val testExprs = mutable.ArrayBuffer[(String, RexNode, String)]()
  private val env = StreamExecutionEnvironment.createLocalEnvironment(4)
  private val setting = EnvironmentSettings.newInstance().inStreamingMode().build()
  // use impl class instead of interface class to avoid
  // "Static methods in interface require -target:jvm-1.8"
  private val tEnv = StreamTableEnvironmentImpl.create(env, setting, config)
    .asInstanceOf[StreamTableEnvironmentImpl]
  private val resolvedDataType = if (containsLegacyTypes) {
    TypeConversions.fromLegacyInfoToDataType(typeInfo)
  } else {
    tEnv.getCatalogManager.getDataTypeFactory.createDataType(testDataType)
  }
  private val planner = tEnv.getPlanner.asInstanceOf[PlannerBase]
  private val relBuilder = planner.getRelBuilder
  private val calcitePlanner = planner.createFlinkPlanner
  private val parser = planner.plannerContext.createCalciteParser()

  // setup test utils
  private val tableName = "testTable"
  protected val nullable = "null"
  protected val notNullable = "not null"

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  @Before
  def prepare(): Unit = {
    if (containsLegacyTypes) {
      val ds = env.fromCollection(Collections.emptyList[Row](), typeInfo)
      tEnv.createTemporaryView(tableName, ds)
      functions.foreach(f => tEnv.registerFunction(f._1, f._2))
    } else {
      tEnv.createTemporaryView(tableName, tEnv.fromValues(resolvedDataType))
      testSystemFunctions.asScala.foreach(e => tEnv.createTemporarySystemFunction(e._1, e._2))
    }

    // prepare RelBuilder
    relBuilder.scan(tableName)

    // reset test exprs
    testExprs.clear()
  }

  @After
  def evaluateExprs(): Unit = {
    val ctx = CodeGeneratorContext(config)
    val inputType = if (containsLegacyTypes) {
      fromTypeInfoToLogicalType(typeInfo)
    } else {
      resolvedDataType.getLogicalType
    }
    val exprGenerator = new ExprCodeGenerator(ctx, nullableInput = false).bindInput(inputType)

    // cast expressions to String
    val stringTestExprs = testExprs.map(expr => relBuilder.cast(expr._2, VARCHAR))

    // generate code
    val resultType = RowType.of(Seq.fill(testExprs.size)(
      new VarCharType(VarCharType.MAX_LENGTH)): _*)

    val exprs = stringTestExprs.map(exprGenerator.generateExpression)
    val genExpr = exprGenerator.generateResultExpression(exprs, resultType, classOf[BinaryRowData])

    val bodyCode =
      s"""
         |${genExpr.code}
         |return ${genExpr.resultTerm};
        """.stripMargin

    val genFunc = FunctionCodeGenerator.generateFunction[MapFunction[RowData, BinaryRowData]](
      ctx,
      "TestFunction",
      classOf[MapFunction[RowData, BinaryRowData]],
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
        classOf[ExpressionTestBase].getClassLoader,
        env.getConfig,
        Collections.emptyMap(),
        Collections.emptyMap(),
        null)
      richMapper.setRuntimeContext(t)
      richMapper.open(new Configuration())
    }

    val testRow = if (containsLegacyTypes) {
      val converter = DataFormatConverters
        .getConverterForDataType(resolvedDataType)
        .asInstanceOf[DataFormatConverter[RowData, Row]]
      converter.toInternal(testData)
    } else {
      val converter = DataStructureConverters
        .getConverter(resolvedDataType)
        .asInstanceOf[DataStructureConverter[RowData, Row]]
      converter.toInternalOrNull(testData)
    }

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
          val actual = if(!result.asInstanceOf[BinaryRowData].isNullAt(index)) {
            result.asInstanceOf[BinaryRowData].getString(index).toString
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
    val parsed = parser.parse(s"SELECT $sqlExpr FROM $tableName")
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
    if (!optimized.getInput(0).getInputs.isEmpty) {
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
      sqlExpr: String,
      expected: String): Unit = {
    addTableApiTestExpr(expr, expected)
    addSqlTestExpr(sqlExpr, expected)
  }

  def testTableApi(
      expr: Expression,
      expected: String): Unit = {
    addTableApiTestExpr(expr, expected)
  }

  private def addTableApiTestExpr(tableApiString: String, expected: String): Unit = {
    addTableApiTestExpr(ExpressionParser.parseExpression(tableApiString), expected)
  }

  private def addTableApiTestExpr(tableApiExpr: Expression, expected: String): Unit = {
    // create RelNode from Table API expression
    val relNode = relBuilder
        .queryOperation(tEnv.from(tableName).select(tableApiExpr).getQueryOperation).build()

    addTestExpr(relNode, expected, tableApiExpr.asSummaryString())
  }

  def testSqlApi(
      sqlExpr: String,
      expected: String): Unit = {
    addSqlTestExpr(sqlExpr, expected)
  }

  def testData: Row

  def testDataType: AbstractDataType[_] =
    throw new IllegalArgumentException("Implement this if no legacy types are expected.")

  def testSystemFunctions: java.util.Map[String, ScalarFunction] = Collections.emptyMap();

  // ----------------------------------------------------------------------------------------------
  // Legacy type system
  // ----------------------------------------------------------------------------------------------

  def containsLegacyTypes: Boolean = true

  @deprecated
  def functions: Map[String, ScalarFunction] = Map()

  @deprecated
  def typeInfo: RowTypeInfo =
    throw new IllegalArgumentException("Implement this if legacy types are expected.")

  @deprecated
  def testAllApis(
      expr: Expression,
      exprString: String,
      sqlExpr: String,
      expected: String): Unit = {
    addTableApiTestExpr(expr, expected)
    addTableApiTestExpr(exprString, expected)
    addSqlTestExpr(sqlExpr, expected)
  }

  @deprecated
  def testTableApi(
      expr: Expression,
      exprString: String,
      expected: String): Unit = {
    addTableApiTestExpr(expr, expected)
    addTableApiTestExpr(exprString, expected)
  }
}
