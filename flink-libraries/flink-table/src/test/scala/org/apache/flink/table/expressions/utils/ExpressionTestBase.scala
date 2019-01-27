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

import java.{io, util}
import java.util.concurrent.CompletableFuture
import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgramBuilder}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql2rel.RelDecorrelator
import org.apache.calcite.tools.{Programs, RelBuilder}
import org.apache.flink.api.common.TaskInfo
import org.apache.flink.api.common.accumulators.{AbstractAccumulatorRegistry, Accumulator}
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.functions.util.RuntimeUDFContext
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, RowType, TypeConverters}
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.calcite.FlinkPlannerImpl
import org.apache.flink.table.catalog.CatalogManager
import org.apache.flink.table.codegen.{CodeGeneratorContext, Compiler, ExprCodeGenerator, FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.expressions.{Expression, ExpressionParser, If, IsNull, Literal}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecCalc, BatchExecScan}
import org.apache.flink.table.plan.rules.FlinkBatchExecRuleSets
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.runtime.conversion.DataStructureConverters.createToInternalConverter
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.{After, Before}

import scala.collection.mutable

/**
  * Base test class for expression tests.
  */
abstract class ExpressionTestBase {

  private val testExprs = mutable.ArrayBuffer[(RexNode, String)]()

  val config = new TableConfig()

  // setup test utils
  private val tableName = "testTable"
  private val context = prepareContext(baseRowType)
  private val planner = new FlinkPlannerImpl(
    context._2.getFrameworkConfig,
    context._2.getPlanner,
    context._2.getTypeFactory,
    context._2.sqlToRelConverterConfig,
    context._2.getRelBuilder.getCluster,
    new CatalogManager()
  )
  private val logicalOptProgram = Programs.ofRules(
    FlinkBatchExecRuleSets.BATCH_EXEC_LOGICAL_OPT_RULES)
  private val dataSetOptProgram = Programs.ofRules(FlinkBatchExecRuleSets.BATCH_EXEC_OPT_RULES)

  protected val nullable = "null"
  protected val notNullable = "not null"

  private def hepPlanner = {
    val builder = new HepProgramBuilder
    builder.addMatchOrder(HepMatchOrder.BOTTOM_UP)
    val it = FlinkBatchExecRuleSets.BATCH_EXEC_DEFAULT_REWRITE_RULES.iterator()
    while (it.hasNext) {
      builder.addRuleInstance(it.next())
    }
    new HepPlanner(builder.build, context._2.getFrameworkConfig.getContext)
  }

  private def prepareContext(t: RowType)
    : (RelBuilder, TableEnvironment, StreamExecutionEnvironment) = {

    val env = StreamExecutionEnvironment.createLocalEnvironment(4)
    val tEnv = TableEnvironment.getBatchTableEnvironment(env, config)
    tEnv.registerCollection(tableName, Seq(), TypeConverters.createExternalTypeInfoFromDataType(t))
    functions.foreach(f => tEnv.registerFunction(f._1, f._2))

    // prepare RelBuilder
    val relBuilder = tEnv.getRelBuilder
    relBuilder.scan(tableName)

    (relBuilder, tEnv, env)
  }

  def testData: Any = {
    baseRowTestData
  }

  def rowToBaseRow(row: Row): BaseRow = {
    createToInternalConverter(rowType).apply(row).asInstanceOf[BaseRow]
  }

  def baseRowTestData: BaseRow = rowToBaseRow(rowTestData)

  def rowTestData: Row

  def getDataType: DataType = rowType

  def baseRowType: RowType = getDataType.toInternalType.asInstanceOf[RowType]

  def rowType: RowTypeInfo

  def functions: Map[String, ScalarFunction] = Map()

  @Before
  def resetTestExprs() = {
    testExprs.clear()
  }

  @After
  def evaluateExprs() = {
    val relBuilder = context._1
    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(
      ctx, false, config.getNullCheck).bindInput(baseRowType)

    // cast expressions to String
    val stringTestExprs = testExprs.map(expr => relBuilder.cast(expr._1, VARCHAR))

    // generate code
    val resultType = new RowType(Seq.fill(testExprs.size)(DataTypes.STRING): _*)

    val exprs = stringTestExprs.map(exprGenerator.generateExpression)
    val genExpr = exprGenerator.generateResultExpression(exprs, resultType, classOf[BinaryRow])

    val bodyCode =
      s"""
        |${genExpr.code}
        |return ${genExpr.resultTerm};
        |""".stripMargin

    val genFunc = FunctionCodeGenerator.generateFunction[MapFunction[Any, BinaryRow], BinaryRow](
      ctx,
      "TestFunction",
      classOf[MapFunction[Any, BinaryRow]],
      bodyCode,
      resultType,
      baseRowType,
      config)

    // compile and evaluate
    val clazz = new TestCompiler().compile(genFunc)
    val mapper = clazz.newInstance()

    val isRichFunction = mapper.isInstanceOf[RichFunction]

    // call setRuntimeContext method and open method for RichFunction
    if (isRichFunction) {
      val richMapper = mapper.asInstanceOf[RichMapFunction[_, _]]
      val testRegistry = new AbstractAccumulatorRegistry {
        override def queryPreAggregatedAccumulator[V, A <: io.Serializable](name: String) =
          new CompletableFuture[Accumulator[V, A]]

        override def addPreAggregatedAccumulator[V, A <: io.Serializable](name: String,
          accumulator: Accumulator[V, A]): Unit = ???

        override def getPreAggregatedAccumulators:
        util.Map[String, Accumulator[_, _ <: io.Serializable]] = ???

        override def commitPreAggregatedAccumulator(name: String): Unit = ???
      }
      val t = new RuntimeUDFContext(
        new TaskInfo("ExpressionTest", 1, 0, 1, 1),
        null,
        context._3.getConfig,
        new java.util.HashMap(),
        testRegistry,
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
        case ((expr, expected), index) =>

          val actual = if(!result.asInstanceOf[BinaryRow].isNullAt(index)) {
            result.asInstanceOf[BinaryRow].getBinaryString(index).toString
          } else {
            null
          }

          assertEquals(
            s"Wrong result for: $expr",
            expected,
            if (actual == null) "null" else actual)
      }
  }

  private[flink] def addSqlTestExpr(sqlExpr: String, expected: String): Unit = {
    // create RelNode from SQL expression
    val parsed = planner.parse(s"SELECT $sqlExpr FROM $tableName")
    val validated = planner.validate(parsed)
    val converted = planner.rel(validated).rel

    val decorPlan = RelDecorrelator.decorrelateQuery(converted)

    // default rewrite
    val rewritePlan = if (FlinkBatchExecRuleSets.BATCH_EXEC_DEFAULT_REWRITE_RULES.iterator()
      .hasNext) {
      val planner = hepPlanner
      planner.setRoot(decorPlan)
      planner.findBestExp
    } else {
      decorPlan
    }

    // convert to logical plan
    val logicalProps = converted.getTraitSet.replace(FlinkConventions.LOGICAL).simplify()
    val logicalCalc = logicalOptProgram.run(context._2.getPlanner, rewritePlan, logicalProps,
                                            ImmutableList.of(), ImmutableList.of())

    // convert to dataset plan
    val physicalProps = converted.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL).simplify()
    val dataSetCalc = dataSetOptProgram.run(context._2.getPlanner, logicalCalc, physicalProps,
      ImmutableList.of(), ImmutableList.of())

    // throw exception if plan contains more than a calc
    if (!dataSetCalc.getInput(0).isInstanceOf[BatchExecScan]) {
      fail("Expression is converted into more than a Calc operation. Use a different test method.")
    }

    // extract RexNode
    val calcProgram = dataSetCalc
     .asInstanceOf[BatchExecCalc]
     .getProgram
    val expanded = calcProgram.expandLocalRef(calcProgram.getProjectList.get(0))

    testExprs += ((expanded, expected))
  }

  private def addTableApiTestExpr(tableApiExpr: Expression, expected: String): Unit = {
    // create RelNode from Table API expression
    val env = context._2
    val converted = env
      .scan(tableName)
      .select(tableApiExpr)
      .getRelNode

    // create DataSetCalc
    val decorPlan = RelDecorrelator.decorrelateQuery(converted)

    // convert to logical plan
    val flinkLogicalProps = converted.getTraitSet.replace(FlinkConventions.LOGICAL).simplify()
    val logicalCalc = logicalOptProgram.run(context._2.getPlanner, decorPlan, flinkLogicalProps,
      ImmutableList.of(), ImmutableList.of())

    // convert to dataset plan
    val flinkPhysicalProps = converted.getTraitSet
      .replace(FlinkConventions.BATCH_PHYSICAL).simplify()
    val dataSetCalc = dataSetOptProgram.run(context._2.getPlanner, logicalCalc, flinkPhysicalProps,
      ImmutableList.of(), ImmutableList.of())

    // extract RexNode
    val calcProgram = dataSetCalc
     .asInstanceOf[BatchExecCalc]
     .getProgram
    val expanded = calcProgram.expandLocalRef(calcProgram.getProjectList.get(0))

    testExprs += ((expanded, expected))
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
    if (expected == nullable) {
      testTableNullable(expr, exprString)
      testSqlNullable(sqlExpr)
    }
  }

  def testTableApi(
      expr: Expression,
      exprString: String,
      expected: String)
    : Unit = {
    addTableApiTestExpr(expr, expected)
    addTableApiTestExpr(exprString, expected)
    if (expected == nullable) {
      testTableNullable(expr, exprString)
    }
  }

  def testSqlApi(
      sqlExpr: String,
      expected: String)
    : Unit = {
    addSqlTestExpr(sqlExpr, expected)
    if (expected == nullable) {
      testSqlNullable(sqlExpr)
    }
  }

  def testSqlNullable(nullUdf: String): Unit = {
    addSqlTestExpr(
      s"CASE WHEN ($nullUdf) is null THEN '$nullable' ELSE '$notNullable' END", nullable)
  }

  def testTableNullable(nullExpr: Expression, nullExprString: String): Unit = {
    val retExpr = If(IsNull(nullExpr), Literal(nullable, DataTypes.STRING), Literal(notNullable,
      DataTypes.STRING))
    addTableApiTestExpr(retExpr, nullable)
    val retStrExpr = If(IsNull(ExpressionParser.parseExpression(nullExprString)), Literal(nullable,
      DataTypes.STRING), Literal(notNullable,
      DataTypes.STRING))
    addTableApiTestExpr(retStrExpr, nullable)
  }

  // ----------------------------------------------------------------------------------------------

  // TestCompiler that uses current class loader
  class TestCompiler[F <: Function, T <: Any] extends Compiler[F] {
    def compile(genFunc: GeneratedFunction[F, T]): Class[F] =
      compile(getClass.getClassLoader, genFunc.name, genFunc.code)
  }
}
