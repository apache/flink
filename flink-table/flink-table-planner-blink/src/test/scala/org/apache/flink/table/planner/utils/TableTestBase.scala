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
package org.apache.flink.table.planner.utils

import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, RowTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecEnv}
import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.java.internal.{StreamTableEnvironmentImpl => JavaStreamTableEnvImpl}
import org.apache.flink.table.api.bridge.java.{StreamTableEnvironment => JavaStreamTableEnv}
import org.apache.flink.table.api.bridge.scala.internal.{StreamTableEnvironmentImpl => ScalaStreamTableEnvImpl}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment => ScalaStreamTableEnv}
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.internal.{TableEnvironmentImpl, TableEnvironmentInternal, TableImpl}
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog, GenericInMemoryCatalog, ObjectIdentifier}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.delegation.{Executor, ExecutorFactory, PlannerFactory}
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE
import org.apache.flink.table.descriptors.Schema.SCHEMA
import org.apache.flink.table.descriptors.{CustomConnectorDescriptor, DescriptorProperties, Schema}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.factories.{ComponentFactoryService, StreamTableSourceFactory}
import org.apache.flink.table.functions._
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.operations.{CatalogSinkModifyOperation, ModifyOperation, Operation, QueryOperation}
import org.apache.flink.table.planner.calcite.CalciteConfig
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.operations.{DataStreamQueryOperation, PlannerQueryOperation, RichTableSourceQueryOperation}
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodePlanDumper
import org.apache.flink.table.planner.plan.optimize.program._
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.runtime.utils.{TestingAppendTableSink, TestingRetractTableSink, TestingUpsertTableSink}
import org.apache.flink.table.planner.sinks.CollectRowTableSink
import org.apache.flink.table.planner.utils.PlanKind.PlanKind
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.{StreamTableSource, TableSource}
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.typeutils.FieldInfoUtils
import org.apache.flink.types.Row

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{SqlExplainLevel, SqlIntervalQualifier}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Rule
import org.junit.rules.{ExpectedException, TemporaryFolder, TestName}

import _root_.java.math.{BigDecimal => JBigDecimal}
import _root_.java.util
import java.io.IOException
import java.time.Duration

import _root_.scala.collection.JavaConversions._
import _root_.scala.io.Source

/**
 * Test base for testing Table API / SQL plans.
 */
abstract class TableTestBase {

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  // used for get test case method name
  val testName: TestName = new TestName

  val _tempFolder = new TemporaryFolder

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  @Rule
  def thrown: ExpectedException = expectedException

  @Rule
  def name: TestName = testName

  def streamTestUtil(conf: TableConfig = new TableConfig): StreamTableTestUtil =
    StreamTableTestUtil(this, conf = conf)

  def scalaStreamTestUtil(): ScalaStreamTableTestUtil = ScalaStreamTableTestUtil(this)

  def javaStreamTestUtil(): JavaStreamTableTestUtil = JavaStreamTableTestUtil(this)

  def batchTestUtil(conf: TableConfig = new TableConfig): BatchTableTestUtil =
    BatchTableTestUtil(this, conf = conf)

  def scalaBatchTestUtil(): ScalaBatchTableTestUtil = ScalaBatchTableTestUtil(this)

  def javaBatchTestUtil(): JavaBatchTableTestUtil = JavaBatchTableTestUtil(this)

  def verifyTableEquals(expected: Table, actual: Table): Unit = {
    val expectedString = FlinkRelOptUtil.toString(TableTestUtil.toRelNode(expected))
    val actualString = FlinkRelOptUtil.toString(TableTestUtil.toRelNode(actual))
    assertEquals(
      "Logical plans do not match",
      LogicalPlanFormatUtils.formatTempTableId(expectedString),
      LogicalPlanFormatUtils.formatTempTableId(actualString))
  }
}

abstract class TableTestUtilBase(test: TableTestBase, isStreamingMode: Boolean) {
  protected lazy val diffRepository: DiffRepository = DiffRepository.lookup(test.getClass)

  protected val setting: EnvironmentSettings = if (isStreamingMode) {
    EnvironmentSettings.newInstance().inStreamingMode().build()
  } else {
    EnvironmentSettings.newInstance().inBatchMode().build()
  }

  // a counter for unique table names
  private var counter = 0L

  private def getNextId: Long = {
    counter += 1
    counter
  }

  protected def getTableEnv: TableEnvironment

  protected def isBounded: Boolean = !isStreamingMode

  def getPlanner: PlannerBase = {
    getTableEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[PlannerBase]
  }

  /**
   * Creates a table with the given DDL SQL string.
   */
  def addTable(ddl: String): Unit = {
    getTableEnv.executeSql(ddl)
  }

  /**
   * Create a [[DataStream]] with the given schema,
   * and registers this DataStream under given name into the TableEnvironment's catalog.
   *
   * @param name table name
   * @param fields field names
   * @tparam T field types
   * @return returns the registered [[Table]].
   */
  def addDataStream[T: TypeInformation](name: String, fields: Expression*): Table = {
    val env = new ScalaStreamExecEnv(new LocalStreamEnvironment())
    val dataStream = env.fromElements[T]().javaStream
    val tableEnv = getTableEnv
    TableTestUtil.createTemporaryView(tableEnv, name, dataStream, Some(fields.toArray))
    tableEnv.from(name)
  }

  /**
   * Create a [[TestTableSource]] with the given schema,
   * and registers this TableSource under a unique name into the TableEnvironment's catalog.
   *
   * @param fields field names
   * @tparam T field types
   * @return returns the registered [[Table]].
   */
  def addTableSource[T: TypeInformation](fields: Expression*): Table = {
    addTableSource[T](s"Table$getNextId", fields: _*)
  }

  /**
   * Create a [[TestTableSource]] with the given schema,
   * and registers this TableSource under given name into the TableEnvironment's catalog.
   *
   * @param name table name
   * @param fields field names
   * @tparam T field types
   * @return returns the registered [[Table]].
   */
  def addTableSource[T: TypeInformation](name: String, fields: Expression*): Table = {
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]

    val tableSchema = if (fields.isEmpty) {
      val fieldTypes: Array[TypeInformation[_]] = typeInfo match {
        case tt: TupleTypeInfo[_] => (0 until tt.getArity).map(tt.getTypeAt).toArray
        case ct: CaseClassTypeInfo[_] => (0 until ct.getArity).map(ct.getTypeAt).toArray
        case at: AtomicType[_] => Array[TypeInformation[_]](at)
        case pojo: PojoTypeInfo[_] => (0 until pojo.getArity).map(pojo.getTypeAt).toArray
        case _ => throw new TableException(s"Unsupported type info: $typeInfo")
      }
      val types = fieldTypes.map(TypeConversions.fromLegacyInfoToDataType)
      val names = FieldInfoUtils.getFieldNames(typeInfo)
      TableSchema.builder().fields(names, types).build()
    } else {
      FieldInfoUtils.getFieldsInfo(typeInfo, fields.toArray).toTableSchema
    }

    addTableSource(name, new TestTableSource(isBounded, tableSchema))
  }

  /**
   * Create a [[TestTableSource]] with the given schema, table stats and unique keys,
   * and registers this TableSource under given name into the TableEnvironment's catalog.
   *
   * @param name table name
   * @param types field types
   * @param fields field names
   * @return returns the registered [[Table]].
   */
  def addTableSource(
      name: String,
      types: Array[TypeInformation[_]],
      fields: Array[String]): Table = {
    val schema = new TableSchema(fields, types)
    val tableSource = new TestTableSource(isBounded, schema)
    addTableSource(name, tableSource)
  }

  /**
   * Register this TableSource under given name into the TableEnvironment's catalog.
   *
   * @param name table name
   * @param tableSource table source
   * @return returns the registered [[Table]].
   */
  def addTableSource(
      name: String,
      tableSource: TableSource[_]): Table = {
    getTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      name, tableSource)
    getTableEnv.from(name)
  }

  /**
   * Registers a [[ScalarFunction]] under given name into the TableEnvironment's catalog.
   */
  def addFunction(name: String, function: ScalarFunction): Unit = {
    getTableEnv.registerFunction(name, function)
  }

  /**
   * Registers a [[UserDefinedFunction]] according to FLIP-65.
   */
  def addTemporarySystemFunction(name: String, function: UserDefinedFunction): Unit = {
    getTableEnv.createTemporarySystemFunction(name, function)
  }

  /**
   * Registers a [[UserDefinedFunction]] class according to FLIP-65.
   */
  def addTemporarySystemFunction(name: String, function: Class[_ <: UserDefinedFunction]): Unit = {
    getTableEnv.createTemporarySystemFunction(name, function)
  }

  /**
   * Verify the AST (abstract syntax tree), the optimized rel plan and the optimized exec plan
   * for the given SELECT query.
   * Note: An exception will be thrown if the given query can't be translated to exec plan.
   */
  def verifyPlan(query: String): Unit = {
    doVerifyPlan(
      query,
      Array.empty[ExplainDetail],
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL, PlanKind.OPT_EXEC))
  }

  /**
   * Verify the AST (abstract syntax tree), the optimized rel plan and the optimized exec plan
   * for the given SELECT query. The plans will contain the extra [[ExplainDetail]]s.
   * Note: An exception will be thrown if the given query can't be translated to exec plan.
   */
  def verifyPlan(query: String, extraDetails: ExplainDetail*): Unit = {
    doVerifyPlan(
      query,
      extraDetails.toArray,
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL, PlanKind.OPT_EXEC))
  }

  /**
   * Verify the AST (abstract syntax tree), the optimized rel plan and the optimized exec plan
   * for the given INSERT statement.
   */
  def verifyPlanInsert(insert: String): Unit = {
    doVerifyPlanInsert(
      insert,
      Array.empty[ExplainDetail],
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL, PlanKind.OPT_EXEC))
  }

  /**
   * Verify the AST (abstract syntax tree), the optimized rel plan and the optimized exec plan
   * for the given INSERT statement. The plans will contain the extra [[ExplainDetail]]s.
   */
  def verifyPlanInsert(insert: String, extraDetails: ExplainDetail*): Unit = {
    doVerifyPlanInsert(
      insert,
      extraDetails.toArray,
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL, PlanKind.OPT_EXEC))
  }

  /**
   * Verify the AST (abstract syntax tree), the optimized rel plan and the optimized exec plan
   * for the given [[Table]].
   * Note: An exception will be thrown if the given sql can't be translated to exec plan.
   */
  def verifyPlan(table: Table): Unit = {
    doVerifyPlan(
      table,
      Array.empty[ExplainDetail],
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL, PlanKind.OPT_EXEC))
  }

  /**
   * Verify the AST (abstract syntax tree), the optimized rel plan and the optimized exec plan
   * for the given [[Table]]. The plans will contain the extra [[ExplainDetail]]s.
   * Note: An exception will be thrown if the given sql can't be translated to exec plan.
   */
  def verifyPlan(table: Table, extraDetails: ExplainDetail*): Unit = {
    doVerifyPlan(
      table,
      extraDetails.toArray,
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL, PlanKind.OPT_EXEC))
  }

  /**
   * Verify the AST (abstract syntax tree), the optimized rel plan and the optimized exec plan
   * for the given [[Table]] with the given sink table name.
   * Note: An exception will be thrown if the given sql can't be translated to exec plan.
   */
  def verifyPlanInsert(table: Table, sink: TableSink[_], targetPath: String): Unit = {
    val stmtSet = getTableEnv.createStatementSet()
    getTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(targetPath, sink)
    stmtSet.addInsert(targetPath, table)
    verifyPlan(stmtSet)
  }

  /**
   * Verify the AST (abstract syntax tree), the optimized rel plan and the optimized exec plan
   * for the given [[Table]] with the given sink table name.
   * The plans will contain the extra [[ExplainDetail]]s.
   * Note: An exception will be thrown if the given sql can't be translated to exec plan.
   */
  def verifyPlanInsert(
      table: Table,
      sink: TableSink[_],
      targetPath: String,
      extraDetails: ExplainDetail*): Unit = {
    val stmtSet = getTableEnv.createStatementSet()
    getTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(targetPath, sink)
    stmtSet.addInsert(targetPath, table)
    verifyPlan(stmtSet, extraDetails: _*)
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan and the optimized exec plan
   * for the given [[StatementSet]]. The plans will contain the extra [[ExplainDetail]]s.
   * Note: An exception will be thrown if the given sql can't be translated to exec plan.
   */
  def verifyPlan(stmtSet: StatementSet, extraDetails: ExplainDetail*): Unit = {
    doVerifyPlan(
      stmtSet,
      extraDetails.toArray,
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL, PlanKind.OPT_EXEC),
      () => Unit)
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan for the given SELECT query.
   */
  def verifyRelPlan(query: String): Unit = {
    doVerifyPlan(
      query,
      Array.empty[ExplainDetail],
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL))
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan for the given SELECT query.
   * The plans will contain the extra [[ExplainDetail]]s.
   */
  def verifyRelPlan(query: String, extraDetails: ExplainDetail*): Unit = {
    doVerifyPlan(
      query,
      extraDetails.toArray,
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL))
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan
   * for the given INSERT statement.
   */
  def verifyRelPlanInsert(insert: String): Unit = {
    doVerifyPlanInsert(
      insert,
      Array.empty[ExplainDetail],
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL))
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan
   * for the given INSERT statement. The plans will contain the extra [[ExplainDetail]]s.
   */
  def verifyRelPlanInsert(insert: String, extraDetails: ExplainDetail*): Unit = {
    doVerifyPlanInsert(
      insert,
      extraDetails.toArray,
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL))
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan for the given [[Table]].
   */
  def verifyRelPlan(table: Table): Unit = {
    doVerifyPlan(
      table,
      Array.empty[ExplainDetail],
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL))
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan for the given [[Table]].
   * The plans will contain the extra [[ExplainDetail]]s.
   */
  def verifyRelPlan(table: Table, extraDetails: ExplainDetail*): Unit = {
    doVerifyPlan(
      table,
      extraDetails.toArray,
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL))
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan for the given [[Table]]
   * with the given sink table name.
   */
  def verifyRelPlanInsert(table: Table, sink: TableSink[_], targetPath: String): Unit = {
    val stmtSet = getTableEnv.createStatementSet()
    getTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(targetPath, sink)
    stmtSet.addInsert(targetPath, table)
    verifyRelPlan(stmtSet)
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan for the given [[Table]]
   * with the given sink table name. The plans will contain the extra [[ExplainDetail]]s.
   */
  def verifyRelPlanInsert(
      table: Table,
      sink: TableSink[_],
      targetPath: String,
      extraDetails: ExplainDetail*): Unit = {
    val stmtSet = getTableEnv.createStatementSet()
    getTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(targetPath, sink)
    stmtSet.addInsert(targetPath, table)
    verifyRelPlan(stmtSet, extraDetails: _*)
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan
   * for the given [[StatementSet]].
   */
  def verifyRelPlan(stmtSet: StatementSet): Unit = {
    doVerifyPlan(
      stmtSet,
      Array.empty[ExplainDetail],
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL),
      () => Unit)
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan
   * for the given [[StatementSet]]. The plans will contain the extra [[ExplainDetail]]s.
   */
  def verifyRelPlan(stmtSet: StatementSet, extraDetails: ExplainDetail*): Unit = {
    doVerifyPlan(
      stmtSet,
      extraDetails.toArray,
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_REL),
      () => Unit)
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan for the given SELECT query.
   * The rel plans will contain the output type ([[org.apache.calcite.rel.type.RelDataType]]).
   */
  def verifyRelPlanWithType(query: String): Unit = {
    doVerifyPlan(
      query,
      Array.empty[ExplainDetail],
      withRowType = true,
      Array(PlanKind.AST, PlanKind.OPT_REL))
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan for the given [[Table]].
   * The rel plans will contain the output type ([[org.apache.calcite.rel.type.RelDataType]]).
   */
  def verifyRelPlanWithType(table: Table): Unit = {
    doVerifyPlan(
      table,
      Array.empty[ExplainDetail],
      withRowType = true,
      Array(PlanKind.AST, PlanKind.OPT_REL))
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized rel plan
   * for the given [[StatementSet]].
   * The rel plans will contain the output type ([[org.apache.calcite.rel.type.RelDataType]]).
   */
  def verifyRelPlanWithType(stmtSet: StatementSet): Unit = {
    doVerifyPlan(
      stmtSet,
      Array.empty[ExplainDetail],
      withRowType = true,
      Array(PlanKind.AST, PlanKind.OPT_REL),
      () => Unit)
  }

  /**
   * Verify whether the optimized rel plan for the given SELECT query
   * does not contain the `notExpected` strings.
   */
  def verifyRelPlanNotExpected(query: String, notExpected: String*): Unit = {
    verifyRelPlanNotExpected(getTableEnv.sqlQuery(query), notExpected: _*)
  }

  /**
   * Verify whether the optimized rel plan for the given [[Table]]
   * does not contain the `notExpected` strings.
   */
  def verifyRelPlanNotExpected(table: Table, notExpected: String*): Unit = {
    require(notExpected.nonEmpty)
    val relNode = TableTestUtil.toRelNode(table)
    val optimizedRel = getPlanner.optimize(relNode)
    val optimizedPlan = getOptimizedRelPlan(Array(optimizedRel), Array.empty, withRowType = false)
    val result = notExpected.forall(!optimizedPlan.contains(_))
    val message = s"\nactual plan:\n$optimizedPlan\nnot expected:\n${notExpected.mkString(", ")}"
    assertTrue(message, result)
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized exec plan for the given SELECT query.
   * Note: An exception will be thrown if the given sql can't be translated to exec plan.
   */
  def verifyExecPlan(query: String): Unit = {
    doVerifyPlan(
      query,
      Array.empty[ExplainDetail],
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_EXEC))
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized exec plan
   * for the given INSERT statement.
   * Note: An exception will be thrown if the given sql can't be translated to exec plan.
   */
  def verifyExecPlanInsert(insert: String): Unit = {
    doVerifyPlanInsert(
      insert,
      Array.empty[ExplainDetail],
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_EXEC))
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized exec plan for the given [[Table]].
   * Note: An exception will be thrown if the given sql can't be translated to exec plan.
   */
  def verifyExecPlan(table: Table): Unit = {
    doVerifyPlan(
      table,
      Array.empty[ExplainDetail],
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_EXEC))
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized exec plan
   * for the given [[Table]] with the given sink table name.
   * Note: An exception will be thrown if the given sql can't be translated to exec plan.
   */
  def verifyExecPlanInsert(table: Table, sink: TableSink[_], targetPath: String): Unit = {
    getTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(targetPath, sink)
    val stmtSet = getTableEnv.createStatementSet()
    stmtSet.addInsert(targetPath, table)
    verifyExecPlan(stmtSet)
  }

  /**
   * Verify the AST (abstract syntax tree) and the optimized exec plan
   * for the given [[StatementSet]].
   * Note: An exception will be thrown if the given sql can't be translated to exec plan.
   */
  def verifyExecPlan(stmtSet: StatementSet): Unit = {
    doVerifyPlan(
      stmtSet,
      Array.empty[ExplainDetail],
      withRowType = false,
      Array(PlanKind.AST, PlanKind.OPT_EXEC),
      () => Unit)
  }

  /**
   * Verify the explain result for the given SELECT query. See more about [[Table#explain()]].
   */
  def verifyExplain(query: String): Unit = verifyExplain(getTableEnv.sqlQuery(query))

  /**
   * Verify the explain result for the given SELECT query. The explain result will contain
   * the extra [[ExplainDetail]]s. See more about [[Table#explain()]].
   */
  def verifyExplain(query: String, extraDetails: ExplainDetail*): Unit = {
    val table = getTableEnv.sqlQuery(query)
    verifyExplain(table, extraDetails: _*)
  }

  /**
   * Verify the explain result for the given INSERT statement.
   * See more about [[StatementSet#explain()]].
   */
  def verifyExplainInsert(insert: String): Unit = {
    val statSet = getTableEnv.createStatementSet()
    statSet.addInsertSql(insert)
    verifyExplain(statSet)
  }

  /**
   * Verify the explain result for the given INSERT statement. The explain result will contain
   * the extra [[ExplainDetail]]s. See more about [[StatementSet#explain()]].
   */
  def verifyExplainInsert(insert: String, extraDetails: ExplainDetail*): Unit = {
    val statSet = getTableEnv.createStatementSet()
    statSet.addInsertSql(insert)
    verifyExplain(statSet, extraDetails: _*)
  }

  /**
   * Verify the explain result for the given [[Table]]. See more about [[Table#explain()]].
   */
  def verifyExplain(table: Table): Unit = {
    doVerifyExplain(table.explain(), needReplaceEstimatedCost = false)
  }

  /**
   * Verify the explain result for the given [[Table]]. The explain result will contain
   * the extra [[ExplainDetail]]s. See more about [[Table#explain()]].
   */
  def verifyExplain(table: Table, extraDetails: ExplainDetail*): Unit = {
    doVerifyExplain(
      table.explain(extraDetails: _*),
      extraDetails.contains(ExplainDetail.ESTIMATED_COST))
  }

  /**
   * Verify the explain result for the given [[Table]] with the given sink table name.
   * See more about [[StatementSet#explain()]].
   */
  def verifyExplainInsert(table: Table, sink: TableSink[_], targetPath: String): Unit = {
    getTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(targetPath, sink)
    val stmtSet = getTableEnv.createStatementSet()
    stmtSet.addInsert(targetPath, table)
    verifyExplain(stmtSet)
  }

  /**
   * Verify the explain result for the given [[Table]] with the given sink table name.
   * The explain result will contain the extra [[ExplainDetail]]s.
   * See more about [[StatementSet#explain()]].
   */
  def verifyExplainInsert(
      table: Table,
      sink: TableSink[_],
      targetPath: String,
      extraDetails: ExplainDetail*): Unit = {
    getTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(targetPath, sink)
    val stmtSet = getTableEnv.createStatementSet()
    stmtSet.addInsert(targetPath, table)
    verifyExplain(stmtSet, extraDetails: _*)
  }

  /**
   * Verify the explain result for the given [[StatementSet]].
   * See more about [[StatementSet#explain()]].
   */
  def verifyExplain(stmtSet: StatementSet): Unit = {
    doVerifyExplain(stmtSet.explain(), needReplaceEstimatedCost = false)
  }

  /**
   * Verify the explain result for the given [[StatementSet]]. The explain result will contain
   * the extra [[ExplainDetail]]s. See more about [[StatementSet#explain()]].
   */
  def verifyExplain(stmtSet: StatementSet, extraDetails: ExplainDetail*): Unit = {
    doVerifyExplain(
      stmtSet.explain(extraDetails: _*),
      extraDetails.contains(ExplainDetail.ESTIMATED_COST))
  }

  /**
   * Verify the given query and the expected plans translated from the SELECT query.
   *
   * @param query the SELECT query to check
   * @param extraDetails the extra [[ExplainDetail]]s the plans should contain
   * @param withRowType whether the rel plans contain the output type
   * @param expectedPlans the expected [[PlanKind]]s to check
   */
  def doVerifyPlan(
      query: String,
      extraDetails: Array[ExplainDetail],
      withRowType: Boolean,
      expectedPlans: Array[PlanKind]): Unit = {
    val table = getTableEnv.sqlQuery(query)
    val relNode = TableTestUtil.toRelNode(table)

    assertPlanEquals(
      Array(relNode),
      extraDetails,
      withRowType,
      expectedPlans,
      () => assertEqualsOrExpand("sql", query))
  }

  /**
   * Verify the given query and the expected plans translated from the INSERT statement.
   *
   * @param insert the INSERT statement to check
   * @param extraDetails the extra [[ExplainDetail]]s the plans should contain
   * @param withRowType whether the rel plans contain the output type
   * @param expectedPlans the expected [[PlanKind]]s to check
   */
  def doVerifyPlanInsert(
      insert: String,
      extraDetails: Array[ExplainDetail],
      withRowType: Boolean,
      expectedPlans: Array[PlanKind]): Unit = {
    val stmtSet = getTableEnv.createStatementSet()
    stmtSet.addInsertSql(insert)
    doVerifyPlan(
      stmtSet,
      extraDetails,
      withRowType,
      expectedPlans,
      () => assertEqualsOrExpand("sql", insert))
  }

  /**
   * Verify the expected plans translated from the given [[Table]].
   *
   * @param table the [[Table]] to check
   * @param extraDetails the extra [[ExplainDetail]]s the plans should contain
   * @param withRowType whether the rel plans contain the output type
   * @param expectedPlans the expected [[PlanKind]]s to check
   */
  def doVerifyPlan(
      table: Table,
      extraDetails: Array[ExplainDetail],
      withRowType: Boolean = false,
      expectedPlans: Array[PlanKind]): Unit = {
    val relNode = TableTestUtil.toRelNode(table)
    assertPlanEquals(Array(relNode), extraDetails, withRowType, expectedPlans, () => {})
  }

  /**
   * Verify the expected plans translated from the given [[StatementSet]].
   *
   * @param stmtSet the [[StatementSet]] to check
   * @param extraDetails the extra [[ExplainDetail]]s the plans should contain
   * @param withRowType whether the rel plans contain the output type
   * @param expectedPlans the expected [[PlanKind]]s to check
   * @param assertSqlEqualsOrExpandFunc the function to check whether the sql equals to the expected
   * if the `stmtSet` is only translated from sql
   */
  def doVerifyPlan(
      stmtSet: StatementSet,
      extraDetails: Array[ExplainDetail],
      withRowType: Boolean,
      expectedPlans: Array[PlanKind],
      assertSqlEqualsOrExpandFunc: () => Unit): Unit = {
    val testStmtSet = stmtSet.asInstanceOf[TestingStatementSet]

    val relNodes = testStmtSet.getOperations.map(getPlanner.translateToRel)
    if (relNodes.isEmpty) {
      throw new TableException("No output table have been created yet. " +
        "A program needs at least one output table that consumes data.\n" +
        "Please create output table(s) for your program")
    }

    assertPlanEquals(
      relNodes.toArray,
      extraDetails,
      withRowType,
      expectedPlans,
      assertSqlEqualsOrExpandFunc)
  }

  /**
   * Verify the expected plans translated from the given [[RelNode]]s.
   *
   * @param relNodes the original (un-optimized) [[RelNode]]s to check
   * @param extraDetails the extra [[ExplainDetail]]s the plans should contain
   * @param withRowType whether the rel plans contain the output type
   * @param expectedPlans the expected [[PlanKind]]s to check
   * @param assertSqlEqualsOrExpandFunc the function to check whether the sql equals to the expected
   * if the `relNodes` are translated from sql
   */
  private def assertPlanEquals(
      relNodes: Array[RelNode],
      extraDetails: Array[ExplainDetail],
      withRowType: Boolean,
      expectedPlans: Array[PlanKind],
      assertSqlEqualsOrExpandFunc: () => Unit): Unit = {

    // build ast plan
    val astBuilder = new StringBuilder
    relNodes.foreach { sink =>
      astBuilder
        .append(System.lineSeparator)
        .append(FlinkRelOptUtil.toString(
          sink, SqlExplainLevel.EXPPLAN_ATTRIBUTES, withRowType = withRowType))
    }
    val astPlan = astBuilder.toString()

    // build optimized rel plan
    val optimizedRels = getPlanner.optimize(relNodes)
    val optimizedRelPlan = System.lineSeparator +
      getOptimizedRelPlan(optimizedRels.toArray, extraDetails, withRowType = withRowType)

    // build optimized exec plan if `expectedPlans` contains OPT_EXEC
    val optimizedExecPlan = if (expectedPlans.contains(PlanKind.OPT_EXEC)) {
      val execGraph = getPlanner.translateToExecNodeGraph(optimizedRels)
      System.lineSeparator + ExecNodePlanDumper.dagToString(execGraph)
    } else {
      ""
    }

    // check whether the sql equals to the expected if the `relNodes` are translated from sql
    assertSqlEqualsOrExpandFunc()
    // check ast plan
    if (expectedPlans.contains(PlanKind.AST)) {
      assertEqualsOrExpand("ast", astPlan)
    }
    // check optimized rel plan
    if (expectedPlans.contains(PlanKind.OPT_REL)) {
      assertEqualsOrExpand("optimized rel plan", optimizedRelPlan, expand = false)
    }
    // check optimized exec plan
    if (expectedPlans.contains(PlanKind.OPT_EXEC)) {
      assertEqualsOrExpand("optimized exec plan", optimizedExecPlan, expand = false)
    }
  }

  private def doVerifyExplain(explainResult: String, needReplaceEstimatedCost: Boolean): Unit = {
    val actual = if (needReplaceEstimatedCost) {
      replaceEstimatedCost(explainResult)
    } else {
      explainResult
    }
    assertEqualsOrExpand("explain", TableTestUtil.replaceStageId(actual), expand = false)
  }

  protected def getOptimizedRelPlan(
      optimizedRels: Array[RelNode],
      extraDetails: Array[ExplainDetail],
      withRowType: Boolean): String = {
    require(optimizedRels.nonEmpty)
    val explainLevel = if (extraDetails.contains(ExplainDetail.ESTIMATED_COST)) {
      SqlExplainLevel.ALL_ATTRIBUTES
    } else {
      SqlExplainLevel.EXPPLAN_ATTRIBUTES
    }
    val withChangelogTraits = extraDetails.contains(ExplainDetail.CHANGELOG_MODE)

    val optimizedPlan = optimizedRels.head match {
      case _: RelNode =>
        optimizedRels.map { rel =>
          FlinkRelOptUtil.toString(
            rel,
            detailLevel = explainLevel,
            withChangelogTraits = withChangelogTraits,
            withRowType = withRowType)
        }.mkString("\n")
      case o =>
        throw new TableException("The expected optimized plan is RelNode plan, " +
          s"actual plan is ${o.getClass.getSimpleName} plan.")
    }
    replaceEstimatedCost(optimizedPlan)
  }

  /**
   * Replace the estimated costs for the given plan, because it may be unstable.
   */
  protected def replaceEstimatedCost(s: String): String = {
    var str = s.replaceAll("\\r\\n", "\n")
    val scientificFormRegExpr = "[+-]?[\\d]+([\\.][\\d]*)?([Ee][+-]?[0-9]{0,2})?"
    str = str.replaceAll(s"rowcount = $scientificFormRegExpr", "rowcount = ")
    str = str.replaceAll(s"$scientificFormRegExpr rows", "rows")
    str = str.replaceAll(s"$scientificFormRegExpr cpu", "cpu")
    str = str.replaceAll(s"$scientificFormRegExpr io", "io")
    str = str.replaceAll(s"$scientificFormRegExpr network", "network")
    str = str.replaceAll(s"$scientificFormRegExpr memory", "memory")
    str
  }

  protected def assertEqualsOrExpand(tag: String, actual: String, expand: Boolean = true): Unit = {
    val expected = s"$${$tag}"
    if (!expand) {
      diffRepository.assertEquals(test.name.getMethodName, tag, expected, actual)
      return
    }
    val expanded = diffRepository.expand(test.name.getMethodName, tag, expected)
    if (expanded != null && !expanded.equals(expected)) {
      // expected does exist, check result
      diffRepository.assertEquals(test.name.getMethodName, tag, expected, actual)
    } else {
      // expected does not exist, update
      diffRepository.expand(test.name.getMethodName, tag, actual)
    }
  }
}

abstract class TableTestUtil(
    test: TableTestBase,
    // determines if the table environment should work in a batch or streaming mode
    isStreamingMode: Boolean,
    catalogManager: Option[CatalogManager] = None,
    val tableConfig: TableConfig)
  extends TableTestUtilBase(test, isStreamingMode) {
  protected val testingTableEnv: TestingTableEnvironment =
    TestingTableEnvironment.create(setting, catalogManager, tableConfig)
  val tableEnv: TableEnvironment = testingTableEnv
  tableEnv.getConfig.getConfiguration.setString(
    ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
    GlobalDataExchangeMode.ALL_EDGES_PIPELINED.toString)

  private val env: StreamExecutionEnvironment = getPlanner.getExecEnv

  override def getTableEnv: TableEnvironment = tableEnv

  def getStreamEnv: StreamExecutionEnvironment = env

  /**
   * Create a [[TestTableSource]] with the given schema, table stats and unique keys,
   * and registers this TableSource under given name into the TableEnvironment's catalog.
   *
   * @param name table name
   * @param types field types
   * @param fields field names
   * @param statistic statistic of current table
   * @return returns the registered [[Table]].
   */
  def addTableSource(
      name: String,
      types: Array[TypeInformation[_]],
      fields: Array[String],
      statistic: FlinkStatistic = FlinkStatistic.UNKNOWN): Table = {
    val schema = new TableSchema(fields, types)
    val tableSource = new TestTableSource(isBounded, schema)
    addTableSource(name, tableSource, statistic)
  }

  /**
   * Register this TableSource under given name into the TableEnvironment's catalog.
   *
   * @param name table name
   * @param tableSource table source
   * @param statistic statistic of current table
   * @return returns the registered [[Table]].
   */
  def addTableSource(
      name: String,
      tableSource: TableSource[_],
      statistic: FlinkStatistic): Table = {
    // TODO RichTableSourceQueryOperation should be deleted and use registerTableSourceInternal
    //  method instead of registerTable method here after unique key in TableSchema is ready
    //  and setting catalog statistic to TableSourceTable in DatabaseCalciteSchema is ready
    val identifier = ObjectIdentifier.of(
      testingTableEnv.getCurrentCatalog,
      testingTableEnv.getCurrentDatabase,
      name)
    val operation = new RichTableSourceQueryOperation(
      identifier,
      tableSource,
      statistic)
    val table = testingTableEnv.createTable(operation)
    testingTableEnv.registerTable(name, table)
    testingTableEnv.from(name)
  }

  /**
   * @deprecated Use [[addTemporarySystemFunction()]] for the new type inference.
   */
  @deprecated
  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T]): Unit = testingTableEnv.registerFunction(name, function)

  /**
   * @deprecated Use [[addTemporarySystemFunction()]] for the new type inference.
   */
  @deprecated
  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = testingTableEnv.registerFunction(name, function)

  /**
   * @deprecated Use [[addTemporarySystemFunction()]] for the new type inference.
   */
  @deprecated
  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: TableAggregateFunction[T, ACC]): Unit = {
    testingTableEnv.registerFunction(name, function)
  }
}

abstract class ScalaTableTestUtil(
    test: TableTestBase,
    isStreamingMode: Boolean)
  extends TableTestUtilBase(test, isStreamingMode) {
  // scala env
  val env = new ScalaStreamExecEnv(new LocalStreamEnvironment())
  // scala tableEnv
  val tableEnv: ScalaStreamTableEnv = ScalaStreamTableEnv.create(env, setting)

  override def getTableEnv: TableEnvironment = tableEnv

  /**
   * Registers a [[TableFunction]] under given name into the TableEnvironment's catalog.
   */
  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T]): Unit = tableEnv.registerFunction(name, function)

  /**
   * Registers a [[AggregateFunction]] under given name into the TableEnvironment's catalog.
   */
  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = tableEnv.registerFunction(name, function)

  /**
   * Registers a [[TableAggregateFunction]] under given name into the TableEnvironment's catalog.
   */
  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: TableAggregateFunction[T, ACC]): Unit = tableEnv.registerFunction(name, function)
}

abstract class JavaTableTestUtil(
    test: TableTestBase,
    isStreamingMode: Boolean)
  extends TableTestUtilBase(test, isStreamingMode) {
  // java env
  val env = new LocalStreamEnvironment()
  // java tableEnv
  // use impl class instead of interface class to avoid
  // "Static methods in interface require -target:jvm-1.8"
  val tableEnv: JavaStreamTableEnv = JavaStreamTableEnvImpl.create(env, setting, new TableConfig)

  override def getTableEnv: TableEnvironment = tableEnv

  /**
   * Registers a [[TableFunction]] under given name into the TableEnvironment's catalog.
   */
  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T]): Unit = tableEnv.registerFunction(name, function)

  /**
   * Registers a [[AggregateFunction]] under given name into the TableEnvironment's catalog.
   */
  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = tableEnv.registerFunction(name, function)

  /**
   * Registers a [[TableAggregateFunction]] under given name into the TableEnvironment's catalog.
   */
  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: TableAggregateFunction[T, ACC]): Unit = tableEnv.registerFunction(name, function)
}

/**
 * Utility for stream table test.
 */
case class StreamTableTestUtil(
    test: TableTestBase,
    catalogManager: Option[CatalogManager] = None,
    conf: TableConfig = new TableConfig)
  extends TableTestUtil(test, isStreamingMode = true, catalogManager, conf) {

  /**
   * Register a table with specific row time field and offset.
   *
   * @param tableName table name
   * @param sourceTable table to register
   * @param rowtimeField row time field
   * @param offset offset to the row time field value
   */
  def addTableWithWatermark(
      tableName: String,
      sourceTable: Table,
      rowtimeField: String,
      offset: Long): Unit = {
    val sourceRel = TableTestUtil.toRelNode(sourceTable)
    val rowtimeFieldIdx = sourceRel.getRowType.getFieldNames.indexOf(rowtimeField)
    if (rowtimeFieldIdx < 0) {
      throw new TableException(s"$rowtimeField does not exist, please check it")
    }
    val rexBuilder = sourceRel.getCluster.getRexBuilder
    val inputRef = rexBuilder.makeInputRef(sourceRel, rowtimeFieldIdx)
    val offsetLiteral = rexBuilder.makeIntervalLiteral(
      JBigDecimal.valueOf(offset),
      new SqlIntervalQualifier(TimeUnit.MILLISECOND, null, SqlParserPos.ZERO))
    val expr = rexBuilder.makeCall(FlinkSqlOperatorTable.MINUS, inputRef, offsetLiteral)
    val watermarkAssigner = new LogicalWatermarkAssigner(
      sourceRel.getCluster,
      sourceRel.getTraitSet,
      sourceRel,
      rowtimeFieldIdx,
      expr
    )
    val queryOperation = new PlannerQueryOperation(watermarkAssigner)
    testingTableEnv.registerTable(tableName, testingTableEnv.createTable(queryOperation))
  }

  def buildStreamProgram(firstProgramNameToRemove: String): Unit = {
    val program = FlinkStreamProgram.buildProgram(tableEnv.getConfig.getConfiguration)
    var startRemove = false
    program.getProgramNames.foreach {
      name =>
        if (name.equals(firstProgramNameToRemove)) {
          startRemove = true
        }
        if (startRemove) {
          program.remove(name)
        }
    }
    replaceStreamProgram(program)
  }

  def replaceStreamProgram(program: FlinkChainedProgram[StreamOptimizeContext]): Unit = {
    var calciteConfig = TableConfigUtils.getCalciteConfig(tableEnv.getConfig)
    calciteConfig = CalciteConfig.createBuilder(calciteConfig)
      .replaceStreamProgram(program).build()
    tableEnv.getConfig.setPlannerConfig(calciteConfig)
  }

  def getStreamProgram(): FlinkChainedProgram[StreamOptimizeContext] = {
    val tableConfig = tableEnv.getConfig
    val calciteConfig = TableConfigUtils.getCalciteConfig(tableConfig)
    calciteConfig.getStreamProgram.getOrElse(FlinkStreamProgram.buildProgram(
      tableConfig.getConfiguration))
  }

  def enableMiniBatch(): Unit = {
    tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    tableEnv.getConfig.getConfiguration.set(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
    tableEnv.getConfig.getConfiguration.setLong(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 3L)
  }

  def createAppendTableSink(
      fieldNames: Array[String],
      fieldTypes: Array[LogicalType]): AppendStreamTableSink[Row] = {
    require(fieldNames.length == fieldTypes.length)
    val typeInfos = fieldTypes.map(fromLogicalTypeToTypeInfo)
    new TestingAppendTableSink().configure(fieldNames, typeInfos)
  }

  def createUpsertTableSink(
      keys: Array[Int],
      fieldNames: Array[String],
      fieldTypes: Array[LogicalType]): UpsertStreamTableSink[RowData] = {
    require(fieldNames.length == fieldTypes.length)
    val typeInfos = fieldTypes.map(fromLogicalTypeToTypeInfo)
    new TestingUpsertTableSink(keys).configure(fieldNames, typeInfos)
  }

  def createRetractTableSink(
      fieldNames: Array[String],
      fieldTypes: Array[LogicalType]): RetractStreamTableSink[Row] = {
    require(fieldNames.length == fieldTypes.length)
    val typeInfos = fieldTypes.map(fromLogicalTypeToTypeInfo)
    new TestingRetractTableSink().configure(fieldNames, typeInfos)
  }
}

/**
 * Utility for stream scala table test.
 */
case class ScalaStreamTableTestUtil(test: TableTestBase) extends ScalaTableTestUtil(test, true) {
}

/**
 * Utility for stream java table test.
 */
case class JavaStreamTableTestUtil(test: TableTestBase) extends JavaTableTestUtil(test, true) {
}

/**
 * Utility for batch table test.
 */
case class BatchTableTestUtil(
    test: TableTestBase,
    catalogManager: Option[CatalogManager] = None,
    conf: TableConfig = new TableConfig)
  extends TableTestUtil(test, isStreamingMode = false, catalogManager, conf) {

  def buildBatchProgram(firstProgramNameToRemove: String): Unit = {
    val program = FlinkBatchProgram.buildProgram(tableEnv.getConfig.getConfiguration)
    var startRemove = false
    program.getProgramNames.foreach {
      name =>
        if (name.equals(firstProgramNameToRemove)) {
          startRemove = true
        }
        if (startRemove) {
          program.remove(name)
        }
    }
    replaceBatchProgram(program)
  }

  def replaceBatchProgram(program: FlinkChainedProgram[BatchOptimizeContext]): Unit = {
    var calciteConfig = TableConfigUtils.getCalciteConfig(tableEnv.getConfig)
    calciteConfig = CalciteConfig.createBuilder(calciteConfig)
      .replaceBatchProgram(program).build()
    tableEnv.getConfig.setPlannerConfig(calciteConfig)
  }

  def getBatchProgram(): FlinkChainedProgram[BatchOptimizeContext] = {
    val tableConfig = tableEnv.getConfig
    val calciteConfig = TableConfigUtils.getCalciteConfig(tableConfig)
    calciteConfig.getBatchProgram.getOrElse(FlinkBatchProgram.buildProgram(
      tableConfig.getConfiguration))
  }

  def createCollectTableSink(
      fieldNames: Array[String],
      fieldTypes: Array[LogicalType]): TableSink[Row] = {
    require(fieldNames.length == fieldTypes.length)
    val typeInfos = fieldTypes.map(fromLogicalTypeToTypeInfo)
    new CollectRowTableSink().configure(fieldNames, typeInfos)
  }
}

/**
 * Utility for batch scala table test.
 */
case class ScalaBatchTableTestUtil(test: TableTestBase) extends ScalaTableTestUtil(test, false) {
}

/**
 * Utility for batch java table test.
 */
case class JavaBatchTableTestUtil(test: TableTestBase) extends JavaTableTestUtil(test, false) {
}

/**
 * Batch/Stream [[org.apache.flink.table.sources.TableSource]] for testing.
 */
class TestTableSource(override val isBounded: Boolean, schema: TableSchema)
  extends StreamTableSource[Row] {

  override def getDataStream(execEnv: environment.StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.fromCollection(List[Row](), getReturnType)
  }

  override def getReturnType: TypeInformation[Row] = {
    val logicalTypes = schema.getFieldTypes
    new RowTypeInfo(logicalTypes, schema.getFieldNames)
  }

  override def getTableSchema: TableSchema = schema
}

object TestTableSource {
  def createTemporaryTable(
      tEnv: TableEnvironment,
      isBounded: Boolean,
      tableSchema: TableSchema,
      tableName: String): Unit = {
    tEnv.connect(
      new CustomConnectorDescriptor("TestTableSource", 1, false)
        .property("is-bounded", if (isBounded) "true" else "false"))
      .withSchema(new Schema().schema(tableSchema))
      .createTemporaryTable(tableName)

  }
}

class TestTableSourceFactory extends StreamTableSourceFactory[Row] {
  override def createStreamTableSource(
      properties: util.Map[String, String]): StreamTableSource[Row] = {
    val dp = new DescriptorProperties
    dp.putProperties(properties)
    val tableSchema = dp.getTableSchema(SCHEMA)
    val isBounded = dp.getOptionalBoolean("is-bounded").orElse(false)
    new TestTableSource(isBounded, tableSchema)
  }

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestTableSource")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()
    properties.add("*")
    properties
  }
}

class TestingTableEnvironment private(
    catalogManager: CatalogManager,
    moduleManager: ModuleManager,
    tableConfig: TableConfig,
    executor: Executor,
    functionCatalog: FunctionCatalog,
    planner: PlannerBase,
    isStreamingMode: Boolean,
    userClassLoader: ClassLoader)
  extends TableEnvironmentImpl(
    catalogManager,
    moduleManager,
    tableConfig,
    executor,
    functionCatalog,
    planner,
    isStreamingMode,
    userClassLoader) {

  // just for testing, remove this method while
  // `<T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction);`
  // is added into TableEnvironment
  def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit = {
    val typeInfo = UserDefinedFunctionHelper
      .getReturnTypeOfTableFunction(tf, implicitly[TypeInformation[T]])
    functionCatalog.registerTempSystemTableFunction(
      name,
      tf,
      typeInfo
    )
  }

  // just for testing, remove this method while
  // `<T> void registerFunction(String name, TableFunction<T> tableFunction);`
  // is added into TableEnvironment
  def registerFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      f: AggregateFunction[T, ACC]): Unit = {
    registerImperativeAggregateFunction(name, f)
  }

  // just for testing, remove this method while
  // `<T, ACC> void registerFunction(String name, TableAggregateFunction<T, ACC> tableAggFunc);`
  // is added into TableEnvironment
  def registerFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      f: TableAggregateFunction[T, ACC]): Unit = {
    registerImperativeAggregateFunction(name, f)
  }

  private def registerImperativeAggregateFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      f: ImperativeAggregateFunction[T, ACC]): Unit = {
    val typeInfo = UserDefinedFunctionHelper
      .getReturnTypeOfAggregateFunction(f, implicitly[TypeInformation[T]])
    val accTypeInfo = UserDefinedFunctionHelper
      .getAccumulatorTypeOfAggregateFunction(f, implicitly[TypeInformation[ACC]])
    functionCatalog.registerTempSystemAggregateFunction(
      name,
      f,
      typeInfo,
      accTypeInfo
    )
  }

  override def createTable(tableOperation: QueryOperation): TableImpl = {
    super.createTable(tableOperation)
  }

  override def createStatementSet(): StatementSet = new TestingStatementSet(this)
}

class TestingStatementSet(tEnv: TestingTableEnvironment) extends StatementSet {

  private val operations: util.List[ModifyOperation] = new util.ArrayList[ModifyOperation]

  def getOperations: util.List[ModifyOperation] = operations

  override def addInsertSql(statement: String): StatementSet = {
    val operations = tEnv.getParser.parse(statement)

    if (operations.size != 1) {
      throw new TableException("Only single statement is supported.")
    }

    operations.get(0) match {
      case op: ModifyOperation =>
        this.operations.add(op)
      case _ =>
        throw new TableException("Only insert statement is supported now.")
    }
    this
  }

  override def addInsert(targetPath: String, table: Table): StatementSet = {
    this.addInsert(targetPath, table, overwrite = false)
  }

  override def addInsert(targetPath: String, table: Table, overwrite: Boolean): StatementSet = {
    val unresolvedIdentifier = tEnv.getParser.parseIdentifier(targetPath)
    val objectIdentifier = tEnv.getCatalogManager.qualifyIdentifier(unresolvedIdentifier)

    operations.add(new CatalogSinkModifyOperation(
      objectIdentifier,
      table.getQueryOperation,
      util.Collections.emptyMap[String, String],
      overwrite,
      util.Collections.emptyMap[String, String]))
    this
  }

  override def explain(extraDetails: ExplainDetail*): String = {
    tEnv.explainInternal(operations.map(o => o.asInstanceOf[Operation]), extraDetails: _*)
  }

  override def execute(): TableResult = {
    try {
      tEnv.executeInternal(operations)
    } finally {
      operations.clear()
    }
  }
}

object TestingTableEnvironment {

  def create(
      settings: EnvironmentSettings,
      catalogManager: Option[CatalogManager] = None,
      tableConfig: TableConfig): TestingTableEnvironment = {

    // temporary solution until FLINK-15635 is fixed
    val classLoader = Thread.currentThread.getContextClassLoader

    val moduleManager = new ModuleManager

    val catalogMgr = catalogManager match {
      case Some(c) => c
      case _ =>
        CatalogManager.newBuilder
          .classLoader(classLoader)
          .config(tableConfig.getConfiguration)
          .defaultCatalog(
            settings.getBuiltInCatalogName,
            new GenericInMemoryCatalog(
              settings.getBuiltInCatalogName,
              settings.getBuiltInDatabaseName))
          .build
    }

    val functionCatalog = new FunctionCatalog(tableConfig, catalogMgr, moduleManager)

    val executorProperties = settings.toExecutorProperties
    val executor = ComponentFactoryService.find(classOf[ExecutorFactory],
      executorProperties).create(executorProperties)

    val plannerProperties = settings.toPlannerProperties
    val planner = ComponentFactoryService.find(classOf[PlannerFactory], plannerProperties)
      .create(plannerProperties, executor, tableConfig, functionCatalog, catalogMgr)
      .asInstanceOf[PlannerBase]

    new TestingTableEnvironment(
      catalogMgr,
      moduleManager,
      tableConfig,
      executor,
      functionCatalog,
      planner,
      settings.isStreamingMode,
      classLoader)
  }
}

/**
 * [[PlanKind]] defines the types of plans to check in test cases.
 */
object PlanKind extends Enumeration {
  type PlanKind = Value
  /** Abstract Syntax Tree */
  val AST: Value = Value("AST")
  /** Optimized Rel Plan */
  val OPT_REL: Value = Value("OPT_REL")
  /** Optimized Execution Plan */
  val OPT_EXEC: Value = Value("OPT_EXEC")
}

object TableTestUtil {

  val STREAM_SETTING: EnvironmentSettings =
    EnvironmentSettings.newInstance().inStreamingMode().build()
  val BATCH_SETTING: EnvironmentSettings = EnvironmentSettings.newInstance().inBatchMode().build()

  /**
   * Converts operation tree in the given table to a RelNode tree.
   */
  def toRelNode(table: Table): RelNode = {
    table.asInstanceOf[TableImpl]
      .getTableEnvironment.asInstanceOf[TableEnvironmentImpl]
      .getPlanner.asInstanceOf[PlannerBase]
      .getRelBuilder.queryOperation(table.getQueryOperation).build()
  }

  def createTemporaryView[T](
      tEnv: TableEnvironment,
      name: String,
      dataStream: DataStream[T],
      fields: Option[Array[Expression]] = None,
      fieldNullables: Option[Array[Boolean]] = None,
      statistic: Option[FlinkStatistic] = None): Unit = {
    val planner = tEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[PlannerBase]
    val execEnv = planner.getExecEnv
    val streamType = dataStream.getType
    // get field names and types for all non-replaced fields
    val typeInfoSchema = fields.map((f: Array[Expression]) => {
      val fieldsInfo = FieldInfoUtils.getFieldsInfo(streamType, f)
      // check if event-time is enabled
      if (fieldsInfo.isRowtimeDefined &&
        (execEnv.getStreamTimeCharacteristic ne TimeCharacteristic.EventTime)) {
        throw new ValidationException(String.format(
          "A rowtime attribute requires an EventTime time characteristic in stream " +
            "environment. But is: %s",
          execEnv.getStreamTimeCharacteristic))
      }
      fieldsInfo
    }).getOrElse(FieldInfoUtils.getFieldsInfo(streamType))

    val fieldCnt = typeInfoSchema.getFieldTypes.length
    val dataStreamQueryOperation = new DataStreamQueryOperation(
      ObjectIdentifier.of(tEnv.getCurrentCatalog, tEnv.getCurrentDatabase, name),
      dataStream,
      typeInfoSchema.getIndices,
      typeInfoSchema.toTableSchema,
      fieldNullables.getOrElse(Array.fill(fieldCnt)(true)),
      statistic.getOrElse(FlinkStatistic.UNKNOWN)
    )
    val table = createTable(tEnv, dataStreamQueryOperation)
    tEnv.registerTable(name, table)
  }

  def createTable(tEnv: TableEnvironment, queryOperation: QueryOperation): Table = {
    val createTableMethod = tEnv match {
      case _: ScalaStreamTableEnvImpl | _: JavaStreamTableEnvImpl =>
        tEnv.getClass.getSuperclass.getDeclaredMethod("createTable", classOf[QueryOperation])
      case t: TableEnvironmentImpl =>
        t.getClass.getDeclaredMethod("createTable", classOf[QueryOperation])
      case _ => throw new TableException(s"Unsupported class: ${tEnv.getClass.getCanonicalName}")
    }
    createTableMethod.setAccessible(true)
    createTableMethod.invoke(tEnv, queryOperation).asInstanceOf[Table]
  }

  def readFromResource(path: String): String = {
    val inputStream = getClass.getResource(path).getFile
    val source = Source.fromFile(inputStream)
    val str = source.mkString
    source.close()
    str
  }

  @throws[IOException]
  def getFormattedJson(json: String): String = {
    val parser = new ObjectMapper().getFactory.createParser(json)
    val jsonNode: JsonNode = parser.readValueAsTree[JsonNode]
    jsonNode.toString
  }

  /**
   * Stage {id} is ignored, because id keeps incrementing in test class
   * while StreamExecutionEnvironment is up
   */
  def replaceStageId(s: String): String = {
    s.replaceAll("\\r\\n", "\n").replaceAll("Stage \\d+", "")
  }

  /**
   * Stream node {id} is ignored, because id keeps incrementing in test class
   * while StreamExecutionEnvironment is up
   */
  def replaceStreamNodeId(s: String): String = {
    s.replaceAll("\"id\" : \\d+", "\"id\" : ").trim
  }

  /**
   * ExecNode {id} is ignored, because id keeps incrementing in test class.
   */
  def replaceExecNodeId(s: String): String = {
    s.replaceAll("\"id\":\\d+", "\"id\" :")
      .replaceAll("\"source\":\\d+", "\"source\" :")
      .replaceAll("\"target\":\\d+", "\"target\" :")
  }

  /**
   * Ignore flink version value.
   */
  def replaceFlinkVersion(s: String): String = {
    s.replaceAll("\"flinkVersion\":\"\\d+.\\d+(-SNAPSHOT)?\"", "\"flinkVersion\":\"\"")
  }
}
