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
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.internal.{TableEnvironmentImpl, TableEnvironmentInternal, TableImpl}
import org.apache.flink.table.api.bridge.java.internal.{StreamTableEnvironmentImpl => JavaStreamTableEnvImpl}
import org.apache.flink.table.api.bridge.java.{StreamTableEnvironment => JavaStreamTableEnv}
import org.apache.flink.table.api.bridge.scala.internal.{StreamTableEnvironmentImpl => ScalaStreamTableEnvImpl}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment => ScalaStreamTableEnv}
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
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.optimize.program._
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.plan.utils.{ExecNodePlanDumper, FlinkRelOptUtil}
import org.apache.flink.table.planner.runtime.utils.{TestingAppendTableSink, TestingRetractTableSink, TestingUpsertTableSink}
import org.apache.flink.table.planner.sinks.CollectRowTableSink
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.{StreamTableSource, TableSource}
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.typeutils.FieldInfoUtils
import org.apache.flink.types.Row
import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{SqlExplainLevel, SqlIntervalQualifier}
import org.apache.commons.lang3.SystemUtils
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Rule
import org.junit.rules.{ExpectedException, TemporaryFolder, TestName}
import _root_.java.math.{BigDecimal => JBigDecimal}
import _root_.java.util
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

  def verifyPlan(sql: String): Unit = {
    doVerifyPlan(sql, Array.empty[ExplainDetail], withRowType = false, printPlanBefore = true)
  }

  def verifyPlan(sql: String, extraDetails: ExplainDetail*): Unit = {
    doVerifyPlan(sql, extraDetails.toArray, withRowType = false, printPlanBefore = true)
  }

  def verifyPlan(table: Table): Unit = {
    doVerifyPlan(table, Array.empty[ExplainDetail], withRowType = false, printPlanBefore = true)
  }

  def verifyPlan(table: Table, extraDetails: ExplainDetail*): Unit = {
    doVerifyPlan(table, extraDetails.toArray, withRowType = false, printPlanBefore = true)
  }

  def verifyPlanWithType(sql: String): Unit = {
    doVerifyPlan(sql, Array.empty[ExplainDetail], withRowType = true, printPlanBefore = true)
  }

  def verifyPlanWithType(table: Table): Unit = {
    doVerifyPlan(table, Array.empty[ExplainDetail], withRowType = true, printPlanBefore = true)
  }

  def verifyPlanNotExpected(sql: String, notExpected: String*): Unit = {
    verifyPlanNotExpected(getTableEnv.sqlQuery(sql), notExpected: _*)
  }

  def verifyPlanNotExpected(table: Table, notExpected: String*): Unit = {
    require(notExpected.nonEmpty)
    val relNode = TableTestUtil.toRelNode(table)
    val optimizedPlan = getOptimizedPlan(Array(relNode), Array.empty, withRowType = false)
    val result = notExpected.forall(!optimizedPlan.contains(_))
    val message = s"\nactual plan:\n$optimizedPlan\nnot expected:\n${notExpected.mkString(", ")}"
    assertTrue(message, result)
  }

  def verifyExplain(stmtSet: StatementSet, extraDetails: ExplainDetail*): Unit = {
    doVerifyExplain(
      stmtSet.explain(extraDetails: _*),
      extraDetails.contains(ExplainDetail.ESTIMATED_COST))
  }

  def verifyExplain(sql: String): Unit = verifyExplain(getTableEnv.sqlQuery(sql))

  def verifyExplain(sql: String, extraDetails: ExplainDetail*): Unit = {
    val table = getTableEnv.sqlQuery(sql)
    verifyExplain(table, extraDetails: _*)
  }

  def verifyExplain(table: Table): Unit = {
    doVerifyExplain(table.explain(), needReplaceEstimatedCost = false)
  }

  def verifyExplain(table: Table, extraDetails: ExplainDetail*): Unit = {
    doVerifyExplain(
      table.explain(extraDetails: _*),
      extraDetails.contains(ExplainDetail.ESTIMATED_COST))
  }

  def verifyExplainInsert(
      table: Table,
      sink: TableSink[_],
      targetPath: String,
      extraDetails: ExplainDetail*): Unit = {
    val stmtSet = getTableEnv.createStatementSet()
    getTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(targetPath, sink)
    stmtSet.addInsert(targetPath, table)
    verifyExplain(stmtSet, extraDetails: _*)
  }

  def doVerifyPlan(
      sql: String,
      extraDetails: Array[ExplainDetail],
      withRowType: Boolean,
      printPlanBefore: Boolean): Unit = {
    val table = getTableEnv.sqlQuery(sql)
    val relNode = TableTestUtil.toRelNode(table)
    val optimizedPlan = getOptimizedPlan(Array(relNode), extraDetails, withRowType = withRowType)

    assertEqualsOrExpand("sql", sql)

    if (printPlanBefore) {
      val planBefore = SystemUtils.LINE_SEPARATOR +
        FlinkRelOptUtil.toString(
          relNode,
          SqlExplainLevel.EXPPLAN_ATTRIBUTES,
          withRowType = withRowType)
      assertEqualsOrExpand("planBefore", planBefore)
    }

    val actual = SystemUtils.LINE_SEPARATOR + optimizedPlan
    assertEqualsOrExpand("planAfter", actual.toString, expand = false)
  }

  def verifyResource(sql: String): Unit = {
    assertEqualsOrExpand("sql", sql)
    val table = getTableEnv.sqlQuery(sql)
    doVerifyPlan(
      table,
      Array.empty,
      withRowType = false,
      printResource = true,
      printPlanBefore = false)
  }

  def doVerifyPlan(
      table: Table,
      extraDetails: Array[ExplainDetail],
      withRowType: Boolean,
      printPlanBefore: Boolean): Unit = {
    doVerifyPlan(
      table = table,
      extraDetails,
      withRowType = withRowType,
      printPlanBefore = printPlanBefore,
      printResource = false)
  }

  def doVerifyPlan(
      table: Table,
      extraDetails: Array[ExplainDetail],
      withRowType: Boolean,
      printPlanBefore: Boolean,
      printResource: Boolean): Unit = {
    val relNode = TableTestUtil.toRelNode(table)
    val optimizedPlan = getOptimizedPlan(
      Array(relNode),
      extraDetails,
      withRowType = withRowType,
      withResource = printResource)

    if (printPlanBefore) {
      val planBefore = SystemUtils.LINE_SEPARATOR +
        FlinkRelOptUtil.toString(
          relNode,
          SqlExplainLevel.EXPPLAN_ATTRIBUTES,
          withRowType = withRowType)
      assertEqualsOrExpand("planBefore", planBefore)
    }

    val actual = SystemUtils.LINE_SEPARATOR + optimizedPlan
    assertEqualsOrExpand("planAfter", actual.toString, expand = false)
  }

  private def doVerifyExplain(explainResult: String, needReplaceEstimatedCost: Boolean): Unit = {
    val actual = if (needReplaceEstimatedCost) {
      replaceEstimatedCost(explainResult)
    } else {
      explainResult
    }
    assertEqualsOrExpand("explain", TableTestUtil.replaceStageId(actual), expand = false)
  }

  protected def getOptimizedPlan(
      relNodes: Array[RelNode],
      extraDetails: Array[ExplainDetail],
      withRowType: Boolean,
      withResource: Boolean = false): String = {
    require(relNodes.nonEmpty)
    val planner = getPlanner
    val optimizedRels = planner.optimize(relNodes)
    val explainLevel = if (extraDetails.contains(ExplainDetail.ESTIMATED_COST)) {
      SqlExplainLevel.ALL_ATTRIBUTES
    } else {
      SqlExplainLevel.EXPPLAN_ATTRIBUTES
    }
    val withChangelogTraits = extraDetails.contains(ExplainDetail.CHANGELOG_MODE)

    optimizedRels.head match {
      case _: ExecNode[_, _] =>
        val optimizedNodes = planner.translateToExecNodePlan(optimizedRels)
        require(optimizedNodes.length == optimizedRels.length)
        ExecNodePlanDumper.dagToString(
          optimizedNodes,
          detailLevel = explainLevel,
          withChangelogTraits = withChangelogTraits,
          withOutputType = withRowType,
          withResource = withResource)
      case _ =>
        optimizedRels.map { rel =>
          FlinkRelOptUtil.toString(
            rel,
            detailLevel = explainLevel,
            withChangelogTraits = withChangelogTraits,
            withRowType = withRowType)
        }.mkString("\n")
    }
  }

  /**
    * ignore estimated cost, because it may be unstable.
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
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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

  def verifyPlanInsert(sql: String): Unit = {
    doVerifyPlanInsert(sql, Array.empty, withRowType = false, printPlanBefore = true)
  }

  def verifyPlanInsert(
      table: Table,
      sink: TableSink[_],
      targetPath: String,
      extraDetails: ExplainDetail*): Unit = {
    val stmtSet = tableEnv.createStatementSet()
    tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(targetPath, sink)
    stmtSet.addInsert(targetPath, table)
    verifyPlan(stmtSet, extraDetails: _*)
  }

  def verifyPlan(stmtSet: StatementSet, extraDetails: ExplainDetail*): Unit = {
    doVerifyPlan(stmtSet, extraDetails.toArray, withRowType = false, printPlanBefore = true)
  }

  def doVerifyPlanInsert(
      sql: String,
      extraDetails: Array[ExplainDetail],
      withRowType: Boolean,
      printPlanBefore: Boolean): Unit = {
    assertEqualsOrExpand("sql", sql)
    val stmtSet = tableEnv.createStatementSet()
    stmtSet.addInsertSql(sql)
    doVerifyPlan(stmtSet, extraDetails, withRowType, printPlanBefore)
  }

  def doVerifyPlan(
      stmtSet: StatementSet,
      extraDetails: Array[ExplainDetail],
      withRowType: Boolean,
      printPlanBefore: Boolean): Unit = {
    val testStmtSet = stmtSet.asInstanceOf[TestingStatementSet]

    val relNodes = testStmtSet.getOperations.map(getPlanner.translateToRel)
    if (relNodes.isEmpty) {
      throw new TableException("No output table have been created yet. " +
        "A program needs at least one output table that consumes data.\n" +
        "Please create output table(s) for your program")
    }

    val optimizedPlan = getOptimizedPlan(
      relNodes.toArray,
      extraDetails,
      withRowType = withRowType)

    if (printPlanBefore) {
      val planBefore = new StringBuilder
      relNodes.foreach { sink =>
        planBefore.append(System.lineSeparator)
        planBefore.append(FlinkRelOptUtil.toString(sink, SqlExplainLevel.EXPPLAN_ATTRIBUTES))
      }
      assertEqualsOrExpand("planBefore", planBefore.toString())
    }

    val actual = if (extraDetails.contains(ExplainDetail.ESTIMATED_COST)) {
      SystemUtils.LINE_SEPARATOR + replaceEstimatedCost(optimizedPlan)
    } else {
      SystemUtils.LINE_SEPARATOR + optimizedPlan
    }
    assertEqualsOrExpand("planAfter", actual.toString, expand = false)
  }
}

abstract class ScalaTableTestUtil(
    test: TableTestBase,
    isStreamingMode: Boolean)
  extends TableTestUtilBase(test, isStreamingMode) {
  // scala env
  val env = new ScalaStreamExecEnv(new LocalStreamEnvironment())
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
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
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
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
    Source.fromFile(inputStream).mkString
  }

  /**
    * Stage {id} is ignored, because id keeps incrementing in test class
    * while StreamExecutionEnvironment is up
    */
  def replaceStageId(s: String): String = {
    s.replaceAll("\\r\\n", "\n").replaceAll("Stage \\d+", "")
  }
}
