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
package org.apache.flink.table.util

import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.java.{BatchTableEnvironment => JavaBatchTableEnv, StreamTableEnvironment => JavaStreamTableEnv}
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnv, StreamTableEnvironment => ScalaStreamTableEnv, _}
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.plan.nodes.exec.ExecNode
import org.apache.flink.table.plan.optimize.program.{FlinkBatchProgram, FlinkStreamProgram}
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.plan.stats.{FlinkStatistic, TableStats}
import org.apache.flink.table.plan.util.{ExecNodePlanDumper, FlinkRelOptUtil}
import org.apache.flink.table.runtime.utils.{BatchTableEnvUtil, TestingAppendTableSink, TestingRetractTableSink, TestingUpsertTableSink}
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.table.types.TypeInfoLogicalTypeConverter
import org.apache.flink.table.types.TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.types.Row

import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.commons.lang3.SystemUtils

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Rule
import org.junit.rules.{ExpectedException, TestName}

import _root_.java.util.Optional

import _root_.scala.collection.JavaConversions._

/**
  * Test base for testing Table API / SQL plans.
  */
abstract class TableTestBase {

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  // used for get test case method name
  val testName: TestName = new TestName

  @Rule
  def thrown: ExpectedException = expectedException

  @Rule
  def name: TestName = testName

  def streamTestUtil(): StreamTableTestUtil = StreamTableTestUtil(this)

  def batchTestUtil(): BatchTableTestUtil = BatchTableTestUtil(this)

  def verifyTableEquals(expected: Table, actual: Table): Unit = {
    val expectedString = FlinkRelOptUtil.toString(expected.asInstanceOf[TableImpl].getRelNode)
    val actualString = FlinkRelOptUtil.toString(actual.asInstanceOf[TableImpl].getRelNode)
    assertEquals(
      "Logical plans do not match",
      LogicalPlanFormatUtils.formatTempTableId(expectedString),
      LogicalPlanFormatUtils.formatTempTableId(actualString))
  }
}

abstract class TableTestUtil(test: TableTestBase) {
  protected lazy val diffRepository: DiffRepository = DiffRepository.lookup(test.getClass)

  // java env
  val javaEnv = new LocalStreamEnvironment()
  // scala env
  val env = new StreamExecutionEnvironment(javaEnv)

  // a counter for unique table names
  private var counter = 0

  def getTableEnv: TableEnvironment

  /**
    * Create a [[TestTableSource]] with the given schema,
    * and registers this TableSource under a unique name into the TableEnvironment's catalog.
    *
    * TODO Change fields type to `Expression*` after [Expression] introduced
    *
    * @param fields field names
    * @tparam T field types
    * @return returns the registered [[Table]].
    */
  def addTableSource[T: TypeInformation](fields: Symbol*): Table = {
    counter += 1
    addTableSource[T](s"Table$counter", fields: _*)
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
  def addTableSource[T: TypeInformation](name: String, fields: Symbol*): Table = {
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    val fieldTypes: Array[TypeInformation[_]] = typeInfo match {
      case tt: TupleTypeInfo[_] => (0 until tt.getArity).map(tt.getTypeAt).toArray
      case ct: CaseClassTypeInfo[_] => (0 until ct.getArity).map(ct.getTypeAt).toArray
      case at: AtomicType[_] => Array[TypeInformation[_]](at)
      case _ => throw new TableException(s"Unsupported type info: $typeInfo")
    }
    val dataType = TypeConversions.fromLegacyInfoToDataType(typeInfo)
    val tableEnv = getTableEnv
    val (fieldNames, _) = tableEnv.getFieldInfo(dataType, fields.map(_.name).toArray)
    addTableSource(name, fieldTypes, fieldNames)
  }

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
    val tableEnv = getTableEnv
    val schema = new TableSchema(fields, types)
    val isBatch = tableEnv.isBatch
    val tableSource = new TestTableSource(isBatch, schema)
    val table = new TableSourceTable[BaseRow](tableSource, isBatch, statistic)
    tableEnv.registerTableInternal(name, table)
    tableEnv.scan(name)
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
  def addDataStream[T: TypeInformation](name: String, fields: Symbol*): Table

  /**
    * Registers a [[TableFunction]] under given name into the TableEnvironment's catalog.
    */
  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T]): Unit = getTableEnv.registerFunction(name, function)

  /**
    * Registers a [[ScalarFunction]] under given name into the TableEnvironment's catalog.
    */
  def addFunction(name: String, function: ScalarFunction): Unit = {
    getTableEnv.registerFunction(name, function)
  }

  /**
    * Registers a [[AggregateFunction]] under given name into the TableEnvironment's catalog.
    */
  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = getTableEnv.registerFunction(name, function)

  def verifyPlan(): Unit = {
    doVerifyPlan(
      SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRowType = false,
      withRetractTraits = false,
      printPlanBefore = true)
  }

  def verifyPlan(sql: String): Unit = {
    doVerifyPlan(
      sql,
      SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRowType = false,
      printPlanBefore = true)
  }

  def verifyPlan(table: Table): Unit = {
    doVerifyPlan(
      table,
      SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRowType = false,
      printPlanBefore = true)
  }

  def verifyPlanWithType(sql: String): Unit = {
    doVerifyPlan(
      sql,
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRowType = true,
      printPlanBefore = true)
  }

  def verifyPlanWithType(table: Table): Unit = {
    doVerifyPlan(
      table,
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRowType = true,
      printPlanBefore = true)
  }

  def verifyPlanNotExpected(sql: String, notExpected: String*): Unit = {
    verifyPlanNotExpected(getTableEnv.sqlQuery(sql), notExpected: _*)
  }

  def verifyPlanNotExpected(table: Table, notExpected: String*): Unit = {
    require(notExpected.nonEmpty)
    val relNode = table.asInstanceOf[TableImpl].getRelNode
    val optimizedPlan = getOptimizedPlan(
      Array(relNode),
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRetractTraits = false,
      withRowType = false)
    val result = notExpected.forall(!optimizedPlan.contains(_))
    val message = s"\nactual plan:\n$optimizedPlan\nnot expected:\n${notExpected.mkString(", ")}"
    assertTrue(message, result)
  }

  def verifyExplain(): Unit = verifyExplain(extended = false)

  def verifyExplain(extended: Boolean): Unit = doVerifyExplain(extended = extended)

  def verifyExplain(sql: String): Unit = verifyExplain(sql, extended = false)

  def verifyExplain(sql: String, extended: Boolean): Unit = {
    val table = getTableEnv.sqlQuery(sql)
    verifyExplain(table, extended)
  }

  def verifyExplain(table: Table): Unit = verifyExplain(table, extended = false)

  def verifyExplain(table: Table, extended: Boolean): Unit = {
    doVerifyExplain(Some(table), extended = extended)
  }

  def doVerifyPlan(
      sql: String,
      explainLevel: SqlExplainLevel,
      withRowType: Boolean,
      printPlanBefore: Boolean): Unit = {
    doVerifyPlan(
      sql = sql,
      explainLevel = explainLevel,
      withRetractTraits = false,
      withRowType = withRowType,
      printPlanBefore = printPlanBefore)
  }

  def doVerifyPlan(
      sql: String,
      explainLevel: SqlExplainLevel,
      withRetractTraits: Boolean,
      withRowType: Boolean,
      printPlanBefore: Boolean): Unit = {
    val table = getTableEnv.sqlQuery(sql)
    val relNode = table.asInstanceOf[TableImpl].getRelNode
    val optimizedPlan = getOptimizedPlan(
      Array(relNode),
      explainLevel,
      withRetractTraits = withRetractTraits,
      withRowType = withRowType)

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
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRowType = false,
      withRetractTraits = false,
      printResource = true,
      printPlanBefore = false)
  }

  def doVerifyPlan(
      table: Table,
      explainLevel: SqlExplainLevel,
      withRowType: Boolean,
      printPlanBefore: Boolean): Unit = {
    doVerifyPlan(
      table = table,
      explainLevel = explainLevel,
      withRetractTraits = false,
      withRowType = withRowType,
      printPlanBefore = printPlanBefore)
  }

  def doVerifyPlan(
      table: Table,
      explainLevel: SqlExplainLevel,
      withRowType: Boolean,
      withRetractTraits: Boolean,
      printPlanBefore: Boolean,
      printResource: Boolean = false): Unit = {
    val relNode = table.asInstanceOf[TableImpl].getRelNode
    val optimizedPlan = getOptimizedPlan(
      Array(relNode),
      explainLevel,
      withRetractTraits = withRetractTraits,
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

  def doVerifyPlan(
      explainLevel: SqlExplainLevel,
      withRowType: Boolean,
      withRetractTraits: Boolean,
      printPlanBefore: Boolean): Unit = {
    val tableEnv = getTableEnv
    if (tableEnv.sinkNodes.isEmpty) {
      throw new TableException("No output table have been created yet. " +
        "A program needs at least one output table that consumes data.\n" +
        "Please create output table(s) for your program")
    }
    val relNodes = tableEnv.sinkNodes.toArray
    val optimizedPlan = getOptimizedPlan(
      relNodes.toArray,
      explainLevel,
      withRetractTraits = withRetractTraits,
      withRowType = withRowType)
    tableEnv.sinkNodes.clear()

    if (printPlanBefore) {
      val planBefore = new StringBuilder
      relNodes.foreach { sink =>
        planBefore.append(System.lineSeparator)
        planBefore.append(FlinkRelOptUtil.toString(sink, SqlExplainLevel.EXPPLAN_ATTRIBUTES))
      }
      assertEqualsOrExpand("planBefore", planBefore.toString())
    }

    val actual = SystemUtils.LINE_SEPARATOR + optimizedPlan
    assertEqualsOrExpand("planAfter", actual.toString, expand = false)
  }

  private def doVerifyExplain(table: Option[Table] = None, extended: Boolean = false): Unit = {
    val explainResult = table match {
      case Some(t) => getTableEnv.explain(t, extended = extended)
      case _ => getTableEnv.explain(extended = extended)
    }
    val actual = if (extended) {
      replaceEstimatedCost(explainResult)
    } else {
      explainResult
    }
    assertEqualsOrExpand("explain", replaceStageId(actual), expand = false)
  }

  private def getOptimizedPlan(
      relNodes: Array[RelNode],
      explainLevel: SqlExplainLevel,
      withRetractTraits: Boolean,
      withRowType: Boolean,
      withResource: Boolean = false): String = {
    require(relNodes.nonEmpty)
    val tEnv = getTableEnv
    val optimizedRels = tEnv.optimize(relNodes)
    optimizedRels.head match {
      case _: ExecNode[_, _] =>
        val optimizedNodes = tEnv.translateToExecNodeDag(optimizedRels)
        require(optimizedNodes.length == optimizedRels.length)
        ExecNodePlanDumper.dagToString(
          optimizedNodes,
          detailLevel = explainLevel,
          withRetractTraits = withRetractTraits,
          withOutputType = withRowType,
          withResource = withResource)
      case _ =>
        optimizedRels.map { rel =>
          FlinkRelOptUtil.toString(
            rel,
            detailLevel = explainLevel,
            withRetractTraits = withRetractTraits,
            withRowType = withRowType)
        }.mkString("\n")
    }
  }

  /**
    * Stage {id} is ignored, because id keeps incrementing in test class
    * while StreamExecutionEnvironment is up
    */
  protected def replaceStageId(s: String): String = {
    s.replaceAll("\\r\\n", "\n").replaceAll("Stage \\d+", "")
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

  private def assertEqualsOrExpand(tag: String, actual: String, expand: Boolean = true): Unit = {
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

/**
  * Utility for stream table test.
  */
case class StreamTableTestUtil(test: TableTestBase) extends TableTestUtil(test) {
  javaEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  // java tableEnv
  val javaTableEnv: JavaStreamTableEnv = JavaStreamTableEnv.create(javaEnv)
  // scala tableEnv
  val tableEnv: ScalaStreamTableEnv = ScalaStreamTableEnv.create(env)

  override def getTableEnv: TableEnvironment = tableEnv

  override def addDataStream[T: TypeInformation](name: String, fields: Symbol*): Table = {
    val table = env.fromElements[T]().toTable(tableEnv, fields: _*)
    tableEnv.registerTable(name, table)
    tableEnv.scan(name)
  }

  def verifyPlanWithTrait(): Unit = {
    doVerifyPlan(
      SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRetractTraits = true,
      withRowType = false,
      printPlanBefore = true)
  }

  def verifyPlanWithTrait(sql: String): Unit = {
    doVerifyPlan(
      sql,
      SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRetractTraits = true,
      withRowType = false,
      printPlanBefore = true)
  }

  def verifyPlanWithTrait(table: Table): Unit = {
    doVerifyPlan(
      table,
      SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRetractTraits = true,
      withRowType = false,
      printPlanBefore = true)
  }

  def buildStreamProgram(firstProgramNameToRemove: String): Unit = {
    val program = FlinkStreamProgram.buildProgram(tableEnv.getConfig.getConf)
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
    val calciteConfig = CalciteConfig.createBuilder(tableEnv.getConfig.getCalciteConfig)
      .replaceStreamProgram(program).build()
    tableEnv.getConfig.setCalciteConfig(calciteConfig)
  }

  def enableMiniBatch(): Unit = {
    tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    tableEnv.getConfig.getConf.setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_SIZE, 3L)
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
      fieldTypes: Array[LogicalType]): UpsertStreamTableSink[BaseRow] = {
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
  * Utility for batch table test.
  */
case class BatchTableTestUtil(test: TableTestBase) extends TableTestUtil(test) {
  // java tableEnv
  val javaTableEnv: JavaBatchTableEnv = JavaBatchTableEnv.create(javaEnv)
  // scala tableEnv
  val tableEnv: ScalaBatchTableEnv = ScalaBatchTableEnv.create(env)

  override def getTableEnv: TableEnvironment = tableEnv

  override def addDataStream[T: TypeInformation](
      name: String, fields: Symbol*): Table = {
    // TODO use BatchTableEnvironment#fromBoundedStream when it's introduced

    val typeInfo = implicitly[TypeInformation[T]]
    BatchTableEnvUtil.registerCollection(
      tableEnv,
      name,
      Seq(),
      typeInfo,
      fields.map(_.name).mkString(", "))
    tableEnv.scan(name)
  }

  def buildBatchProgram(firstProgramNameToRemove: String): Unit = {
    val program = FlinkBatchProgram.buildProgram(tableEnv.getConfig.getConf)
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
    val calciteConfig = CalciteConfig.createBuilder(tableEnv.getConfig.getCalciteConfig)
      .replaceBatchProgram(program).build()
    tableEnv.getConfig.setCalciteConfig(calciteConfig)
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
  * Batch/Stream [[org.apache.flink.table.sources.TableSource]] for testing.
  */
class TestTableSource(isBatch: Boolean, schema: TableSchema, tableStats: Option[TableStats] = None)
  extends StreamTableSource[BaseRow] {

  override def isBounded: Boolean = isBatch

  override def getDataStream(
      execEnv: environment.StreamExecutionEnvironment): DataStream[BaseRow] = {
    execEnv.fromCollection(List[BaseRow](), getReturnType)
  }

  override def getReturnType: TypeInformation[BaseRow] = {
    val LogicalTypes = schema.getFieldTypes.map(
      TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType)
    new BaseRowTypeInfo(LogicalTypes, schema.getFieldNames)
  }

  override def getTableSchema: TableSchema = schema

  /** Returns the statistics of the table, returns null if don't know the statistics. */
  override def getTableStats: Optional[TableStats] = {
    Optional.ofNullable(tableStats.orNull)
  }
}
