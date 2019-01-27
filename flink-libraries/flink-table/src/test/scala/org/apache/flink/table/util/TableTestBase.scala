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
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, StreamExecutionEnvironment => JStreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.api.java.{BatchTableEnvironment => JBatchTableEnvironment, StreamTableEnvironment => JStreamTableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment, _}
import org.apache.flink.table.api.types.{DataType, DataTypes, InternalType, TypeConverters}
import org.apache.flink.table.errorcode.TableErrors

import java.util.{ArrayList => JArrayList, HashSet => JHashSet}
import org.apache.flink.table.api.{Table, TableEnvironment, TableException, TableSchema, _}
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.exec.BatchExecNode
import org.apache.flink.table.plan.nodes.process.ChainedDAGProcessors
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.plan.util.{FlinkNodeOptUtil, FlinkRelOptUtil}
import org.apache.flink.table.sources.{BatchTableSource, LimitableTableSource, TableSource}
import org.apache.flink.types.Row

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.tools.RuleSet
import org.apache.calcite.util.ImmutableBitSet
import org.apache.commons.lang3.SystemUtils
import org.junit.Assert.{assertEquals, _}
import org.junit.Rule
import org.junit.rules.{ExpectedException, TestName}
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

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

  def nullableBatchTestUtil(fieldsNullable: Boolean): BatchTableTestUtil = {
    new NullableBatchTableTestUtil(fieldsNullable, this)
  }

  def verifyTableEquals(expected: Table, actual: Table): Unit = {
    assertEquals(
      "Logical plans do not match",
      LogicalPlanFormatUtils.formatTempTableId(FlinkRelOptUtil.toString(expected.getRelNode)),
      LogicalPlanFormatUtils.formatTempTableId(FlinkRelOptUtil.toString(actual.getRelNode)))
  }

  def injectRules(tEnv: TableEnvironment, phase: String, injectRuleSet: RuleSet): Unit = {
    val programs = FlinkStreamPrograms.buildPrograms(tEnv.getConfig.getConf)
    programs.get(phase) match {
      case Some(groupProg: FlinkGroupProgram[StreamOptimizeContext]) =>
        groupProg.addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(injectRuleSet).build(), "test rules")
      case Some(ruleSetProgram: FlinkHepRuleSetProgram[StreamOptimizeContext]) =>
        ruleSetProgram.add(injectRuleSet)
      case _ =>
        throw new RuntimeException(s"$phase does not exist")
    }
    val builder = CalciteConfig.createBuilder(tEnv.getConfig.getCalciteConfig)
      .replaceStreamPrograms(programs)
    tEnv.getConfig.setCalciteConfig(builder.build())
  }
}

abstract class TableTestUtil {

  private var counter = 0

  def addTable[T: TypeInformation](fields: Expression*): Table = {
    counter += 1
    addTable[T](s"Table$counter", fields: _*)
  }

  def addTable[T: TypeInformation](name: String, fields: Expression*): Table

  def addFunction[T: TypeInformation](name: String, function: TableFunction[T]): TableFunction[T]

  def addFunction(name: String, function: ScalarFunction): Unit

  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit

  def verifyPlan(sql: String): Unit

  def verifyPlan(table: Table): Unit
}

case class StreamTableTestUtil(test: TableTestBase) extends TableTestUtil {

  private lazy val diffRepository = DiffRepository.lookup(test.getClass)
  val javaEnv = new LocalStreamEnvironment()
  javaEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val javaTableEnv: JStreamTableEnvironment = TableEnvironment.getTableEnvironment(javaEnv)
  val env = new StreamExecutionEnvironment(javaEnv)
  val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

  def addTable[T: TypeInformation](
      name: String,
      fields: Expression*)
  : Table = {
    val table = env.fromElements().toTable(tableEnv, fields: _*)
    tableEnv.registerTable(name, table)
    table
  }

  def addJavaTable[T](typeInfo: TypeInformation[T], name: String, fields: String): Table = {
    val stream = javaEnv.addSource(new EmptySource[T], typeInfo)
    val table = javaTableEnv.fromDataStream(stream, fields)
    javaTableEnv.registerTable(name, table)
    table
  }

  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T]): TableFunction[T] = {
    tableEnv.registerFunction(name, function)
    function
  }

  def addFunction(name: String, function: ScalarFunction): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def verifyPlan(sql: String): Unit = {
    val resultTable = tableEnv.sqlQuery(sql)
    verifyPlan(resultTable)
  }

  def verifySqlPlansIdentical(query1: String, queries: String*): Unit = {
    val resultTable1 = tableEnv.sqlQuery(query1)
    queries.foreach(s => verify2Tables(resultTable1, tableEnv.sqlQuery(s)))
  }

  def verify2Tables(resultTable1: Table, resultTable2: Table): Unit = {
    val relNode1 = resultTable1.getRelNode
    val optimized1 = tableEnv.optimize(relNode1, updatesAsRetraction = false)
    val relNode2 = resultTable2.getRelNode
    val optimized2 = tableEnv.optimize(relNode2, updatesAsRetraction = false)
    assertEquals(FlinkRelOptUtil.toString(optimized1), FlinkRelOptUtil.toString(optimized2))
  }

  def verifyPlan(): Unit = {
    doVerifyPlanWithSubsectionOptimization(explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES)
  }

  def verifyPlan(table: Table): Unit = {
    val relNode = table.getRelNode
    val optimized = tableEnv.optimize(relNode, updatesAsRetraction = false)
    val actual = SystemUtils.LINE_SEPARATOR + FlinkRelOptUtil.toString(optimized)

    verifyPlan(test.name.getMethodName, actual)
  }

  def verifyPlan(name: String, plan: String): Unit = {
    diffRepository.assertEquals(name, "plan", "${plan}", plan)
  }

  def verifyUniqueKeys(sql: String, expect: Set[Int]*): Unit = {
    val table = tableEnv.sqlQuery(sql)
    verifyUniqueKeys(table, expect: _*)
  }

  def verifyUniqueKeys(table: Table, expect: Set[Int]*): Unit = {
    val node = tableEnv.optimize(table.getRelNode, updatesAsRetraction = false)
    val mq: FlinkRelMetadataQuery = FlinkRelMetadataQuery.instance()
    val actual = mq.getUniqueKeys(node)
    val expectSet = new JHashSet[ImmutableBitSet]
    expect.filter(_.nonEmpty).foreach { array =>
      val keys = new JArrayList[Integer]()
      array.foreach(keys.add(_))
      expectSet.add(ImmutableBitSet.of(keys))
    }
    if (actual == null) {
      assert(expectSet == null || expectSet.isEmpty)
    } else {
      assertEquals(expectSet, actual)
    }
  }

  def verifyPlanAndTrait(sql: String): Unit = {
    val resultTable = tableEnv.sqlQuery(sql)
    verifyPlanAndTrait(resultTable)
  }

  def verifyPlanAndTrait(table: Table): Unit = {
    val relNode = table.getRelNode
    val optimized = tableEnv.optimize(relNode, updatesAsRetraction = false)
    val actualPlan = SystemUtils.LINE_SEPARATOR +
      FlinkRelOptUtil.toString(optimized, withRetractTraits = true)
    assertEqualsOrExpand("plan", actualPlan)
  }

  def explainSql(query: String): String = {
    val relNode = tableEnv.sqlQuery(query).getRelNode
    val optimized = tableEnv.optimize(relNode, updatesAsRetraction = false)
    FlinkRelOptUtil.toString(optimized)
  }

  def explain(resultTable: Table): String = {
    tableEnv.explain(resultTable)
  }

  def verifySchema(resultTable: Table, fields: Seq[(String, InternalType)]): Unit = {
    val actual = resultTable.getSchema
    val expected = new TableSchema(fields.map(_._1).toArray, fields.map(_._2).toArray)
    assertEquals(expected, actual)
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

  private def doVerifyPlanWithSubsectionOptimization(
      explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      printPlanBefore: Boolean = true): Unit = {
    if (!tableEnv.getConfig.getSubsectionOptimization) {
      throw new TableException(
        "subsection optimization is false, please use other method to verify result.")
    }
    if (tableEnv.sinkNodes.isEmpty) {
      throw new TableException(TableErrors.INST.sqlCompileNoSinkTblError())
    }

    if (printPlanBefore) {
      val planBefore = new StringBuilder
      tableEnv.sinkNodes.foreach { sink =>
        val table = new Table(tableEnv, sink.children.head)
        val ast = table.getRelNode
        planBefore.append(System.lineSeparator)
        planBefore.append(FlinkRelOptUtil.toString(ast, SqlExplainLevel.EXPPLAN_ATTRIBUTES))
      }
      assertEqualsOrExpand("planBefore", planBefore.toString())
    }

    val optSinkNodes = tableEnv.tableServiceManager.cachePlanBuilder
      .buildPlanIfNeeded(tableEnv.sinkNodes)
    val sinkExecNodes = tableEnv.optimizeAndTranslateNodeDag(true, optSinkNodes: _*)

    tableEnv.sinkNodes.clear()

    val planAfter =  FlinkNodeOptUtil.dagToString(sinkExecNodes, detailLevel = explainLevel)
    assertEqualsOrExpand("planAfter", planAfter, expand = false)
  }
}

case class BatchTableTestUtil(test: TableTestBase) extends TableTestUtil {

  def answer[T](f: InvocationOnMock => T): Answer[T] = new Answer[T] {
    override def answer(i: InvocationOnMock): T = f(i)
  }

  private lazy val diffRepository = DiffRepository.lookup(test.getClass)
  val javaEnv = new LocalStreamEnvironment()
  val javaTableEnv: JBatchTableEnvironment = TableEnvironment.getBatchTableEnvironment(javaEnv)
  val env = new StreamExecutionEnvironment(javaEnv)
  val tableEnv: BatchTableEnvironment = TableEnvironment.getBatchTableEnvironment(env)
  tableEnv.getConfig.getConf.setBoolean(
    TableConfigOptions.SQL_EXEC_DATA_EXCHANGE_MODE_ALL_BATCH, false)
  tableEnv.getConfig.setCalciteConfig(CalciteConfig.createBuilder().build())
  tableEnv.getConfig.setSubsectionOptimization(true)

  def disableBroadcastHashJoin(): Unit = {
    tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_HASH_JOIN_BROADCAST_THRESHOLD, -1)
  }

  def setJoinReorderEnabled(joinReorderEnabled: Boolean): Unit = {
    tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, joinReorderEnabled)
  }

  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T])
  : TableFunction[T] = {
    tableEnv.registerFunction(name, function)
    function
  }

  def addFunction(name: String, function: ScalarFunction): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def addTable[T: TypeInformation](
      name: String,
      fields: Expression*): Table = {

    val bs = env.fromElements()
    val t = tableEnv.fromBoundedStream(bs, fields: _*)
    tableEnv.registerTable(name, t)
    t
  }

  def addTableSource(
      name: String,
      tableSchema: TableSchema,
      limitPushDown: Boolean = false,
      stats: TableStats = null): TableSource = {

    val table = new TestBatchTableSource(tableSchema, limitPushDown, stats)
    addTable(name, table)
    table
  }

  def addJavaTable[T](typeInfo: TypeInformation[T], name: String, fields: Expression*): Table = {
    val stream = javaEnv.addSource(new EmptySource[T], typeInfo)
    val t = tableEnv.fromBoundedStream(new DataStream[T](stream), fields: _*)
    tableEnv.registerTable(name, t)
    t
  }

  def addTable(name: String, t: TableSource): Unit = {
    tableEnv.registerTableSource(name, t)
  }

  def addTable[T: TypeInformation](
      name: String,
      uniqueKeys: Set[Set[String]],
      fields: Expression*): Table = {
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    val physicalSchema = TableSchemaUtil.fromDataType(typeInfo)
    val (fieldNames, fieldIdxs) =
      tableEnv.getFieldInfo(typeInfo, fields.toArray)
    val fieldTypes: Array[InternalType] = fieldIdxs.map(physicalSchema.getType)
    val tableSchemaBuilder = TableSchema.builder()
    fieldNames.zip(fieldTypes).foreach { case (fn, ft) => tableSchemaBuilder.column(fn, ft) }
    uniqueKeys.foreach { key => tableSchemaBuilder.uniqueKey((key.toArray[String]): _*) }
    val tableSchema = tableSchemaBuilder.build()
    val mapping = fieldNames.zipWithIndex.map {
      case (name: String, idx: Int) =>
        (name, physicalSchema.getColumnName(fieldIdxs.apply(idx)))
    }.toMap
    val ts = new TestTableSourceWithTime(tableSchema, typeInfo, Seq(), mapping = mapping)
    tableEnv.registerTableSource(name, ts)
    tableEnv.scan(name)
  }

  def getTableEnv: BatchTableEnvironment = tableEnv

  def verifySqlNotExpected(query: String, notExpected: String*): Unit = {
    verifyTableNotExpected(tableEnv.sqlQuery(query), notExpected: _*)
  }

  def verifyTableNotExpected(resultTable: Table, notExpected: String*): Unit = {
    val actual = getPlan(resultTable.getRelNode)
    val result = notExpected.forall(!actual.contains(_))
    assertTrue(s"\n actual: \n$actual \n not expected: \n${notExpected.mkString(", ")}", result)
  }

  def verifyPlan(): Unit = {
    doVerifyPlanWithSubsectionOptimization(explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES)
  }

  override def verifyPlan(sql: String): Unit = {
    verifyPlan(sql, SqlExplainLevel.EXPPLAN_ATTRIBUTES)
  }

  def verifyPlan(sql: String, explainLevel: SqlExplainLevel): Unit = {
    verifyPlan(sql, explainLevel, printPlanBefore = true)
  }

  def verifyPlan(sql: String, explainLevel: SqlExplainLevel, printPlanBefore: Boolean): Unit = {
    val resultTable = tableEnv.sqlQuery(sql)
    assertEqualsOrExpand("sql", sql)
    verifyPlan(resultTable, explainLevel = explainLevel, printPlanBefore = printPlanBefore)
  }

  override def verifyPlan(resultTable: Table): Unit = {
    verifyPlan(resultTable, SqlExplainLevel.EXPPLAN_ATTRIBUTES)
  }

  def verifyPlan(resultTable: Table, explainLevel: SqlExplainLevel): Unit = {
    doVerifyPlan(resultTable, explainLevel = explainLevel)
  }

  def verifyPlan(
      resultTable: Table,
      explainLevel: SqlExplainLevel,
      printPlanBefore: Boolean): Unit = {
    doVerifyPlan(resultTable, explainLevel = explainLevel, printPlanBefore = printPlanBefore)
  }

  def verifyResource(sql: String): Unit = {
    assertEqualsOrExpand("sql", sql)
    doVerifyPlan(
      tableEnv.sqlQuery(sql),
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      printResource = true,
      printPlanBefore = false)
  }

  def verifyResourceWithSubsectionOptimization(): Unit = {
    doVerifyPlanWithSubsectionOptimization(
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      printResource = true,
      printPlanBefore = false)
  }

  def verifyPlanWithRunningUnit(sql: String): Unit = {
    assertEqualsOrExpand("sql", sql)
    doVerifyPlan(
      tableEnv.sqlQuery(sql),
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      printPlanBefore = false,
      printRunningUnit = true)
  }

  private def doVerifyPlan(
      resultTable: Table,
      explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      printResource: Boolean = false,
      printPlanBefore: Boolean = true,
      printRunningUnit: Boolean = false): Unit = {
    val relNode = resultTable.getRelNode
    if (printPlanBefore) {
      val planBefore = SystemUtils.LINE_SEPARATOR +
        FlinkRelOptUtil.toString(relNode, SqlExplainLevel.EXPPLAN_ATTRIBUTES)
      assertEqualsOrExpand("planBefore", planBefore)
    }

    val actual = SystemUtils.LINE_SEPARATOR +
        getPlan(relNode, explainLevel, printResource, printRunningUnit)
    assertEqualsOrExpand("planAfter", actual.toString, expand = false)
  }

  private def doVerifyPlanWithSubsectionOptimization(
      explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      printResource: Boolean = false,
      printPlanBefore: Boolean = true): Unit = {
    if (!tableEnv.getConfig.getSubsectionOptimization) {
      throw new TableException(
        "subsection optimization is false, please use other method to verify result.")
    }
    if (tableEnv.sinkNodes.isEmpty) {
      throw new TableException(TableErrors.INST.sqlCompileNoSinkTblError())
    }

    if (printPlanBefore) {
      val planBefore = new StringBuilder
      tableEnv.sinkNodes.foreach { sink =>
        val table = new Table(tableEnv, sink.children.head)
        val ast = table.getRelNode
        planBefore.append(System.lineSeparator)
        planBefore.append(FlinkRelOptUtil.toString(ast, SqlExplainLevel.EXPPLAN_ATTRIBUTES))
      }
      assertEqualsOrExpand("planBefore", planBefore.toString())
    }

    val optSinkNodes = tableEnv.tableServiceManager.cachePlanBuilder
      .buildPlanIfNeeded(tableEnv.sinkNodes)
    if (!printResource) {
      tableEnv.getConfig.setBatchDAGProcessors(new ChainedDAGProcessors)
    }
    val sinkExecNodes = tableEnv.optimizeAndTranslateNodeDag(true, optSinkNodes: _*)

    tableEnv.sinkNodes.clear()

    // set resource
    val planAfter =  FlinkNodeOptUtil.dagToString(sinkExecNodes, detailLevel = explainLevel,
      withResource = printResource)
    assertEqualsOrExpand("planAfter", planAfter, expand = false)
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

  def printTable(
      resultTable: Table,
      explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES): Unit = {
    println(getPlan(resultTable.getRelNode, explainLevel))
  }

  def printSql(
      query: String,
      explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES): Unit = {
    printTable(tableEnv.sqlQuery(query), explainLevel)
  }

  private def getPlan(
      relNode: RelNode,
      explainLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      printResource: Boolean = false,
      printRunningUnit: Boolean = false): String = {
    val optimized = tableEnv.optimize(relNode)
    optimized match {
      case batchExecNode: BatchExecNode[_] =>
        if (!printResource) {
          tableEnv.getConfig.setBatchDAGProcessors(new ChainedDAGProcessors())
        }
        val optimizedNodes = tableEnv.translateNodeDag(Seq(batchExecNode))
        require(optimizedNodes.length == 1)
        val optimizedNode = optimizedNodes.head

        if (printRunningUnit) {
            val ruList = tableEnv.getRUKeeper.getRunningUnits.map(x => x.toString)
            ruList.sorted
            val ruString = SystemUtils.LINE_SEPARATOR + String.join("\n", ruList)
            assertEqualsOrExpand("runningUnit", ruString)
        }

        FlinkNodeOptUtil.treeToString(
            optimizedNode,
            detailLevel = explainLevel,
            withResource = printResource)
      case _ =>
        FlinkRelOptUtil.toString(optimized, detailLevel = explainLevel)
    }
  }
}

class NullableBatchTableTestUtil(fieldsNullable: Boolean, test: TableTestBase)
  extends BatchTableTestUtil(test) {

  override def addTable[T: TypeInformation](name: String, fields: Expression*): Table = {
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    val fieldTypes = typeInfo match {
      case tt: TupleTypeInfo[_] => tt.getGenericParameters.values().asScala.toArray
      case ct: CaseClassTypeInfo[_] => ct.getGenericParameters.values().asScala.toArray
      case bt: AtomicType[_] => Array[TypeInformation[_]](bt)
      case _ => throw new TableException(s"Unsupported type info: $typeInfo")
    }
    val fieldNullables = Array.fill(fields.size)(fieldsNullable)
    val (fieldNames, _) = tableEnv.getFieldInfo(typeInfo, fields.toArray)
    val ts = new TestTableSourceWithFieldNullables(fieldNames, fieldTypes, fieldNullables)
    tableEnv.registerTableSource(name, ts)
    tableEnv.scan(name)
  }
}

class TestBatchTableSource(tableSchema: TableSchema,
                           limitPushDown: Boolean = false,
                           stats: TableStats = null)
  extends BatchTableSource[Row] with LimitableTableSource {

  override def getReturnType: DataType =
    DataTypes.createRowType(
      tableSchema.getTypes.asInstanceOf[Array[DataType]],
      tableSchema.getColumnNames)

  override def getTableStats: TableStats = if (stats == null) {
    TableStats(10L, new mutable.HashMap[String, ColumnStats]())
  } else {
    stats
  }

  /** Returns the table schema of the table source */
  override def getTableSchema: TableSchema = TableSchemaUtil.fromDataType(getReturnType)

  override def explainSource(): String = ""

  /**
    * Returns the data of the table as a [[JDataStream]].
    *
    * NOTE: This method is for internal use only for defining a [[TableSource]].
    * Do not use it in Table API programs.
    */
  override def getBoundedStream(javaEnv: JStreamExecutionEnvironment): JDataStream[Row] = {
    val transformation = mock(classOf[StreamTransformation[Row]])
    when(transformation.getMaxParallelism).thenReturn(-1)
    val bs = mock(classOf[JDataStream[Row]])
    when(bs.getTransformation).thenReturn(transformation)
    when(transformation.getOutputType).thenReturn(
      TypeConverters.createExternalTypeInfoFromDataType(getReturnType)
          .asInstanceOf[TypeInformation[Row]])
    bs
  }

  /**
    * Check and push down the limit to the table source.
    *
    * @param limit the value which limit the number of records.
    * @return A new cloned instance of [[TableSource]]
    */
  override def applyLimit(limit: Long): TableSource = this

  /**
    * Return the flag to indicate whether limit push down has been tried. Must return true on
    * the returned instance of [[applyLimit]].
    */
  override def isLimitPushedDown: Boolean = limitPushDown
}

class EmptySource[T]() extends SourceFunction[T] {
  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
  }

  override def cancel(): Unit = {
  }
}
