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
import org.apache.flink.table.`type`.TypeConverters
import org.apache.flink.table.api.java.{BatchTableEnvironment => JavaBatchTableEnv, StreamTableEnvironment => JavaStreamTableEnv}
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnv, StreamTableEnvironment => ScalaStreamTableEnv, _}
import org.apache.flink.table.api.{BatchTableEnvironment => _, StreamTableEnvironment => _, _}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.plan.util.{FlinkRelOptUtil, _}
import org.apache.flink.table.sources.{BatchTableSource, StreamTableSource}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.commons.lang3.SystemUtils
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.rules.{ExpectedException, TestName}

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

  protected def getTableEnv: TableEnvironment

  // a counter for unique table names
  private var counter = 0

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
    val tableEnv = getTableEnv
    val (fieldNames, _) = tableEnv.getFieldInfo(typeInfo, fields.map(_.name).toArray)
    val schema = new TableSchema(fieldNames, fieldTypes)
    val tableSource = new TestTableSource(schema)
    tableEnv.registerTableSource(name, tableSource)
    tableEnv.scan(name)
  }

  /**
    * Create a [[TestTableSource]] with the given schema,
    * and registers this TableSource under given name into the TableEnvironment's catalog.
    *
    * @param name table name
    * @param types field types
    * @param names field names
    * @return returns the registered [[Table]].
    */
  def addTableSource(
      name: String,
      types: Array[TypeInformation[_]],
      names: Array[String]): Table = {
    val tableEnv = getTableEnv
    val schema = new TableSchema(names, types)
    val tableSource = new TestTableSource(schema)
    tableEnv.registerTableSource(name, tableSource)
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
    * TODO implements this method after `registerFunction` added
    */
  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T]): TableFunction[T] = ???

  /**
    * Registers a [[ScalarFunction]] under given name into the TableEnvironment's catalog.
    * TODO implements this method after `registerFunction` added
    */
  def addFunction(name: String, function: ScalarFunction): Unit = ???

  /**
    * Registers a [[AggregateFunction]] under given name into the TableEnvironment's catalog.
    * TODO implements this method after `registerFunction` added
    */
  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = ???

  def verifyPlan(): Unit = {
    // TODO implements this method when supporting multi-sinks
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

  def verifyExplain(): Unit = doVerifyExplain()

  def verifyExplain(sql: String): Unit = {
    val table = getTableEnv.sqlQuery(sql)
    verifyExplain(table)
  }

  def verifyExplain(table: Table): Unit = {
    doVerifyExplain(Some(table))
  }

  def doVerifyPlan(
      sql: String,
      explainLevel: SqlExplainLevel,
      withRowType: Boolean,
      printPlanBefore: Boolean): Unit = {
    val table = getTableEnv.sqlQuery(sql)
    val relNode = table.asInstanceOf[TableImpl].getRelNode
    val optimizedPlan = getOptimizedPlan(relNode, explainLevel, withRowType = withRowType)

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

  def doVerifyPlan(
      table: Table,
      explainLevel: SqlExplainLevel,
      withRowType: Boolean,
      printPlanBefore: Boolean): Unit = {
    val relNode = table.asInstanceOf[TableImpl].getRelNode
    val optimizedPlan = getOptimizedPlan(relNode, explainLevel, withRowType = withRowType)

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

  private def doVerifyExplain(table: Option[Table] = None): Unit = {
    val explainResult = table match {
      case Some(t) => getTableEnv.explain(t)
      case _ => getTableEnv.explain()
    }
    assertEqualsOrExpand("explain", replaceStageId(explainResult), expand = false)
  }

  private def getOptimizedPlan(
      relNode: RelNode,
      explainLevel: SqlExplainLevel,
      withRowType: Boolean): String = {
    val optimized = getTableEnv.optimize(relNode)
    FlinkRelOptUtil.toString(optimized, detailLevel = explainLevel, withRowType = withRowType)
  }

  /* Stage {id} is ignored, because id keeps incrementing in test class
     * while StreamExecutionEnvironment is up
     */
  protected def replaceStageId(s: String): String = {
    s.replaceAll("\\r\\n", "\n").replaceAll("Stage \\d+", "")
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

  override def addDataStream[T: TypeInformation](
      name: String, fields: Symbol*): Table = {
    val table = env.fromElements[T]().toTable(tableEnv, fields: _*)
    tableEnv.registerTable(name, table)
    tableEnv.scan(name)
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

  // TODO implements this method when a DataStream could be converted into a Table
  override def addDataStream[T: TypeInformation](
      name: String, fields: Symbol*): Table = ???
}

/**
  * Batch/Stream [[org.apache.flink.table.sources.TableSource]] for testing.
  */
class TestTableSource(schema: TableSchema)
  extends BatchTableSource[BaseRow]
  with StreamTableSource[BaseRow] {

  override def getBoundedStream(
      streamEnv: environment.StreamExecutionEnvironment): DataStream[BaseRow] = ???

  override def getDataStream(
      execEnv: environment.StreamExecutionEnvironment): DataStream[BaseRow] = ???

  override def getReturnType: TypeInformation[BaseRow] = {
    val internalTypes = schema.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo)
    new BaseRowTypeInfo(internalTypes: _*)
  }

  override def getTableSchema: TableSchema = schema
}
