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

package org.apache.flink.table.planner.plan.stream.sql.join

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.testutils.FlinkMatchers.containsMessage
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.data.RowData
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE
import org.apache.flink.table.descriptors.{CustomConnectorDescriptor, DescriptorProperties, Schema}
import org.apache.flink.table.factories.TableSourceFactory
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction, UserDefinedFunction}
import org.apache.flink.table.planner.plan.utils._
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.sources._
import org.apache.flink.table.types.DataType
import org.apache.flink.table.utils.EncodingUtils

import org.junit.Assert.{assertThat, assertTrue, fail}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Assume, Before, Test}

import _root_.java.lang.{Boolean => JBoolean}
import _root_.java.sql.Timestamp
import _root_.java.util
import _root_.java.util.{ArrayList => JArrayList, Collection => JCollection, HashMap => JHashMap, List => JList, Map => JMap}

import _root_.scala.collection.JavaConversions._

/**
 * The physical plans for legacy [[org.apache.flink.table.sources.LookupableTableSource]] and new
 * [[org.apache.flink.table.connector.source.LookupTableSource]] should be identical.
 */
@RunWith(classOf[Parameterized])
class LookupJoinTest(legacyTableSource: Boolean) extends TableTestBase with Serializable {

  private val util = streamTestUtil()

  @Before
  def before(): Unit ={
    util.addDataStream[(Int, String, Long)](
      "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
    util.addDataStream[(Int, String, Long, Double)]("T1", 'a, 'b, 'c, 'd)
    util.addDataStream[(Int, String, Int)]("nonTemporal", 'id, 'name, 'age)

    if (legacyTableSource) {
      TestTemporalTable.createTemporaryTable(util.tableEnv, "LookupTable")
    } else {
      util.addTable(
        """
          |CREATE TABLE LookupTable (
          |  `id` INT,
          |  `name` STRING,
          |  `age` INT
          |) WITH (
          |  'connector' = 'values'
          |)
          |""".stripMargin)

      util.addTable(
        """
          |CREATE TABLE LookupTableWithComputedColumn (
          |  `id` INT,
          |  `name` STRING,
          |  `age` INT,
          |  `nominal_age` as age + 1
          |) WITH (
          |  'connector' = 'values',
          |  'bounded' = 'true'
          |)
          |""".stripMargin)
    }
  }

  @Test
  def testJoinInvalidJoinTemporalTable(): Unit = {
    // must follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN LookupTable T.proctime AS D ON T.a = D.id",
      "SQL parse failed",
      classOf[SqlParserException])

    // can't query a dim table directly
    expectExceptionThrown(
      "SELECT * FROM LookupTable FOR SYSTEM_TIME AS OF TIMESTAMP '2017-08-09 14:36:11'",
      "Temporal table can only be used in temporal join and only supports " +
        "'FOR SYSTEM_TIME AS OF' left table's time attribute field.\n" +
        "Querying a temporal table using 'FOR SYSTEM TIME AS OF' syntax with a constant " +
        "timestamp '2017-08-09 14:36:11' is not supported yet",
      classOf[AssertionError]
    )

    // only support left or inner join
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T RIGHT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id",
      "Correlate has invalid join type RIGHT",
      classOf[AssertionError]
    )

    // only support join on raw key of right table
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a + 1 = D.id + 2",
      "Temporal table join requires an equality condition on fields of table " +
        "[default_catalog.default_database.LookupTable].",
      classOf[TableException]
    )

    // only support "FOR SYSTEM_TIME AS OF" left table's proctime
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF PROCTIME() AS D ON T.a = D.id",
      "Temporal table can only be used in temporal join and only supports " +
        "'FOR SYSTEM_TIME AS OF' left table's time attribute field.\n" +
        "Querying a temporal table using 'FOR SYSTEM TIME AS OF' syntax with " +
        "an expression call 'PROCTIME()' is not supported yet.",
      classOf[AssertionError]
    )
  }

  @Test
  def testNotDistinctFromInJoinCondition(): Unit = {

    // does not support join condition contains `IS NOT DISTINCT`
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a IS NOT  DISTINCT FROM D.id",
      "LookupJoin doesn't support join condition contains 'a IS NOT DISTINCT FROM b' (or " +
        "alternative '(a = b) or (a IS NULL AND b IS NULL)')",
      classOf[TableException]
    )

    // does not support join condition contains `IS NOT  DISTINCT` and similar syntax
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id OR (T.a IS NULL AND D.id IS NULL)",
      "LookupJoin doesn't support join condition contains 'a IS NOT DISTINCT FROM b' (or " +
        "alternative '(a = b) or (a IS NULL AND b IS NULL)')",
      classOf[TableException]
    )
  }

  @Test
  def testInvalidLookupTableFunction(): Unit = {
    if (legacyTableSource) {
      return
    }
    util.addDataStream[(Int, String, Long, Timestamp)](
      "T", 'a, 'b, 'c, 'ts, 'proctime.proctime)
    createLookupTable("LookupTable1", new InvalidTableFunctionResultType)
    expectExceptionThrown(
      "SELECT * FROM T JOIN LookupTable1 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      s"output class can simply be a Row or RowData class",
      classOf[ValidationException]
    )

    createLookupTable("LookupTable2", new InvalidTableFunctionEvalSignature)
    expectExceptionThrown(
      "SELECT * FROM T JOIN LookupTable2 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Could not find an implementation method 'eval' in class " +
          "'org.apache.flink.table.planner.plan.utils.InvalidTableFunctionEvalSignature' " +
          "for function 'default_catalog.default_database.LookupTable2' that matches the " +
          "following signature:\n" +
          "void eval(java.lang.Integer, org.apache.flink.table.data.StringData, " +
          "org.apache.flink.table.data.TimestampData)",
      classOf[ValidationException]
    )

    createLookupTable("LookupTable3", new TableFunctionWithRowDataVarArg)
    verifyTranslationSuccess("SELECT * FROM T JOIN LookupTable3 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    createLookupTable("LookupTable4", new TableFunctionWithRow)
    verifyTranslationSuccess("SELECT * FROM T JOIN LookupTable4 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    createLookupTable("LookupTable5", new AsyncTableFunctionWithRowDataVarArg)
    verifyTranslationSuccess("SELECT * FROM T JOIN LookupTable5 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    createLookupTable("LookupTable6", new AsyncTableFunctionWithRow)
    verifyTranslationSuccess("SELECT * FROM T JOIN LookupTable6 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    createLookupTable("LookupTable7", new InvalidAsyncTableFunctionEvalSignature1)
    expectExceptionThrown(
      "SELECT * FROM T JOIN LookupTable7 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Could not find an implementation method 'eval' in class " +
          "'org.apache.flink.table.planner.plan.utils.InvalidAsyncTableFunctionEvalSignature1' " +
          "for function 'default_catalog.default_database.LookupTable7' that matches the " +
          "following signature:\n" +
          "void eval(java.util.concurrent.CompletableFuture, java.lang.Integer, " +
          "org.apache.flink.table.data.StringData, org.apache.flink.table.data.TimestampData)",
      classOf[ValidationException]
    )

    createLookupTable("LookupTable8", new InvalidAsyncTableFunctionEvalSignature2)
    expectExceptionThrown(
      "SELECT * FROM T JOIN LookupTable8 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Could not find an implementation method 'eval' in class " +
          "'org.apache.flink.table.planner.plan.utils.InvalidAsyncTableFunctionEvalSignature2' " +
          "for function 'default_catalog.default_database.LookupTable8' that matches the " +
          "following signature:\nvoid eval(java.util.concurrent.CompletableFuture, " +
          "java.lang.Integer, java.lang.String, " +
          "java.time.LocalDateTime)",
      classOf[ValidationException]
    )

    createLookupTable("LookupTable9", new AsyncTableFunctionWithRowDataVarArg)
    verifyTranslationSuccess("SELECT * FROM T JOIN LookupTable9 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    createLookupTable("LookupTable10", new InvalidAsyncTableFunctionEvalSignature3)
    expectExceptionThrown(
      "SELECT * FROM T JOIN LookupTable10 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Could not find an implementation method 'eval' in class " +
          "'org.apache.flink.table.planner.plan.utils.InvalidAsyncTableFunctionEvalSignature3' " +
          "for function 'default_catalog.default_database.LookupTable10' that matches the " +
          "following signature:\n" +
          "void eval(java.util.concurrent.CompletableFuture, java.lang.Integer, " +
          "org.apache.flink.table.data.StringData, org.apache.flink.table.data.TimestampData)",
      classOf[ValidationException]
    )
  }

  @Test
  def testJoinOnDifferentKeyTypes(): Unit = {
    // Will do implicit type coercion.
    thrown.expect(classOf[TableException])
    thrown.expectMessage("VARCHAR(2147483647) and INTEGER does not have common type now")
    util.verifyExecPlan("SELECT * FROM MyTable AS T JOIN LookupTable "
      + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.b = D.id")
  }

  @Test
  def testJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T JOIN LookupTable " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"

    util.verifyExecPlan(sql)
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T LEFT JOIN LookupTable " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithNestedQuery(): Unit = {
    val sql = "SELECT * FROM " +
      "(SELECT a, b, proctime FROM MyTable WHERE c > 1000) AS T " +
      "JOIN LookupTable " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithProjectionPushDown(): Unit = {
    val sql =
      """
        |SELECT T.*, D.id
        |FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithFilterPushDown(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND D.age = 10
        |WHERE T.c > 1000
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithCalcPushDown(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND D.age = 10
        |WHERE cast(D.name as bigint) > 1000
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithMultiIndexColumn(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND D.age = 10 AND D.name = 'AAA'
        |WHERE T.c > 1000
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testAvoidAggregatePushDown(): Unit = {
    val sql1 =
      """
        |SELECT b, a, sum(c) c, sum(d) d, PROCTIME() as proc
        |FROM T1
        |GROUP BY a, b
      """.stripMargin

    val sql2 =
      s"""
         |SELECT T.* FROM ($sql1) AS T
         |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proc AS D
         |ON T.a = D.id
         |WHERE D.age > 10
      """.stripMargin

    val sql =
      s"""
         |SELECT b, count(a), sum(c), sum(d)
         |FROM ($sql2) AS T
         |GROUP BY b
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithTrueCondition(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON true
        |WHERE T.c > 1000
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithFunctionAndConstantCondition(): Unit = {

    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.b = concat(D.name, '!') AND D.age = 11
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithMultiFunctionAndConstantCondition(): Unit = {

    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id + 1 AND T.b = concat(D.name, '!') AND D.age = 11
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithFunctionAndReferenceCondition(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND T.b = concat(D.name, '!')
        |WHERE D.name LIKE 'Jack%'
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithUdfEqualFilter(): Unit = {
    val sql =
      """
        |SELECT
        |  T.a, T.b, T.c, D.name
        |FROM
        |  MyTable AS T JOIN LookupTable FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id
        |WHERE CONCAT('Hello-', D.name) = 'Hello-Jark'
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithComputedColumn(): Unit = {
    //Computed column do not support in legacyTableSource.
    Assume.assumeFalse(legacyTableSource)
    val sql =
      """
        |SELECT
        |  T.a, T.b, T.c, D.name, D.age, D.nominal_age
        |FROM
        |  MyTable AS T JOIN LookupTableWithComputedColumn FOR SYSTEM_TIME AS OF T.proctime AS D
        |  ON T.a = D.id
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithComputedColumnAndPushDown(): Unit = {
    //Computed column do not support in legacyTableSource.
    Assume.assumeFalse(legacyTableSource)
    val sql =
      """
        |SELECT
        |  T.a, T.b, T.c, D.name, D.age, D.nominal_age
        |FROM
        |  MyTable AS T JOIN LookupTableWithComputedColumn FOR SYSTEM_TIME AS OF T.proctime AS D
        |  ON T.a = D.id and D.nominal_age > 12
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  // ==========================================================================================

  private def createLookupTable(tableName: String, lookupFunction: UserDefinedFunction): Unit = {
    if (legacyTableSource) {
      lookupFunction match {
        case tf: TableFunction[_] =>
          TestInvalidTemporalTable.createTemporaryTable(
            util.tableEnv,
            tableName,
            tf)
        case atf: AsyncTableFunction[_] =>
          TestInvalidTemporalTable.createTemporaryTable(
            util.tableEnv,
            tableName,
            atf)
      }
    } else {
      util.addTable(
        s"""
           |CREATE TABLE $tableName (
           |  `id` INT,
           |  `name` STRING,
           |  `age` INT,
           |  `ts` TIMESTAMP(3)
           |) WITH (
           |  'connector' = 'values',
           |  'lookup-function-class' = '${lookupFunction.getClass.getName}'
           |)
           |""".stripMargin)
    }
  }

  private def expectExceptionThrown(
    sql: String,
    message: String,
    clazz: Class[_ <: Throwable] = classOf[ValidationException])
  : Unit = {
    try {
      verifyTranslationSuccess(sql)
      fail(s"Expected a $clazz, but no exception is thrown.")
    } catch { case e: Throwable =>
        assertTrue(clazz.isAssignableFrom(e.getClass))
        assertThat(e, containsMessage(message))
    }
  }

  private def verifyTranslationSuccess(sql: String): Unit = {
    util.tableEnv.sqlQuery(sql).explain()
  }
}

object LookupJoinTest {
  @Parameterized.Parameters(name = "LegacyTableSource={0}")
  def parameters(): JCollection[Array[Object]] = {
    Seq[Array[AnyRef]](Array(JBoolean.TRUE), Array(JBoolean.FALSE))
  }
}


class TestTemporalTable(bounded: Boolean = false)
  extends LookupableTableSource[RowData] with StreamTableSource[RowData] {

  val fieldNames: Array[String] = Array("id", "name", "age")
  val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.STRING, Types.INT)

  override def getLookupFunction(lookupKeys: Array[String]): TableFunction[RowData] = {
    new TableFunctionWithRowDataVarArg()
  }

  override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[RowData] = {
    new AsyncTableFunctionWithRowDataVarArg()
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[RowData] = {
    throw new UnsupportedOperationException("This TableSource is only used for unit test, " +
      "this method should never be called.")
  }

  override def isAsyncEnabled: Boolean = false

  override def isBounded: Boolean = this.bounded

  override def getProducedDataType: DataType = TestTemporalTable.tableSchema.toRowDataType

  override def getTableSchema: TableSchema = TestTemporalTable.tableSchema
}

object TestTemporalTable {
  lazy val tableSchema = TableSchema.builder()
    .field("id", DataTypes.INT())
    .field("name", DataTypes.STRING())
    .field("age", DataTypes.INT())
    .build()

  def createTemporaryTable(
      tEnv: TableEnvironment,
      tableName: String,
      isBounded: Boolean = false): Unit = {
    val desc = new CustomConnectorDescriptor("TestTemporalTable", 1, false)
    if (isBounded) {
      desc.property("is-bounded", "true")
    }
    tEnv.connect(desc)
      .withSchema(new Schema().schema(TestTemporalTable.tableSchema))
      .createTemporaryTable(tableName)
  }
}

class TestTemporalTableFactory extends TableSourceFactory[RowData] {

  override def createTableSource(properties: JMap[String, String]): TableSource[RowData] = {
    val dp = new DescriptorProperties
    dp.putProperties(properties)
    val isBounded = dp.getOptionalBoolean("is-bounded").orElse(false)
    new TestTemporalTable(isBounded)
  }

  override def requiredContext(): JMap[String, String] = {
    val context = new JHashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestTemporalTable")
    context
  }

  override def supportedProperties(): JList[String] = {
    val properties = new JArrayList[String]()
    properties.add("*")
    properties
  }
}

class TestInvalidTemporalTable private(
    async: Boolean,
    fetcher: TableFunction[_],
    asyncFetcher: AsyncTableFunction[_])
  extends LookupableTableSource[RowData] with StreamTableSource[RowData] {

  def this(fetcher: TableFunction[_]) {
    this(false, fetcher, null)
  }

  def this(asyncFetcher: AsyncTableFunction[_]) {
    this(true, null, asyncFetcher)
  }

  override def getProducedDataType: DataType = {
    TestInvalidTemporalTable.tableScheam.toRowDataType
  }

  override def getTableSchema: TableSchema = TestInvalidTemporalTable.tableScheam

  override def getLookupFunction(lookupKeys: Array[String]): TableFunction[RowData] = {
    fetcher.asInstanceOf[TableFunction[RowData]]
  }

  override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[RowData] = {
    asyncFetcher.asInstanceOf[AsyncTableFunction[RowData]]
  }


  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[RowData] = {
    throw new UnsupportedOperationException("This TableSource is only used for unit test, " +
      "this method should never be called.")
  }

  override def isAsyncEnabled: Boolean = async
}

object TestInvalidTemporalTable {
  lazy val tableScheam = TableSchema.builder()
    .field("id", DataTypes.INT())
    .field("name", DataTypes.STRING())
    .field("age", DataTypes.INT())
    .field("ts", DataTypes.TIMESTAMP(3))
    .build()

  def createTemporaryTable(
    tEnv: TableEnvironment,
    tableName: String,
    fetcher: TableFunction[_]): Unit = {
    tEnv
      .connect(
        new CustomConnectorDescriptor("TestInvalidTemporalTable", 1, false)
          .property("is-async", "false")
          .property(
            "fetcher",
            EncodingUtils.encodeObjectToString(fetcher)))
      .withSchema(new Schema().schema(TestInvalidTemporalTable.tableScheam))
      .createTemporaryTable(tableName)
  }

  def createTemporaryTable(
    tEnv: TableEnvironment,
    tableName: String,
    asyncFetcher: AsyncTableFunction[_]): Unit = {
    tEnv
      .connect(
        new CustomConnectorDescriptor("TestInvalidTemporalTable", 1, false)
          .property("is-async", "true")
          .property(
            "async-fetcher",
            EncodingUtils.encodeObjectToString(asyncFetcher)))
      .withSchema(new Schema().schema(TestInvalidTemporalTable.tableScheam))
      .createTemporaryTable(tableName)
  }
}

class TestInvalidTemporalTableFactory extends TableSourceFactory[RowData] {

  override def createTableSource(properties: JMap[String, String]): TableSource[RowData] = {
    val dp = new DescriptorProperties
    dp.putProperties(properties)
    val async = dp.getOptionalBoolean("is-async").orElse(false)
    if (!async) {
      val fetcherBase64 = dp.getOptionalString("fetcher")
        .orElseThrow(
          new util.function.Supplier[Throwable] {
            override def get() = new TableException(
              "Synchronous LookupableTableSource should provide a TableFunction.")
          })
      val fetcher = EncodingUtils.decodeStringToObject(fetcherBase64, classOf[TableFunction[_]])
      new TestInvalidTemporalTable(fetcher)
    } else {
      val asyncFetcherBase64 = dp.getOptionalString("async-fetcher")
        .orElseThrow(
          new util.function.Supplier[Throwable] {
            override def get() = new TableException(
              "Asynchronous LookupableTableSource should provide a AsyncTableFunction.")
          })
      val asyncFetcher = EncodingUtils.decodeStringToObject(
        asyncFetcherBase64, classOf[AsyncTableFunction[_]])
      new TestInvalidTemporalTable(asyncFetcher)
    }
  }

  override def requiredContext(): JMap[String, String] = {
    val context = new JHashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestInvalidTemporalTable")
    context
  }

  override def supportedProperties(): JList[String] = {
    val properties = new JArrayList[String]()
    properties.add("*")
    properties
  }
}
