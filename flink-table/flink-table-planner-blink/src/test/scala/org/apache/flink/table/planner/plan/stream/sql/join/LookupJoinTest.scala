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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.data.{RowData, StringData}
import org.apache.flink.table.descriptors.{CustomConnectorDescriptor, DescriptorProperties, Schema}
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction}
import org.apache.flink.table.factories.TableSourceFactory
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.sources.{LookupableTableSource, StreamTableSource, TableSource}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.utils.EncodingUtils
import org.apache.flink.types.Row

import org.junit.Assert.{assertTrue, fail}
import org.junit.Test

import _root_.java.lang.{Long => JLong}
import _root_.java.sql.Timestamp
import _root_.java.time.LocalDateTime
import _root_.java.util
import _root_.java.util.concurrent.CompletableFuture
import _root_.java.util.{Collection => JCollection, List => JList, Map => JMap}

import _root_.scala.annotation.varargs

class LookupJoinTest extends TableTestBase with Serializable {
  private val streamUtil = scalaStreamTestUtil()
  streamUtil.addDataStream[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
  streamUtil.addDataStream[(Int, String, Long, Double)]("T1", 'a, 'b, 'c, 'd)
  streamUtil.addDataStream[(Int, String, Int)]("nonTemporal", 'id, 'name, 'age)

  TestTemporalTable.createTemporaryTable(streamUtil.tableEnv, "temporalTest")

  @Test
  def testJoinInvalidJoinTemporalTable(): Unit = {
    // must follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN temporalTest T.proctime AS D ON T.a = D.id",
      "SQL parse failed",
      classOf[SqlParserException])

    // can't as of non-proctime field
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.rowtime AS D ON T.a = D.id",
      "Temporal table join currently only supports 'FOR SYSTEM_TIME AS OF' " +
        "left table's proctime field",
      classOf[TableException])

    // can't query a dim table directly
    expectExceptionThrown(
      "SELECT * FROM temporalTest FOR SYSTEM_TIME AS OF TIMESTAMP '2017-08-09 14:36:11'",
      "Cannot generate a valid execution plan for the given query",
      classOf[TableException]
    )

    // only support left or inner join
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T RIGHT JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id",
      "Correlate has invalid join type RIGHT",
      classOf[AssertionError]
    )

    // only support join on raw key of right table
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a + 1 = D.id + 2",
      "Temporal table join requires an equality condition on fields of table " +
        "[TestTemporalTable(id, name, age)].",
      classOf[TableException]
    )

    // only support "FOR SYSTEM_TIME AS OF" left table's proctime
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF PROCTIME() AS D ON T.a = D.id",
      "Temporal table join currently only supports 'FOR SYSTEM_TIME AS OF' " +
        "left table's proctime field, doesn't support 'PROCTIME()'",
      classOf[TableException]
    )
  }

  @Test
  def testNotDistinctFromInJoinCondition(): Unit = {

    // does not support join condition contains `IS NOT DISTINCT`
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a IS NOT  DISTINCT FROM D.id",
      "LookupJoin doesn't support join condition contains 'a IS NOT DISTINCT FROM b' (or " +
        "alternative '(a = b) or (a IS NULL AND b IS NULL)')",
      classOf[TableException]
    )

    // does not support join condition contains `IS NOT  DISTINCT` and similar syntax
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id OR (T.a IS NULL AND D.id IS NULL)",
      "LookupJoin doesn't support join condition contains 'a IS NOT DISTINCT FROM b' (or " +
        "alternative '(a = b) or (a IS NULL AND b IS NULL)')",
      classOf[TableException]
    )
  }

  @Test
  def testInvalidLookupTableFunction(): Unit = {
    streamUtil.addDataStream[(Int, String, Long, Timestamp)](
      "T", 'a, 'b, 'c, 'ts, 'proctime.proctime)

    TestInvalidTemporalTable.createTemporaryTable(
      streamUtil.tableEnv,
      "temporalTable",
      new InvalidTableFunctionResultType)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "The TableSource [TestInvalidTemporalTable(id, name, age, ts)] " +
        "return type Row(id: Integer, name: String, age: Integer, ts: LocalDateTime) " +
        "does not match its lookup function extracted return type String",
      classOf[TableException]
    )

    TestInvalidTemporalTable.createTemporaryTable(
      streamUtil.tableEnv,
      "temporalTable2",
      new InvalidTableFunctionEvalSignature1)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable2 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Expected: eval(java.lang.Integer, org.apache.flink.table.data.StringData, " +
        "org.apache.flink.table.data.TimestampData) \n" +
        "Actual: eval(java.lang.Integer, java.lang.String, java.time.LocalDateTime)",
      classOf[TableException]
    )

    TestInvalidTemporalTable.createTemporaryTable(
      streamUtil.tableEnv,
      "temporalTable3",
      new ValidTableFunction)
    verifyTranslationSuccess("SELECT * FROM T AS T JOIN temporalTable3 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    TestInvalidTemporalTable.createTemporaryTable(
      streamUtil.tableEnv,
      "temporalTable4",
      new ValidTableFunction2)
    verifyTranslationSuccess("SELECT * FROM T AS T JOIN temporalTable4 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    TestInvalidTemporalTable.createTemporaryTable(
      streamUtil.tableEnv,
      "temporalTable5",
      new ValidAsyncTableFunction)
    verifyTranslationSuccess("SELECT * FROM T AS T JOIN temporalTable5 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    TestInvalidTemporalTable.createTemporaryTable(
      streamUtil.tableEnv,
      "temporalTable6",
      new InvalidAsyncTableFunctionResultType)
    verifyTranslationSuccess("SELECT * FROM T AS T JOIN temporalTable6 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    TestInvalidTemporalTable.createTemporaryTable(
      streamUtil.tableEnv,
      "temporalTable7",
      new InvalidAsyncTableFunctionEvalSignature1)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable7 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Expected: eval(java.util.concurrent.CompletableFuture, " +
        "java.lang.Integer, org.apache.flink.table.data.StringData, " +
        "org.apache.flink.table.data.TimestampData) \n" +
        "Actual: eval(java.lang.Integer, org.apache.flink.table.data.StringData, " +
        "java.time.LocalDateTime)",
      classOf[TableException]
    )

    TestInvalidTemporalTable.createTemporaryTable(
      streamUtil.tableEnv,
      "temporalTable8",
      new InvalidAsyncTableFunctionEvalSignature2)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable8 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
        "Expected: eval(java.util.concurrent.CompletableFuture, " +
        "java.lang.Integer, org.apache.flink.table.data.StringData, " +
        "org.apache.flink.table.data.TimestampData) \n" +
        "Actual: eval(java.util.concurrent.CompletableFuture, " +
        "java.lang.Integer, java.lang.String, java.time.LocalDateTime)",
      classOf[TableException]
    )

    TestInvalidTemporalTable.createTemporaryTable(
      streamUtil.tableEnv,
      "temporalTable9",
      new ValidAsyncTableFunction)
    verifyTranslationSuccess("SELECT * FROM T AS T JOIN temporalTable9 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    TestInvalidTemporalTable.createTemporaryTable(
      streamUtil.tableEnv,
      "temporalTable10",
      new InvalidAsyncTableFunctionEvalSignature3)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable10 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Expected: eval(java.util.concurrent.CompletableFuture, " +
        "java.lang.Integer, org.apache.flink.table.data.StringData, " +
        "org.apache.flink.table.data.TimestampData) \n" +
        "Actual: eval(org.apache.flink.streaming.api.functions.async.ResultFuture, " +
        "java.lang.Integer, org.apache.flink.table.data.StringData, java.lang.Long)",
      classOf[TableException]
    )
  }

  @Test
  def testJoinOnDifferentKeyTypes(): Unit = {
    // Will do implicit type coercion.
    thrown.expect(classOf[TableException])
    thrown.expectMessage("VARCHAR(2147483647) and INTEGER does not have common type now")
    streamUtil.verifyPlan("SELECT * FROM MyTable AS T JOIN temporalTest "
      + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.b = D.id")
  }

  @Test
  def testJoinInvalidNonTemporalTable(): Unit = {
    // can't follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN nonTemporal " +
        "FOR SYSTEM_TIME AS OF T.rowtime AS D ON T.a = D.id",
      "Temporal table join only support join on a LookupableTableSource",
      classOf[TableException])
  }

  @Test
  def testJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T JOIN temporalTest " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T LEFT JOIN temporalTest " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithNestedQuery(): Unit = {
    val sql = "SELECT * FROM " +
      "(SELECT a, b, proctime FROM MyTable WHERE c > 1000) AS T " +
      "JOIN temporalTest " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithProjectionPushDown(): Unit = {
    val sql =
      """
        |SELECT T.*, D.id
        |FROM MyTable AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithFilterPushDown(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND D.age = 10
        |WHERE T.c > 1000
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithCalcPushDown(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND D.age = 10
        |WHERE cast(D.name as bigint) > 1000
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithMultiIndexColumn(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND D.age = 10 AND D.name = 'AAA'
        |WHERE T.c > 1000
      """.stripMargin

    streamUtil.verifyPlan(sql)
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
         |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proc AS D
         |ON T.a = D.id
         |WHERE D.age > 10
      """.stripMargin

    val sql =
      s"""
         |SELECT b, count(a), sum(c), sum(d)
         |FROM ($sql2) AS T
         |GROUP BY b
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithTrueCondition(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON true
        |WHERE T.c > 1000
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithFunctionAndConstantCondition(): Unit = {

    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.b = concat(D.name, '!') AND D.age = 11
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithMultiFunctionAndConstantCondition(): Unit = {

    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id + 1 AND T.b = concat(D.name, '!') AND D.age = 11
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithFunctionAndReferenceCondition(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proctime AS D
        |ON T.a = D.id AND T.b = concat(D.name, '!')
        |WHERE D.name LIKE 'Jack%'
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  // ==========================================================================================

  private def expectExceptionThrown(
    sql: String,
    keywords: String,
    clazz: Class[_ <: Throwable] = classOf[ValidationException])
  : Unit = {
    try {
      streamUtil.tableEnv.toAppendStream[Row](streamUtil.tableEnv.sqlQuery(sql))
      fail(s"Expected a $clazz, but no exception is thrown.")
    } catch {
      case e if e.getClass == clazz =>
              if (keywords != null) {
                assertTrue(
                  s"The actual exception message \n${e.getMessage}\n" +
                    s"doesn't contain expected keyword \n$keywords\n",
                  e.getMessage.contains(keywords))
              }
      case e: Throwable =>
        e.printStackTrace()
        fail(s"Expected throw ${clazz.getSimpleName}, but is $e.")
    }
  }

  private def verifyTranslationSuccess(sql: String): Unit = {
    streamUtil.tableEnv.toAppendStream[Row](streamUtil.tableEnv.sqlQuery(sql))
  }
}

class TestTemporalTable(bounded: Boolean = false)
  extends LookupableTableSource[RowData] with StreamTableSource[RowData] {

  override def getLookupFunction(lookupKeys: Array[String]): TableFunction[RowData] = {
    throw new UnsupportedOperationException("This TableSource is only used for unit test, " +
      "this method should never be called.")
  }

  override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[RowData] = {
    throw new UnsupportedOperationException("This TableSource is only used for unit test, " +
      "this method should never be called.")
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[RowData] = {
    throw new UnsupportedOperationException("This TableSource is only used for unit test, " +
      "this method should never be called.")
  }

  override def isBounded: Boolean = this.bounded

  override def isAsyncEnabled: Boolean = false

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
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestTemporalTable")
    context
  }

  override def supportedProperties(): JList[String] = {
    val properties = new util.ArrayList[String]()
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
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestInvalidTemporalTable")
    context
  }

  override def supportedProperties(): JList[String] = {
    val properties = new util.ArrayList[String]()
    properties.add("*")
    properties
  }
}

@SerialVersionUID(1L)
class InvalidTableFunctionResultType extends TableFunction[String] {
  @varargs
  def eval(obj: AnyRef*): Unit = {
  }
}

@SerialVersionUID(1L)
class InvalidTableFunctionEvalSignature1 extends TableFunction[RowData] {
  def eval(a: Integer, b: String, c: LocalDateTime): Unit = {
  }
}

@SerialVersionUID(1L)
class ValidTableFunction extends TableFunction[RowData] {
  @varargs
  def eval(obj: AnyRef*): Unit = {
  }
}

@SerialVersionUID(1L)
class ValidTableFunction2 extends TableFunction[Row] {
  def eval(a: Integer, b: String, c: LocalDateTime): Unit = {
  }
}

@SerialVersionUID(1L)
class InvalidAsyncTableFunctionResultType extends AsyncTableFunction[Row] {
  @varargs
  def eval(obj: AnyRef*): Unit = {
  }
}

@SerialVersionUID(1L)
class InvalidAsyncTableFunctionEvalSignature1 extends AsyncTableFunction[RowData] {
  def eval(a: Integer, b: StringData, c: LocalDateTime): Unit = {
  }
}

@SerialVersionUID(1L)
class InvalidAsyncTableFunctionEvalSignature2 extends AsyncTableFunction[RowData] {
  def eval(resultFuture: CompletableFuture[JCollection[RowData]],
    a: Integer, b: String,  c: LocalDateTime): Unit = {
  }
}

@SerialVersionUID(1L)
class InvalidAsyncTableFunctionEvalSignature3 extends AsyncTableFunction[RowData] {
  def eval(resultFuture: ResultFuture[RowData],
    a: Integer, b: StringData,  c: JLong): Unit = {
  }
}

@SerialVersionUID(1L)
class ValidAsyncTableFunction extends AsyncTableFunction[RowData] {
  @varargs
  def eval(resultFuture: CompletableFuture[JCollection[RowData]], objs: AnyRef*): Unit = {
  }
}

@SerialVersionUID(1L)
class ValidAsyncTableFunction2 extends AsyncTableFunction[RowData] {
  def eval(resultFuture: CompletableFuture[JCollection[RowData]],
    a: Integer, b: StringData, c: JLong): Unit = {
  }
}
