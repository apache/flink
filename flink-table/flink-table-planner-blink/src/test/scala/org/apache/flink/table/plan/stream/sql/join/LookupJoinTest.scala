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

package org.apache.flink.table.plan.stream.sql.join

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.api._
import org.apache.flink.table.dataformat.{BaseRow, BinaryString}
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction}
import org.apache.flink.table.sources._
import org.apache.flink.api.scala._
import org.apache.flink.table.types.logical.{IntType, TimestampType, VarCharType}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.types.Row

import org.junit.Assert.{assertTrue, fail}
import org.junit.Test

import _root_.java.util
import _root_.java.lang.{Long => JLong}
import _root_.java.sql.Timestamp

import _root_.scala.annotation.varargs

class LookupJoinTest extends TableTestBase with Serializable {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addDataStream[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'proctime, 'rowtime)
  streamUtil.addDataStream[(Int, String, Long, Double)]("T1", 'a, 'b, 'c, 'd)
  streamUtil.addDataStream[(Int, String, Int)]("nonTemporal", 'id, 'name, 'age)
  streamUtil.tableEnv.registerTableSource("temporalTest", new TestTemporalTable)

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

    // can't on non-key fields
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.age",
      "Temporal table join requires an equality condition on ALL fields of table " +
        "[TestTemporalTable(id, name, age)]'s PRIMARY KEY or (UNIQUE) INDEX(s).",
      classOf[TableException]
    )

    // only support left or inner join
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T RIGHT JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id",
      "Unsupported join type for semi-join RIGHT",
      classOf[IllegalArgumentException]
    )

    // only support join on raw key of right table
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a + 1 = D.id + 2",
      "Temporal table join requires an equality condition on ALL fields of table " +
        "[TestTemporalTable(id, name, age)]'s PRIMARY KEY or (UNIQUE) INDEX(s).",
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
  def testInvalidLookupTableFunction(): Unit = {
    streamUtil.addDataStream[(Int, String, Long, Timestamp)]("T", 'a, 'b, 'c, 'ts, 'proctime)

    val temporalTable = new TestInvalidTemporalTable(new InvalidTableFunctionResultType)
    streamUtil.tableEnv.registerTableSource("temporalTable", temporalTable)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "The TableSource [TestInvalidTemporalTable(id, name, age, ts)] " +
        "return type BaseRow(id: Integer, name: String, age: Integer, ts: Timestamp) " +
        "does not match its lookup function extracted return type String",
      classOf[TableException]
    )

    val temporalTable2 = new TestInvalidTemporalTable(new InvalidTableFunctionEvalSignature1)
    streamUtil.tableEnv.registerTableSource("temporalTable2", temporalTable2)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable2 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Expected: eval(java.lang.Integer, org.apache.flink.table.dataformat.BinaryString, " +
        "java.lang.Long) \n" +
        "Actual: eval(java.lang.Integer, java.lang.String, java.sql.Timestamp)",
      classOf[TableException]
    )

    val temporalTable3 = new TestInvalidTemporalTable(new ValidTableFunction)
    streamUtil.tableEnv.registerTableSource("temporalTable3", temporalTable3)
    verifyTranslationSuccess("SELECT * FROM T AS T JOIN temporalTable3 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    val temporalTable4 = new TestInvalidTemporalTable(new ValidTableFunction2)
    streamUtil.tableEnv.registerTableSource("temporalTable4", temporalTable4)
    verifyTranslationSuccess("SELECT * FROM T AS T JOIN temporalTable4 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    val temporalTable5 = new TestInvalidTemporalTable(new ValidAsyncTableFunction)
    streamUtil.tableEnv.registerTableSource("temporalTable5", temporalTable5)
    verifyTranslationSuccess("SELECT * FROM T AS T JOIN temporalTable5 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    val temporalTable6 = new TestInvalidTemporalTable(new InvalidAsyncTableFunctionResultType)
    streamUtil.tableEnv.registerTableSource("temporalTable6", temporalTable6)
    verifyTranslationSuccess("SELECT * FROM T AS T JOIN temporalTable6 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    val temporalTable7 = new TestInvalidTemporalTable(new InvalidAsyncTableFunctionEvalSignature1)
    streamUtil.tableEnv.registerTableSource("temporalTable7", temporalTable7)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable7 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Expected: eval(org.apache.flink.streaming.api.functions.async.ResultFuture, " +
        "java.lang.Integer, org.apache.flink.table.dataformat.BinaryString, java.lang.Long) \n" +
        "Actual: eval(java.lang.Integer, org.apache.flink.table.dataformat.BinaryString, " +
        "java.sql.Timestamp)",
      classOf[TableException]
    )

    val temporalTable8 = new TestInvalidTemporalTable(new InvalidAsyncTableFunctionEvalSignature2)
    streamUtil.tableEnv.registerTableSource("temporalTable8", temporalTable8)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable8 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
        "Expected: eval(org.apache.flink.streaming.api.functions.async.ResultFuture, " +
        "java.lang.Integer, org.apache.flink.table.dataformat.BinaryString, java.lang.Long) \n" +
        "Actual: eval(org.apache.flink.streaming.api.functions.async.ResultFuture, " +
        "java.lang.Integer, java.lang.String, java.sql.Timestamp)",
      classOf[TableException]
    )

    val temporalTable9 = new TestInvalidTemporalTable(new ValidAsyncTableFunction)
    streamUtil.tableEnv.registerTableSource("temporalTable9", temporalTable9)
    verifyTranslationSuccess("SELECT * FROM T AS T JOIN temporalTable9 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")
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
      "Table 'nonTemporal' is not a temporal table",
      classOf[ValidationException])
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


class TestTemporalTable
  extends LookupableTableSource[BaseRow]
  with DefinedIndexes {

  val fieldNames: Array[String] = Array("id", "name", "age")
  val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.STRING, Types.INT)

  override def getLookupFunction(lookupKeys: Array[String]): TableFunction[BaseRow] = {
    throw new UnsupportedOperationException("This TableSource is only used for unit test, " +
      "this method should never be called.")
  }

  override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[BaseRow] = {
    throw new UnsupportedOperationException("This TableSource is only used for unit test, " +
      "this method should never be called.")
  }

  override def getLookupConfig: LookupConfig = LookupConfig.DEFAULT

  override def getReturnType: TypeInformation[BaseRow] = {
    new BaseRowTypeInfo(
      Array(new IntType(), new VarCharType(VarCharType.MAX_LENGTH), new IntType()),
      fieldNames)
  }

  override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)

  override def getIndexes: util.Collection[TableIndex] = {
    val index1 = TableIndex.builder()
      .normalIndex()
      .indexedColumns("name")
      .build()
    val index2 = TableIndex.builder()
      .uniqueIndex()
      .indexedColumns("id")
      .build()
    util.Arrays.asList(index1, index2)
  }
}

class TestInvalidTemporalTable private(
    async: Boolean,
    fetcher: TableFunction[_],
    asyncFetcher: AsyncTableFunction[_])
  extends StreamTableSource[BaseRow]
  with LookupableTableSource[BaseRow]
  with DefinedIndexes {

  val fieldNames: Array[String] = Array("id", "name", "age", "ts")
  val fieldTypes: Array[TypeInformation[_]] = Array(
    Types.INT, Types.STRING, Types.INT, Types.SQL_TIMESTAMP)

  def this(fetcher: TableFunction[_]) {
    this(false, fetcher, null)
  }

  def this(asyncFetcher: AsyncTableFunction[_]) {
    this(true, null, asyncFetcher)
  }

  override def getReturnType: TypeInformation[BaseRow] = {
    new BaseRowTypeInfo(
      Array(new IntType(), new VarCharType(VarCharType.MAX_LENGTH),
        new IntType(), new TimestampType(3)),
      fieldNames)
  }

  override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)

  override def getLookupFunction(lookupKeys: Array[String]): TableFunction[BaseRow] = {
    fetcher.asInstanceOf[TableFunction[BaseRow]]
  }

  override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[BaseRow] = {
    asyncFetcher.asInstanceOf[AsyncTableFunction[BaseRow]]
  }

  override def getLookupConfig: LookupConfig = {
    LookupConfig.builder()
        .setAsyncEnabled(async)
        .build()
  }

  override def getIndexes: util.Collection[TableIndex] = {
    util.Collections.singleton(TableIndex.builder()
      .uniqueIndex()
      .indexedColumns("id", "name", "ts")
      .build())
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
    throw new UnsupportedOperationException("This TableSource is only used for unit test, " +
      "this method should never be called.")
  }

}

class InvalidTableFunctionResultType extends TableFunction[String] {
  @varargs
  def eval(obj: AnyRef*): Unit = {
  }
}

class InvalidTableFunctionEvalSignature1 extends TableFunction[BaseRow] {
  def eval(a: Integer, b: String, c: Timestamp): Unit = {
  }
}

class ValidTableFunction extends TableFunction[BaseRow] {
  @varargs
  def eval(obj: AnyRef*): Unit = {
  }
}

class ValidTableFunction2 extends TableFunction[Row] {
  def eval(a: Integer, b: String, c: Timestamp): Unit = {
  }
}

class InvalidAsyncTableFunctionResultType extends AsyncTableFunction[Row] {
  @varargs
  def eval(obj: AnyRef*): Unit = {
  }
}

class InvalidAsyncTableFunctionEvalSignature1 extends AsyncTableFunction[BaseRow] {
  def eval(a: Integer, b: BinaryString, c: Timestamp): Unit = {
  }
}

class InvalidAsyncTableFunctionEvalSignature2 extends AsyncTableFunction[BaseRow] {
  def eval(resultFuture: ResultFuture[BaseRow], a: Integer, b: String,  c: Timestamp): Unit = {
  }
}

class ValidAsyncTableFunction extends AsyncTableFunction[BaseRow] {
  @varargs
  def eval(resultFuture: ResultFuture[BaseRow], objs: AnyRef*): Unit = {
  }
}

class ValidAsyncTableFunction2 extends AsyncTableFunction[BaseRow] {
  def eval(resultFuture: ResultFuture[BaseRow], a: Integer, b: BinaryString, c: JLong): Unit = {
  }
}
