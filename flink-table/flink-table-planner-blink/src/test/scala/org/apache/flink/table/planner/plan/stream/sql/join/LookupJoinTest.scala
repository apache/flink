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
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.dataformat.{BaseRow, BinaryString}
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction}
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.sources._
import org.apache.flink.table.types.logical.{IntType, TimestampType, VarCharType}
import org.apache.flink.types.Row

import org.junit.Assert.{assertTrue, fail}
import org.junit.Test

import _root_.java.lang.{Long => JLong}
import _root_.java.sql.Timestamp
import _root_.java.time.LocalDateTime
import _root_.java.util.concurrent.CompletableFuture
import _root_.java.util.{Collection => JCollection}

import _root_.scala.annotation.varargs

class LookupJoinTest extends TableTestBase with Serializable {
  private val streamUtil = scalaStreamTestUtil()
  streamUtil.addDataStream[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
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

    val temporalTable = new TestInvalidTemporalTable(new InvalidTableFunctionResultType)
    streamUtil.tableEnv.registerTableSource("temporalTable", temporalTable)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "The TableSource [TestInvalidTemporalTable(id, name, age, ts)] " +
        "return type BaseRow(id: INT, name: STRING, age: INT, ts: TIMESTAMP(3)) " +
        "does not match its lookup function extracted return type String",
      classOf[TableException]
    )

    val temporalTable2 = new TestInvalidTemporalTable(new InvalidTableFunctionEvalSignature1)
    streamUtil.tableEnv.registerTableSource("temporalTable2", temporalTable2)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable2 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Expected: eval(java.lang.Integer, org.apache.flink.table.dataformat.BinaryString, " +
        "org.apache.flink.table.dataformat.SqlTimestamp) \n" +
        "Actual: eval(java.lang.Integer, java.lang.String, java.time.LocalDateTime)",
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
      "Expected: eval(java.util.concurrent.CompletableFuture, " +
        "java.lang.Integer, org.apache.flink.table.dataformat.BinaryString, " +
        "org.apache.flink.table.dataformat.SqlTimestamp) \n" +
        "Actual: eval(java.lang.Integer, org.apache.flink.table.dataformat.BinaryString, " +
        "java.time.LocalDateTime)",
      classOf[TableException]
    )

    val temporalTable8 = new TestInvalidTemporalTable(new InvalidAsyncTableFunctionEvalSignature2)
    streamUtil.tableEnv.registerTableSource("temporalTable8", temporalTable8)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable8 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
        "Expected: eval(java.util.concurrent.CompletableFuture, " +
        "java.lang.Integer, org.apache.flink.table.dataformat.BinaryString, " +
        "org.apache.flink.table.dataformat.SqlTimestamp) \n" +
        "Actual: eval(java.util.concurrent.CompletableFuture, " +
        "java.lang.Integer, java.lang.String, java.time.LocalDateTime)",
      classOf[TableException]
    )

    val temporalTable9 = new TestInvalidTemporalTable(new ValidAsyncTableFunction)
    streamUtil.tableEnv.registerTableSource("temporalTable9", temporalTable9)
    verifyTranslationSuccess("SELECT * FROM T AS T JOIN temporalTable9 " +
      "FOR SYSTEM_TIME AS OF T.proctime AS D " +
      "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    val temporalTable10 = new TestInvalidTemporalTable(new InvalidAsyncTableFunctionEvalSignature3)
    streamUtil.tableEnv.registerTableSource("temporalTable10", temporalTable10)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable10 " +
        "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Expected: eval(java.util.concurrent.CompletableFuture, " +
        "java.lang.Integer, org.apache.flink.table.dataformat.BinaryString, " +
        "org.apache.flink.table.dataformat.SqlTimestamp) \n" +
        "Actual: eval(org.apache.flink.streaming.api.functions.async.ResultFuture, " +
        "java.lang.Integer, org.apache.flink.table.dataformat.BinaryString, java.lang.Long)",
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


class TestTemporalTable
  extends LookupableTableSource[BaseRow] {

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

  override def isAsyncEnabled: Boolean = false

  override def getReturnType: TypeInformation[BaseRow] = {
    new BaseRowTypeInfo(
      Array(new IntType(), new VarCharType(VarCharType.MAX_LENGTH), new IntType()),
      fieldNames)
  }

  override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)
}

class TestInvalidTemporalTable private(
    async: Boolean,
    fetcher: TableFunction[_],
    asyncFetcher: AsyncTableFunction[_])
  extends LookupableTableSource[BaseRow] {

  val fieldNames: Array[String] = Array("id", "name", "age", "ts")
  val fieldTypes: Array[TypeInformation[_]] = Array(
    Types.INT, Types.STRING, Types.INT, Types.LOCAL_DATE_TIME)

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

  override def isAsyncEnabled: Boolean = async
}

@SerialVersionUID(1L)
class InvalidTableFunctionResultType extends TableFunction[String] {
  @varargs
  def eval(obj: AnyRef*): Unit = {
  }
}

@SerialVersionUID(1L)
class InvalidTableFunctionEvalSignature1 extends TableFunction[BaseRow] {
  def eval(a: Integer, b: String, c: LocalDateTime): Unit = {
  }
}

@SerialVersionUID(1L)
class ValidTableFunction extends TableFunction[BaseRow] {
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
class InvalidAsyncTableFunctionEvalSignature1 extends AsyncTableFunction[BaseRow] {
  def eval(a: Integer, b: BinaryString, c: LocalDateTime): Unit = {
  }
}

@SerialVersionUID(1L)
class InvalidAsyncTableFunctionEvalSignature2 extends AsyncTableFunction[BaseRow] {
  def eval(resultFuture: CompletableFuture[JCollection[BaseRow]],
    a: Integer, b: String,  c: LocalDateTime): Unit = {
  }
}

@SerialVersionUID(1L)
class InvalidAsyncTableFunctionEvalSignature3 extends AsyncTableFunction[BaseRow] {
  def eval(resultFuture: ResultFuture[BaseRow],
    a: Integer, b: BinaryString,  c: JLong): Unit = {
  }
}

@SerialVersionUID(1L)
class ValidAsyncTableFunction extends AsyncTableFunction[BaseRow] {
  @varargs
  def eval(resultFuture: CompletableFuture[JCollection[BaseRow]], objs: AnyRef*): Unit = {
  }
}

@SerialVersionUID(1L)
class ValidAsyncTableFunction2 extends AsyncTableFunction[BaseRow] {
  def eval(resultFuture: CompletableFuture[JCollection[BaseRow]],
    a: Integer, b: BinaryString, c: JLong): Unit = {
  }
}
