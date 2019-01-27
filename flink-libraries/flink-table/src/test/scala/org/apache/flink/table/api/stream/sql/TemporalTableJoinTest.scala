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
package org.apache.flink.table.api.stream.sql

import java.sql.Timestamp
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.api.functions.{AsyncTableFunction, TableFunction}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, InternalType, RowType}
import org.apache.flink.table.api.{SqlParserException, TableException, TableSchema, ValidationException}
import org.apache.flink.table.dataformat.{BaseRow, BinaryString}
import org.apache.flink.table.sources._
import org.apache.flink.table.util.{StreamTableTestUtil, TableSchemaUtil, TableTestBase}
import org.apache.flink.types.Row

import org.junit.Assert.{assertTrue, fail}
import org.junit.Test

import scala.annotation.varargs

class TemporalTableJoinTest extends TableTestBase with Serializable {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'proc.proctime, 'rt.rowtime)
  streamUtil.addTable[(Int, String, Long, Double)]("T1", 'a, 'b, 'c, 'd)
  streamUtil.addTable[(Int, String, Int)]("nonTemporal", 'id, 'name, 'age)
  streamUtil.tableEnv.registerTableSource("temporalTest", new TestTemporalTable)

  @Test
  def testJoinInvalidJoinTemporalTable(): Unit = {
    // must follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN temporalTest T.proc AS D ON T.a = D.id",
      "SQL parse failed",
      classOf[SqlParserException])

    // can't as of non-proctime field
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.rt AS D ON T.a = D.id",
      "Currently only support join temporal table as of on left table's proctime field",
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
        "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.age",
      "Temporal table join requires an equality condition on ALL of " +
        "temporal table's primary key(s) or unique key(s) or index field(s)",
      classOf[TableException]
    )

    // only support left or inner join
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T RIGHT JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.id",
      "Unsupported join type for semi-join RIGHT",
      classOf[IllegalArgumentException]
    )

    // only support join on raw key of right table
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T LEFT JOIN temporalTest " +
        "FOR SYSTEM_TIME AS OF T.proc AS D ON concat('rk:', T.a) = concat(D.id, '=rk')",
      "Temporal table join requires an equality condition on ALL of " +
        "temporal table's primary key(s) or unique key(s) or index field(s)",
      classOf[TableException]
    )
  }

  @Test
  def testInvalidLookupTableFunction(): Unit = {
    streamUtil.addTable[(Int, String, Long, Timestamp)]("T", 'a, 'b, 'c, 'ts, 'proc.proctime)

    val temporalTable = new TestInvalidTemporalTable(new InvalidTableFunctionResultType)
    streamUtil.tableEnv.registerTableSource("temporalTable", temporalTable)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable " +
        "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "The TableSource [TestInvalidTemporalTable(id, name, age, ts)] " +
        "return type Row(id: Integer, name: String, age: Integer, ts: Timestamp) " +
        "do not match its lookup function extracted return type String",
      classOf[TableException]
    )

    val temporalTable2 = new TestInvalidTemporalTable(new InvalidTableFunctionEvalSignature1)
    streamUtil.tableEnv.registerTableSource("temporalTable2", temporalTable2)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable2 " +
        "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Expected: eval(java.lang.Integer, org.apache.flink.table.dataformat.BinaryString, " +
        "java.lang.Long) \n" +
        "Actual: eval(java.lang.Integer, java.lang.String, java.sql.Timestamp)",
      classOf[TableException]
    )

    val temporalTable3 = new TestInvalidTemporalTable(new ValidTableFunction)
    streamUtil.tableEnv.registerTableSource("temporalTable3", temporalTable3)
    streamUtil.explainSql("SELECT * FROM T AS T JOIN temporalTable3 " +
                            "FOR SYSTEM_TIME AS OF T.proc AS D " +
                            "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    val temporalTable4 = new TestInvalidTemporalTable(new ValidTableFunction2)
    streamUtil.tableEnv.registerTableSource("temporalTable4", temporalTable4)
    streamUtil.explainSql("SELECT * FROM T AS T JOIN temporalTable3 " +
                            "FOR SYSTEM_TIME AS OF T.proc AS D " +
                            "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    val temporalTable5 = new TestInvalidTemporalTable(new ValidAsyncTableFunction)
    streamUtil.tableEnv.registerTableSource("temporalTable5", temporalTable5)
    streamUtil.explainSql("SELECT * FROM T AS T JOIN temporalTable4 " +
                            "FOR SYSTEM_TIME AS OF T.proc AS D " +
                            "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    val temporalTable6 = new TestInvalidTemporalTable(new InvalidAsyncTableFunctionResultType)
    streamUtil.tableEnv.registerTableSource("temporalTable6", temporalTable6)
    streamUtil.explainSql("SELECT * FROM T AS T JOIN temporalTable6 " +
        "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")

    val temporalTable7 = new TestInvalidTemporalTable(new InvalidAsyncTableFunctionEvalSignature1)
    streamUtil.tableEnv.registerTableSource("temporalTable7", temporalTable7)
    expectExceptionThrown(
      "SELECT * FROM T AS T JOIN temporalTable7 " +
        "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
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
        "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.id AND T.b = D.name AND T.ts = D.ts",
      "Expected: eval(org.apache.flink.streaming.api.functions.async.ResultFuture, " +
        "java.lang.Integer, org.apache.flink.table.dataformat.BinaryString, java.lang.Long) \n" +
        "Actual: eval(org.apache.flink.streaming.api.functions.async.ResultFuture, " +
        "java.lang.Integer, java.lang.String, java.sql.Timestamp)",
      classOf[TableException]
    )

    val temporalTable9 = new TestInvalidTemporalTable(new ValidAsyncTableFunction)
    streamUtil.tableEnv.registerTableSource("temporalTable9", temporalTable9)
    streamUtil.explainSql("SELECT * FROM T AS T JOIN temporalTable9 " +
                            "FOR SYSTEM_TIME AS OF T.proc AS D " +
                            "ON T.a = D.id AND T.b = D.name AND T.ts = D.ts")
  }

  @Test
  def testJoinOnDifferentKeyTypes(): Unit = {
    // Will do implicit type coercion.
    streamUtil.verifyPlan("SELECT * FROM MyTable AS T JOIN temporalTest "
      + "FOR SYSTEM_TIME AS OF T.proc AS D ON T.b = D.id")
  }

  @Test
  def testJoinInvalidNonTemporalTable(): Unit = {
    // can't follow a period specification
    expectExceptionThrown(
      "SELECT * FROM MyTable AS T JOIN nonTemporal " +
        "FOR SYSTEM_TIME AS OF T.rt AS D ON T.a = D.id",
      "Table 'nonTemporal' is not a temporal table",
      classOf[ValidationException])
  }

  @Test
  def testJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T JOIN temporalTest " +
      "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.id"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val sql = "SELECT * FROM MyTable AS T LEFT JOIN temporalTest " +
      "FOR SYSTEM_TIME AS OF PROCTIME() AS D ON T.a = D.id"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithNestedQuery(): Unit = {
    val sql = "SELECT * FROM " +
      "(SELECT a, b, proc FROM MyTable WHERE c > 1000) AS T " +
      "JOIN temporalTest " +
      "FOR SYSTEM_TIME AS OF T.proc AS D ON T.a = D.id"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithProjectionPushDown(): Unit = {
    val sql =
      """
        |SELECT T.*, D.id
        |FROM MyTable AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF PROCTIME() AS D
        |ON T.a = D.id
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testJoinTemporalTableWithFilterPushDown(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proc AS D
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
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proc AS D
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
        |JOIN temporalTest FOR SYSTEM_TIME AS OF T.proc AS D
        |ON T.a = D.id AND D.age = 10 AND D.name = 'AAA'
        |WHERE T.c > 1000
      """.stripMargin

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testAvoidAggregatePushDown(): Unit = {
    val sql1 =
      """
        |SELECT b, a, sum(c) c, sum(d) d
        |FROM T1
        |GROUP BY a, b
      """.stripMargin

    val sql2 =
      s"""
        |SELECT T.* FROM ($sql1) AS T
        |JOIN temporalTest FOR SYSTEM_TIME AS OF PROCTIME() AS D
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
      streamUtil.tableEnv.explain(streamUtil.tableEnv.sqlQuery(sql))
      fail(s"Expected a $clazz, but no exception is thrown.")
    } catch {
      case e if e.getClass == clazz =>
        if (keywords != null) {
          assertTrue(
            s"The exception message '${e.getMessage}' doesn't contain keyword '$keywords'",
            e.getMessage.contains(keywords))
        }
      case e: Throwable =>
        e.printStackTrace()
        fail(s"Expected throw ${clazz.getSimpleName}, but is $e.")
    }
  }

  class TestTemporalTable extends StreamTableSource[BaseRow] with LookupableTableSource[BaseRow] {

    override def getReturnType: DataType = {
      new RowType(
        Array[DataType](DataTypes.INT, DataTypes.STRING, DataTypes.INT),
        Array("id", "name", "age"))
    }

    override def getTableSchema: TableSchema = {
      TableSchemaUtil
        .builderFromDataType(getReturnType)
        .normalIndex("name")
        .uniqueIndex("id")
        .build()
    }

    override def getLookupFunction(lookupKeys: Array[Int]): TableFunction[BaseRow] = null

    override def getAsyncLookupFunction(lookupKeys: Array[Int]): AsyncTableFunction[BaseRow] = null

    override def getLookupConfig: LookupConfig = new LookupConfig

    override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
      throw new UnsupportedOperationException
    }
  }

}


class TestInvalidTemporalTable private(
  async: Boolean,
  fetcher: TableFunction[_],
  asyncFetcher: AsyncTableFunction[_]
) extends StreamTableSource[BaseRow] with LookupableTableSource[BaseRow] {

  def this(fetcher: TableFunction[_]) {
    this(false, fetcher, null)
  }

  def this(asyncFetcher: AsyncTableFunction[_]) {
    this(true, null, asyncFetcher)
  }

  override def getReturnType: DataType = {
    new RowType(
      Array[DataType](DataTypes.INT, DataTypes.STRING, DataTypes.INT, DataTypes.TIMESTAMP),
      Array("id", "name", "age", "ts"))
  }

  override def getTableSchema: TableSchema = {
    TableSchemaUtil
    .builderFromDataType(getReturnType)
    .uniqueIndex("id", "name", "ts")
    .build()
  }

  override def getLookupFunction(lookupKeys: Array[Int]): TableFunction[BaseRow] = {
    fetcher.asInstanceOf[TableFunction[BaseRow]]
  }

  override def getAsyncLookupFunction(lookupKeys: Array[Int]): AsyncTableFunction[BaseRow] = {
    asyncFetcher.asInstanceOf[AsyncTableFunction[BaseRow]]
  }

  override def getLookupConfig: LookupConfig = {
    val config = new LookupConfig
    config.setAsyncEnabled(async)
    config
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[BaseRow] = {
    throw new UnsupportedOperationException
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

class ValidTableFunction2 extends TableFunction[BaseRow] {
  def eval(a: Integer, b: String, c: java.lang.Long): Unit = {
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
  def eval(resultFuture: ResultFuture[BaseRow], a: Integer, b: BinaryString, c: java.lang.Long)
  : Unit = {
  }
}
