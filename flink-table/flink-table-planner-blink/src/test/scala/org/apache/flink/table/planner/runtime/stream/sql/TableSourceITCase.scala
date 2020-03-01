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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, TableSchema, Types}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestData, TestingAppendSink}
import org.apache.flink.table.planner.utils.{TestDataTypeTableSource, TestFilterableTableSource, TestInputFormatTableSource, TestNestedProjectableTableSource, TestPartitionableSourceFactory, TestProjectableTableSource, TestStreamTableSource, TestTableSources}
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.Test

import java.lang.{Boolean => JBool, Integer => JInt, Long => JLong}

import scala.collection.mutable

class TableSourceITCase extends StreamingTestBase {

  @Test
  def testProjectWithoutRowtimeProctime(): Unit = {
    val data = Seq(
      Row.of(new JInt(1), "Mary", new JLong(10L), new JLong(1)),
      Row.of(new JInt(2), "Bob", new JLong(20L), new JLong(2)),
      Row.of(new JInt(3), "Mike", new JLong(30L), new JLong(2)),
      Row.of(new JInt(4), "Liz", new JLong(40L), new JLong(2001)))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    tEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(false, tableSchema, returnType, data, "rtime", "ptime"))

    val result = tEnv.sqlQuery("SELECT name, val, id FROM T").toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "Mary,10,1",
      "Bob,20,2",
      "Mike,30,3",
      "Liz,40,4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProjectWithoutProctime(): Unit = {
    val data = Seq(
      Row.of(new JInt(1), "Mary", new JLong(10L), new JLong(1)),
      Row.of(new JInt(2), "Bob", new JLong(20L), new JLong(2)),
      Row.of(new JInt(3), "Mike", new JLong(30L), new JLong(2)),
      Row.of(new JInt(4), "Liz", new JLong(40L), new JLong(2001)))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(
        Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    tEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(false, tableSchema, returnType, data, "rtime", "ptime"))

    val result = tEnv.sqlQuery("SELECT rtime, name, id FROM T").toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01T00:00:00.001,Mary,1",
      "1970-01-01T00:00:00.002,Bob,2",
      "1970-01-01T00:00:00.002,Mike,3",
      "1970-01-01T00:00:02.001,Liz,4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProjectWithoutRowtime(): Unit = {
    val data = Seq(
      Row.of(new JInt(1), "Mary", new JLong(10L), new JLong(1)),
      Row.of(new JInt(2), "Bob", new JLong(20L), new JLong(2)),
      Row.of(new JInt(3), "Mike", new JLong(30L), new JLong(2)),
      Row.of(new JInt(4), "Liz", new JLong(40L), new JLong(2001)))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    tEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(false, tableSchema, returnType, data, "rtime", "ptime"))

    val sqlQuery = "SELECT name, id FROM T"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "Mary,1",
      "Bob,2",
      "Mike,3",
      "Liz,4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  def testProjectOnlyProctime(): Unit = {
    val data = Seq(
      Row.of(new JInt(1), new JLong(1), new JLong(10L), "Mary"),
      Row.of(new JInt(2), new JLong(2L), new JLong(20L), "Bob"),
      Row.of(new JInt(3), new JLong(2L), new JLong(30L), "Mike"),
      Row.of(new JInt(4), new JLong(2001L), new JLong(30L), "Liz"))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    tEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(false, tableSchema, returnType, data, "rtime", "ptime"))

    val sqlQuery = "SELECT COUNT(1) FROM T WHERE ptime > 0"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  def testProjectOnlyRowtime(): Unit = {
    val data = Seq(
      Row.of(new JInt(1), new JLong(1), new JLong(10L), "Mary"),
      Row.of(new JInt(2), new JLong(2L), new JLong(20L), "Bob"),
      Row.of(new JInt(3), new JLong(2L), new JLong(30L), "Mike"),
      Row.of(new JInt(4), new JLong(2001L), new JLong(30L), "Liz"))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    tEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(false, tableSchema, returnType, data, "rtime", "ptime"))

    val result = tEnv.sqlQuery("SELECT rtime FROM T").toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.001",
      "1970-01-01 00:00:00.002",
      "1970-01-01 00:00:00.002",
      "1970-01-01 00:00:02.001")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProjectWithMapping(): Unit = {
    val data = Seq(
      Row.of(new JLong(1), new JInt(1), "Mary", new JLong(10)),
      Row.of(new JLong(2), new JInt(2), "Bob", new JLong(20)),
      Row.of(new JLong(2), new JInt(3), "Mike", new JLong(30)),
      Row.of(new JLong(2001), new JInt(4), "Liz", new JLong(40)))

    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.LONG, Types.INT, Types.STRING, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("p-rtime", "p-id", "p-name", "p-val"))
    val mapping = Map("rtime" -> "p-rtime", "id" -> "p-id", "val" -> "p-val", "name" -> "p-name")

    tEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(
        false, tableSchema, returnType, data, "rtime", "ptime", mapping))

    val result = tEnv.sqlQuery("SELECT name, rtime, val FROM T").toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "Mary,1970-01-01T00:00:00.001,10",
      "Bob,1970-01-01T00:00:00.002,20",
      "Mike,1970-01-01T00:00:00.002,30",
      "Liz,1970-01-01T00:00:02.001,40")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNestedProject(): Unit = {
    val data = Seq(
      Row.of(new JLong(1),
        Row.of(
          Row.of("Sarah", new JInt(100)),
          Row.of(new JInt(1000), new JBool(true))
        ),
        Row.of("Peter", new JInt(10000)),
        "Mary"),
      Row.of(new JLong(2),
        Row.of(
          Row.of("Rob", new JInt(200)),
          Row.of(new JInt(2000), new JBool(false))
        ),
        Row.of("Lucy", new JInt(20000)),
        "Bob"),
      Row.of(new JLong(3),
        Row.of(
          Row.of("Mike", new JInt(300)),
          Row.of(new JInt(3000), new JBool(true))
        ),
        Row.of("Betty", new JInt(30000)),
        "Liz"))

    val nested1 = new RowTypeInfo(
      Array(Types.STRING, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("name", "value")
    )
    val nested2 = new RowTypeInfo(
      Array(Types.INT, Types.BOOLEAN).asInstanceOf[Array[TypeInformation[_]]],
      Array("num", "flag")
    )
    val deepNested = new RowTypeInfo(
      Array(nested1, nested2).asInstanceOf[Array[TypeInformation[_]]],
      Array("nested1", "nested2")
    )
    val tableSchema = new TableSchema(
      Array("id", "deepNested", "nested", "name"),
      Array(Types.LONG, deepNested, nested1, Types.STRING))

    val returnType = new RowTypeInfo(
      Array(Types.LONG, deepNested, nested1, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "deepNested", "nested", "name"))

    tEnv.registerTableSource(
      "T",
      new TestNestedProjectableTableSource(false, tableSchema, returnType, data))

    val sqlQuery =
      """
        |SELECT id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.nested2.flag AS nestedFlag,
        |    deepNested.nested2.num AS nestedNum
        |FROM T
      """.stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "1,Sarah,10000,true,1000",
      "2,Rob,20000,false,2000",
      "3,Mike,30000,true,3000")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testTableSourceWithFilterable(): Unit = {
    tEnv.registerTableSource("MyTable", TestFilterableTableSource(false))

    val sqlQuery = "SELECT id, name FROM MyTable WHERE amount > 4 AND price < 9"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("5,Record_5", "6,Record_6", "7,Record_7", "8,Record_8")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testTableSourceWithPartitionable(): Unit = {
    TestPartitionableSourceFactory.registerTableSource(tEnv, "PartitionableTable", true)

    val sqlQuery = "SELECT * FROM PartitionableTable WHERE part2 > 1 and id > 2 AND part1 = 'A'"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("3,John,A,2", "4,nosharp,A,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCsvTableSource(): Unit = {
    val csvTable = TestTableSources.getPersonCsvTableSource
    tEnv.registerTableSource("persons", csvTable)

    val sink = new TestingAppendSink()
    tEnv.sqlQuery(
      "SELECT id, `first`, `last`, score FROM persons WHERE id < 4 ")
      .toAppendStream[Row]
      .addSink(sink)

    env.execute()

    val expected = mutable.MutableList(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,45.6",
      "3,Sam,Miller,7.89")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLookupJoinCsvTemporalTable(): Unit = {
    val orders = TestTableSources.getOrdersCsvTableSource
    val rates = TestTableSources.getRatesCsvTableSource
    tEnv.registerTableSource("orders", orders)
    tEnv.registerTableSource("rates", rates)

    val sql =
      """
        |SELECT o.amount, o.currency, r.rate
        |FROM (SELECT *, PROCTIME() as proc FROM orders) AS o
        |JOIN rates FOR SYSTEM_TIME AS OF o.proc AS r
        |ON o.currency = r.currency
      """.stripMargin

    val sink = new TestingAppendSink()
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)

    env.execute()

    val expected = Seq(
      "2,Euro,119",
      "1,US Dollar,102",
      "50,Yen,1",
      "3,Euro,119",
      "5,US Dollar,102"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testInputFormatSource(): Unit = {
    val tableSchema = TableSchema.builder().fields(
      Array("a", "b", "c"),
      Array(DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING())).build()
    val tableSource = new TestInputFormatTableSource(
      tableSchema, tableSchema.toRowType, TestData.smallData3)
    tEnv.registerTableSource("MyInputFormatTable", tableSource)

    val sink = new TestingAppendSink()
    tEnv.sqlQuery("SELECT a, c FROM MyInputFormatTable").toAppendStream[Row].addSink(sink)

    env.execute()

    val expected = Seq(
      "1,Hi",
      "2,Hello",
      "3,Hello world"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testDecimalSource(): Unit = {
    val tableSchema = TableSchema.builder().fields(
      Array("a", "b", "c", "d"),
      Array(
        DataTypes.INT(),
        DataTypes.DECIMAL(5, 2),
        DataTypes.VARCHAR(5),
        DataTypes.CHAR(5))).build()
    val tableSource = new TestDataTypeTableSource(
      tableSchema,
      Seq(
        row(1, new java.math.BigDecimal(5.1), "1", "1"),
        row(2, new java.math.BigDecimal(6.1), "12", "12"),
        row(3, new java.math.BigDecimal(7.1), "123", "123")
      ))
    tEnv.registerTableSource("MyInputFormatTable", tableSource)

    val sink = new TestingAppendSink()
    tEnv.sqlQuery("SELECT a, b, c, d FROM MyInputFormatTable").toAppendStream[Row].addSink(sink)

    env.execute()

    val expected = Seq(
      "1,5.10,1,1",
      "2,6.10,12,12",
      "3,7.10,123,123"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  /**
    * StreamTableSource must use type info in DataStream, so it will loose precision.
    * Just support default precision decimal.
    */
  @Test
  def testLegacyDecimalSourceUsingStreamTableSource(): Unit = {
    val tableSchema = new TableSchema(
      Array("a", "b", "c"),
      Array(
        Types.INT(),
        Types.DECIMAL(),
        Types.STRING()
      ))
    val tableSource = new TestStreamTableSource(
      tableSchema,
      Seq(
        row(1, new java.math.BigDecimal(5.1), "1"),
        row(2, new java.math.BigDecimal(6.1), "12"),
        row(3, new java.math.BigDecimal(7.1), "123")
      ))
    tEnv.registerTableSource("MyInputFormatTable", tableSource)

    val sink = new TestingAppendSink()
    tEnv.sqlQuery("SELECT a, b, c FROM MyInputFormatTable").toAppendStream[Row].addSink(sink)

    env.execute()

    val expected = Seq(
      "1,5.099999999999999645,1",
      "2,6.099999999999999645,12",
      "3,7.099999999999999645,123"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}
