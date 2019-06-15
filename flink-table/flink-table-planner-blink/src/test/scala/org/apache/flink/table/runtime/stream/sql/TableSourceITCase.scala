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

package org.apache.flink.table.runtime.stream.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.table.util._
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.Test

import java.lang.{Boolean => JBool, Integer => JInt, Long => JLong}

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
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
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
        Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
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
      "1970-01-01 00:00:00.001,Mary,1",
      "1970-01-01 00:00:00.002,Bob,2",
      "1970-01-01 00:00:00.002,Mike,3",
      "1970-01-01 00:00:02.001,Liz,4")
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
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
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
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
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
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
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
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
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
      "Mary,1970-01-01 00:00:00.001,10",
      "Bob,1970-01-01 00:00:00.002,20",
      "Mike,1970-01-01 00:00:00.002,30",
      "Liz,1970-01-01 00:00:02.001,40")
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
}
