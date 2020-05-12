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

package org.apache.flink.table.api.stream.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{Over, TableSchema, Tumble, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{TableTestBase, TestNestedProjectableTableSource, TestProjectableTableSource, TestTableSourceWithTime}
import org.apache.flink.types.Row
import org.junit.Test

class TableSourceTest extends TableTestBase {

  @Test
  def testTableSourceWithLongRowTimeField(): Unit = {

    val tableSchema = new TableSchema(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rowtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "rowTimeT",
      new TestTableSourceWithTime[Row](tableSchema, returnType, Seq(), rowtime = "rowtime"))

    val t = util.tableEnv.scan("rowTimeT").select($"rowtime", $"id", $"name", $"val")

    val expected = "StreamTableSourceScan(table=[[default_catalog, default_database, rowTimeT]], " +
      "fields=[rowtime, id, name, val], " +
      "source=[TestTableSourceWithTime(id, rowtime, val, name)])"
    util.verifyTable(t, expected)
  }

  @Test
  def testTableSourceWithTimestampRowTimeField(): Unit = {

    val tableSchema = new TableSchema(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rowtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "rowTimeT",
      new TestTableSourceWithTime[Row](tableSchema, returnType, Seq(), rowtime = "rowtime"))

    val t = util.tableEnv.scan("rowTimeT").select($"rowtime", $"id", $"name", $"val")

    val expected = "StreamTableSourceScan(table=[[default_catalog, default_database, rowTimeT]], " +
      "fields=[rowtime, id, name, val], " +
      "source=[TestTableSourceWithTime(id, rowtime, val, name)])"
    util.verifyTable(t, expected)
  }

  @Test
  def testRowTimeTableSourceGroupWindow(): Unit = {

    val tableSchema = new TableSchema(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rowtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "rowTimeT",
      new TestTableSourceWithTime[Row](tableSchema, returnType, Seq(), rowtime = "rowtime"))

    val t = util.tableEnv.scan("rowTimeT")
      .filter($"val" > 100)
      .window(Tumble over 10.minutes on 'rowtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.end, 'val.avg)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            "StreamTableSourceScan(table=[[default_catalog, default_database, rowTimeT]], " +
              "fields=[rowtime, val, name], " +
              "source=[TestTableSourceWithTime(id, rowtime, val, name)])",
            term("select", "rowtime", "val", "name"),
            term("where", ">(val, 100)")
          ),
          term("groupBy", "name"),
          term("window", "TumblingGroupWindow('w, 'rowtime, 600000.millis)"),
          term("select", "name", "AVG(val) AS EXPR$1", "end('w) AS EXPR$0")
        ),
        term("select", "name", "EXPR$0", "EXPR$1")
      )
    util.verifyTable(t, expected)
  }

  @Test
  def testProcTimeTableSourceSimple(): Unit = {

    val tableSchema = new TableSchema(
      Array("id", "proctime", "val", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "procTimeT",
      new TestTableSourceWithTime[Row](tableSchema, returnType, Seq(), proctime = "proctime"))

    val t = util.tableEnv.scan("procTimeT").select($"proctime", $"id", $"name", $"val")

    val expected =
      unaryNode(
        "DataStreamCalc",
        "StreamTableSourceScan(table=[[default_catalog, default_database, procTimeT]], " +
          "fields=[id, proctime, val, name], " +
          "source=[TestTableSourceWithTime(id, proctime, val, name)])",
        term("select", "PROCTIME(proctime) AS proctime", "id", "name", "val")
      )
    util.verifyTable(t, expected)
  }

  @Test
  def testProcTimeTableSourceOverWindow(): Unit = {

    val tableSchema = new TableSchema(
      Array("id", "proctime", "val", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "procTimeT",
      new TestTableSourceWithTime[Row](tableSchema, returnType, Seq(), proctime = "proctime"))

    val t = util.tableEnv.scan("procTimeT")
      .window(Over partitionBy 'id orderBy 'proctime preceding 2.hours as 'w)
      .select('id, 'name, 'val.sum over 'w as 'valSum)
      .filter('valSum > 100)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          "StreamTableSourceScan(table=[[default_catalog, default_database, procTimeT]], " +
            "fields=[id, proctime, val, name], " +
            "source=[TestTableSourceWithTime(id, proctime, val, name)])",
          term("partitionBy", "id"),
          term("orderBy", "proctime"),
          term("range", "BETWEEN 7200000 PRECEDING AND CURRENT ROW"),
          term("select", "id", "proctime", "val", "name", "SUM(val) AS w0$o0")
        ),
        term("select", "id", "name", "w0$o0 AS valSum"),
        term("where", ">(w0$o0, 100)")
      )
    util.verifyTable(t, expected)
  }

  @Test
  def testProjectWithRowtimeProctime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('name, 'val, 'id)

    val expected = "StreamTableSourceScan(table=[[default_catalog, default_database, T]], " +
      "fields=[name, val, id], " +
      "source=[TestSource(physical fields: name, val, id)])"
    util.verifyTable(t, expected)
  }

  @Test
  def testProjectWithoutRowtime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('ptime, 'name, 'val, 'id)

    val expected = unaryNode(
      "DataStreamCalc",
      "StreamTableSourceScan(table=[[default_catalog, default_database, T]], " +
        "fields=[ptime, name, val, id], " +
        "source=[TestSource(physical fields: name, val, id)])",
      term("select", "PROCTIME(ptime) AS ptime", "name", "val", "id")
    )
    util.verifyTable(t, expected)
  }

  def testProjectWithoutProctime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('name, 'val, 'rtime, 'id)

    val expected = "StreamTableSourceScan(table=[[default_catalog, default_database, T]], " +
      "fields=[name, val, rtime, id], " +
      "source=[TestSource(physical fields: name, val, rtime, id)])"
    util.verifyTable(t, expected)
  }

  def testProjectOnlyProctime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('ptime)

    val expected = "StreamTableSourceScan(table=[[default_catalog, default_database, T]], " +
      "fields=[ptime], " +
      "source=[TestSource(physical fields: )])"
    util.verifyTable(t, expected)
  }

  def testProjectOnlyRowtime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('rtime)

    val expected = "StreamTableSourceScan(table=[[default_catalog, default_database, T]], " +
      "fields=[rtime], " +
      "source=[TestSource(physical fields: rtime)])"
    util.verifyTable(t, expected)
  }

  @Test
  def testProjectWithMapping(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.LONG, Types.INT, Types.STRING, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("p-rtime", "p-id", "p-name", "p-val"))
    val mapping = Map("rtime" -> "p-rtime", "id" -> "p-id", "val" -> "p-val", "name" -> "p-name")

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, Seq(), "rtime", "ptime", mapping))

    val t = util.tableEnv.scan("T").select('name, 'rtime, 'val)

    val expected = "StreamTableSourceScan(table=[[default_catalog, default_database, T]], " +
      "fields=[name, rtime, val], " +
      "source=[TestSource(physical fields: remapped-p-name, remapped-p-rtime, remapped-p-val)])"
    util.verifyTable(t, expected)
  }

  @Test
  def testNestedProject(): Unit = {

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
      Array(Types.INT, deepNested, nested1, Types.STRING))

    val returnType = new RowTypeInfo(
      Array(Types.INT, deepNested, nested1, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
        Array("id", "deepNested", "nested", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestNestedProjectableTableSource(tableSchema, returnType, Seq()))

    val t = util.tableEnv
      .scan("T")
      .select('id,
        'deepNested.get("nested1").get("name") as 'nestedName,
        'nested.get("value") as 'nestedValue,
        'deepNested.get("nested2").get("flag") as 'nestedFlag,
        'deepNested.get("nested2").get("num") as 'nestedNum)

    val expected = unaryNode(
      "DataStreamCalc",
      "StreamTableSourceScan(table=[[default_catalog, default_database, T]], " +
        "fields=[id, deepNested, nested], " +
        "source=[TestSource(read nested fields: " +
          "id.*, deepNested.nested2.num, deepNested.nested2.flag, " +
          "deepNested.nested1.name, nested.value)])",
      term("select", "id", "deepNested.nested1.name AS nestedName", "nested.value AS nestedValue",
        "deepNested.nested2.flag AS nestedFlag", "deepNested.nested2.num AS nestedNum")
    )
    util.verifyTable(t, expected)
  }

}
