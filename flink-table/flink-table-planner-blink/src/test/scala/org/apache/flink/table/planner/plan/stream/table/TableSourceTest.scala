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

package org.apache.flink.table.planner.plan.stream.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Over, TableSchema, Tumble, Types}
import org.apache.flink.table.planner.utils.{TableTestBase, TestNestedProjectableTableSource, TestProjectableTableSource, TestTableSourceWithTime}
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
      new TestTableSourceWithTime[Row](
        false, tableSchema, returnType, Seq(), rowtime = "rowtime"))

    val t = util.tableEnv.scan("rowTimeT").select($"rowtime", $"id", $"name", $"val")
    util.verifyPlan(t)
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
      new TestTableSourceWithTime[Row](
        false, tableSchema, returnType, Seq(), rowtime = "rowtime"))

    val t = util.tableEnv.scan("rowTimeT").select($"rowtime", $"id", $"name", $"val")
    util.verifyPlan(t)
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
      new TestTableSourceWithTime[Row](
        false, tableSchema, returnType, Seq(), rowtime = "rowtime"))

    val t = util.tableEnv.scan("rowTimeT")
      .where($"val" > 100)
      .window(Tumble over 10.minutes on 'rowtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.end, 'val.avg)
    util.verifyPlan(t)
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
      new TestTableSourceWithTime[Row](
        false, tableSchema, returnType, Seq(), proctime = "proctime"))

    val t = util.tableEnv.scan("procTimeT").select($"proctime", $"id", $"name", $"val")
    util.verifyPlan(t)
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
      new TestTableSourceWithTime[Row](
        false, tableSchema, returnType, Seq(), proctime = "proctime"))

    val t = util.tableEnv.scan("procTimeT")
      .window(Over partitionBy 'id orderBy 'proctime preceding 2.hours as 'w)
      .select('id, 'name, 'val.sum over 'w as 'valSum)
      .filter('valSum > 100)
    util.verifyPlan(t)
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
      new TestProjectableTableSource(
        false, tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('name, 'val, 'id)
    util.verifyPlan(t)
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
      new TestProjectableTableSource(
        false, tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('ptime, 'name, 'val, 'id)
    util.verifyPlan(t)
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
      new TestProjectableTableSource(
        false, tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('name, 'val, 'rtime, 'id)
    util.verifyPlan(t)
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
      new TestProjectableTableSource(
        false, tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('ptime)
    util.verifyPlan(t)
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
      new TestProjectableTableSource(
        false, tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('rtime)
    util.verifyPlan(t)
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
      new TestProjectableTableSource(
        false, tableSchema, returnType, Seq(), "rtime", "ptime", mapping))

    val t = util.tableEnv.scan("T").select('name, 'rtime, 'val)
    util.verifyPlan(t)
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
      new TestNestedProjectableTableSource(
        false, tableSchema, returnType, Seq()))

    val t = util.tableEnv
      .scan("T")
      .select('id,
        'deepNested.get("nested1").get("name") as 'nestedName,
        'nested.get("value") as 'nestedValue,
        'deepNested.get("nested2").get("flag") as 'nestedFlag,
        'deepNested.get("nested2").get("num") as 'nestedNum)
    util.verifyPlan(t)
  }

}
