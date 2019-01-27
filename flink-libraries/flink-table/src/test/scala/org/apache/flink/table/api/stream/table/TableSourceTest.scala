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

import org.apache.calcite.tools.RuleSets
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataTypes, TypeConverters}
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.plan.optimize.FlinkStreamPrograms
import org.apache.flink.table.runtime.utils.{CommonTestData, StreamTestData}
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.util._
import org.apache.flink.types.Row

import org.junit.Test

class TableSourceTest extends TableTestBase {

  @Test
  def testStreamProjectableSourceScanPlanTableApi(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = streamTestUtil()

    util.tableEnv.registerTableSource(tableName, tableSource)

    val result = util.tableEnv
        .scan(tableName)
        .select('last, 'id.floor(), 'score * 2)

    util.verifyPlan(result)
  }

  @Test
  def testStreamProjectableSourceScanPlanSQL(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = streamTestUtil()

    util.tableEnv.registerTableSource(tableName, tableSource)

    val sqlQuery = s"SELECT `last`, floor(id), score * 2 FROM $tableName"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testStreamProjectableSourceScanNoIdentityCalc(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = streamTestUtil()

    util.tableEnv.registerTableSource(tableName, tableSource)

    val result = util.tableEnv
        .scan(tableName)
        .select('id, 'score, 'first)

    util.verifyPlan(result)
  }

  @Test
  def testStreamFilterableSourceScanPlan(): Unit = {
    val tableSource = new TestFilterableTableSource
    val tableName = "filterableTable"
    val util = streamTestUtil()

    util.tableEnv.registerTableSource(tableName, tableSource)

    val result = util.tableEnv
        .scan(tableName)
        .select('price, 'id, 'amount)
        .where("amount > 2 && price * 2 < 32")

    util.verifyPlan(result)
  }

  @Test
  def testStreamFilterableSourceWithTrim(): Unit = {
    val tableSource = new TestFilterableTableSource
    val tableName = "filterableTable"
    val util = streamTestUtil()

    util.tableEnv.registerTableSource(tableName, tableSource)

    val result = util.tableEnv
      .scan(tableName)
      .where('name.trim() === "Test" ||
        'name.trim(removeLeading = false) === "Test " ||
        'name.trim(removeTrailing = false) === " Test" ||
        'name.trim(character = "   ") === "Test")

    util.verifyPlan(result)
  }

  @Test
  def testTableSourceWithLongRowTimeField(): Unit = {

    val tableSchema = new TableSchema(
      Array("id", "rowtime", "val", "name"),
      Array(DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
          .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rowtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "rowTimeT",
      new TestTableSourceWithTime[Row](tableSchema, returnType, Seq(), rowtime = "rowtime"))

    val t = util.tableEnv.scan("rowTimeT").select("rowtime, id, name, val")

    util.verifyPlan(t)
  }

  @Test
  def testTableSourceWithTimestampRowTimeField(): Unit = {

    val tableSchema = new TableSchema(
      Array("id", "rowtime", "val", "name"),
      Array(DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING)
          .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rowtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "rowTimeT",
      new TestTableSourceWithTime[Row](tableSchema, returnType, Seq(), rowtime = "rowtime"))

    val t = util.tableEnv.scan("rowTimeT").select("rowtime, id, name, val")

    util.verifyPlan(t)
  }

  @Test
  def testRowTimeTableSourceGroupWindow(): Unit = {

    val tableSchema = new TableSchema(
      Array("id", "rowtime", "val", "name"),
      Array(DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING)
          .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rowtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "rowTimeT",
      new TestTableSourceWithTime[Row](tableSchema, returnType, Seq(), rowtime = "rowtime"))

    val t = util.tableEnv.scan("rowTimeT")
        .filter("val > 100")
        .window(Tumble over 10.minutes on 'rowtime as 'w)
        .groupBy('name, 'w)
        .select('name, 'w.end, 'val.avg)

    util.verifyPlan(t)
  }

  @Test
  def testProcTimeTableSourceSimple(): Unit = {

    val tableSchema = new TableSchema(
      Array("id", "pTime", "val", "name"),
      Array(DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "procTimeT",
      new TestTableSourceWithTime[Row](tableSchema, returnType, Seq(), proctime = "pTime"))

    val t = util.tableEnv.scan("procTimeT").select("pTime, id, name, val")

    util.verifyPlan(t)
  }

  @Test
  def testProcTimeTableSourceOverWindow(): Unit = {

    val tableSchema = new TableSchema(
      Array("id", "pTime", "val", "name"),
      Array(DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "procTimeT",
      new TestTableSourceWithTime[Row](tableSchema, returnType, Seq(), proctime = "pTime"))

    val t = util.tableEnv.scan("procTimeT")
      .window(Over partitionBy 'id orderBy 'pTime preceding 2.hours as 'w)
      .select('id, 'name, 'val.sum over 'w as 'valSum)
      .filter('valSum > 100)

    util.verifyPlan(t)
  }

  @Test
  def testProjectWithRowtimeProctime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(
        DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('name, 'val, 'id)

    util.verifyPlan(t)
  }

  @Test
  def testProjectWithoutRowtime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(DataTypes.INT, DataTypes.TIMESTAMP,
        DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('ptime, 'name, 'val, 'id)

    util.verifyPlan(t)
  }

  def testProjectWithoutProctime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(
        DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('name, 'val, 'rtime, 'id)

    util.verifyPlan(t)
  }

  def testProjectOnlyProctime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(
        DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('ptime)

    util.verifyPlan(t)
  }

  def testProjectOnlyRowtime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(
        DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(tableSchema, returnType, Seq(), "rtime", "ptime"))

    val t = util.tableEnv.scan("T").select('rtime)

    util.verifyPlan(t)
  }

  @Test
  def testProjectWithMapping(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(
        DataTypes.INT, DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.TIMESTAMP, DataTypes.STRING))
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
      Array(
        DataTypes.INT, TypeConverters.createInternalTypeFromTypeInfo(deepNested),
        TypeConverters.createInternalTypeFromTypeInfo(nested1), DataTypes.STRING));

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

    util.verifyPlan(t)
  }

  // FIXME do calc before shuffle when refactoring stream cost.
  @Test
  def testWithPkMultiRowUpdateTransposeWithCalc(): Unit = {
    val util = streamTestUtil()

    val rowTypeInfo = new RowTypeInfo(Types.INT, Types.LONG, Types.STRING)

    util.tableEnv.registerTableSource("MyTable", new TestTableSourceWithUniqueKeys(
      StreamTestData.get3TupleData,
      Array("a", "pk", "c"),
      Array(0, 1, 2),
      ImmutableSet.of(ImmutableSet.copyOf(Array[String]("pk")))
    )(rowTypeInfo.asInstanceOf[TypeInformation[(Int, Long, String)]]))

    injectRules(
      util.tableEnv,
      FlinkStreamPrograms.LOGICAL_REWRITE,
      RuleSets.ofList(TestFlinkLogicalLastRowRule.INSTANCE))

    val resultTable = util.tableEnv.scan("MyTable")
      .groupBy('pk)
      .select('pk, 'a.count)

    util.verifyPlanAndTrait(resultTable)
  }

  @Test
  def testWithPkMultiRowUpdate(): Unit = {
    val util = streamTestUtil()

    val rowTypeInfo = new RowTypeInfo(Types.INT, Types.LONG, Types.STRING)

    util.tableEnv.registerTableSource("MyTable", new TestTableSourceWithUniqueKeys(
      StreamTestData.get3TupleData,
      Array("a", "pk", "c"),
      Array(0, 1, 2),
      ImmutableSet.of(ImmutableSet.copyOf(Array[String]("pk")))
    )(rowTypeInfo.asInstanceOf[TypeInformation[(Int, Long, String)]]))

    injectRules(
      util.tableEnv,
      FlinkStreamPrograms.LOGICAL_REWRITE,
      RuleSets.ofList(TestFlinkLogicalLastRowRule.INSTANCE))

    val resultTable = util.tableEnv.scan("MyTable")
      .groupBy('pk)
      .select('pk, 'a.count, 'c.count)

    util.verifyPlanAndTrait(resultTable)
  }

  def csvTable: (CsvTableSource, String) = {
    val csvTable = CommonTestData.getCsvTableSource
    val tableName = "csvTable"
    (csvTable, tableName)
  }
}
