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

package org.apache.flink.table.runtime.batch.table

import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{CommonTestData, TableProgramsCollectionTestBase}
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.utils.TestFilterableTableSource
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class TableSourceITCase(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testCsvTableSourceWithProjection(): Unit = {
    val csvTable = CommonTestData.getCsvTableSource

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    tEnv.registerTableSource("csvTable", csvTable)

    val results = tEnv
      .scan("csvTable")
      .where('score < 20)
      .select('last, 'id.floor(), 'score * 2)
      .collect()

    val expected = Seq(
      "Smith,1,24.6",
      "Miller,3,15.78",
      "Smith,4,0.24",
      "Miller,6,13.56",
      "Williams,8,4.68").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableSourceWithFilterable(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    tableEnv.registerTableSource(tableName, TestFilterableTableSource())
    val results = tableEnv
      .scan(tableName)
      .where("amount > 4 && price < 9")
      .select("id, name")
      .collect()

    val expected = Seq(
      "5,Record_5", "6,Record_6", "7,Record_7", "8,Record_8").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableSourceWithFilterableDate(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)

    val rowTypeInfo = new RowTypeInfo(
      Array[TypeInformation[_]](BasicTypeInfo.INT_TYPE_INFO, SqlTimeTypeInfo.DATE),
      Array("id", "date_val"))

    val rows = Seq(
      makeRow(23, Date.valueOf("2017-04-23")),
      makeRow(24, Date.valueOf("2017-04-24")),
      makeRow(25, Date.valueOf("2017-04-25")),
      makeRow(26, Date.valueOf("2017-04-26"))
    )

    val query =
      """
        |select id from MyTable
        |where date_val >= DATE '2017-04-24' and date_val < DATE '2017-04-26'
      """.stripMargin
    val tableSource = TestFilterableTableSource(rowTypeInfo, rows, Set("date_val"))
    tableEnv.registerTableSource(tableName, tableSource)
    val results = tableEnv
      .sqlQuery(query)
      .collect()

    val expected = Seq(24, 25).mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableSourceWithFilterableTime(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)

    val rowTypeInfo = new RowTypeInfo(
      Array[TypeInformation[_]](BasicTypeInfo.INT_TYPE_INFO, SqlTimeTypeInfo.TIME),
      Array("id", "time_val"))

    val rows = Seq(
      makeRow(1, Time.valueOf("7:23:19")),
      makeRow(2, Time.valueOf("11:45:00")),
      makeRow(3, Time.valueOf("11:45:01")),
      makeRow(4, Time.valueOf("12:14:23")),
      makeRow(5, Time.valueOf("13:33:12"))
    )

    val query =
      """
        |select id from MyTable
        |where time_val >= TIME '11:45:00' and time_val < TIME '12:14:23'
      """.stripMargin
    val tableSource = TestFilterableTableSource(rowTypeInfo, rows, Set("time_val"))
    tableEnv.registerTableSource(tableName, tableSource)
    val results = tableEnv
      .sqlQuery(query)
      .collect()

    val expected = Seq(2, 3).mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }



  @Test
  def testTableSourceWithFilterableTimestamp(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)

    val rowTypeInfo = new RowTypeInfo(
      Array[TypeInformation[_]](BasicTypeInfo.INT_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP),
      Array("id", "ts"))

    val rows = Seq(
      makeRow(1, Timestamp.valueOf("2017-07-11 7:23:19")),
      makeRow(2, Timestamp.valueOf("2017-07-12 11:45:00")),
      makeRow(3, Timestamp.valueOf("2017-07-13 11:45:01")),
      makeRow(4, Timestamp.valueOf("2017-07-14 12:14:23")),
      makeRow(5, Timestamp.valueOf("2017-07-13 13:33:12"))
    )

    val query =
      """
        |select id from MyTable
        |where ts >= TIMESTAMP '2017-07-12 11:45:00' and ts < TIMESTAMP '2017-07-14 12:14:23'
      """.stripMargin
    val tableSource = TestFilterableTableSource(rowTypeInfo, rows, Set("ts"))
    tableEnv.registerTableSource(tableName, tableSource)
    val results = tableEnv
      .sqlQuery(query)
      .collect()

    val expected = Seq(2, 3, 5).mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  private def makeRow(fields: Any*): Row = {
    val row = new Row(fields.length)
    val addField = (value: Any, pos: Int) => row.setField(pos, value)
    fields.zipWithIndex.foreach(addField.tupled)
    row
  }
}
