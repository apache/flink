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

package org.apache.flink.table.plan.stats

import java.sql.{Date => SqlDate, Time => SqlTime, Timestamp => SqlTimestamp}

import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.CollectionBatchExecTable
import org.apache.flink.util.TimeConvertUtils
import org.apache.flink.util.TimeConvertUtils.MILLIS_PER_DAY
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}

import scala.collection.JavaConverters._

/**
  * Tests for [[AnalyzeStatistic]].
  */
class AnalyzeStatisticTest extends BatchTestBase {

  @Test
  def testGenerateTableStats_EmptyColumns(): Unit = {
    val ds = CollectionBatchExecTable.get7TupleDataSet(tEnv)
    tEnv.registerTable("MyTable", ds)

    val tableStats = AnalyzeStatistic.generateTableStats(tEnv, "MyTable", Array.empty[String])

    val expectedTableStats = TableStats(11L, Map[String, ColumnStats]().asJava)
    assertEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_AllColumns(): Unit = {
    val ds = CollectionBatchExecTable.get7TupleDataSet(tEnv, "a, b, c, d, e, f, g")
    tEnv.registerTable("MyTable", ds)

    val tableStats = AnalyzeStatistic.generateTableStats(tEnv, "MyTable", Array("*"))

    val allStrLen = ds.select('d.charLength() as 'd_len).select('d_len.sum)
      .collect().head.getField(0).asInstanceOf[Integer]
    val expectedTableStats = TableStats(11L, Map(
      "a" -> ColumnStats(11L, 0L, 4.0, 4, 11, 1),
      "b" -> ColumnStats(5L, 0L, 8.0, 8, 5, 1),
      "c" -> ColumnStats(6L, 0L, 8.0, 8, 99.99, 49.49),
      "d" -> ColumnStats(9L, 2L, allStrLen / 9.0, 14, "Luke Skywalker", "Comment#1"),
      "e" -> ColumnStats(6L, 0L, 12.0, 12,
        new SqlDate(TimeConvertUtils.dateStringToUnixDate("2017-10-15") * MILLIS_PER_DAY),
        new SqlDate(TimeConvertUtils.dateStringToUnixDate("2017-10-10") * MILLIS_PER_DAY)),
      "f" -> ColumnStats(7L, 0L, 12.0, 12,
        new SqlTime(TimeConvertUtils.timeStringToUnixDate("22:35:24")),
        new SqlTime(TimeConvertUtils.timeStringToUnixDate("22:23:24"))),
      "g" -> ColumnStats(8L, 0L, 12.0, 12,
        new SqlTimestamp(TimeConvertUtils.timestampStringToUnixDate("2017-10-12 09:00:00")),
        new SqlTimestamp(TimeConvertUtils.timestampStringToUnixDate("2017-10-12 02:00:00")))
    ).asJava)
    assertEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_PartialColumns(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    val tableStats = AnalyzeStatistic.generateTableStats(tEnv, "MyTable", Array("a", "b"))

    val expectedTableStats = TableStats(21L, Map(
      "a" -> ColumnStats(21L, 0L, 4.0, 4, 21, 1),
      "b" -> ColumnStats(6L, 0L, 8.0, 8, 6, 1)
    ).asJava)
    assertEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_Decimal(): Unit = {
    tEnv.registerTableSource("MyTable",
      CommonTestData.createCsvTableSource(
        Seq(row(Decimal.castFrom("3.14", 3, 2), Decimal.castFrom("11.200000000000000006", 20, 18)),
          row(Decimal.castFrom("4.15", 3, 2), Decimal.castFrom("12.200000000000000006", 20, 18)),
          row(Decimal.castFrom("5.16", 3, 2), Decimal.castFrom("13.200000000000000006", 20, 18)),
          row(Decimal.castFrom("6.17", 3, 2), Decimal.castFrom("14.200000000000000006", 20, 18)),
          row(Decimal.castFrom("6.17", 3, 2), Decimal.castFrom("15.200000000000000006", 20, 18)),
          row(Decimal.castFrom("7.18", 3, 2), Decimal.castFrom("16.200000000000000006", 20, 18)),
          row(Decimal.castFrom("7.18", 3, 2), Decimal.castFrom("16.200000000000000006", 20, 18))),
        Array("a", "b"),
        Array(DataTypes.createDecimalType(3, 2), DataTypes.createDecimalType(20, 18))
      )
    )

    val tableStats = AnalyzeStatistic.generateTableStats(tEnv, "MyTable", Array("*"))

    val expectedTableStats = TableStats(7L, Map(
      "a" -> ColumnStats(5L, 0L, 12.0, 12,
        Decimal.castFrom("7.18", 3, 2).toBigDecimal,
        Decimal.castFrom("3.14", 3, 2).toBigDecimal),
      "b" -> ColumnStats(6L, 0L, 12.0, 12,
        Decimal.castFrom("16.200000000000000006", 20, 18).toBigDecimal,
        Decimal.castFrom("11.200000000000000006", 20, 18).toBigDecimal)
    ).asJava)
    assertEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_SingleTablePath(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    val tableStats = AnalyzeStatistic.generateTableStats(tEnv, Array("MyTable"), Array("a", "b"))

    val expectedTableStats = TableStats(21L, Map(
      "a" -> ColumnStats(21L, 0L, 4.0, 4, 21, 1),
      "b" -> ColumnStats(6L, 0L, 8.0, 8, 6, 1)
    ).asJava)
    assertEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_NestTablePath(): Unit = {
    tEnv.registerCatalog(
      "test",
      CommonTestData.getTestFlinkInMemoryCatalog(tEnv.isInstanceOf[StreamTableEnvironment]))

    val tableStats = AnalyzeStatistic.generateTableStats(
      tEnv, Array("test", "db1", "tb1"), Array.empty[String])

    val expectedTableStats = TableStats(3L, Map[String, ColumnStats]().asJava)
    assertEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_ColumnNameIsKeyword(): Unit = {
    tEnv.registerTableSource("MyTable", CommonTestData.getCsvTableSource)

    val tableStats = AnalyzeStatistic.generateTableStats(tEnv, "MyTable", Array("*"))
    val expectedTableStats = TableStats(8L, Map(
      "first" -> ColumnStats(8L, 0L, 33.0 / 8.0, 5, "Sam", "Alice"),
      "id" -> ColumnStats(8L, 0L, 4.0, 4, 8, 1),
      "score" -> ColumnStats(8L, 0L, 8.0, 8, 90.1, 0.12),
      "last" -> ColumnStats(4L, 0L, 49.0 / 8.0, 8, "Williams", "Miller")
    ).asJava)
    assertEquals(expectedTableStats, tableStats)
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_NotExistTable(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    AnalyzeStatistic.generateTableStats(tEnv, "MyTable123", Array("a"))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_NotExistColumn(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    AnalyzeStatistic.generateTableStats(tEnv, "MyTable", Array("a", "d"))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_NotExistColumnWithQuoting(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    AnalyzeStatistic.generateTableStats(tEnv, "MyTable", Array("`a`", "b"))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_StarAndColumns(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    AnalyzeStatistic.generateTableStats(tEnv, "MyTable", Array("*", "a"))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_UnsupportedType(): Unit = {
    val ds = CollectionBatchExecTable.getSmallPojoDataSet(tEnv, "a")
    tEnv.registerTable("MyTable", ds)

    AnalyzeStatistic.generateTableStats(tEnv, "MyTable", Array("a"))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_CaseSensitive(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "A, b, c")
    tEnv.registerTable("MyTable", ds)

    AnalyzeStatistic.generateTableStats(tEnv, "MyTable", Array("a", "B"))
  }
}
