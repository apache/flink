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

package org.apache.flink.table.planner.plan.stats

import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.planner.plan.stats.StatisticGenerator.generateTableStats
import org.apache.flink.table.planner.plan.stats.StatisticGeneratorTest._
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.{BatchTestBase, CollectionBatchExecTable}
import org.apache.flink.table.planner.utils.TestTableSources
import org.apache.flink.table.types.logical.DecimalType

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Ignore, Test}

import scala.collection.JavaConversions._

/**
  * Tests for [[StatisticGenerator]].
  */
class StatisticGeneratorTest extends BatchTestBase {

  @Test
  def testGenerateTableStats_EmptyColumns(): Unit = {
    val ds = CollectionBatchExecTable.get7TupleDataSet(tEnv, "a, b, c, d, e, f, g")
    tEnv.registerTable("MyTable", ds)

    val tableStats = generateTableStats(tEnv, Array("MyTable"), Some(Array.empty[String]))

    val expectedTableStats = new TableStats(11L, Map[String, ColumnStats]())
    assertTableStatsEquals(expectedTableStats, tableStats)
  }

  @Ignore("Not support dataType: TIMESTAMP(9)")
  @Test
  def testGenerateTableStats_AllColumns(): Unit = {
    val ds = CollectionBatchExecTable.get7TupleDataSet(tEnv, "a, b, c, d, e, f, g")
    tEnv.registerTable("MyTable", ds)

    val tableStats = generateTableStats(tEnv, Array("MyTable"), None)

    val expectedTableStats = new TableStats(11L, Map(
      "a" -> new ColumnStats(11L, 0L, 4.0, 4, 11, 1),
      "b" -> new ColumnStats(5L, 0L, 8.0, 8, 5L, 1L),
      "c" -> new ColumnStats(6L, 0L, 8.0, 8, 99.99d, 49.49d),
      "d" -> new ColumnStats(9L, 2L, 76.0 / 9.0, 14, null, null),
      "e" -> new ColumnStats(6L, 0L, 12.0, 12, null, null),
      "f" -> new ColumnStats(7L, 0L, 12.0, 12, null, null),
      "g" -> new ColumnStats(8L, 0L, 12.0, 12, null, null)
    ))
    assertTableStatsEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_PartialColumns(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    val tableStats = generateTableStats(tEnv, Array("MyTable"), Some(Array("a", "b")))

    val expectedTableStats = new TableStats(21L, Map(
      "a" -> new ColumnStats(21L, 0L, 4.0, 4, 21, 1),
      "b" -> new ColumnStats(6L, 0L, 8.0, 8, 6L, 1L)
    ))
    assertTableStatsEquals(expectedTableStats, tableStats)
  }

  @Ignore(" Type LEGACY(Decimal(3,2)) of table field 'a' does not match with type Decimal(3,2) " +
    "of the field 'a' of the TableSource return type.")
  @Test
  def testGenerateTableStats_Decimal(): Unit = {
    tEnv.registerTableSource("MyTable",
      TestTableSources.createCsvTableSource(
        Seq(row(Decimal.castFrom("3.14", 3, 2), Decimal.castFrom("11.200000000000000006", 20, 18)),
          row(Decimal.castFrom("4.15", 3, 2), Decimal.castFrom("12.200000000000000006", 20, 18)),
          row(Decimal.castFrom("5.16", 3, 2), Decimal.castFrom("13.200000000000000006", 20, 18)),
          row(Decimal.castFrom("6.17", 3, 2), Decimal.castFrom("14.200000000000000006", 20, 18)),
          row(Decimal.castFrom("6.17", 3, 2), Decimal.castFrom("15.200000000000000006", 20, 18)),
          row(Decimal.castFrom("7.18", 3, 2), Decimal.castFrom("16.200000000000000006", 20, 18)),
          row(Decimal.castFrom("7.18", 3, 2), Decimal.castFrom("16.200000000000000006", 20, 18))),
        Array("a", "b"),
        Array(new DecimalType(3, 2), new DecimalType(20, 18))
      )
    )

    val tableStats = generateTableStats(tEnv, Array("MyTable"), None)

    val expectedTableStats = new TableStats(7L, Map(
      "a" -> new ColumnStats(5L, 0L, 12.0, 12,
        Decimal.castFrom("7.18", 3, 2).toBigDecimal,
        Decimal.castFrom("3.14", 3, 2).toBigDecimal),
      "b" -> new ColumnStats(6L, 0L, 12.0, 12,
        Decimal.castFrom("16.200000000000000006", 20, 18).toBigDecimal,
        Decimal.castFrom("11.200000000000000006", 20, 18).toBigDecimal)
    ))
    assertTableStatsEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_Pojo(): Unit = {
    val ds = CollectionBatchExecTable.getSmallPojoDataSet(tEnv)
    tEnv.registerTable("MyTable", ds)

    val tableStats = generateTableStats(tEnv, Array("MyTable"), None)

    val expectedTableStats = new TableStats(3L, Map[String, ColumnStats](
      "nestedPojo" -> new ColumnStats(3L, 0L, 8.0, 8, null, null),
      "nestedTupleWithCustom" -> new ColumnStats(3L, 0L, 28.0, 28, null, null),
      "number" -> new ColumnStats(3L, 0L, 4.0, 4, 3, 1),
      "str" -> new ColumnStats(3L, 0L, 16.0 / 3, 6, null, null)
    ))
    assertTableStatsEquals(expectedTableStats, tableStats)
  }

  @Test
  def testGenerateTableStats_ColumnNameIsKeyword(): Unit = {
    tEnv.registerTableSource("test", TestTableSources.getPersonCsvTableSource)

    val tableStats = generateTableStats(tEnv, Array("test"), None)
    val expectedTableStats = new TableStats(8L, Map(
      "first" -> new ColumnStats(8L, 0L, 33.0 / 8.0, 5, null, null),
      "id" -> new ColumnStats(8L, 0L, 4.0, 4, 8, 1),
      "score" -> new ColumnStats(8L, 0L, 8.0, 8, 90.1, 0.12),
      "last" -> new ColumnStats(4L, 0L, 49.0 / 8.0, 8, null, null)
    ))
    assertTableStatsEquals(expectedTableStats, tableStats)
  }

  @Test(expected = classOf[ValidationException])
  def testGenerateTableStats_NotExistTable(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    generateTableStats(tEnv, Array("MyTable123"), Some(Array("a")))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_NotExistColumn(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    generateTableStats(tEnv, Array("MyTable"), Some(Array("a", "d")))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_NotExistColumnWithQuoting(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    generateTableStats(tEnv, Array("MyTable"), Some(Array("`a`", "b")))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_StarAndColumns(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("MyTable", ds)

    generateTableStats(tEnv, Array("MyTable"), Some(Array("*", "a")))
  }

  @Test(expected = classOf[ValidationException])
  def testGenerateTableStats_UnsupportedType(): Unit = {
    val ds = CollectionBatchExecTable.getSmallPojoDataSet(tEnv, "a")
    tEnv.registerTable("MyTable", ds)

    generateTableStats(tEnv, Array("MyTable"), Some(Array("a")))
  }

  @Test(expected = classOf[TableException])
  def testGenerateTableStats_CaseSensitive(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "A, b, c")
    tEnv.registerTable("MyTable", ds)

    generateTableStats(tEnv, Array("MyTable"), Some(Array("a", "B")))
  }

}

object StatisticGeneratorTest{

  def assertTableStatsEquals(expected: TableStats, actual: TableStats): Unit = {
    assertEquals("rowCount is not equal", expected.getRowCount, actual.getRowCount)
    assertEquals("size of columnStats map is not equal",
      expected.getColumnStats.size(), actual.getColumnStats.size())
    expected.getColumnStats.foreach {
      case (fieldName, expectedColumnStats) =>
        assertTrue(actual.getColumnStats.contains(fieldName))
        val actualColumnStats = actual.getColumnStats.get(fieldName)
        assertColumnStatsEquals(fieldName, expectedColumnStats, actualColumnStats)
    }
  }

  def assertColumnStatsEquals(
      fieldName: String, expected: ColumnStats, actual: ColumnStats): Unit = {
    assertEquals(s"ndv of '$fieldName' is not equal", expected.getNdv, actual.getNdv)
    assertEquals(s"nullCount of '$fieldName' is not equal",
      expected.getNullCount, actual.getNullCount)
    assertEquals(s"avgLen of '$fieldName' is not equal", expected.getAvgLen, actual.getAvgLen)
    assertEquals(s"maxLen of '$fieldName' is not equal", expected.getMaxLen, actual.getMaxLen)
    assertEquals(s"max of '$fieldName' is not equal", expected.getMaxValue, actual.getMaxValue)
    assertEquals(s"min of '$fieldName' is not equal", expected.getMinValue, actual.getMinValue)
  }
}
