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

import TestData._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.expressions.{Rank, RowNumber}
import org.apache.flink.table.expressions._
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.util.DateTimeTestUtil.{UTCDate, UTCTime, UTCTimestamp}
import org.apache.flink.types.Row
import org.junit.{Assert, Before, Test}

import scala.collection.Seq

class OverWindowAggITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    registerCollection(
      "Table1", data1, type1, "month, area, product", nullablesOfData1)
    registerCollection(
      "Table2", data2, type2, "d, e, f, g, h", nullablesOfData2)
    registerCollection(
      "NullTable2", nullData2, type2, "d, e, f, g, h", nullablesOfNullData2)
    registerCollection(
      "Table3", data3, type3, "a, b, c, d, e, f", nullablesOfData3)
    registerCollection(
      "Table4", data4, type4, "category, shopId, num", nullablesOfData4)
    registerCollection(
      "NullTable4", nullData4, type4, "category, shopId, num", nullablesOfNullData4)
    registerCollection(
      "Table5", data5, type5, "a, b", nullablesOfData5)
  }

  // refer column in inner select block
  // test rank, row_number functions in table api as well
  @Test
  def testOverWindowWithInnerSelectColumnReference(): Unit = {
    val table = tEnv.scan("Table1")

    checkResult(
      table
        .select('month, 'area, 'product, 1 as 'tmp1)
        .window(
          Over partitionBy 'area orderBy 'month
            preceding UNBOUNDED_ROW following CURRENT_ROW as 'w)
        .select('area, (Rank() over 'w) + 'tmp1, RowNumber() over 'w),
      Seq(
        row("a", 2, 1),
        row("a", 3, 2),
        row("b", 2, 1),
        row("b", 3, 2),
        row("c", 2, 1),
        row("c", 3, 2)
      )
    )
  }

  @Test
  def testSingleRowOverWindow(): Unit = {
    val table = tEnv.scan("Table1")

    val expected = Seq(
      row("a", 1, 5),
      row("a", 2, 6),
      row("b", 3, 7),
      row("b", 4, 8),
      row("c", 5, 9),
      row("c", 6, 10)
    )

    checkResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month preceding 0.rows following 0.rows as 'w)
        .select('area, 'month, Sum('product) over 'w),
      expected
    )

    checkResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month preceding CURRENT_ROW following 0.rows as 'w)
        .select('area, 'month, Sum('product) over 'w),
      expected
    )

    checkResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month preceding 0.rows following CURRENT_ROW as 'w)
        .select('area, 'month, Sum('product) over 'w),
      expected
    )

    checkResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month preceding CURRENT_ROW following CURRENT_ROW as 'w)
        .select('area, 'month, Sum('product) over 'w),
      expected
    )
  }

  @Test
  def testUnboundedRowOverWindow(): Unit = {
    val table = tEnv.scan("Table1")

    checkResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month
            preceding UNBOUNDED_ROW following UNBOUNDED_ROW as 'w
        )
        .select('area, 'month, Sum('product) over 'w),
      Seq(
        row("a", 1, 11),
        row("a", 2, 11),
        row("b", 3, 15),
        row("b", 4, 15),
        row("c", 5, 19),
        row("c", 6, 19)
      )
    )
  }

  @Test
  def testWindowAggregationNormalWindowAgg(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc as 'w1,
          Over partitionBy 'd orderBy 'e as 'w2
        )
        .select(
          'd, 'e,
          RowNumber() over 'w1,
          Rank() over 'w1,
          DenseRank() over 'w1,
          Sum('e) over 'w1,
          Count("*") over 'w1,
          Avg('e) over 'w1,
          Max('e) over 'w2,
          Max('e) over 'w1,
          Min('e) over 'w2,
          Min('e) over 'w1
        ),
      Seq(
        row(1, 1, 1, 1, 1, 1, 1, 1.0, 1, 1, 1, 1),

        row(2, 2, 2, 2, 2, 5, 2, 2.5, 2, 3, 2, 2),
        row(2, 3, 1, 1, 1, 3, 1, 3.0, 3, 3, 2, 3),

        row(3, 4, 3, 3, 3, 15, 3, 5.0, 4, 6, 4, 4),
        row(3, 5, 2, 2, 2, 11, 2, 5.5, 5, 6, 4, 5),
        row(3, 6, 1, 1, 1, 6, 1, 6.0, 6, 6, 4, 6),

        row(4, 7, 4, 4, 4, 34, 4, 8.5, 7, 10, 7, 7),
        row(4, 8, 3, 3, 3, 27, 3, 9.0, 8, 10, 7, 8),
        row(4, 9, 2, 2, 2, 19, 2, 9.5, 9, 10, 7, 9),
        row(4, 10, 1, 1, 1, 10, 1, 10.0, 10, 10, 7, 10),

        row(5, 11, 5, 5, 5, 65, 5, 13.0, 11, 15, 11, 11),
        row(5, 12, 4, 4, 4, 54, 4, 13.5, 12, 15, 11, 12),
        row(5, 13, 3, 3, 3, 42, 3, 14.0, 13, 15, 11, 13),
        row(5, 14, 2, 2, 2, 29, 2, 14.5, 14, 15, 11, 14),
        row(5, 15, 1, 1, 1, 15, 1, 15.0, 15, 15, 11, 15)
      )
    )

    checkResult(
      table
        .window(Over partitionBy 'd orderBy 'g as 'w)
        .select('d, 'g, Rank() over 'w),
      Seq(
        // d  g rank
        row( 1, "Hallo", 1),

        row( 2, "Hallo Welt", 1),
        row( 2, "Hallo Welt wie", 2),

        row( 3, "ABC", 1),
        row( 3, "BCD", 2),
        row( 3, "Hallo Welt wie gehts?", 3),

        row( 4, "CDE", 1),
        row( 4, "DEF", 2),
        row( 4, "EFG", 3),
        row( 4, "FGH", 4),

        row( 5, "GHI", 1),
        row( 5, "HIJ", 2),
        row( 5, "IJK", 3),
        row( 5, "JKL", 4),
        row( 5, "KLM", 5)
      )
    )
  }

  @Test
  def testMultiKeysOrderBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table.
        window(
          Over orderBy('h, 'd.desc, 'e) preceding UNBOUNDED_ROW following CURRENT_ROW as 'w
        )
        .select(
          'h, 'd, 'e, Sum('d) over 'w
        ),
      Seq(
        row(1, 5, 11, 5),
        row(1, 4, 8, 9),
        row(1, 4, 9, 13),
        row(1, 2, 3, 15),
        row(1, 1, 1, 16),
        row(2, 5, 14, 21),
        row(2, 5, 15, 26),
        row(2, 4, 7, 30),
        row(2, 4, 10, 34),
        row(2, 3, 4, 37),
        row(2, 3, 5, 40),
        row(2, 2, 2, 42),
        row(3, 5, 12, 47),
        row(3, 5, 13, 52),
        row(3, 3, 6, 55)
      )
    )
  }

  @Test
  def testOrderByStringTypeField(): Unit = {
    val table2 = tEnv.scan("Table2")

    checkResult(
      table2
        .window(
          Over partitionBy 'd orderBy 'g.desc preceding UNBOUNDED_ROW following CURRENT_ROW as 'w
        )
        .select('d, 'g, Count('e) over 'w),
      Seq(
        row(1, "Hallo", 1),
        row(2, "Hallo Welt wie", 1),
        row(2, "Hallo Welt", 2),
        row(3, "ABC", 3),
        row(3, "BCD", 2),
        row(3, "Hallo Welt wie gehts?", 1),
        row(4, "CDE", 4),
        row(4, "DEF", 3),
        row(4, "EFG", 2),
        row(4, "FGH", 1),
        row(5, "GHI", 5),
        row(5, "HIJ", 4),
        row(5, "IJK", 3),
        row(5, "JKL", 2),
        row(5, "KLM", 1)
      )
    )

    checkResult(
      table2
        .window(
          Over partitionBy 'd orderBy 'g preceding UNBOUNDED_ROW following CURRENT_ROW as 'w
        )
        .select('d, 'g, Count('e) over 'w),
      Seq(
        row(1, "Hallo", 1),
        row(2, "Hallo Welt wie", 2),
        row(2, "Hallo Welt", 1),
        row(3, "ABC", 1),
        row(3, "BCD", 2),
        row(3, "Hallo Welt wie gehts?", 3),
        row(4, "CDE", 1),
        row(4, "DEF", 2),
        row(4, "EFG", 3),
        row(4, "FGH", 4),
        row(5, "GHI", 1),
        row(5, "HIJ", 2),
        row(5, "IJK", 3),
        row(5, "JKL", 4),
        row(5, "KLM", 5)
      )
    )

    val table3 = tEnv.scan("Table3")
    checkResult(
      table3
        .window(
          Over partitionBy 'a orderBy('c, 'd) preceding UNBOUNDED_ROW following CURRENT_ROW as 'w
        )
        .select('a, 'c, Count('e) over 'w),
      Seq(
        row(1, "a", 1),
        row(2, "abc", 1),
        row(2, "abcd", 2),
        row(3, "ABC", 1),
        row(3, "BCD", 2),
        row(3, "abc?", 3),
        row(4, "CDE", 1),
        row(4, "DEF", 2),
        row(4, "EFG", 3),
        row(4, "FGH", 4),
        row(5, "GHI", 1),
        row(5, "HIJ", 2),
        row(5, "IJK", 3),
        row(5, "JKL", 4),
        row(5, "KLM", 5)
      )
    )
  }

  @Test
  def testWindowAggregationRank(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over orderBy 'd as 'w
        )
        .select('d, Rank() over 'w),
      Seq(
        row(1, 1),
        row(2, 2),
        row(2, 2),
        row(3, 4),
        row(3, 4),
        row(3, 4),
        row(4, 7),
        row(4, 7),
        row(4, 7),
        row(4, 7),
        row(5, 11),
        row(5, 11),
        row(5, 11),
        row(5, 11),
        row(5, 11)
      )
    )
  }

  @Test
  def testWindowAggregationRankDesc(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over orderBy 'e.desc as 'w
        )
        .select('d, 'e, Rank() over 'w, DenseRank() over 'w),
      Seq(
        row(5, 15, 1, 1),
        row(5, 14, 2, 2),
        row(5, 13, 3, 3),
        row(5, 12, 4, 4),
        row(5, 11, 5, 5),
        row(4, 10, 6, 6),
        row(4, 9, 7, 7),
        row(4, 8, 8, 8),
        row(4, 7, 9, 9),
        row(3, 6, 10, 10),
        row(3, 5, 11, 11),
        row(3, 4, 12, 12),
        row(2, 3, 13, 13),
        row(2, 2, 14, 14),
        row(1, 1, 15, 15)
      )
    )
  }

  @Test
  def testWindowAggregationDenseRank(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over orderBy 'd as 'w
        )
        .select('d, DenseRank() over 'w),
      Seq(
        row(1, 1),
        row(2, 2),
        row(2, 2),
        row(3, 3),
        row(3, 3),
        row(3, 3),
        row(4, 4),
        row(4, 4),
        row(4, 4),
        row(4, 4),
        row(5, 5),
        row(5, 5),
        row(5, 5),
        row(5, 5),
        row(5, 5)
      )
    )

    checkResult(
      table
        .window(
          Over orderBy 'e as 'w
        )
        .select('d, 'e, Rank() over 'w, DenseRank() over 'w),
      Seq(
        row(1, 1, 1, 1),
        row(2, 2, 2, 2),
        row(2, 3, 3, 3),
        row(3, 4, 4, 4),
        row(3, 5, 5, 5),
        row(3, 6, 6, 6),
        row(4, 7, 7, 7),
        row(4, 8, 8, 8),
        row(4, 9, 9, 9),
        row(4, 10, 10, 10),
        row(5, 11, 11, 11),
        row(5, 12, 12, 12),
        row(5, 13, 13, 13),
        row(5, 14, 14, 14),
        row(5, 15, 15, 15)
      )
    )
  }

  @Test
  def testWindowAggregationRowNumber(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over orderBy 'd as 'w
        )
        .select('d, RowNumber() over 'w),
      Seq(
        row(1, 1),
        row(2, 2),
        row(2, 3),
        row(3, 4),
        row(3, 5),
        row(3, 6),
        row(4, 7),
        row(4, 8),
        row(4, 9),
        row(4, 10),
        row(5, 11),
        row(5, 12),
        row(5, 13),
        row(5, 14),
        row(5, 15)
      )
    )
  }

  @Test
  def testWindowAggregationSumWithOrderBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc as 'w
        )
        .select('d, 'e, Sum('e) over 'w),
      Seq(
        row(1, 1, 1),
        row(2, 3, 3),
        row(2, 2, 5),
        row(3, 6, 6),
        row(3, 5, 11),
        row(3, 4, 15),
        row(4, 10, 10),
        row(4, 9, 19),
        row(4, 8, 27),
        row(4, 7, 34),
        row(5, 15, 15),
        row(5, 14, 29),
        row(5, 13, 42),
        row(5, 12, 54),
        row(5, 11, 65)
      )
    )
  }

  @Test
  def testWindowAggregationSumWithoutOrderBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd as 'w
        )
        .select('d, Sum('e) over 'w),
      Seq(
        row(1, 1),
        row(2, 5),
        row(2, 5),
        row(3, 15),
        row(3, 15),
        row(3, 15),
        row(4, 34),
        row(4, 34),
        row(4, 34),
        row(4, 34),
        row(5, 65),
        row(5, 65),
        row(5, 65),
        row(5, 65),
        row(5, 65)
      )
    )
  }

  @Test
  def testWindowAggregationSumWithoutOrderByAndPartitionBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over as 'w
        )
        .select('d, Sum('e) over 'w),
      Seq(
        row(1, 120),
        row(2, 120),
        row(2, 120),
        row(3, 120),
        row(3, 120),
        row(3, 120),
        row(4, 120),
        row(4, 120),
        row(4, 120),
        row(4, 120),
        row(5, 120),
        row(5, 120),
        row(5, 120),
        row(5, 120),
        row(5, 120)
      )
    )
  }

  @Test
  def testWindowAggregationSumWithOrderByAndWithoutPartitionBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over orderBy 'e as 'w
        )
        .select('d, 'e, Sum('e) over 'w),
      Seq(
        row(1, 1, 1),
        row(2, 2, 3),
        row(2, 3, 6),
        row(3, 4, 10),
        row(3, 5, 15),
        row(3, 6, 21),
        row(4, 7, 28),
        row(4, 8, 36),
        row(4, 9, 45),
        row(4, 10, 55),
        row(5, 11, 66),
        row(5, 12, 78),
        row(5, 13, 91),
        row(5, 14, 105),
        row(5, 15, 120)
      )
    )
  }

  @Test
  def testWindowAggregationCountWithOrderBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc as 'w
        )
        .select('d, 'e, Count("*") over 'w),
      Seq(
        row(1, 1, 1),
        row(2, 3, 1),
        row(2, 2, 2),
        row(3, 6, 1),
        row(3, 5, 2),
        row(3, 4, 3),
        row(4, 10, 1),
        row(4, 9, 2),
        row(4, 8, 3),
        row(4, 7, 4),
        row(5, 15, 1),
        row(5, 14, 2),
        row(5, 13, 3),
        row(5, 12, 4),
        row(5, 11, 5)
      )
    )
  }

  @Test
  def testWindowAggregationCountWithoutOrderBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd as 'w
        )
        .select('d, Count("*") over 'w),
      Seq(
        row(1, 1),
        row(2, 2),
        row(2, 2),
        row(3, 3),
        row(3, 3),
        row(3, 3),
        row(4, 4),
        row(4, 4),
        row(4, 4),
        row(4, 4),
        row(5, 5),
        row(5, 5),
        row(5, 5),
        row(5, 5),
        row(5, 5)
      )
    )
  }

  @Test
  def testWindowAggregationAvgWithOrderBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc as 'w
        )
        .select('d, 'e, Avg('e) over 'w),
      Seq(
        row(1, 1, 1.0),
        row(2, 3, 3.0),
        row(2, 2, 2.5),
        row(3, 6, 6.0),
        row(3, 5, 5.5),
        row(3, 4, 5.0),
        row(4, 10, 10.0),
        row(4, 9, 9.5),
        row(4, 8, 9.0),
        row(4, 7, 8.5),
        row(5, 15, 15.0),
        row(5, 14, 14.5),
        row(5, 13, 14.0),
        row(5, 12, 13.5),
        row(5, 11, 13.0)
      )
    )
  }

  @Test
  def testWindowAggregationAvgWithoutOrderBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd as 'w
        )
        .select('d, Avg('e) over 'w),
      Seq(
        row(1, 1.0),
        row(2, 2.5),
        row(2, 2.5),
        row(3, 5.0),
        row(3, 5.0),
        row(3, 5.0),
        row(4, 8.5),
        row(4, 8.5),
        row(4, 8.5),
        row(4, 8.5),
        row(5, 13.0),
        row(5, 13.0),
        row(5, 13.0),
        row(5, 13.0),
        row(5, 13.0)
      )
    )
  }

  @Test
  def testWindowAggregationSumWithOrderByWithRowsBetween(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding UNBOUNDED_ROW following CURRENT_ROW as 'w
        )
        .select('d, 'e, Sum('e) over 'w),
      Seq(
        row(1, 1, 1),
        row(2, 3, 3),
        row(2, 2, 5),
        row(3, 6, 6),
        row(3, 5, 11),
        row(3, 4, 15),
        row(4, 10, 10),
        row(4, 9, 19),
        row(4, 8, 27),
        row(4, 7, 34),
        row(5, 15, 15),
        row(5, 14, 29),
        row(5, 13, 42),
        row(5, 12, 54),
        row(5, 11, 65)
      )
    )
  }

  @Test
  def testWindowAggregationSumWithOrderByShrinkWindow(): Unit = {
    val table = tEnv.scan("Table2")

    val expected = Seq(
      row(1, 1, 1),

      row(2, 3, 5),
      row(2, 2, 2),

      row(3, 6, 15),
      row(3, 5, 9),
      row(3, 4, 4),

      row(4, 10, 34),
      row(4, 9, 24),
      row(4, 8, 15),
      row(4, 7, 7),

      row(5, 15, 65),
      row(5, 14, 50),
      row(5, 13, 36),
      row(5, 12, 23),
      row(5, 11, 11)
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding CURRENT_ROW following UNBOUNDED_ROW as 'w
        )
        .select('d, 'e, Sum('e) over 'w),
      expected
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc
            preceding CURRENT_ROW following 2147483648L.rows as 'w
        )
        .select('d, 'e, Sum('e) over 'w),
      expected
    )
  }

  @Test
  def testWindowAggregationSumWithOrderByWithRangeBetween(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding 10.rows following 1.rows as 'w1,
          Over partitionBy 'd orderBy 'e.desc preceding 2.rows following 3.rows as 'w2,
          Over partitionBy 'd orderBy 'e.desc
            preceding UNBOUNDED_RANGE following CURRENT_RANGE as 'w3,
          Over partitionBy 'd orderBy 'e.desc
            preceding CURRENT_RANGE following UNBOUNDED_RANGE as 'w4,
          Over partitionBy 'd orderBy 'e.desc preceding 1.range following 2.range as 'w5,
          Over partitionBy 'd orderBy 'e preceding 3.range following 3.range as 'w6,
          Over partitionBy 'd orderBy 'h preceding 1.range following 0.range as 'w7
        )
        .select('d, 'e,
          Sum('e) over 'w1,
          Sum('e) over 'w2,
          Sum('e) over 'w3,
          Sum('e) over 'w4,
          Sum('e) over 'w5,
          Sum('e) over 'w6,
          Sum('h) over 'w7,
          'f, 'h
        ),
      Seq(
        row(1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1),
        row(2, 3, 5, 5, 3, 5, 5, 5, 1, 2, 1),
        row(2, 2, 5, 5, 5, 2, 5, 5, 3, 1, 2),
        row(3, 6, 11, 15, 6, 15, 15, 15, 7, 5, 3),
        row(3, 5, 15, 15, 11, 9, 15, 15, 4, 4, 2),
        row(3, 4, 15, 15, 15, 4, 9, 15, 4, 3, 2),
        row(4, 10, 19, 34, 10, 34, 27, 34, 6, 9, 2),
        row(4, 9, 27, 34, 19, 24, 34, 34, 2, 8, 1),
        row(4, 8, 34, 34, 27, 15, 24, 34, 2, 7, 1),
        row(4, 7, 34, 24, 34, 7, 15, 34, 6, 6, 2),
        row(5, 15, 29, 54, 15, 65, 42, 54, 5, 14, 2),
        row(5, 14, 42, 65, 29, 50, 54, 65, 5, 13, 2),
        row(5, 13, 54, 65, 42, 36, 50, 65, 10, 12, 3),
        row(5, 12, 65, 50, 54, 23, 36, 65, 10, 11, 3),
        row(5, 11, 65, 36, 65, 11, 23, 50, 1, 10, 1)
      )
    )
  }

  @Test
  def testWindowAggregationWithOrderByWithRowBetween(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding 5.rows following 2.rows as 'w1,
          Over partitionBy 'd orderBy 'e.desc preceding 6.rows following 2.rows as 'w2,
          Over partitionBy 'd orderBy 'e preceding UNBOUNDED_ROW following CURRENT_ROW as 'w3,
          Over partitionBy 'd orderBy 'e.desc preceding CURRENT_ROW following UNBOUNDED_ROW as 'w4
        )
        .select(
          'd, 'e, 'f,
          Sum('e) over 'w1,
          Count("*") over 'w2,
          Max('f) over 'w3,
          Min('h) over 'w4,
          'h
        ),
      Seq(
        //d   e   f sum cnt max min h
        row(1, 1, 0, 1, 1, 0, 1, 1),

        row(2, 2, 1, 5, 2, 1, 2, 2),
        row(2, 3, 2, 5, 2, 2, 1, 1),

        row(3, 4, 3, 15, 3, 3, 2, 2),
        row(3, 5, 4, 15, 3, 4, 2, 2),
        row(3, 6, 5, 15, 3, 5, 2, 3),

        row(4, 7, 6, 24, 4, 6, 2, 2),
        row(4, 8, 7, 34, 4, 7, 1, 1),
        row(4, 9, 8, 34, 4, 8, 1, 1),
        row(4, 10, 9, 34, 3, 9, 1, 2),

        row(5, 11, 10, 36, 5, 10, 1, 1),
        row(5, 12, 11, 50, 5, 11, 1, 3),
        row(5, 13, 12, 65, 5, 12, 1, 3),
        row(5, 14, 13, 65, 4, 13, 1, 2),
        row(5, 15, 14, 65, 3, 14, 1, 2)
      )
    )
  }

  @Test
  def testWindowAggregationMaxWithOrderBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc as 'w
        )
        .select('d, 'e, Max('e) over 'w),
      Seq(
        row(1, 1, 1),
        row(2, 3, 3),
        row(2, 2, 3),
        row(3, 6, 6),
        row(3, 5, 6),
        row(3, 4, 6),
        row(4, 10, 10),
        row(4, 9, 10),
        row(4, 8, 10),
        row(4, 7, 10),
        row(5, 15, 15),
        row(5, 14, 15),
        row(5, 13, 15),
        row(5, 12, 15),
        row(5, 11, 15)
      )
    )
  }

  @Test
  def testWindowAggregationMaxWithoutOrderBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd as 'w
        )
        .select('d, Max('e) over 'w),
      Seq(
        row(1, 1),
        row(2, 3),
        row(2, 3),
        row(3, 6),
        row(3, 6),
        row(3, 6),
        row(4, 10),
        row(4, 10),
        row(4, 10),
        row(4, 10),
        row(5, 15),
        row(5, 15),
        row(5, 15),
        row(5, 15),
        row(5, 15)
      )
    )
  }

  @Test
  def testWindowAggregationMinWithOrderBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc as 'w
        )
        .select('d, 'e, Min('e) over 'w),
      Seq(
        row(1, 1, 1),
        row(2, 3, 3),
        row(2, 2, 2),
        row(3, 6, 6),
        row(3, 5, 5),
        row(3, 4, 4),
        row(4, 10, 10),
        row(4, 9, 9),
        row(4, 8, 8),
        row(4, 7, 7),
        row(5, 15, 15),
        row(5, 14, 14),
        row(5, 13, 13),
        row(5, 12, 12),
        row(5, 11, 11)
      )
    )
  }

  @Test
  def testWindowAggregationMinWithoutOrderBy(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd as 'w
        )
        .select('d, Min('e) over 'w),
      Seq(
        row(1, 1),
        row(2, 2),
        row(2, 2),
        row(3, 4),
        row(3, 4),
        row(3, 4),
        row(4, 7),
        row(4, 7),
        row(4, 7),
        row(4, 7),
        row(5, 11),
        row(5, 11),
        row(5, 11),
        row(5, 11),
        row(5, 11)
      )
    )
  }

  @Test
  def testWindowAggregationRankWithMultiOrderKey(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over orderBy('d, 'h.desc) as 'w
        )
        .select('d, 'h, Rank() over 'w),
      Seq(
        row(1, 1, 1),
        row(2, 2, 2),
        row(2, 1, 3),
        row(3, 3, 4),
        row(3, 2, 5),
        row(3, 2, 5),
        row(4, 2, 7),
        row(4, 2, 7),
        row(4, 1, 9),
        row(4, 1, 9),
        row(5, 3, 11),
        row(5, 3, 11),
        row(5, 2, 13),
        row(5, 2, 13),
        row(5, 1, 15)
      )
    )
  }

  @Test
  def testWindowAggregationDenseRankWithMultiOrderKey(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over orderBy('d, 'h.desc) as 'w
        )
        .select('d, 'h, DenseRank() over 'w),
      Seq(
        row(1, 1, 1),
        row(2, 2, 2),
        row(2, 1, 3),
        row(3, 3, 4),
        row(3, 2, 5),
        row(3, 2, 5),
        row(4, 2, 6),
        row(4, 2, 6),
        row(4, 1, 7),
        row(4, 1, 7),
        row(5, 3, 8),
        row(5, 3, 8),
        row(5, 2, 9),
        row(5, 2, 9),
        row(5, 1, 10)
      )
    )
  }

  @Test
  def testWindowAggregationSumRankWithMultiWindow(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over orderBy 'd as 'w1,
          Over orderBy('d, 'e.desc) as 'w2,
          Over partitionBy 'd orderBy('h.desc, 'e) preceding UNBOUNDED_ROW following 0.rows as 'w3
        )
        .select('d, 'e, 'h,
          Rank() over 'w1,
          DenseRank() over 'w1,
          RowNumber() over 'w2,
          Sum('e) over 'w3
        ),
      Seq(
        //d  e h  rank dense_rank rowN  sum
        row(1, 1, 1, 1, 1, 1, 1),

        row(2, 2, 2, 2, 2, 3, 2),
        row(2, 3, 1, 2, 2, 2, 5),

        row(3, 6, 3, 4, 3, 4, 6),
        row(3, 4, 2, 4, 3, 6, 10),
        row(3, 5, 2, 4, 3, 5, 15),

        row(4, 7, 2, 7, 4, 10, 7),
        row(4, 10, 2, 7, 4, 7, 17),
        row(4, 8, 1, 7, 4, 9, 25),
        row(4, 9, 1, 7, 4, 8, 34),

        row(5, 12, 3, 11, 5, 14, 12),
        row(5, 13, 3, 11, 5, 13, 25),
        row(5, 14, 2, 11, 5, 12, 39),
        row(5, 15, 2, 11, 5, 11, 54),
        row(5, 11, 1, 11, 5, 15, 65)
      )
    )
  }

  @Test
  def testWindowAggregationSumRankWithMultiWindowAndWithNullData(): Unit = {
    val table = tEnv.scan("NullTable2")

    checkResult(
      table
        .window(
          Over orderBy 'd.nullsFirst as 'w1,
          Over orderBy('d.nullsFirst, 'e.desc, 'h.desc) as 'w2,
          Over partitionBy 'd orderBy 'e as 'w3,
          Over partitionBy 'd orderBy('h.desc, 'e)
            preceding UNBOUNDED_ROW following CURRENT_ROW as 'w4
        )
        .select('d, 'e, 'h,
          Rank() over 'w1,
          DenseRank() over 'w1,
          RowNumber() over 'w2,
          Sum('d) over 'w3,
          Min('d) over 'w3,
          Max('e) over 'w4
        ),
      Seq(
        row(null, 3, 3, 1, 1, 2, null, null, 3),
        row(null, 3, 3, 1, 1, 1, null, null, 3),

        row(1, 1, 1, 3, 2, 3, 1, 1, 1),

        row(2, 2, 2, 4, 3, 5, 2, 2, 2),
        row(2, 3, 1, 4, 3, 4, 4, 2, 3),

        row(3, 6, 3, 6, 4, 6, 9, 3, 6),
        row(3, 4, 2, 6, 4, 8, 3, 3, 6),
        row(3, 5, 2, 6, 4, 7, 6, 3, 6),

        row(4, 7, 2, 9, 5, 12, 4, 4, 7),
        row(4, 10, 2, 9, 5, 9, 16, 4, 10),
        row(4, 8, 1, 9, 5, 11, 8, 4, 10),
        row(4, 9, 1, 9, 5, 10, 12, 4, 10),

        row(5, 12, 3, 13, 6, 16, 10, 5, 12),
        row(5, 13, 3, 13, 6, 15, 15, 5, 13),
        row(5, 14, 2, 13, 6, 14, 20, 5, 14),
        row(5, 15, 2, 13, 6, 13, 25, 5, 15),
        row(5, 11, 1, 13, 6, 17, 5, 5, 15)
      )
    )
  }

  @Test
  def testWindowAggregationSumRankWithMultiWindowDate(): Unit = {
    val table = tEnv.scan("Table3")

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'd as 'w1,
          Over partitionBy 'a orderBy 'e.desc as 'w2,
          Over partitionBy 'a orderBy 'f as 'w3,
          Over partitionBy 'a orderBy 'c preceding UNBOUNDED_ROW following 0.rows as 'w4
        )
        .select('a, 'b, 'c, 'd, 'e, 'f,
          Rank() over 'w1,
          DenseRank() over 'w2,
          RowNumber() over 'w3,
          Max('b) over 'w4
        ),
      Seq(
        //  a  b      c       d            e             f
        row(1, 1.1, "a", UTCDate("2017-04-08"), UTCTime("12:00:59"),
          UTCTimestamp("2015-05-20 10:00:00"), 1, 1, 1, 1.1),

        row(2, 2.5, "abc", UTCDate("2017-04-09"), UTCTime("12:00:59"),
          UTCTimestamp("2019-09-19 08:03:09"), 2, 1, 2, 2.5),
        row(2, -2.4, "abcd", UTCDate("2017-04-08"), UTCTime("00:00:00"),
          UTCTimestamp("2016-09-01 23:07:06"), 1, 2, 1, 2.5),

        row(3, -9.77, "ABC", UTCDate("2016-08-08"), UTCTime("04:15:00"),
          UTCTimestamp("1999-12-12 10:00:02"), 1, 2, 2, -9.77),
        row(3, 0.08, "BCD", UTCDate("2017-04-10"), UTCTime("02:30:00"),
          UTCTimestamp("1999-12-12 10:03:00"), 2, 3, 3, 0.08),
        row(3, 0.0, "abc?", UTCDate("2017-10-11"), UTCTime("23:59:59"),
          UTCTimestamp("1999-12-12 10:00:00"), 3, 1, 1, 0.08),

        row(4, 3.14, "CDE", UTCDate("2017-11-11"), UTCTime("02:30:00"),
          UTCTimestamp("2017-11-20 09:00:00"), 4, 4, 4, 3.14),
        row(4, 3.15, "DEF", UTCDate("2017-02-06"), UTCTime("06:00:00"),
          UTCTimestamp("2015-11-19 10:00:00"), 1, 3, 1, 3.15),
        row(4, 3.14, "EFG", UTCDate("2017-05-20"), UTCTime("09:46:18"),
          UTCTimestamp("2015-11-19 10:00:01"), 3, 2, 2, 3.15),
        row(4, 3.16, "FGH", UTCDate("2017-05-19"), UTCTime("11:11:11"),
          UTCTimestamp("2015-11-20 08:59:59"), 2, 1, 3, 3.16),

        row(5, -5.9, "GHI", UTCDate("2017-07-20"), UTCTime("22:22:22"),
          UTCTimestamp("1989-06-04 10:00:00.78"), 3, 1, 2, -5.9),
        row(5, 2.71, "HIJ", UTCDate("2017-09-08"), UTCTime("20:09:09"),
          UTCTimestamp("1997-07-01 09:00:00.99"), 4, 2, 3, 2.71),
        row(5, 3.9, "IJK", UTCDate("2017-02-02"), UTCTime("03:03:03"),
          UTCTimestamp("2000-01-01 00:00:00.09"), 1, 5, 4, 3.9),
        row(5, 0.7, "JKL", UTCDate("2017-10-01"), UTCTime("19:00:00"),
          UTCTimestamp("2010-06-01 10:00:00.999"), 5, 3, 5, 3.9),
        row(5, -2.8, "KLM", UTCDate("2017-07-01"), UTCTime("12:00:59"),
          UTCTimestamp("1937-07-07 08:08:08.888"), 2, 4, 1, 3.9)
      )
    )
  }

  @Test
  def testTopN(): Unit = {
    val table = tEnv.scan("Table4")

    checkResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num.desc as 'w
        )
        .select(
          'category, 'shopId, 'num,
          RowNumber() over 'w as 'rank_num
        )
        .filter('rank_num <= 2),
      Seq(
        row("book", 2, 19, 1),
        row("book", 1, 12, 2),
        row("fruit", 3, 44, 1),
        row("fruit", 4, 33, 2)
      )
    )
  }

  @Test
  def testAvg(): Unit = {
    val table = tEnv.scan("Table4")

    checkResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding UNBOUNDED_ROW following 1.rows as 'w
        )
        .select(
          'category, 'num, Avg('num) over 'w
        ),
      Seq(
        row("book", 11, 11.5),
        row("book", 12, 14.0),
        row("book", 19, 14.0),
        row("fruit", 22, 27.5),
        row("fruit", 33, 33.0),
        row("fruit", 44, 33.0)
      )
    )
  }

  @Test
  def testRangeFrame(): Unit = {
    val table = tEnv.scan("Table4")

    // sliding frame case: 1 - 1
    checkResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding 1.range following 1.range as 'w
        )
        .select(
          'category, 'num, Sum('num) over 'w
        ),
      Seq(
        row("book", 11, 23),
        row("book", 12, 23),
        row("book", 19, 19),
        row("fruit", 22, 22),
        row("fruit", 33, 33),
        row("fruit", 44, 44)
      )
    )

    // sliding frame case: unbounded - 1
    checkResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding UNBOUNDED_RANGE following 1.range as 'w
        )
        .select(
          'category, 'num, Sum('num) over 'w
        ),
      Seq(
        row("book", 11, 23),
        row("book", 12, 23),
        row("book", 19, 42),
        row("fruit", 22, 22),
        row("fruit", 33, 55),
        row("fruit", 44, 99)
      )
    )

    // sliding frame case: 1 - unbounded
    checkResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding 1.range following UNBOUNDED_RANGE as 'w
        )
        .select(
          'category, 'num, Sum('num) over 'w
        ),
      Seq(
        row("book", 11, 42),
        row("book", 12, 42),
        row("book", 19, 19),
        row("fruit", 22, 99),
        row("fruit", 33, 77),
        row("fruit", 44, 44)
      )
    )
  }

  @Test
  def testRowsFrame(): Unit = {
    val table = tEnv.scan("Table4")

    // sliding frame case: unbounded - 1
    checkResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding UNBOUNDED_ROW following 1.rows as 'w
        )
        .select(
          'category, 'num, Sum('num) over 'w
        ),
      Seq(
        row("book", 11, 23),
        row("book", 12, 42),
        row("book", 19, 42),
        row("fruit", 22, 55),
        row("fruit", 33, 99),
        row("fruit", 44, 99)
      )
    )

    // sliding frame case: 1 - unbounded
    checkResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding 1.rows following UNBOUNDED_ROW as 'w
        )
        .select(
          'category, 'num, Sum('num) over 'w
        ),
      Seq(
        row("book", 11, 42),
        row("book", 12, 42),
        row("book", 19, 31),
        row("fruit", 22, 99),
        row("fruit", 33, 99),
        row("fruit", 44, 77)
      )
    )
  }

  @Test
  def testRangeFrameWithNullValue(): Unit = {
    val table = tEnv.scan("NullTable4")

    checkResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding 1.range following 1.range as 'w
        )
        .select(
          'category, 'num, Sum('num) over 'w
        ),
      Seq(
        row("book", 11, 23),
        row("book", 12, 23),
        row("book", null, null),
        row("fruit", 44, 44),
        row("fruit", null, null),
        row("fruit", null, null)
      )
    )
  }

  @Test
  def testNegativeRows(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding 3.rows following -1.rows as 'w
        )
        .select(
          'd, 'e, Sum('e) over 'w
        ),
      Seq(
        row(1, 1, null),
        row(2, 2, null),
        row(2, 3, 2),
        row(3, 4, null),
        row(3, 5, 4),
        row(3, 6, 9),
        row(4, 7, null),
        row(4, 8, 7),
        row(4, 9, 15),
        row(4, 10, 24),
        row(5, 11, null),
        row(5, 12, 11),
        row(5, 13, 23),
        row(5, 14, 36),
        row(5, 15, 39)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding -1.rows following 3.rows as 'w
        )
        .select(
          'd, 'e, Sum('e) over 'w
        ),
      Seq(
        row(1, 1, null),
        row(2, 2, 3),
        row(2, 3, null),
        row(3, 4, 11),
        row(3, 5, 6),
        row(3, 6, null),
        row(4, 7, 27),
        row(4, 8, 19),
        row(4, 9, 10),
        row(4, 10, null),
        row(5, 11, 39),
        row(5, 12, 42),
        row(5, 13, 29),
        row(5, 14, 15),
        row(5, 15, null)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding 5.rows following -4.rows as 'w
        )
        .select(
          'd, 'e, Sum('e) over 'w
        ),
      Seq(
        row(1, 1, null),
        row(2, 2, null),
        row(2, 3, null),
        row(3, 4, null),
        row(3, 5, null),
        row(3, 6, null),
        row(4, 10, null),
        row(4, 7, null),
        row(4, 8, null),
        row(4, 9, null),
        row(5, 11, 15),
        row(5, 12, null),
        row(5, 13, null),
        row(5, 14, null),
        row(5, 15, null)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding 5.rows following -4.rows as 'w
        )
        .select(
          'd, 'e, Sum('e) over 'w
        ),
      Seq(
        row(1, 1, null),
        row(2, 2, null),
        row(2, 3, null),
        row(3, 4, null),
        row(3, 5, null),
        row(3, 6, null),
        row(4, 10, null),
        row(4, 7, null),
        row(4, 8, null),
        row(4, 9, null),
        row(5, 11, null),
        row(5, 12, null),
        row(5, 13, null),
        row(5, 14, null),
        row(5, 15, 11)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding -4.rows following 5.rows as 'w
        )
        .select(
          'd, 'e, Sum('e) over 'w
        ),
      Seq(
        row(1, 1, null),
        row(2, 2, null),
        row(2, 3, null),
        row(3, 4, null),
        row(3, 5, null),
        row(3, 6, null),
        row(4, 10, null),
        row(4, 7, null),
        row(4, 8, null),
        row(4, 9, null),
        row(5, 11, null),
        row(5, 12, null),
        row(5, 13, null),
        row(5, 14, null),
        row(5, 15, 11)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding -4.rows following 5.rows as 'w
        )
        .select(
          'd, 'e, Sum('e) over 'w
        ),
      Seq(
        row(1, 1, null),
        row(2, 2, null),
        row(2, 3, null),
        row(3, 4, null),
        row(3, 5, null),
        row(3, 6, null),
        row(4, 10, null),
        row(4, 7, null),
        row(4, 8, null),
        row(4, 9, null),
        row(5, 11, 15),
        row(5, 12, null),
        row(5, 13, null),
        row(5, 14, null),
        row(5, 15, null)
      )
    )
  }

  @Test
  def testNegativeRange(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'h preceding 2.range following -1.range as 'w
        )
        .select(
          'd, 'h, Sum('h) over 'w
        ),
      Seq(
        row(1, 1, null),
        row(2, 1, null),
        row(2, 2, 1),
        row(3, 2, null),
        row(3, 2, null),
        row(3, 3, 4),
        row(4, 1, null),
        row(4, 1, null),
        row(4, 2, 2),
        row(4, 2, 2),
        row(5, 1, null),
        row(5, 2, 1),
        row(5, 2, 1),
        row(5, 3, 5),
        row(5, 3, 5)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'h preceding -1.range following 2.range as 'w
        )
        .select(
          'd, 'h, Sum('h) over 'w
        ),
      Seq(
        row(1, 1, null),
        row(2, 1, 2),
        row(2, 2, null),
        row(3, 2, 3),
        row(3, 2, 3),
        row(3, 3, null),
        row(4, 1, 4),
        row(4, 1, 4),
        row(4, 2, null),
        row(4, 2, null),
        row(5, 1, 10),
        row(5, 2, 6),
        row(5, 2, 6),
        row(5, 3, null),
        row(5, 3, null)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding 5.range following -4.range as 'w
        )
        .select(
          'd, 'e, Sum('e) over 'w
        ),
      Seq(
        row(1, 1, null),
        row(2, 2, null),
        row(2, 3, null),
        row(3, 4, null),
        row(3, 5, null),
        row(3, 6, null),
        row(4, 10, null),
        row(4, 7, null),
        row(4, 8, null),
        row(4, 9, null),
        row(5, 11, 15),
        row(5, 12, null),
        row(5, 13, null),
        row(5, 14, null),
        row(5, 15, null)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding 5.range following -4.range as 'w
        )
        .select(
          'd, 'e, Sum('e) over 'w
        ),
      Seq(
        row(1, 1, null),
        row(2, 2, null),
        row(2, 3, null),
        row(3, 4, null),
        row(3, 5, null),
        row(3, 6, null),
        row(4, 10, null),
        row(4, 7, null),
        row(4, 8, null),
        row(4, 9, null),
        row(5, 11, null),
        row(5, 12, null),
        row(5, 13, null),
        row(5, 14, null),
        row(5, 15, 11)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding -4.range following 5.range as 'w
        )
        .select(
          'd, 'e, Sum('e) over 'w
        ),
      Seq(
        row(1, 1, null),
        row(2, 2, null),
        row(2, 3, null),
        row(3, 4, null),
        row(3, 5, null),
        row(3, 6, null),
        row(4, 10, null),
        row(4, 7, null),
        row(4, 8, null),
        row(4, 9, null),
        row(5, 11, null),
        row(5, 12, null),
        row(5, 13, null),
        row(5, 14, null),
        row(5, 15, 11)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding -4.range following 5.range as 'w
        )
        .select(
          'd, 'e, Sum('e) over 'w
        ),
      Seq(
        row(1, 1, null),
        row(2, 2, null),
        row(2, 3, null),
        row(3, 4, null),
        row(3, 5, null),
        row(3, 6, null),
        row(4, 10, null),
        row(4, 7, null),
        row(4, 8, null),
        row(4, 9, null),
        row(5, 11, 15),
        row(5, 12, null),
        row(5, 13, null),
        row(5, 14, null),
        row(5, 15, null)
      )
    )
  }

  @Test
  def testFractionalRange(): Unit = {
    val table = tEnv.scan("Table5")

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'b preceding 0.15.range following 0.05.range as 'w
        )
        .select('a, 'b, Sum('b) over 'w),
      Seq(
        row(1, 0.1, 0.1),
        row(2, 0.2, 0.4),
        row(2, 0.2, 0.4),
        row(3, 0.3, 0.6),
        row(3, 0.3, 0.6),
        row(3, 0.4, 1.0),
        row(4, 0.5, 1.0),
        row(4, 0.5, 1.0),
        row(4, 0.6, 2.2),
        row(4, 0.6, 2.2),
        row(5, 0.7, 1.4),
        row(5, 0.7, 1.4),
        row(5, 0.8, 3.0),
        row(5, 0.8, 3.0),
        row(5, 0.9, 2.5)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'b preceding 0.15.range following -0.05.range as 'w
        )
        .select('a, 'b, Sum('b) over 'w),
      Seq(
        row(1, 0.1, null),
        row(2, 0.2, null),
        row(2, 0.2, null),
        row(3, 0.3, null),
        row(3, 0.3, null),
        row(3, 0.4, 0.6),
        row(4, 0.5, null),
        row(4, 0.5, null),
        row(4, 0.6, 1.0),
        row(4, 0.6, 1.0),
        row(5, 0.7, null),
        row(5, 0.7, null),
        row(5, 0.8, 1.4),
        row(5, 0.8, 1.4),
        row(5, 0.9, 1.6)
      )
    )
  }

  @Test
  def testWindowAggWithConstants(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over as 'w
        )
        .select(
          Count(1) over 'w
        ),
      Seq(
        row(15),
        row(15),
        row(15),
        row(15),
        row(15),
        row(15),
        row(15),
        row(15),
        row(15),
        row(15),
        row(15),
        row(15),
        row(15),
        row(15),
        row(15)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding 1.range following 10.range as 'w
        )
        .select('d, 'e, Sum('e) over 'w, Count(1) over 'w),
      Seq(
        row(1, 1, 1, 1),
        row(2, 2, 5, 2),
        row(2, 3, 5, 2),
        row(3, 4, 15, 3),
        row(3, 5, 15, 3),
        row(3, 6, 11, 2),
        row(4, 10, 19, 2),
        row(4, 7, 34, 4),
        row(4, 8, 34, 4),
        row(4, 9, 27, 3),
        row(5, 11, 65, 5),
        row(5, 12, 65, 5),
        row(5, 13, 54, 4),
        row(5, 14, 42, 3),
        row(5, 15, 29, 2)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding 1.range following 10.range as 'w
        )
        .select('d, 'e, Sum('e) over 'w, Count(1) over 'w),
      Seq(
        row(1, 1, 1, 1),
        row(2, 2, 5, 2),
        row(2, 3, 5, 2),
        row(3, 4, 9, 2),
        row(3, 5, 15, 3),
        row(3, 6, 15, 3),
        row(4, 10, 34, 4),
        row(4, 7, 15, 2),
        row(4, 8, 24, 3),
        row(4, 9, 34, 4),
        row(5, 11, 23, 2),
        row(5, 12, 36, 3),
        row(5, 13, 50, 4),
        row(5, 14, 65, 5),
        row(5, 15, 65, 5)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding -1.range following 10.range as 'w
        )
        .select('d, 'e, Sum('e) over 'w, Count(1) over 'w),
      Seq(
        row(1, 1, null, 0),
        row(2, 2, null, 0),
        row(2, 3, 2, 1),
        row(3, 4, null, 0),
        row(3, 5, 4, 1),
        row(3, 6, 9, 2),
        row(4, 10, 24, 3),
        row(4, 7, null, 0),
        row(4, 8, 7, 1),
        row(4, 9, 15, 2),
        row(5, 11, null, 0),
        row(5, 12, 11, 1),
        row(5, 13, 23, 2),
        row(5, 14, 36, 3),
        row(5, 15, 50, 4)
      )
    )
  }

  @Test
  def testWindowAggWithNull(): Unit = {
    val table = tEnv.scan("NullTable2")

    checkResult(
      table
        .window(
          Over partitionBy 'h orderBy 'd preceding 1.range following 2.range as 'w
        )
        .select(
          'h, 'd, Count("*") over 'w
        ),
      Seq(
        row(1, 1, 2),
        row(1, 2, 4),
        row(1, 4, 3),
        row(1, 4, 3),
        row(1, 5, 3),
        row(2, 2, 5),
        row(2, 3, 7),
        row(2, 3, 7),
        row(2, 4, 6),
        row(2, 4, 6),
        row(2, 5, 4),
        row(2, 5, 4),
        row(3, 3, 3),
        row(3, 5, 2),
        row(3, 5, 2),
        row(3, null, 2),
        row(3, null, 2)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'h orderBy 'd as 'w
        )
        .select(
          'h, 'd, Count("*") over 'w
        ),
      Seq(
        row(1, 1, 1),
        row(1, 2, 2),
        row(1, 4, 4),
        row(1, 4, 4),
        row(1, 5, 5),
        row(2, 2, 1),
        row(2, 3, 3),
        row(2, 3, 3),
        row(2, 4, 5),
        row(2, 4, 5),
        row(2, 5, 7),
        row(2, 5, 7),
        row(3, 3, 3),
        row(3, 5, 5),
        row(3, 5, 5),
        row(3, null, 2),
        row(3, null, 2)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'h orderBy 'd.nullsFirst as 'w
        )
        .select(
          'h, 'd, Count("*") over 'w
        ),
      Seq(
        row(1, 1, 1),
        row(1, 2, 2),
        row(1, 4, 4),
        row(1, 4, 4),
        row(1, 5, 5),
        row(2, 2, 1),
        row(2, 3, 3),
        row(2, 3, 3),
        row(2, 4, 5),
        row(2, 4, 5),
        row(2, 5, 7),
        row(2, 5, 7),
        row(3, 3, 3),
        row(3, 5, 5),
        row(3, 5, 5),
        row(3, null, 2),
        row(3, null, 2)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'h orderBy 'd as 'w
        )
        .select(
          'h, 'd, Rank() over 'w
        ),
      Seq(
        row(1, 1, 1),
        row(1, 2, 2),
        row(1, 4, 3),
        row(1, 4, 3),
        row(1, 5, 5),
        row(2, 2, 1),
        row(2, 3, 2),
        row(2, 3, 2),
        row(2, 4, 4),
        row(2, 4, 4),
        row(2, 5, 6),
        row(2, 5, 6),
        row(3, 3, 3),
        row(3, 5, 4),
        row(3, 5, 4),
        row(3, null, 1),
        row(3, null, 1)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'h orderBy 'd.nullsFirst as 'w
        )
        .select(
          'h, 'd, Rank() over 'w
        ),
      Seq(
        row(1, 1, 1),
        row(1, 2, 2),
        row(1, 4, 3),
        row(1, 4, 3),
        row(1, 5, 5),
        row(2, 2, 1),
        row(2, 3, 2),
        row(2, 3, 2),
        row(2, 4, 4),
        row(2, 4, 4),
        row(2, 5, 6),
        row(2, 5, 6),
        row(3, 3, 3),
        row(3, 5, 4),
        row(3, 5, 4),
        row(3, null, 1),
        row(3, null, 1)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'h orderBy 'd.desc preceding 1.range following 2.range as 'w
        )
        .select(
          'h, 'd, Count("*") over 'w
        ),
      Seq(
        row(1, 1, 2),
        row(1, 2, 2),
        row(1, 4, 4),
        row(1, 4, 4),
        row(1, 5, 3),
        row(2, 2, 3),
        row(2, 3, 5),
        row(2, 3, 5),
        row(2, 4, 7),
        row(2, 4, 7),
        row(2, 5, 6),
        row(2, 5, 6),
        row(3, 3, 1),
        row(3, 5, 3),
        row(3, 5, 3),
        row(3, null, 2),
        row(3, null, 2)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'h orderBy 'd.desc.nullsFirst preceding 1.range following 2.range as 'w
        )
        .select(
          'h, 'd, Count("*") over 'w
        ),
      Seq(
        row(1, 1, 2),
        row(1, 2, 2),
        row(1, 4, 4),
        row(1, 4, 4),
        row(1, 5, 3),
        row(2, 2, 3),
        row(2, 3, 5),
        row(2, 3, 5),
        row(2, 4, 7),
        row(2, 4, 7),
        row(2, 5, 6),
        row(2, 5, 6),
        row(3, 3, 1),
        row(3, 5, 3),
        row(3, 5, 3),
        row(3, null, 2),
        row(3, null, 2)
      )
    )
  }

  @Test
  def testWindowAggregationAtDate(): Unit = {
    val table = tEnv.scan("Table3")

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'd preceding 0.day following 2.day as 'w
        )
        .select(
          'a, 'd, Count("*") over 'w
        ),
      Seq(
        row(1, UTCDate("2017-04-08"), 1),
        row(2, UTCDate("2017-04-08"), 2),
        row(2, UTCDate("2017-04-09"), 1),
        row(3, UTCDate("2016-08-08"), 1),
        row(3, UTCDate("2017-04-10"), 1),
        row(3, UTCDate("2017-10-11"), 1),
        row(4, UTCDate("2017-02-06"), 1),
        row(4, UTCDate("2017-05-19"), 2),
        row(4, UTCDate("2017-05-20"), 1),
        row(4, UTCDate("2017-11-11"), 1),
        row(5, UTCDate("2017-02-02"), 1),
        row(5, UTCDate("2017-07-01"), 1),
        row(5, UTCDate("2017-07-20"), 1),
        row(5, UTCDate("2017-09-08"), 1),
        row(5, UTCDate("2017-10-01"), 1)
      )
    )
  }

  @Test
  def testWindowAggregationAtTime(): Unit = {
    val table = tEnv.scan("Table3")

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'e preceding 0.hour following 2.hour as 'w
        )
        .select(
          'a, Count("*") over 'w
        ),
      Seq(
        row(1, 1),
        row(2, 1),
        row(2, 1),
        row(3, 1),
        row(3, 1),
        row(3, 2),
        row(4, 1),
        row(4, 1),
        row(4, 1),
        row(4, 2),
        row(5, 1),
        row(5, 1),
        row(5, 1),
        row(5, 1),
        row(5, 2)
      )
    )
  }

  @Test
  def testLeadLagFunc(): Unit = {
    val table = tEnv.scan("Table3")

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'c as 'w
        )
        .select('a, 'c, Lag('c) over 'w),
      Seq(
        row(1, "a", null),
        row(5, "GHI", null),
        row(5, "HIJ", "GHI"),
        row(5, "IJK", "HIJ"),
        row(5, "JKL", "IJK"),
        row(5, "KLM", "JKL"),
        row(4, "CDE", null),
        row(4, "DEF", "CDE"),
        row(4, "EFG", "DEF"),
        row(4, "FGH", "EFG"),
        row(2, "abc", null),
        row(2, "abcd", "abc"),
        row(3, "ABC", null),
        row(3, "BCD", "ABC"),
        row(3, "abc?", "BCD")
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'b as 'w
        )
        .select('a, 'b, Lead('b, 2, 3.0) over 'w, Lag('b, 1, 3.0) over 'w),
      Seq(
        row(1, 1.1, 3.0, 3.0),
        row(5, -5.9, 0.7, 3.0),
        row(5, -2.8, 2.71, -5.9),
        row(5, 0.7, 3.9, -2.8),
        row(5, 2.71, 3.0, 0.7),
        row(5, 3.9, 3.0, 2.71),
        row(4, 3.14, 3.15, 3.0),
        row(4, 3.14, 3.16, 3.14),
        row(4, 3.15, 3.0, 3.14),
        row(4, 3.16, 3.0, 3.15),
        row(2, -2.4, 3.0, 3.0),
        row(2, 2.5, 3.0, -2.4),
        row(3, -9.77, 0.08, 3.0),
        row(3, 0.0, 3.0, -9.77),
        row(3, 0.08, 3.0, 0.0)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'b as 'w
        )
        .select('a, 'b, Lead('b, 2, 3.0) over 'w),
      Seq(
        row(1, 1.1, 3.0),
        row(5, -5.9, 0.7),
        row(5, -2.8, 2.71),
        row(5, 0.7, 3.9),
        row(5, 2.71, 3.0),
        row(5, 3.9, 3.0),
        row(4, 3.14, 3.15),
        row(4, 3.14, 3.16),
        row(4, 3.15, 3.0),
        row(4, 3.16, 3.0),
        row(2, -2.4, 3.0),
        row(2, 2.5, 3.0),
        row(3, -9.77, 0.08),
        row(3, 0.0, 3.0),
        row(3, 0.08, 3.0)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'b as 'w
        )
        .select('a, 'b, Lead('b, -2, 3.0) over 'w),
      Seq(
        row(1, 1.1, 3.0),
        row(5, -5.9, 3.0),
        row(5, -2.8, 3.0),
        row(5, 0.7, -5.9),
        row(5, 2.71, -2.8),
        row(5, 3.9, 0.7),
        row(4, 3.14, 3.0),
        row(4, 3.14, 3.0),
        row(4, 3.15, 3.14),
        row(4, 3.16, 3.14),
        row(2, -2.4, 3.0),
        row(2, 2.5, 3.0),
        row(3, -9.77, 3.0),
        row(3, 0.0, 3.0),
        row(3, 0.08, -9.77)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'b as 'w
        )
        .select('a, 'b, Lag('b, 2, 3.0) over 'w),
      Seq(
        row(1, 1.1, 3.0),
        row(5, -5.9, 3.0),
        row(5, -2.8, 3.0),
        row(5, 0.7, -5.9),
        row(5, 2.71, -2.8),
        row(5, 3.9, 0.7),
        row(4, 3.14, 3.0),
        row(4, 3.14, 3.0),
        row(4, 3.15, 3.14),
        row(4, 3.16, 3.14),
        row(2, -2.4, 3.0),
        row(2, 2.5, 3.0),
        row(3, -9.77, 3.0),
        row(3, 0.0, 3.0),
        row(3, 0.08, -9.77)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'b as 'w
        )
        .select('a, 'b, Lag('b, -2, 3.0) over 'w),
      Seq(
        row(1, 1.1, 3.0),
        row(5, -5.9, 0.7),
        row(5, -2.8, 2.71),
        row(5, 0.7, 3.9),
        row(5, 2.71, 3.0),
        row(5, 3.9, 3.0),
        row(4, 3.14, 3.15),
        row(4, 3.14, 3.16),
        row(4, 3.15, 3.0),
        row(4, 3.16, 3.0),
        row(2, -2.4, 3.0),
        row(2, 2.5, 3.0),
        row(3, -9.77, 0.08),
        row(3, 0.0, 3.0),
        row(3, 0.08, 3.0)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'b as 'w
        )
        .select('a, 'b, Lead('b, -2, 3.0) over 'w),
      Seq(
        row(1, 1.1, 3.0),
        row(5, -5.9, 3.0),
        row(5, -2.8, 3.0),
        row(5, 0.7, -5.9),
        row(5, 2.71, -2.8),
        row(5, 3.9, 0.7),
        row(4, 3.14, 3.0),
        row(4, 3.14, 3.0),
        row(4, 3.15, 3.14),
        row(4, 3.16, 3.14),
        row(2, -2.4, 3.0),
        row(2, 2.5, 3.0),
        row(3, -9.77, 3.0),
        row(3, 0.0, 3.0),
        row(3, 0.08, -9.77)
      )
    )

    checkResult(
      table
        .window(
          Over partitionBy 'a orderBy 'b as 'w
        )
        .select('a, 'b, Lag('b, -2, 'b) over 'w),
      Seq(
        row(1, 1.1, 1.1),
        row(5, -5.9, 0.7),
        row(5, -2.8, 2.71),
        row(5, 0.7, 3.9),
        row(5, 2.71, 2.71),
        row(5, 3.9, 3.9),
        row(4, 3.14, 3.15),
        row(4, 3.14, 3.16),
        row(4, 3.15, 3.15),
        row(4, 3.16, 3.16),
        row(2, -2.4, -2.4),
        row(2, 2.5, 2.5),
        row(3, -9.77, 0.08),
        row(3, 0.0, 0.0),
        row(3, 0.08, 0.08)
      )
    )
  }

  @Test
  def testMultiOverWindowRangeType(): Unit = {
    val table = tEnv.scan("Table3")
    checkResult(
      table
        .filter('a === 5)
        .window(
          Over partitionBy('a, 'c) as 'w1,
          Over partitionBy 'a orderBy 'c as 'w2
        )
        .select('a, 'c, Count("*") over 'w1, Rank() over 'w2),
      Seq(
        row(5, "GHI", 1, 1),
        row(5, "HIJ", 1, 2),
        row(5, "IJK", 1, 3),
        row(5, "JKL", 1, 4),
        row(5, "KLM", 1, 5)))
  }

  @Test
  def testBatchCurrentRange(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(
          Over orderBy 'h preceding UNBOUNDED_RANGE following CURRENT_RANGE as 'w
        )
        .select('h, Sum('h) over 'w),
      Seq(
        row(1, 5),
        row(1, 5),
        row(1, 5),
        row(1, 5),
        row(1, 5),
        row(2, 19),
        row(2, 19),
        row(2, 19),
        row(2, 19),
        row(2, 19),
        row(2, 19),
        row(2, 19),
        row(3, 28),
        row(3, 28),
        row(3, 28)
      )
    )

    checkResult(
      table
        .window(
          Over orderBy 'h preceding CURRENT_RANGE following UNBOUNDED_RANGE as 'w
        )
        .select('h, Sum('h) over 'w),
      Seq(
        row(1, 28),
        row(1, 28),
        row(1, 28),
        row(1, 28),
        row(1, 28),
        row(2, 23),
        row(2, 23),
        row(2, 23),
        row(2, 23),
        row(2, 23),
        row(2, 23),
        row(2, 23),
        row(3, 9),
        row(3, 9),
        row(3, 9)
      )
    )

    checkResult(
      table
        .window(
          Over orderBy 'h.desc preceding UNBOUNDED_RANGE following CURRENT_RANGE as 'w
        )
        .select('h, Sum('h) over 'w),
      Seq(
        row(3, 9),
        row(3, 9),
        row(3, 9),
        row(2, 23),
        row(2, 23),
        row(2, 23),
        row(2, 23),
        row(2, 23),
        row(2, 23),
        row(2, 23),
        row(1, 28),
        row(1, 28),
        row(1, 28),
        row(1, 28),
        row(1, 28)
      )
    )

    checkResult(
      table
        .window(
          Over orderBy 'h.desc preceding CURRENT_RANGE following UNBOUNDED_RANGE as 'w
        )
        .select('h, Sum('h) over 'w),
      Seq(
        row(3, 28),
        row(3, 28),
        row(3, 28),
        row(2, 19),
        row(2, 19),
        row(2, 19),
        row(2, 19),
        row(2, 19),
        row(2, 19),
        row(2, 19),
        row(1, 5),
        row(1, 5),
        row(1, 5),
        row(1, 5),
        row(1, 5)
      )
    )
  }

  @Test
  def testAggWithLiteral(): Unit = {
    val table = tEnv.scan("Table2")

    checkResult(
      table
        .window(Over as 'w)
        .select(
          'd, 'e,
          Sum(2) over 'w,
          Avg(1) over 'w,
          Count(1) over 'w,
          Min(1) over 'w,
          Max(1) over 'w,
          FirstValue(true) over 'w,
          FirstValue(Null(DataTypes.BOOLEAN)) over 'w,
          LastValue(false) over 'w,
          LastValue(Null(DataTypes.BOOLEAN)) over 'w),
      Seq(
        row(1, 1L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(2, 2L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(2, 3L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(3, 4L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(3, 5L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(3, 6L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(4, 7L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(4, 8L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(4, 9L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(4, 10L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(5, 11L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(5, 12L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(5, 13L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(5, 14L, 30, 1.0, 15, 1, 1, true, null, false, null),
        row(5, 15L, 30, 1.0, 15, 1, 1, true, null, false, null)))
  }

  private[OverWindowAggITCase] def checkResult(table: Table, expected: Seq[Row]): Unit = {
    val results = table.collect()
    checkSame(expected, results).foreach { results =>
      val plan = explainLogical(table)
      Assert.fail(
        s"""
           |Results do not match:
           |$results
           |Plan:
           |  $plan
       """.stripMargin)
    }
  }
}
