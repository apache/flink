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

package org.apache.flink.table.planner.runtime.batch.table

import org.apache.flink.table.api.Over
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.utils.DateTimeTestUtil._

import org.junit.{Before, Ignore, Test}

// TODO OverWindow on Batch should support to order by non-time attribute
@Ignore
class OverWindowITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection(
      "Table1", data1, type1, "month, area, product", nullablesOfData1)
    registerCollection(
      "Table2", data2, type2, "d, e, f, g, h", nullablesOfData2)
    registerCollection(
      "NullTable2", nullData2, type2, "d, e, f, g, h", nullablesOfNullData2)
    registerCollection(
      "Table3", data6, type6, "a, b, c, d, e, f", nullablesOfData6)
    registerCollection(
      "Table4", data4, type4, "category, shopId, num", nullablesOfData4)
    registerCollection(
      "NullTable4", nullData4, type4, "category, shopId, num", nullablesOfNullData4)
    registerCollection(
      "Table5", simpleData2, simpleType2, "a, b", nullableOfSimpleData2)
  }

  @Test
  def testSingleRowOverWindow(): Unit = {
    val table = tEnv.from("Table1")

    val expected = Seq(
      row("a", 1, 5),
      row("a", 2, 6),
      row("b", 3, 7),
      row("b", 4, 8),
      row("c", 5, 9),
      row("c", 6, 10)
    )
    checkTableResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month preceding 0.rows following 0.rows as 'w)
        .select('area, 'month, 'product.sum over 'w),
      expected
    )

    checkTableResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month preceding CURRENT_ROW following 0.rows as 'w)
        .select('area, 'month, 'product.sum over 'w),
      expected
    )

    checkTableResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month preceding 0.rows following CURRENT_ROW as 'w)
        .select('area, 'month, 'product.sum over 'w),
      expected
    )

    checkTableResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month preceding CURRENT_ROW following CURRENT_ROW as 'w)
        .select('area, 'month, 'product.sum over 'w),
      expected
    )
  }

  @Test
  def testUnboundedRowOverWindow(): Unit = {
    val table = tEnv.from("Table1")

    checkTableResult(
      table
        .window(
          Over partitionBy 'area orderBy 'month
            preceding UNBOUNDED_ROW following UNBOUNDED_ROW as 'w
        )
        .select('area, 'month, 'product.sum over 'w),
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
  def testOrderByStringTypeField(): Unit = {
    val table2 = tEnv.from("Table2")

    checkTableResult(
      table2
        .window(
          Over partitionBy 'd orderBy 'g.desc preceding UNBOUNDED_ROW following CURRENT_ROW as 'w
        )
        .select('d, 'g, 'e.count over 'w),
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

    checkTableResult(
      table2
        .window(
          Over partitionBy 'd orderBy 'g preceding UNBOUNDED_ROW following CURRENT_ROW as 'w
        )
        .select('d, 'g, 'e.count over 'w),
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
  }

  @Test
  def testWindowAggregationSumWithOrderBy(): Unit = {
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc as 'w
        )
        .select('d, 'e, 'e.sum over 'w),
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
  def testWindowAggregationSumWithOrderByAndWithoutPartitionBy(): Unit = {
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over orderBy 'e as 'w
        )
        .select('d, 'e, 'e.sum over 'w),
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
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc as 'w
        )
        .select('d, 'e, '*.count over 'w),
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
  def testWindowAggregationAvgWithOrderBy(): Unit = {
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc as 'w
        )
        .select('d, 'e, 'e.avg over 'w),
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
  def testWindowAggregationSumWithOrderByWithRowsBetween(): Unit = {
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding UNBOUNDED_ROW following CURRENT_ROW as 'w
        )
        .select('d, 'e, 'e.sum over 'w),
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
    val table = tEnv.from("Table2")

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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding CURRENT_ROW following UNBOUNDED_ROW as 'w
        )
        .select('d, 'e, 'e.sum over 'w),
      expected
    )

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc
            preceding CURRENT_ROW following 2147483648L.rows as 'w
        )
        .select('d, 'e, 'e.sum over 'w),
      expected
    )
  }

  @Test
  def testWindowAggregationSumWithOrderByWithRangeBetween(): Unit = {
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding 10.rows following 1.rows as 'w1,
          Over partitionBy 'd orderBy 'e.desc preceding 2.rows following 3.rows as 'w2,
          Over partitionBy 'd orderBy 'e.desc
            preceding UNBOUNDED_RANGE following CURRENT_RANGE as 'w3,
          Over partitionBy 'd orderBy 'e.desc
            preceding CURRENT_RANGE following UNBOUNDED_RANGE as 'w4,
          Over partitionBy 'd orderBy 'e.desc preceding 1 following 2 as 'w5,
          Over partitionBy 'd orderBy 'e preceding 3 following 3 as 'w6,
          Over partitionBy 'd orderBy 'h preceding 1 following 0 as 'w7
        )
        .select('d, 'e,
          'e.sum over 'w1,
          'e.sum over 'w2,
          'e.sum over 'w3,
          'e.sum over 'w4,
          'e.sum over 'w5,
          'e.sum over 'w6,
          'h.sum over 'w7,
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
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding 5.rows following 2.rows as 'w1,
          Over partitionBy 'd orderBy 'e.desc preceding 6.rows following 2.rows as 'w2,
          Over partitionBy 'd orderBy 'e preceding UNBOUNDED_ROW following CURRENT_ROW as 'w3,
          Over partitionBy 'd orderBy 'e.desc preceding CURRENT_ROW following UNBOUNDED_ROW as 'w4
        )
        .select(
          'd, 'e, 'f,
          'e.sum over 'w1,
          '*.count over 'w2,
          'f.max over 'w3,
          'h.min over 'w4,
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
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc as 'w
        )
        .select('d, 'e, 'e.max over 'w),
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
  def testWindowAggregationMinWithOrderBy(): Unit = {
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc as 'w
        )
        .select('d, 'e, 'e.min over 'w),
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
  def testAvg(): Unit = {
    val table = tEnv.from("Table4")

    checkTableResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding UNBOUNDED_ROW following 1.rows as 'w
        )
        .select(
          'category, 'num, 'num.avg over 'w
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
    val table = tEnv.from("Table4")

    // sliding frame case: 1 - 1
    checkTableResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding 1 following 1 as 'w
        )
        .select(
          'category, 'num, 'num.sum over 'w
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
    checkTableResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding UNBOUNDED_RANGE following 1 as 'w
        )
        .select(
          'category, 'num, 'num.sum over 'w
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
    checkTableResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding 1 following UNBOUNDED_RANGE as 'w
        )
        .select(
          'category, 'num, 'num.sum over 'w
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
    val table = tEnv.from("Table4")

    // sliding frame case: unbounded - 1
    checkTableResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding UNBOUNDED_ROW following 1.rows as 'w
        )
        .select(
          'category, 'num, 'num.sum over 'w
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
    checkTableResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding 1.rows following UNBOUNDED_ROW as 'w
        )
        .select(
          'category, 'num, 'num.sum over 'w
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
    val table = tEnv.from("NullTable4")

    checkTableResult(
      table
        .window(
          Over partitionBy 'category orderBy 'num preceding 1 following 1 as 'w
        )
        .select(
          'category, 'num, 'num.sum over 'w
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
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding 3.rows following -1.rows as 'w
        )
        .select(
          'd, 'e, 'e.sum over 'w
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding -1.rows following 3.rows as 'w
        )
        .select(
          'd, 'e, 'e.sum over 'w
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding 5.rows following -4.rows as 'w
        )
        .select(
          'd, 'e, 'e.sum over 'w
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding 5.rows following -4.rows as 'w
        )
        .select(
          'd, 'e, 'e.sum over 'w
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding -4.rows following 5.rows as 'w
        )
        .select(
          'd, 'e, 'e.sum over 'w
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding -4.rows following 5.rows as 'w
        )
        .select(
          'd, 'e, 'e.sum over 'w
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
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'h preceding 2 following -1 as 'w
        )
        .select(
          'd, 'h, 'h.sum over 'w
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'h preceding -1 following 2 as 'w
        )
        .select(
          'd, 'h, 'h.sum over 'w
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding 5 following -4 as 'w
        )
        .select(
          'd, 'e, 'e.sum over 'w
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding 5 following -4 as 'w
        )
        .select(
          'd, 'e, 'e.sum over 'w
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding -4 following 5 as 'w
        )
        .select(
          'd, 'e, 'e.sum over 'w
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding -4 following 5 as 'w
        )
        .select(
          'd, 'e, 'e.sum over 'w
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
    val table = tEnv.from("Table5")

    checkTableResult(
      table
        .window(
          Over partitionBy 'a orderBy 'b preceding 0.15 following 0.05 as 'w
        )
        .select('a, 'b, 'b.sum over 'w),
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'a orderBy 'b preceding 0.15 following -0.05 as 'w
        )
        .select('a, 'b, 'b.sum over 'w),
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
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e preceding 1 following 10 as 'w
        )
        .select('d, 'e, 'e.sum over 'w, 1.count over 'w),
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding 1 following 10 as 'w
        )
        .select('d, 'e, 'e.sum over 'w, 1.count over 'w),
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'd orderBy 'e.desc preceding -1 following 10 as 'w
        )
        .select('d, 'e, 'e.sum over 'w, 1.count over 'w),
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
    val table = tEnv.from("NullTable2")

    checkTableResult(
      table
        .window(
          Over partitionBy 'h orderBy 'd preceding 1 following 2 as 'w
        )
        .select(
          'h, 'd, '*.count over 'w
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'h orderBy 'd as 'w
        )
        .select(
          'h, 'd, '*.count over 'w
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

    checkTableResult(
      table
        .window(
          Over partitionBy 'h orderBy 'd.desc preceding 1 following 2 as 'w
        )
        .select(
          'h, 'd, '*.count over 'w
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
    val table = tEnv.from("Table3")

    checkTableResult(
      table
        .window(
          Over partitionBy 'a orderBy 'd preceding 0.day following 2.day as 'w
        )
        .select(
          'a, 'd, '*.count over 'w
        ),
      Seq(
        row(1, localDate("2017-04-08"), 1),
        row(2, localDate("2017-04-08"), 2),
        row(2, localDate("2017-04-09"), 1),
        row(3, localDate("2016-08-08"), 1),
        row(3, localDate("2017-04-10"), 1),
        row(3, localDate("2017-10-11"), 1),
        row(4, localDate("2017-02-06"), 1),
        row(4, localDate("2017-05-19"), 2),
        row(4, localDate("2017-05-20"), 1),
        row(4, localDate("2017-11-11"), 1),
        row(5, localDate("2017-02-02"), 1),
        row(5, localDate("2017-07-01"), 1),
        row(5, localDate("2017-07-20"), 1),
        row(5, localDate("2017-09-08"), 1),
        row(5, localDate("2017-10-01"), 1)
      )
    )
  }

  @Test
  def testWindowAggregationAtTime(): Unit = {
    val table = tEnv.from("Table3")

    checkTableResult(
      table
        .window(
          Over partitionBy 'a orderBy 'e preceding 0.hour following 2.hour as 'w
        )
        .select(
          'a, '*.count over 'w
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
  def testBatchCurrentRange(): Unit = {
    val table = tEnv.from("Table2")

    checkTableResult(
      table
        .window(
          Over orderBy 'h preceding UNBOUNDED_RANGE following CURRENT_RANGE as 'w
        )
        .select('h, 'h.sum over 'w),
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

    checkTableResult(
      table
        .window(
          Over orderBy 'h preceding CURRENT_RANGE following UNBOUNDED_RANGE as 'w
        )
        .select('h, 'h.sum over 'w),
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

    checkTableResult(
      table
        .window(
          Over orderBy 'h.desc preceding UNBOUNDED_RANGE following CURRENT_RANGE as 'w
        )
        .select('h, 'h.sum over 'w),
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

    checkTableResult(
      table
        .window(
          Over orderBy 'h.desc preceding CURRENT_RANGE following UNBOUNDED_RANGE as 'w
        )
        .select('h, 'h.sum over 'w),
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

}
