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
package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{DOUBLE_TYPE_INFO, FLOAT_TYPE_INFO, INT_TYPE_INFO, SHORT_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.utils.DateTimeTestUtil._
import org.apache.flink.types.Row

import org.junit.{Before, Test}

import java.lang.{Iterable => JIterable, Long => JLong}

import scala.collection.Seq
import scala.util.Random

class OverWindowITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("Table5", data5, type5, "d, e, f, g, h", nullablesOfData5)
    registerCollection("ShuflledTable5",
      Random.shuffle(data5), type5, "sd, se, sf, sg, sh", nullablesOfData5)
    registerCollection("Table6", data6, type6, "a, b, c, d, e, f", nullablesOfData6)
    registerCollection("NullTable5", nullData5, type5, "d, e, f, g, h", nullablesOfNullData5)
  }

  @Test
  def testOverWindowWithUDAGG(): Unit = {

    registerFunction("countFun", new CountAggFunction())

    checkResult(
      "SELECT sd, sf, sh, countFun(sh) " +
          "over (partition by sh order by sh rows between 0 PRECEDING and 0 FOLLOWING) " +
          "FROM ShuflledTable5",
      Seq(
        row(1, 0, 1, 1),
        row(2, 2, 1, 1),
        row(4, 7, 1, 1),
        row(4, 8, 1, 1),
        row(5, 10, 1, 1),
        row(2, 1, 2, 1),
        row(3, 3, 2, 1),
        row(3, 4, 2, 1),
        row(4, 6, 2, 1),
        row(4, 9, 2, 1),
        row(5, 13, 2, 1),
        row(5, 14, 2, 1),
        row(3, 5, 3, 1),
        row(5, 11, 3, 1),
        row(5, 12, 3, 1)
      )
    )

    /**
      * sum over range ubounded preceding and current row is not supported right now
      */
    // last_value, first_value
    checkResult(
      "SELECT sh, SUM(sh) over (partition by sh order by sh) FROM ShuflledTable5",
      Seq(
        row(1, 5),
        row(1, 5),
        row(1, 5),
        row(1, 5),
        row(1, 5),
        row(2, 14),
        row(2, 14),
        row(2, 14),
        row(2, 14),
        row(2, 14),
        row(2, 14),
        row(2, 14),
        row(3, 9),
        row(3, 9),
        row(3, 9)))

  }

  @Test
  def testSingleRowOverWindowFrameAggregate(): Unit = {
    checkResult(
      "SELECT sh, count(*) " +
          "over (partition by sh order by sh rows between 0 PRECEDING and 0 FOLLOWING) " +
          "FROM ShuflledTable5",
      Seq(
        row(1, 1),
        row(1, 1),
        row(1, 1),
        row(1, 1),
        row(1, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(3, 1),
        row(3, 1),
        row(3, 1)
      )
    )

    checkResult(
      "SELECT sh, count(*) " +
          "over (partition by sh order by sh rows between 0 PRECEDING and CURRENT ROW) " +
          "FROM ShuflledTable5",
      Seq(
        row(1, 1),
        row(1, 1),
        row(1, 1),
        row(1, 1),
        row(1, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(3, 1),
        row(3, 1),
        row(3, 1)
      )
    )

    checkResult(
      "SELECT sh, count(*) " +
          "over (partition by sh order by sh rows between CURRENT ROW and 0 FOLLOWING) " +
          "FROM ShuflledTable5",
      Seq(
        row(1, 1),
        row(1, 1),
        row(1, 1),
        row(1, 1),
        row(1, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(3, 1),
        row(3, 1),
        row(3, 1)
      )
    )

    checkResult(
      "SELECT sh, count(*) " +
          "over (partition by sh order by sh rows between CURRENT ROW and CURRENT ROW) " +
          "FROM ShuflledTable5",
      Seq(
        row(1, 1),
        row(1, 1),
        row(1, 1),
        row(1, 1),
        row(1, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(2, 1),
        row(3, 1),
        row(3, 1),
        row(3, 1)
      )
    )
  }

  @Test
  def testWindowAggregationNormalWindowAgg(): Unit = {
    /**
      * sum,count,max,min
      * over range ubounded preceding and current row is not supported right now
      * row_number
      * over rows ubounded preceding and current row is supported right now
      */
    checkResult(
      "SELECT d, e, row_number() over (partition by d order by e desc), " +
          "rank() over (partition by d order by e desc)," +
          "dense_rank() over (partition by d order by e desc), " +
          "sum(e) over (partition by d order by e desc)," +
          "count(*) over (partition by d order by e desc)," +
          "avg(e) over (partition by d order by e desc)," +
          "max(e) over (partition by d order by e)," +
          "max(e) over (partition by d order by e desc)," +
          "min(e) over (partition by d order by e)," +
          "min(e) over (partition by d order by e desc) FROM Table5",
      Seq(
        // d  e  r  r  d  s  c  a  ma m mi m
        row( 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),

        row( 2, 2, 2, 2, 2, 5, 2, 2, 2, 3, 2, 2),
        row( 2, 3, 1, 1, 1, 3, 1, 3, 3, 3, 2, 3),

        row( 3, 4, 3, 3, 3,15, 3, 5, 4, 6, 4, 4),
        row( 3, 5, 2, 2, 2,11, 2, 5, 5, 6, 4, 5),
        row( 3, 6, 1, 1, 1, 6, 1, 6, 6, 6, 4, 6),

        row( 4, 7, 4, 4, 4,34, 4, 8, 7,10, 7, 7),
        row( 4, 8, 3, 3, 3,27, 3, 9, 8,10, 7, 8),
        row( 4, 9, 2, 2, 2,19, 2, 9, 9,10, 7, 9),
        row( 4,10, 1, 1, 1,10, 1,10,10,10, 7,10),

        row( 5,11, 5, 5, 5,65, 5,13,11,15,11,11),
        row( 5,12, 4, 4, 4,54, 4,13,12,15,11,12),
        row( 5,13, 3, 3, 3,42, 3,14,13,15,11,13),
        row( 5,14, 2, 2, 2,29, 2,14,14,15,11,14),
        row( 5,15, 1, 1, 1,15, 1,15,15,15,11,15)
      ))

    checkResult(
      "SELECT d, g, rank() over (partition by d order by g ) FROM Table5",
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
  def testWindowAggregationRank2(): Unit = {

    checkResult(
      "SELECT d, e, rank() over (order by e desc), dense_rank() over (order by e desc) FROM Table5",
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
  def testRankByDecimal(): Unit = {
    checkResult(
      "SELECT d, de, rank() over (order by de desc), dense_rank() over (order by de desc) FROM" +
          " (select d, cast(e as decimal(10, 0)) as de from Table5)",
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
  def testWindowAggregationRank3(): Unit = {

    checkResult(
      "SELECT d, rank() over (order by d) FROM Table5",
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

    // deal with input with 0 as the first row's rank field
    checkResult(
      "SELECT f, rank() over (order by f) FROM Table5",
      Seq(
        row(0, 1),
        row(1, 2),
        row(2, 3),
        row(3, 4),
        row(4, 5),
        row(5, 6),
        row(6, 7),
        row(7, 8),
        row(8, 9),
        row(9, 10),
        row(10, 11),
        row(11, 12),
        row(12, 13),
        row(13, 14),
        row(14, 15)
      )
    )
  }

  @Test
  def testWindowAggregationNormalDenseRank(): Unit = {

    checkResult(
      "SELECT d, dense_rank() over (order by d) FROM Table5",
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

    // deal with input with 0 as the first row's rank field
    checkResult(
      "SELECT f, dense_rank() over (order by f) FROM Table5",
      Seq(
        row(0, 1),
        row(1, 2),
        row(2, 3),
        row(3, 4),
        row(4, 5),
        row(5, 6),
        row(6, 7),
        row(7, 8),
        row(8, 9),
        row(9, 10),
        row(10, 11),
        row(11, 12),
        row(12, 13),
        row(13, 14),
        row(14, 15)
      )
    )
  }

  @Test
  def testWindowAggregationNormalRowNumber(): Unit = {

    checkResult(
      "SELECT d, row_number() over (order by d) FROM Table5",
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

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e desc) FROM Table5",
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
    /**
      * sum over range unbounded preceding and current row is not supported
      */
    checkResult(
      "SELECT d, sum(e) over (partition by d) FROM Table5",
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

    checkResult(
      "SELECT d, sum(e) over () FROM Table5",
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

    checkResult(
      "SELECT d, e, sum(e) over (order by e) FROM Table5",
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

    checkResult(
      "SELECT d, e, count(*) over (partition by d order by e desc) FROM Table5",
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
    // count range unbounded to unbounded
    checkResult(
      "SELECT d, count(*) over (partition by d) FROM Table5",
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

    checkResult(
      "SELECT d, e, avg(e) over (partition by d order by e desc) FROM Table5",
      Seq(
        row(1, 1, 1),
        row(2, 3, 3),
        row(2, 2, 2),
        row(3, 6, 6),
        row(3, 5, 5),
        row(3, 4, 5),
        row(4, 10, 10),
        row(4, 9, 9),
        row(4, 8, 9),
        row(4, 7, 8),
        row(5, 15, 15),
        row(5, 14, 14),
        row(5, 13, 14),
        row(5, 12, 13),
        row(5, 11, 13)
      )
    )

    // order by string type field
    checkResult(
      "SELECT d, g, count(e) over (partition by d order by g desc rows between UNBOUNDED " +
          "PRECEDING and CURRENT ROW) FROM Table5",
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

    // order by string type field
    checkResult(
      "SELECT d, g, count(e) over (partition by d order by g rows between UNBOUNDED PRECEDING and" +
          " CURRENT ROW) FROM Table5",
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

    checkResult(
      "SELECT a, c, count(e) over (partition by a order by c, d rows between UNBOUNDED PRECEDING " +
          "and CURRENT ROW) FROM Table6",
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
  def testWindowAggregationAvgWithoutOrderBy(): Unit = {

    checkResult(
      "SELECT d, avg(e) over (partition by d) FROM Table5",
      Seq(
        row(1, 1),
        row(2, 2),
        row(2, 2),
        row(3, 5),
        row(3, 5),
        row(3, 5),
        row(4, 8),
        row(4, 8),
        row(4, 8),
        row(4, 8),
        row(5, 13),
        row(5, 13),
        row(5, 13),
        row(5, 13),
        row(5, 13)
      )
    )
  }

  @Test
  def testWindowAggregationSumWithOrderByWithRowsBetween_1(): Unit = {

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e desc " +
          "rows between UNBOUNDED PRECEDING and CURRENT ROW) FROM Table5",
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

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e desc " +
          "rows between CURRENT ROW and UNBOUNDED FOLLOWING) FROM Table5",
      Seq(
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
    )

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e desc " +
          "rows between CURRENT ROW and 2147483648 FOLLOWING) FROM Table5",
      Seq(
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
    )
  }

  @Test
  def testWindowAggregationSumWithOrderByWithRowsBetween_2(): Unit = {

    checkResult("SELECT d, e, f, " +
        "sum(e) over (partition by d order by e rows between 5 PRECEDING and 2 FOLLOWING)," +
        "count(*) over (partition by d order by e desc rows between 6 PRECEDING and 2 FOLLOWING)," +
        "max(f) over (partition by d order by e rows between UNBOUNDED PRECEDING and CURRENT ROW),"
        + "min(h) over (partition by d order by e desc rows between CURRENT ROW and " +
        "UNBOUNDED FOLLOWING),h FROM Table5",
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
  def testWindowAggregationSumWithOrderByWithRangeBetween(): Unit = {

    checkResult(
      "SELECT d, e," +
        "sum(e) over (partition by d order by e desc rows between 10 PRECEDING and 1 FOLLOWING)," +
        "sum(e) over (partition by d order by e desc rows between 2 PRECEDING and 3 FOLLOWING)," +
        "sum(e) over (partition by d order by e desc range between UNBOUNDED PRECEDING " +
          "and CURRENT ROW), " +
        "sum(e) over (partition by d order by e desc range between CURRENT ROW and UNBOUNDED " +
          "FOLLOWING)," +
        "sum(e) over (partition by d order by e desc range between 1 PRECEDING and 2 FOLLOWING), " +
        "sum(e) over (partition by d order by e range between 3 PRECEDING and 3 FOLLOWING), f " +
      "FROM Table5",
      Seq(
        row(1, 1, 1, 1, 1, 1, 1, 1, 0),

        row(2, 3, 5, 5, 3, 5, 5, 5, 2),
        row(2, 2, 5, 5, 5, 2, 5, 5, 1),

        row(3, 6, 11, 15, 6, 15, 15, 15, 5),
        row(3, 5, 15, 15, 11, 9, 15, 15, 4),
        row(3, 4, 15, 15, 15, 4, 9, 15, 3),

        row(4, 10, 19, 34, 10, 34, 27, 34, 9),
        row(4, 9, 27, 34, 19, 24, 34, 34, 8),
        row(4, 8, 34, 34, 27, 15, 24, 34, 7),
        row(4, 7, 34, 24, 34, 7, 15, 34, 6),

        row(5, 15, 29, 54, 15, 65, 42, 54, 14),
        row(5, 14, 42, 65, 29, 50, 54, 65, 13),
        row(5, 13, 54, 65, 42, 36, 50, 65, 12),
        row(5, 12, 65, 50, 54, 23, 36, 65, 11),
        row(5, 11, 65, 36, 65, 11, 23, 50, 10)
      )
    )
  }

  @Test
  def testWindowAggregationMaxWithOrderBy(): Unit = {

    checkResult(
      "SELECT d, e, max(e) over (partition by d order by e desc) FROM Table5",
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

    checkResult(
      "SELECT d, max(e) over (partition by d) FROM Table5",
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

    checkResult(
      "SELECT d, e, min(e) over (partition by d order by e desc) FROM Table5",
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

    checkResult(
      "SELECT d, min(e) over (partition by d) FROM Table5",
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

    checkResult(
      "SELECT d, h, rank() over (order by d, h desc) FROM Table5",
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

    checkResult(
      "SELECT d, h, dense_rank() over (order by d, h desc) FROM Table5",
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

    checkResult(
      "SELECT d, e, h, rank() over (order by d), " +
          "dense_rank() over (order by d), " +
          "row_number() over (order by d, e desc), " +
          "sum(e) over(partition by d order by h desc, e rows BETWEEN unbounded preceding AND 0 " +
          "FOLLOWING ) FROM Table5",
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

    checkResult(
      "SELECT d, e, h, rank() over (order by d nulls first), " +
          "dense_rank() over (order by d nulls first), " +
          "row_number() over (order by d nulls first, e desc, h desc), " +
          "sum(d) over(partition by d order by e), " +
          "min(d) over(partition by d order by e), " +
          "max(e) over(partition by d order by h desc, e rows BETWEEN unbounded preceding AND " +
          "CURRENT ROW) FROM NullTable5",
      Seq(
        //   d      e    h   rank   drank r_n sumd     min   sume
        row(null, 999, 999, 1, 1, 2, null, null, 999),
        row(null, 999, 999, 1, 1, 1, null, null, 999),

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

    checkResult(
      "SELECT a,b,c,d,e,f,rank() over (partition by a order by d), " +
          "dense_rank() over (partition by a order by e desc), " +
          "row_number() over (partition by a order by f), " +
          "max(b) over(partition by a order by c rows BETWEEN unbounded preceding AND 0 " +
          "FOLLOWING) FROM Table6",
      Seq(
        //  a  b      c       d            e             f
        row(1, 1.1, "a", localDate("2017-04-08"), localTime("12:00:59"),
          localDateTime("2015-05-20 10:00:00"), 1, 1, 1, 1.1),

        row(2, 2.5, "abc", localDate("2017-04-09"), localTime("12:00:59"),
          localDateTime("2019-09-19 08:03:09"), 2, 1, 2, 2.5),
        row(2, -2.4, "abcd", localDate("2017-04-08"), localTime("00:00:00"),
          localDateTime("2016-09-01 23:07:06"), 1, 2, 1, 2.5),

        row(3, -9.77, "ABC", localDate("2016-08-08"), localTime("04:15:00"),
          localDateTime("1999-12-12 10:00:02"), 1, 2, 2, -9.77),
        row(3, 0.08, "BCD", localDate("2017-04-10"), localTime("02:30:00"),
          localDateTime("1999-12-12 10:03:00"), 2, 3, 3, 0.08),
        row(3, 0.0, "abc?", localDate("2017-10-11"), localTime("23:59:59"),
          localDateTime("1999-12-12 10:00:00"), 3, 1, 1, 0.08),

        row(4, 3.14, "CDE", localDate("2017-11-11"), localTime("02:30:00"),
          localDateTime("2017-11-20 09:00:00"), 4, 4, 4, 3.14),
        row(4, 3.15, "DEF", localDate("2017-02-06"), localTime("06:00:00"),
          localDateTime("2015-11-19 10:00:00"), 1, 3, 1, 3.15),
        row(4, 3.14, "EFG", localDate("2017-05-20"), localTime("09:46:18"),
          localDateTime("2015-11-19 10:00:01"), 3, 2, 2, 3.15),
        row(4, 3.16, "FGH", localDate("2017-05-19"), localTime("11:11:11"),
          localDateTime("2015-11-20 08:59:59"), 2, 1, 3, 3.16),

        row(5, -5.9, "GHI", localDate("2017-07-20"), localTime("22:22:22"),
          localDateTime("1989-06-04 10:00:00.78"), 3, 1, 2, -5.9),
        row(5, 2.71, "HIJ", localDate("2017-09-08"), localTime("20:09:09"),
          localDateTime("1997-07-01 09:00:00.99"), 4, 2, 3, 2.71),
        row(5, 3.9, "IJK", localDate("2017-02-02"), localTime("03:03:03"),
          localDateTime("2000-01-01 00:00:00.09"), 1, 5, 4, 3.9),
        row(5, 0.7, "JKL", localDate("2017-10-01"), localTime("19:00:00"),
          localDateTime("2010-06-01 10:00:00.999"), 5, 3, 5, 3.9),
        row(5, -2.8, "KLM", localDate("2017-07-01"), localTime("12:00:59"),
          localDateTime("1937-07-07 08:08:08.888"), 2, 4, 1, 3.9)
      )
    )
  }

  @Test
  def testTopN(): Unit = {
    val data = Seq(
      row("book", 1, 12),
      row("book", 2, 19),
      row("book", 4, 11),
      row("fruit", 4, 33),
      row("fruit", 3, 44),
      row("fruit", 5, 22))
    registerCollection("T", data, new RowTypeInfo(STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO),
      "category, shopId, num")
    checkResult(
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num,
        |      ROW_NUMBER() OVER (PARTITION BY category ORDER BY num DESC) as rank_num
        |  FROM T)
        |WHERE rank_num <= 2
      """.stripMargin,
      Seq(
        row("book", 2, 19, 1),
        row("book", 1, 12, 2),
        row("fruit", 3, 44, 1),
        row("fruit", 4, 33, 2)
      ))
  }

  @Test
  def testAvg(): Unit = {
    val data = Seq(
      row("book", 1, 12.0D),
      row("book", 2, 19.0D),
      row("book", 4, 11.0D),
      row("fruit", 4, 33.0D),
      row("fruit", 3, 44.0D),
      row("fruit", 5, 22.0D))
    registerCollection("T",
      data, new RowTypeInfo(STRING_TYPE_INFO, INT_TYPE_INFO, DOUBLE_TYPE_INFO),
      "category, shopId, num")
    checkResult(
      """
        |SELECT category, num,
        | avg(num)
        | OVER (PARTITION BY category ORDER BY num
        |   rows BETWEEN unbounded preceding AND 1 FOLLOWING)
        |FROM T
      """.stripMargin,
      Seq(
        row("book", 11.0, 11.5),
        row("book", 12.0, 14.0),
        row("book", 19.0, 14.0),
        row("fruit", 22.0, 27.5),
        row("fruit", 33.0, 33.0),
        row("fruit", 44.0, 33.0)
      ))
  }

  @Test
  def testRangeFrame(): Unit = {
    val data = Seq(
      row("book", 1, 12),
      row("book", 2, 19),
      row("book", 4, 11),
      row("fruit", 4, 33),
      row("fruit", 3, 44),
      row("fruit", 5, 22))
    registerCollection("T", data, new RowTypeInfo(STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO),
      "category, shopId, num")
    // sliding frame case: 1 - 1
    checkResult(
      """
        |SELECT category, num,
        | sum(num)
        | OVER (PARTITION BY category ORDER BY num RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
        |FROM T
      """.stripMargin,
      Seq(
        row("book", 11, 23),
        row("book", 12, 23),
        row("book", 19, 19),
        row("fruit", 22, 22),
        row("fruit", 33, 33),
        row("fruit", 44, 44)
      ))
    // sliding frame case: unbounded - 1
    checkResult(
      """
        |SELECT category, num,
        | sum(num)
        | OVER (PARTITION BY category ORDER BY num
        | RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)
        |FROM T
      """.stripMargin,
      Seq(
        row("book", 11, 23),
        row("book", 12, 23),
        row("book", 19, 42),
        row("fruit", 22, 22),
        row("fruit", 33, 55),
        row("fruit", 44, 99)
      ))
    // sliding frame case: 1 - unbounded
    checkResult(
      """
        |SELECT category, num,
        | sum(num)
        | OVER
        | (PARTITION BY category ORDER BY num RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)
        |FROM T
      """.stripMargin,
      Seq(
        row("book", 11, 42),
        row("book", 12, 42),
        row("book", 19, 19),
        row("fruit", 22, 99),
        row("fruit", 33, 77),
        row("fruit", 44, 44)
      ))
  }

  @Test
  def testRowsFrame(): Unit = {
    val data = Seq(
      row("book", 1, 12),
      row("book", 2, 19),
      row("book", 4, 11),
      row("fruit", 4, 33),
      row("fruit", 3, 44),
      row("fruit", 5, 22))
    registerCollection("T", data, new RowTypeInfo(STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO),
      "category, shopId, num")
    // sliding frame case: unbounded - 1
    checkResult(
      """
        |SELECT category, num,
        | sum(num)
        | OVER
        | (PARTITION BY category ORDER BY num ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)
        |FROM T
      """.stripMargin,
      Seq(
        row("book", 11, 23),
        row("book", 12, 42),
        row("book", 19, 42),
        row("fruit", 22, 55),
        row("fruit", 33, 99),
        row("fruit", 44, 99)
      ))
    // sliding frame case: 1 - unbounded
    checkResult(
      """
        |SELECT category, num,
        | sum(num)
        | OVER
        | (PARTITION BY category ORDER BY num ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)
        |FROM T
      """.stripMargin,
      Seq(
        row("book", 11, 42),
        row("book", 12, 42),
        row("book", 19, 31),
        row("fruit", 22, 99),
        row("fruit", 33, 99),
        row("fruit", 44, 77)
      ))
  }

  @Test
  def testRangeFrameWithNullValue(): Unit = {
    val data = Seq(
      row("book", 1, 12),
      row("book", 2, null),
      row("book", 4, 11),
      row("fruit", 4, null),
      row("fruit", 3, 44),
      row("fruit", 5, null))
    registerCollection("T", data, new RowTypeInfo(STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO),
      "category, shopId, num")
    checkResult(
      """
        |SELECT category, num,
        | sum(num)
        | OVER (PARTITION BY category ORDER BY num DESC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
        |FROM T
      """.stripMargin,
      Seq(
        row("book", 11, 23),
        row("book", 12, 23),
        row("book", null, null),
        row("fruit", 44, 44),
        row("fruit", null, null),
        row("fruit", null, null)
      ))

    checkResult(
      """
        |SELECT category, num,
        |sum(num)
        |OVER (PARTITION BY category ORDER BY num ASC RANGE BETWEEN 1.3 PRECEDING AND 1.2 FOLLOWING)
        |FROM T
      """.stripMargin,
      Seq(
        row("book", 11, 23),
        row("book", 12, 23),
        row("book", null, null),
        row("fruit", 44, 44),
        row("fruit", null, null),
        row("fruit", null, null)
      ))
  }

  @Test
  def testSumWithFloatValue(): Unit = {
    val data = Seq(
      row("book", 1f, 12),
      row("book", 2f, null),
      row("book", 4f, 11),
      row("fruit", 4f, null),
      row("fruit", 3f, 44),
      row("fruit", 5f, null))
    registerCollection("T", data, new RowTypeInfo(STRING_TYPE_INFO, FLOAT_TYPE_INFO, INT_TYPE_INFO),
      "category, money, num")
    checkResult(
      """
        |SELECT category, money,
        | sum(money)
        | OVER (PARTITION BY category)
        |FROM T
      """.stripMargin,
      Seq(
        row("book", 1.0, 7.0),
        row("book", 2.0, 7.0),
        row("book", 4.0, 7.0),
        row("fruit", 3.0, 12.0),
        row("fruit", 4.0, 12.0),
        row("fruit", 5.0, 12.0)
      ))
  }

  @Test
  def testWindowAggWithConstants(): Unit = {

    checkResult(
      "SELECT COUNT(cast(null as int)) OVER () FROM Table5",
      Seq(
        row(0),
        row(0),
        row(0),
        row(0),
        row(0),
        row(0),
        row(0),
        row(0),
        row(0),
        row(0),
        row(0),
        row(0),
        row(0),
        row(0),
        row(0)))

    checkResult(
      "SELECT COUNT(1) OVER () FROM Table5",
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
        row(15)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e range between -1 FOLLOWING and 10 " +
          "FOLLOWING)" +
          ", count(1) over (partition by d order by e range between -1 FOLLOWING and 10 " +
          "FOLLOWING)  FROM Table5",
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
        row(5, 15, 29, 2)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e desc range between -1 FOLLOWING and 10" +
          " FOLLOWING)" +
          ", count(1) over (partition by d order by e desc range between -1 FOLLOWING and 10 " +
          "FOLLOWING)  FROM Table5",
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
        row(5, 15, 65, 5)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e desc range between 1 FOLLOWING and 10 " +
          "FOLLOWING)" +
          ", count(1) over (partition by d order by e desc range between 1 FOLLOWING and 10 " +
          "FOLLOWING)  FROM Table5",
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
        row(5, 15, 50, 4)))
  }

  @Test
  def testWindowAggWithPreceding(): Unit = {
    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e desc range between 5 PRECEDING and 4 " +
          "PRECEDING) FROM Table5",
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
        row(5, 15, null)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e range between 5 PRECEDING and 4 " +
          "PRECEDING) FROM Table5",
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
        row(5, 15, 11)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e desc rows between 5 PRECEDING and 4 " +
          "PRECEDING) FROM Table5",
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
        row(5, 15, null)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e rows between 5 PRECEDING and 4 " +
          "PRECEDING) FROM Table5",
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
        row(5, 15, 11)))
  }

  @Test
  def testWindowAggWithFollowing(): Unit = {

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e desc range between 4 FOLLOWING and 5 " +
          "FOLLOWING) FROM Table5",
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
        row(5, 15, 11)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e range between 4 FOLLOWING and 5 " +
          "FOLLOWING) FROM Table5",
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
        row(5, 15, null)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e desc rows between 4 FOLLOWING and 5 " +
          "FOLLOWING) FROM Table5",
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
        row(5, 15, 11)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e rows between 4 FOLLOWING and 5 " +
          "FOLLOWING) FROM Table5",
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
        row(5, 15, null)))
  }

  @Test
  def testWindowAggWithNullAndUnbounded(): Unit = {

    val nullData: Seq[Row] = data5 ++ Seq(
      row(null, 3L, 3, "NullTuple", 3L),
      row(null, 3L, 3, "NullTuple", 3L)
    )
    registerCollection("NullTable", nullData, type5, "d, e, f, g, h", nullablesOfNullData5)

    checkResult(
      "SELECT h, d, count(*) over (partition by h order by d range between 0 PRECEDING and " +
          "UNBOUNDED FOLLOWING) FROM NullTable",
      Seq(
        row(1, 1, 5),
        row(1, 2, 4),
        row(1, 4, 3),
        row(1, 4, 3),
        row(1, 5, 1),
        row(2, 2, 7),
        row(2, 3, 6),
        row(2, 3, 6),
        row(2, 4, 4),
        row(2, 4, 4),
        row(2, 5, 2),
        row(2, 5, 2),
        row(3, null, 5),
        row(3, null, 5),
        row(3, 3, 3),
        row(3, 5, 2),
        row(3, 5, 2)))

    checkResult(
      "SELECT h, d, count(*) over (partition by h order by d desc range between 0 PRECEDING and " +
          "UNBOUNDED FOLLOWING) FROM NullTable",
      Seq(
        row(1, 5, 5),
        row(1, 4, 4),
        row(1, 4, 4),
        row(1, 2, 2),
        row(1, 1, 1),
        row(2, 5, 7),
        row(2, 5, 7),
        row(2, 4, 5),
        row(2, 4, 5),
        row(2, 3, 3),
        row(2, 3, 3),
        row(2, 2, 1),
        row(3, 5, 5),
        row(3, 5, 5),
        row(3, 3, 3),
        row(3, null, 2),
        row(3, null, 2)))

    checkResult(
      "SELECT h, d, count(*) over (partition by h order by d range between current row and " +
          "UNBOUNDED FOLLOWING) FROM NullTable",
      Seq(
        row(1, 1, 5),
        row(1, 2, 4),
        row(1, 4, 3),
        row(1, 4, 3),
        row(1, 5, 1),
        row(2, 2, 7),
        row(2, 3, 6),
        row(2, 3, 6),
        row(2, 4, 4),
        row(2, 4, 4),
        row(2, 5, 2),
        row(2, 5, 2),
        row(3, null, 5),
        row(3, null, 5),
        row(3, 3, 3),
        row(3, 5, 2),
        row(3, 5, 2)))

    checkResult(
      "SELECT h, d, count(*) over (partition by h order by d desc range between current row and " +
          "UNBOUNDED FOLLOWING) FROM NullTable",
      Seq(
        row(1, 5, 5),
        row(1, 4, 4),
        row(1, 4, 4),
        row(1, 2, 2),
        row(1, 1, 1),
        row(2, 5, 7),
        row(2, 5, 7),
        row(2, 4, 5),
        row(2, 4, 5),
        row(2, 3, 3),
        row(2, 3, 3),
        row(2, 2, 1),
        row(3, 5, 5),
        row(3, 5, 5),
        row(3, 3, 3),
        row(3, null, 2),
        row(3, null, 2)))
  }

  @Test
  def testUnboundedWindowAgg(): Unit = {
    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e desc range between Unbounded PRECEDING" +
          " and Unbounded FOLLOWING) FROM Table5",
      Seq(
        row(1, 1, 1),
        row(2, 2, 5),
        row(2, 3, 5),
        row(3, 4, 15),
        row(3, 5, 15),
        row(3, 6, 15),
        row(4, 10, 34),
        row(4, 7, 34),
        row(4, 8, 34),
        row(4, 9, 34),
        row(5, 11, 65),
        row(5, 12, 65),
        row(5, 13, 65),
        row(5, 14, 65),
        row(5, 15, 65)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e desc rows between Unbounded PRECEDING " +
          "and Unbounded FOLLOWING) FROM Table5",
      Seq(
        row(1, 1, 1),
        row(2, 2, 5),
        row(2, 3, 5),
        row(3, 4, 15),
        row(3, 5, 15),
        row(3, 6, 15),
        row(4, 10, 34),
        row(4, 7, 34),
        row(4, 8, 34),
        row(4, 9, 34),
        row(5, 11, 65),
        row(5, 12, 65),
        row(5, 13, 65),
        row(5, 14, 65),
        row(5, 15, 65)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e range between Unbounded PRECEDING and " +
          "2 FOLLOWING) FROM Table5",
      Seq(
        row(1, 1, 1),
        row(2, 2, 5),
        row(2, 3, 5),
        row(3, 4, 15),
        row(3, 5, 15),
        row(3, 6, 15),
        row(4, 10, 34),
        row(4, 7, 24),
        row(4, 8, 34),
        row(4, 9, 34),
        row(5, 11, 36),
        row(5, 12, 50),
        row(5, 13, 65),
        row(5, 14, 65),
        row(5, 15, 65)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e rows between Unbounded PRECEDING and 2" +
          " FOLLOWING) FROM Table5",
      Seq(
        row(1, 1, 1),
        row(2, 2, 5),
        row(2, 3, 5),
        row(3, 4, 15),
        row(3, 5, 15),
        row(3, 6, 15),
        row(4, 10, 34),
        row(4, 7, 24),
        row(4, 8, 34),
        row(4, 9, 34),
        row(5, 11, 36),
        row(5, 12, 50),
        row(5, 13, 65),
        row(5, 14, 65),
        row(5, 15, 65)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e range between 3 PRECEDING and " +
          "Unbounded FOLLOWING) FROM Table5",
      Seq(
        row(1, 1, 1),
        row(2, 2, 5),
        row(2, 3, 5),
        row(3, 4, 15),
        row(3, 5, 15),
        row(3, 6, 15),
        row(4, 10, 34),
        row(4, 7, 34),
        row(4, 8, 34),
        row(4, 9, 34),
        row(5, 11, 65),
        row(5, 12, 65),
        row(5, 13, 65),
        row(5, 14, 65),
        row(5, 15, 54)))

    checkResult(
      "SELECT d, e, sum(e) over (partition by d order by e rows between 3 PRECEDING and Unbounded" +
          " FOLLOWING) FROM Table5",
      Seq(
        row(1, 1, 1),
        row(2, 2, 5),
        row(2, 3, 5),
        row(3, 4, 15),
        row(3, 5, 15),
        row(3, 6, 15),
        row(4, 10, 34),
        row(4, 7, 34),
        row(4, 8, 34),
        row(4, 9, 34),
        row(5, 11, 65),
        row(5, 12, 65),
        row(5, 13, 65),
        row(5, 14, 65),
        row(5, 15, 54)))
  }

  @Test
  def testWindowAggWithNull(): Unit = {
    val nullData: Seq[Row] = data5 ++ Seq(
      row(null, 3L, 3, "NullTuple", 3L),
      row(null, 3L, 3, "NullTuple", 3L)
    )
    registerCollection("NullTable", nullData, type5, "d, e, f, g, h", nullablesOfNullData5)
    checkResult(
      "SELECT h, d, count(*) over (partition by h order by d range between 1 PRECEDING and 2 " +
          "FOLLOWING) FROM NullTable",
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
        row(3, null, 2)))

    checkResult(
      "SELECT h, d, count(*) over (partition by h order by d) FROM NullTable",
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
        row(3, null, 2)))

    checkResult(
      "SELECT h, d, count(*) over (partition by h order by d nulls first) FROM NullTable",
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
        row(3, null, 2)))


    checkResult(
      "SELECT h, d, rank() over (partition by h order by d) FROM NullTable",
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
        row(3, null, 1)))

    checkResult(
      "SELECT h, d, rank() over (partition by h order by d nulls first) FROM NullTable",
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
        row(3, null, 1)))

    checkResult(
      "SELECT h, d, count(*) over (partition by h order by d desc range between 1 PRECEDING and 2" +
          " FOLLOWING) FROM NullTable",
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
        row(3, null, 2)))

    checkResult(
      "SELECT h, d, count(*) over (partition by h order by d desc nulls first range between 1 " +
          "PRECEDING and 2 FOLLOWING) FROM NullTable",
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
        row(3, null, 2)))

    checkResult(
      "SELECT h, d, count(*) over (partition by h order by d range between 1 FOLLOWING and 2 " +
          "FOLLOWING) FROM NullTable",
      Seq(
        row(1, 1, 1),
        row(1, 2, 2),
        row(1, 4, 1),
        row(1, 4, 1),
        row(1, 5, 0),
        row(2, 2, 4),
        row(2, 3, 4),
        row(2, 3, 4),
        row(2, 4, 2),
        row(2, 4, 2),
        row(2, 5, 0),
        row(2, 5, 0),
        row(3, 3, 2),
        row(3, 5, 0),
        row(3, 5, 0),
        row(3, null, 0),
        row(3, null, 0)))

    checkResult(
      "SELECT h, d, count(*) over (partition by h order by d desc range between 1 FOLLOWING and 2" +
          " FOLLOWING) FROM NullTable",
      Seq(
        row(1, 1, 0),
        row(1, 2, 1),
        row(1, 4, 1),
        row(1, 4, 1),
        row(1, 5, 2),
        row(2, 2, 0),
        row(2, 3, 1),
        row(2, 3, 1),
        row(2, 4, 3),
        row(2, 4, 3),
        row(2, 5, 4),
        row(2, 5, 4),
        row(3, 3, 0),
        row(3, 5, 1),
        row(3, 5, 1),
        row(3, null, 0),
        row(3, null, 0)))
  }

  @Test
  def testWindowAggregationAtDate(): Unit = {

    checkResult(
      "SELECT a,d, count(*) over (partition by a order by d RANGE between INTERVAL '0' DAY " +
          "FOLLOWING and INTERVAL '2' DAY FOLLOWING) FROM Table6",
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

    checkResult(
      "SELECT a, count(*) over (partition by a order by e RANGE between INTERVAL '0' HOUR " +
          "FOLLOWING and INTERVAL '2' HOUR FOLLOWING) FROM Table6",
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
  def testFractionalWindowAggregation(): Unit = {

    checkResult(
      "SELECT count(*) over (partition by a order by b RANGE between " +
          "CURRENT ROW and 3.1 FOLLOWING) FROM Table6",
      Seq(
        row(1),
        row(1),
        row(1),
        row(1),
        row(1),
        row(1),
        row(1),
        row(1),
        row(1),
        row(2),
        row(2),
        row(2),
        row(2),
        row(4),
        row(4)
      )
    )
  }

  @Test
  def testOverWindowBasedStringOrderBy(): Unit = {
    checkResult(
      "SELECT a, c, count(*) over (partition by a order by c) FROM Table6",
      Seq(
        row(1, "a", 1),
        row(5, "GHI", 1),
        row(5, "HIJ", 2),
        row(5, "IJK", 3),
        row(5, "JKL", 4),
        row(5, "KLM", 5),
        row(4, "CDE", 1),
        row(4, "DEF", 2),
        row(4, "EFG", 3),
        row(4, "FGH", 4),
        row(2, "abc", 1),
        row(2, "abcd", 2),
        row(3, "ABC", 1),
        row(3, "BCD", 2),
        row(3, "abc?", 3)))

    checkResult(
      "SELECT a, c, count(*) over (partition by a order by c desc) FROM Table6",
      Seq(
        row(1, "a", 1),
        row(5, "GHI", 5),
        row(5, "HIJ", 4),
        row(5, "IJK", 3),
        row(5, "JKL", 2),
        row(5, "KLM", 1),
        row(4, "CDE", 4),
        row(4, "DEF", 3),
        row(4, "EFG", 2),
        row(4, "FGH", 1),
        row(2, "abc", 2),
        row(2, "abcd", 1),
        row(3, "ABC", 3),
        row(3, "BCD", 2),
        row(3, "abc?", 1)))
  }

  @Test
  def testOverWindowBasedCompositeOrderBy(): Unit = {
    checkResult(
      "SELECT a, b, c, count(*) over (partition by a order by b, c) FROM Table6",
      Seq(
        row(1, 1.1, "a", 1),
        row(5, -5.9, "GHI", 1),
        row(5, -2.8, "KLM", 2),
        row(5, 0.7, "JKL", 3),
        row(5, 2.71, "HIJ", 4),
        row(5, 3.9, "IJK", 5),
        row(4, 3.14, "CDE", 1),
        row(4, 3.14, "EFG", 2),
        row(4, 3.15, "DEF", 3),
        row(4, 3.16, "FGH", 4),
        row(2, -2.4, "abcd", 1),
        row(2, 2.5, "abc", 2),
        row(3, -9.77, "ABC", 1),
        row(3, 0.0, "abc?", 2),
        row(3, 0.08, "BCD", 3)))
  }

  @Test
  def testTopNBasedShort(): Unit = {
    val data = Seq(
      row("book", 1, 1.asInstanceOf[Short]),
      row("book", 2, 3.asInstanceOf[Short]))
    registerCollection("T", data, new RowTypeInfo(STRING_TYPE_INFO, INT_TYPE_INFO, SHORT_TYPE_INFO),
      "category, shopId, num")
    checkResult(
      """
        |SELECT *
        |FROM (
        |  SELECT category, shopId, num,
        |      rank() OVER (PARTITION BY category ORDER BY num DESC) as rank_num
        |  FROM T)
        |WHERE rank_num <= 2
      """.stripMargin,
      Seq(
        row("book", 1, 1, 2),
        row("book", 2, 3, 1)))
  }

  @Test
  def testLeadFunc(): Unit = {

    checkResult(
      "SELECT a, c, lag(c) over (partition by a order by c) FROM Table6",
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
        row(3, "abc?", "BCD")))

    checkResult(
      "SELECT a, b, lead(b, 2, 3) over (partition by a order by b), lag(b, 1, 3) over (partition " +
          "by a order by b) FROM Table6",
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
        row(3, 0.08, 3.0, 0.0)))

    checkResult(
      "SELECT a, b, lead(b, 2, 3) over (partition by a order by b) FROM Table6",
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
        row(3, 0.08, 3.0)))

    checkResult(
      "SELECT a, b, lead(b, -2, 3) over (partition by a order by b) FROM Table6",
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
        row(3, 0.08, -9.77)))

      checkResult(
        "SELECT a, b, lead(b, -2, 3.0) over (partition by a order by b) FROM Table6",
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
          row(3, 0.08, -9.77)))

    checkResult(
      "SELECT a-2, b, lead(b, a-2, 3.0) over (partition by a order by b) FROM Table6",
      Seq(
        row(-1, 1.1, 3.0),
        row(3, -5.9, 2.71),
        row(3, -2.8, 3.9),
        row(3, 0.7, 3.0),
        row(3, 2.71, 3.0),
        row(3, 3.9, 3.0),
        row(2, 3.14, 3.15),
        row(2, 3.14, 3.16),
        row(2, 3.15, 3.0),
        row(2, 3.16, 3.0),
        row(0, -2.4, -2.4),
        row(0, 2.5, 2.5),
        row(1, -9.77, 0.0),
        row(1, 0.0, 0.08),
        row(1, 0.08, 3.0)))
  }

  @Test
  def testLagFunc(): Unit = {
    checkResult(
      "SELECT a, b, lag(b, 2, 3) over (partition by a order by b) FROM Table6",
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
        row(3, 0.08, -9.77)))

    checkResult(
      "SELECT a, b, lag(b, -2, 3) over (partition by a order by b) FROM Table6",
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
        row(3, 0.08, 3.0)))

    checkResult(
      "SELECT a, b, lag(b, -2, b) over (partition by a order by b) FROM Table6",
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
        row(3, 0.08, 0.08)))

    checkResult(
      "SELECT a, b, lag(b, a, 3) over (partition by a order by b) FROM Table6",
      Seq(
        row(1, 1.1, 3.0),
        row(5, -5.9, 3.0),
        row(5, -2.8, 3.0),
        row(5, 0.7, 3.0),
        row(5, 2.71, 3.0),
        row(5, 3.9, 3.0),
        row(4, 3.14, 3.0),
        row(4, 3.14, 3.0),
        row(4, 3.15, 3.0),
        row(4, 3.16, 3.0),
        row(2, -2.4, 3.0),
        row(2, 2.5, 3.0),
        row(3, -9.77, 3.0),
        row(3, 0.0, 3.0),
        row(3, 0.08, 3.0)))

    checkResult(
      "SELECT a-1, b, lag(b, a-1, 3) over (partition by a order by b) FROM Table6",
      Seq(
        row(0, 1.1, 1.1),
        row(4, -5.9, 3.0),
        row(4, -2.8, 3.0),
        row(4, 0.7, 3.0),
        row(4, 2.71, 3.0),
        row(4, 3.9, -5.9),
        row(3, 3.14, 3.0),
        row(3, 3.14, 3.0),
        row(3, 3.15, 3.0),
        row(3, 3.16, 3.14),
        row(1, -2.4, 3.0),
        row(1, 2.5, -2.4),
        row(2, -9.77, 3.0),
        row(2, 0.0, 3.0),
        row(2, 0.08, -9.77)))
  }

  @Test
  def testMultiOverWindowRangeType(): Unit = {
    val sqlQuery = "SELECT a, c, count(*) over (partition by a, c)," +
        "rank() over (partition by a order by c) FROM" +
        " Table6 WHERE a = 5"
    checkResult(
      sqlQuery,
      Seq(
        row(5, "GHI", 1, 1),
        row(5, "HIJ", 1, 2),
        row(5, "IJK", 1, 3),
        row(5, "JKL", 1, 4),
        row(5, "KLM", 1, 5)))
  }

  @Test
  def testAggWithLiteral(): Unit = {
    val sqlQuery =
      """
        |SELECT d, e,
        |sum(2) OVER (),
        |avg(1) OVER (),
        |count(1) OVER (),
        |min(1) OVER (),
        |max(1) OVER (),
        |first_value(true) OVER (),
        |first_value(CAST(NULL AS BOOLEAN)) OVER (),
        |last_value(false) OVER (),
        |last_value(CAST(NULL AS BOOLEAN)) OVER ()
        |FROM Table5
      """.stripMargin
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1L, 30, 1, 15, 1, 1, true, null, false, null),
        row(2, 2L, 30, 1, 15, 1, 1, true, null, false, null),
        row(2, 3L, 30, 1, 15, 1, 1, true, null, false, null),
        row(3, 4L, 30, 1, 15, 1, 1, true, null, false, null),
        row(3, 5L, 30, 1, 15, 1, 1, true, null, false, null),
        row(3, 6L, 30, 1, 15, 1, 1, true, null, false, null),
        row(4, 7L, 30, 1, 15, 1, 1, true, null, false, null),
        row(4, 8L, 30, 1, 15, 1, 1, true, null, false, null),
        row(4, 9L, 30, 1, 15, 1, 1, true, null, false, null),
        row(4, 10L, 30, 1, 15, 1, 1, true, null, false, null),
        row(5, 11L, 30, 1, 15, 1, 1, true, null, false, null),
        row(5, 12L, 30, 1, 15, 1, 1, true, null, false, null),
        row(5, 13L, 30, 1, 15, 1, 1, true, null, false, null),
        row(5, 14L, 30, 1, 15, 1, 1, true, null, false, null),
        row(5, 15L, 30, 1, 15, 1, 1, true, null, false, null)))
  }
}

/** The initial accumulator for count aggregate function */
class CountAccumulator extends JTuple1[Long] {
  f0 = 0L //count
}

class CountAggFunction extends AggregateFunction[JLong, CountAccumulator] {

  def accumulate(acc: CountAccumulator, value: Any): Unit = {
    if (value != null) {
      acc.f0 += 1L
    }
  }

  def accumulate(acc: CountAccumulator): Unit = {
    acc.f0 += 1L
  }

  def retract(acc: CountAccumulator, value: Any): Unit = {
    if (value != null) {
      acc.f0 -= 1L
    }
  }

  def retract(acc: CountAccumulator): Unit = {
    acc.f0 -= 1L
  }

  override def getValue(acc: CountAccumulator): JLong = {
    acc.f0
  }

  def merge(acc: CountAccumulator, its: JIterable[CountAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      acc.f0 += iter.next().f0
    }
  }

  override def createAccumulator(): CountAccumulator = {
    new CountAccumulator
  }

  def resetAccumulator(acc: CountAccumulator): Unit = {
    acc.f0 = 0L
  }

  override def getAccumulatorType: TypeInformation[CountAccumulator] = {
    new TupleTypeInfo(classOf[CountAccumulator], Types.LONG)
  }

  override def getResultType: TypeInformation[JLong] = Types.LONG
}
