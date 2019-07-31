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

package org.apache.flink.table.planner.plan.batch.table.stringexpr

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.Types._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.CollectionBatchExecTable.CustomType
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit._

import java.sql.{Date, Time, Timestamp}

class CalcStringExpressionTest extends TableTestBase {

  @Test
  def testSimpleSelectAllWithAs(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = t.select('a, 'b, 'c)
    val t2 = t.select("a, b, c")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testSimpleSelectWithNaming(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3")

    val t1 = t
      .select('_1 as 'a, '_2 as 'b, '_1 as 'c)
      .select('a, 'b)

    val t2 = t
      .select("_1 as a, _2 as b, _1 as c")
      .select("a, b")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testSimpleSelectRenameAll(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3")

    val t1 = t
      .select('_1 as 'a, '_2 as 'b, '_3 as 'c)
      .select('a, 'b)

    val t2 = t
      .select("_1 as a, _2 as b, _3 as c")
      .select("a, b")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testSelectStar(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = t.select('*)
    val t2 = t.select("*")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter(false)
    val t2 = ds.filter("faLsE")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testAllPassingFilter(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter(true)
    val t2 = ds.filter("trUe")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testFilterOnStringTupleField(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter( 'c.like("%world%") )
    val t2 = ds.filter("c.like('%world%')")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testFilterOnIntegerTupleField(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter( 'a % 2 === 0 )
    val t2 = ds.filter( "a % 2 = 0 ")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testNotEquals(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter( 'a % 2 !== 0 )
    val t2 = ds.filter("a % 2 <> 0")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testDisjunctivePredicate(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter( 'a < 2 || 'a > 20)
    val t2 = ds.filter("a < 2 || a > 20")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testConsecutiveFilters(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter('a % 2 !== 0).filter('b % 2 === 0)
    val t2 = ds.filter("a % 2 != 0").filter("b % 2 = 0")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testFilterBasicType(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTableSource[String]("Table3",'a)

    val t1 = ds.filter( 'a.like("H%") )
    val t2 = ds.filter( "a.like('H%')" )

    verifyTableEquals(t1, t2)
  }

  @Test
  def testFilterOnCustomType(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[CustomType]("Table3",'myInt, 'myLong, 'myString)
      .as('i, 'l, 's)

    val t1 = t.filter( 's.like("%a%") )
    val t2 = t.filter("s.like('%a%')")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testSimpleCalc(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3")

    val t1 = t.select('_1, '_2, '_3)
      .where('_1 < 7)
      .select('_1, '_3)

    val t2 = t.select("_1, _2, _3")
      .where("_1 < 7")
      .select("_1, _3")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testCalcWithTwoFilters(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3")

    val t1 = t.select('_1, '_2, '_3)
      .where('_1 < 7 && '_2 === 3)
      .select('_1, '_3)
      .where('_1 === 4)
      .select('_1)

    val t2 = t.select("_1, _2, _3")
      .where("_1 < 7 && _2 = 3")
      .select("_1, _3")
      .where("_1 === 4")
      .select("_1")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testCalcWithAggregation(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3")

    val t1 = t.select('_1, '_2, '_3)
      .where('_1 < 15)
      .groupBy('_2)
      .select('_1.min, '_2.count as 'cnt)
      .where('cnt > 3)


    val t2 = t.select("_1, _2, _3")
      .where("_1 < 15")
      .groupBy("_2")
      .select("_1.min, _2.count as cnt")
      .where("cnt > 3")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testCalcJoin(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.select('a, 'b).join(ds2).where('b === 'e).select('a, 'b, 'd, 'e, 'f)
      .where('b > 1).select('a, 'd).where('d === 2)
    val t2 = ds1.select("a, b").join(ds2).where("b = e").select("a, b, d, e, f")
      .where("b > 1").select("a, d").where("d = 2")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testAdvancedDataTypes(): Unit = {
    val util = batchTestUtil()
    val t = util
      .addTableSource[(BigDecimal, BigDecimal, Date, Time, Timestamp)]("Table5", 'a, 'b, 'c, 'd, 'e)

    val t1 = t.select('a, 'b, 'c, 'd, 'e, BigDecimal("11.2"), BigDecimal("11.2").bigDecimal,
        "1984-07-12".cast(Types.SQL_DATE), "14:34:24".cast(Types.SQL_TIME),
        "1984-07-12 14:34:24".cast(Types.SQL_TIMESTAMP))
    val t2 = t.select("a, b, c, d, e, 11.2p, 11.2p," +
      "'1984-07-12'.toDate, '14:34:24'.toTime," +
      "'1984-07-12 14:34:24'.toTimestamp")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testIntegerBiggerThan128(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = t.filter('a === 300)
    val t2 = t.filter("a = 300")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testNumericAutoCastInArithmetic() {
    val util = batchTestUtil()
    val table = util.addTableSource[(Byte, Short, Int, Long, Float, Double, Long, Double)](
      "Table",
      '_1, '_2, '_3, '_4, '_5, '_6, '_7, '_8)

    val t1 = table.select('_1 + 1, '_2 + 1, '_3 + 1L, '_4 + 1.0f,
      '_5 + 1.0d, '_6 + 1, '_7 + 1.0d, '_8 + '_1)
    val t2 = table.select("_1 + 1, _2 +" +
      " 1, _3 + 1L, _4 + 1.0f, _5 + 1.0d, _6 + 1, _7 + 1.0d, _8 + _1")

    verifyTableEquals(t1, t2)
  }

  @Test
  @throws[Exception]
  def testNumericAutoCastInComparison() {
    val util = batchTestUtil()
    val table = util.addTableSource[(Byte, Short, Int, Long, Float, Double)](
      "Table",
      'a, 'b, 'c, 'd, 'e, 'f)

    val t1 = table.filter('a > 1 && 'b > 1 && 'c > 1L &&
      'd > 1.0f && 'e > 1.0d && 'f > 1)
    val t2 = table
      .filter("a > 1 && b > 1 && c > 1L && d > 1.0f && e > 1.0d && f > 1")

    verifyTableEquals(t1, t2)
  }

  @Test
  @throws[Exception]
  def testCasting() {
    val util = batchTestUtil()
    val table = util.addTableSource[(Int, Double, Long, Boolean)](
      "Table",
      '_1, '_2, '_3, '_4)

    val t1 = table .select(
      // * -> String
      '_1.cast(STRING), '_2.cast(STRING), '_3.cast(STRING), '_4.cast(STRING),
      // NUMERIC TYPE -> Boolean
      '_1.cast(BOOLEAN), '_2.cast(BOOLEAN), '_3.cast(BOOLEAN),
      // NUMERIC TYPE -> NUMERIC TYPE
      '_1.cast(DOUBLE), '_2.cast(INT), '_3.cast(SHORT),
      // Boolean -> NUMERIC TYPE
      '_4.cast(DOUBLE), // identity casting
      '_1.cast(INT), '_2.cast(DOUBLE), '_3.cast(LONG), '_4.cast(BOOLEAN))
    val t2 = table.select(
      // * -> String
      "_1.cast(STRING), _2.cast(STRING), _3.cast(STRING), _4.cast(STRING)," +
        // NUMERIC TYPE -> Boolean
        "_1.cast(BOOLEAN), _2.cast(BOOLEAN), _3.cast(BOOLEAN)," +
        // NUMERIC TYPE -> NUMERIC TYPE
        "_1.cast(DOUBLE), _2.cast(INT), _3.cast(SHORT)," +
        // Boolean -> NUMERIC TYPE
        "_4.cast(DOUBLE)," +
        // identity casting
        "_1.cast(INT), _2.cast(DOUBLE), _3.cast(LONG), _4.cast(BOOLEAN)")

    verifyTableEquals(t1, t2)
  }

  @Test
  @throws[Exception]
  def testCastFromString() {
    val util = batchTestUtil()
    val table = util.addTableSource[(String, String, String)](
      "Table",
      '_1, '_2, '_3)

    val t1 = table .select('_1.cast(BYTE), '_1.cast(SHORT), '_1.cast(INT), '_1.cast(LONG),
        '_3.cast(DOUBLE), '_3.cast(FLOAT), '_2.cast(BOOLEAN))
    val t2 = table.select(
      "_1.cast(BYTE), _1.cast(SHORT), _1.cast(INT), _1.cast(LONG), " +
        "_3.cast(DOUBLE), _3.cast(FLOAT), _2.cast(BOOLEAN)")

    verifyTableEquals(t1, t2)
  }
}
