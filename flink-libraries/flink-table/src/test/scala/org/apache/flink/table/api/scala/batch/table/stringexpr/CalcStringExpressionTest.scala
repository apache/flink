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

package org.apache.flink.table.api.scala.batch.table.stringexpr

import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets.CustomType
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.LogicalPlanFormatUtils
import org.apache.flink.table.expressions.Literal
import org.apache.flink.table.utils.TableTestBase
import org.junit._

class CalcStringExpressionTest extends TableTestBase {

  @Test
  def testSimpleSelectAllWithAs(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = t.select('a, 'b, 'c)
    val t2 = t.select("a, b, c")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testSimpleSelectWithNaming(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3")

    val t1 = t
      .select('_1 as 'a, '_2 as 'b, '_1 as 'c)
      .select('a, 'b)

    val t2 = t
      .select("_1 as a, _2 as b, _1 as c")
      .select("a, b")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testSimpleSelectRenameAll(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3")

    val t1 = t
      .select('_1 as 'a, '_2 as 'b, '_3 as 'c)
      .select('a, 'b)

    val t2 = t
      .select("_1 as a, _2 as b, _3 as c")
      .select("a, b")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testSelectStar(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = t.select('*)
    val t2 = t.select("*")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter( Literal(false) )
    val t2 = ds.filter("faLsE")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testAllPassingFilter(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter( Literal(true) )
    val t2 = ds.filter("trUe")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testFilterOnStringTupleField(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter( 'c.like("%world%") )
    val t2 = ds.filter("c.like('%world%')")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testFilterOnIntegerTupleField(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter( 'a % 2 === 0 )
    val t2 = ds.filter( "a % 2 = 0 ")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testNotEquals(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter( 'a % 2 !== 0 )
    val t2 = ds.filter("a % 2 <> 0")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testDisjunctivePredicate(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter( 'a < 2 || 'a > 20)
    val t2 = ds.filter("a < 2 || a > 20")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testConsecutiveFilters(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = ds.filter('a % 2 !== 0).filter('b % 2 === 0)
    val t2 = ds.filter("a % 2 != 0").filter("b % 2 = 0")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testFilterBasicType(): Unit = {
    val util = batchTestUtil()
    val ds = util.addTable[String]("Table3",'a)

    val t1 = ds.filter( 'a.like("H%") )
    val t2 = ds.filter( "a.like('H%')" )

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testFilterOnCustomType(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[CustomType]("Table3",'myInt as 'i, 'myLong as 'l, 'myString as 's)

    val t1 = t.filter( 's.like("%a%") )
    val t2 = t.filter("s.like('%a%')")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testSimpleCalc(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3")

    val t1 = t.select('_1, '_2, '_3)
      .where('_1 < 7)
      .select('_1, '_3)

    val t2 = t.select("_1, _2, _3")
      .where("_1 < 7")
      .select("_1, _3")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testCalcWithTwoFilters(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3")

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

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testCalcWithAggregation(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3")

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

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match",
      LogicalPlanFormatUtils.formatTempTableId(lPlan1.toString),
      LogicalPlanFormatUtils.formatTempTableId(lPlan2.toString))
  }

  @Test
  def testCalcJoin(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.select('a, 'b).join(ds2).where('b === 'e).select('a, 'b, 'd, 'e, 'f)
      .where('b > 1).select('a, 'd).where('d === 2)
    val t2 = ds1.select("a, b").join(ds2).where("b = e").select("a, b, d, e, f")
      .where("b > 1").select("a, d").where("d = 2")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testAdvancedDataTypes(): Unit = {
    val util = batchTestUtil()
    val t = util
      .addTable[(BigDecimal, BigDecimal, Date, Time, Timestamp)]("Table5", 'a, 'b, 'c, 'd, 'e)

    val t1 = t.select('a, 'b, 'c, 'd, 'e, BigDecimal("11.2"), BigDecimal("11.2").bigDecimal,
        "1984-07-12".cast(Types.SQL_DATE), "14:34:24".cast(Types.SQL_TIME),
        "1984-07-12 14:34:24".cast(Types.SQL_TIMESTAMP))
    val t2 = t.select("a, b, c, d, e, 11.2, 11.2," +
      "'1984-07-12'.toDate, '14:34:24'.toTime," +
      "'1984-07-12 14:34:24'.toTimestamp")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1.toString, lPlan2.toString)
  }

  @Test
  def testIntegerBiggerThan128(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    val t1 = t.filter('a === 300)
    val t2 = t.filter("a = 300")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }
}
