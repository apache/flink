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
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.LogicalPlanFormatUtils
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.expressions.Literal
import org.junit._

class CalcStringExpressionTest {

  @Test
  def testSimpleSelectAllWithAs(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val t1 = t.select('a, 'b, 'c)
    val t2 = t.select("a, b, c")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testSimpleSelectWithNaming(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)

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
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)

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
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val t1 = t.select('*)
    val t2 = t.select("*")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val t1 = ds.filter( Literal(false) )
    val t2 = ds.filter("false")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testAllPassingFilter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val t1 = ds.filter( Literal(true) )
    val t2 = ds.filter("true")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testFilterOnStringTupleField(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val t1 = ds.filter( 'c.like("%world%") )
    val t2 = ds.filter("c.like('%world%')")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testFilterOnIntegerTupleField(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val t1 = ds.filter( 'a % 2 === 0 )
    val t2 = ds.filter( "a % 2 = 0 ")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testNotEquals(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val t1 = ds.filter( 'a % 2 !== 0 )
    val t2 = ds.filter("a % 2 <> 0")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testDisjunctivePredicate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val t1 = ds.filter( 'a < 2 || 'a > 20)
    val t2 = ds.filter("a < 2 || a > 20")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testConsecutiveFilters(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val t1 = ds.filter('a % 2 !== 0).filter('b % 2 === 0)
    val t2 = ds.filter("a % 2 != 0").filter("b % 2 = 0")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testFilterBasicType(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = CollectionDataSets.getStringDataSet(env).toTable(tEnv, 'a)

    val t1 = ds.filter( 'a.like("H%") )
    val t2 = ds.filter( "a.like('H%')" )

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testFilterOnCustomType(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = CollectionDataSets.getCustomTypeDataSet(env)
    val t = ds.toTable(tEnv, 'myInt as 'i, 'myLong as 'l, 'myString as 's)

    val t1 = t.filter( 's.like("%a%") )
    val t2 = t.filter("s.like('%a%')")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testSimpleCalc(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)

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
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)

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
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)

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
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

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
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = env
      .fromElements((
        BigDecimal("78.454654654654654").bigDecimal,
        BigDecimal("4E+9999").bigDecimal,
        Date.valueOf("1984-07-12"),
        Time.valueOf("14:34:24"),
        Timestamp.valueOf("1984-07-12 14:34:24")))
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e)

    val t1 = t.select('a, 'b, 'c, 'd, 'e, BigDecimal("11.2"), BigDecimal("11.2").bigDecimal,
        "1984-07-12".cast(Types.DATE), "14:34:24".cast(Types.TIME),
        "1984-07-12 14:34:24".cast(Types.TIMESTAMP))
    val t2 = t.select("a, b, c, d, e, 11.2, 11.2," +
      "'1984-07-12'.toDate, '14:34:24'.toTime," +
      "'1984-07-12 14:34:24'.toTimestamp")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1.toString, lPlan2.toString)
  }

  @Test
  def testIntegerBiggerThan128(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val t = env.fromElements((300, 1L, "Hello")).toTable(tableEnv, 'a, 'b, 'c)

    val t1 = t.filter('a === 300)
    val t2 = t.filter("a = 300")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }
}
