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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.LogicalPlanFormatUtils
import org.apache.flink.table.expressions.Literal
import org.apache.flink.table.utils.TableTestBase
import org.junit._

class JoinStringExpressionTest extends TableTestBase {

  @Test
  def testJoin(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1Scala = ds1.join(ds2).where('b === 'e).select('c, 'g)
    val t1Java = ds1.join(ds2).where("b === e").select("c, g")

    val lPlan1 = t1Scala.logicalPlan
    val lPlan2 = t1Java.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testJoinWithFilter(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1Scala = ds1.join(ds2).where('b === 'e && 'b < 2).select('c, 'g)
    val t1Java = ds1.join(ds2).where("b === e && b < 2").select("c, g")

    val lPlan1 = t1Scala.logicalPlan
    val lPlan2 = t1Java.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testJoinWithJoinFilter(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1Scala = ds1.join(ds2).where('b === 'e && 'a < 6 && 'h < 'b).select('c, 'g)
    val t1Java = ds1.join(ds2).where("b === e && a < 6 && h < b").select("c, g")

    val lPlan1 = t1Scala.logicalPlan
    val lPlan2 = t1Java.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testJoinWithMultipleKeys(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1Scala = ds1.join(ds2).filter('a === 'd && 'b === 'h).select('c, 'g)
    val t1Java = ds1.join(ds2).filter("a === d && b === h").select("c, g")

    val lPlan1 = t1Scala.logicalPlan
    val lPlan2 = t1Java.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testJoinWithAggregation(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1Scala = ds1.join(ds2).where('a === 'd).select('g.count)
    val t1Java = ds1.join(ds2).where("a === d").select("g.count")

    val lPlan1 = t1Scala.logicalPlan
    val lPlan2 = t1Java.logicalPlan

    Assert.assertEquals("Logical Plans do not match",
      LogicalPlanFormatUtils.formatTempTableId(lPlan1.toString),
      LogicalPlanFormatUtils.formatTempTableId(lPlan2.toString))
  }

  @Test
  def testJoinWithGroupedAggregation(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.join(ds2)
      .where('a === 'd)
      .groupBy('a, 'd)
      .select('b.sum, 'g.count)
    val t2 = ds1.join(ds2)
      .where("a = d")
      .groupBy("a, d")
      .select("b.sum, g.count")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match",
      LogicalPlanFormatUtils.formatTempTableId(lPlan1.toString),
      LogicalPlanFormatUtils.formatTempTableId(lPlan2.toString))
  }

  @Test
  def testJoinPushThroughJoin(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds3 = util.addTable[(Int, Long, String)]("Table4",'j, 'k, 'l)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.join(ds2)
      .where(Literal(true))
      .join(ds3)
      .where('a === 'd && 'e === 'k)
      .select('a, 'f, 'l)
    val t2 = ds1.join(ds2)
      .where("true")
      .join(ds3)
      .where("a === d && e === k")
      .select("a, f, l")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testJoinWithDisjunctivePred(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.join(ds2).filter('a === 'd && ('b === 'e || 'b === 'e - 10)).select('c, 'g)
    val t2 = ds1.join(ds2).filter("a = d && (b = e || b = e - 10)").select("c, g")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testJoinWithExpressionPreds(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.join(ds2).filter('b === 'h + 1 && 'a - 1 === 'd + 2).select('c, 'g)
    val t2 = ds1.join(ds2).filter("b = h + 1 && a - 1 = d + 2").select("c, g")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testLeftJoinWithMultipleKeys(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.leftOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)
    val t2 = ds1.leftOuterJoin(ds2, "a = d && b = h").select("c, g")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testRightJoinWithMultipleKeys(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.rightOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)
    val t2 = ds1.rightOuterJoin(ds2, "a = d && b = h").select("c, g")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

  @Test
  def testFullOuterJoinWithMultipleKeys(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.fullOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)
    val t2 = ds1.fullOuterJoin(ds2, "a = d && b = h").select("c, g")

    val lPlan1 = t1.logicalPlan
    val lPlan2 = t2.logicalPlan

    Assert.assertEquals("Logical Plans do not match", lPlan1, lPlan2)
  }

}
