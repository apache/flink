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
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit._

class JoinStringExpressionTest extends TableTestBase {

  @Test
  def testJoin(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1Scala = ds1.join(ds2).where('b === 'e).select('c, 'g)
    val t1Java = ds1.join(ds2).where("b === e").select("c, g")

    verifyTableEquals(t1Scala, t1Java)
  }

  @Test
  def testJoinWithFilter(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1Scala = ds1.join(ds2).where('b === 'e && 'b < 2).select('c, 'g)
    val t1Java = ds1.join(ds2).where("b === e && b < 2").select("c, g")

    verifyTableEquals(t1Scala, t1Java)
  }

  @Test
  def testJoinWithJoinFilter(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1Scala = ds1.join(ds2).where('b === 'e && 'a < 6 && 'h < 'b).select('c, 'g)
    val t1Java = ds1.join(ds2).where("b === e && a < 6 && h < b").select("c, g")

    verifyTableEquals(t1Scala, t1Java)
  }

  @Test
  def testJoinWithMultipleKeys(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1Scala = ds1.join(ds2).filter('a === 'd && 'b === 'h).select('c, 'g)
    val t1Java = ds1.join(ds2).filter("a === d && b === h").select("c, g")

    verifyTableEquals(t1Scala, t1Java)
  }

  @Test
  def testJoinWithAggregation(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1Scala = ds1.join(ds2).where('a === 'd).select('g.count)
    val t1Java = ds1.join(ds2).where("a === d").select("g.count")

    verifyTableEquals(t1Scala, t1Java)
  }

  @Test
  def testJoinWithGroupedAggregation(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.join(ds2)
      .where('a === 'd)
      .groupBy('a, 'd)
      .select('b.sum, 'g.count)
    val t2 = ds1.join(ds2)
      .where("a = d")
      .groupBy("a, d")
      .select("b.sum, g.count")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testJoinPushThroughJoin(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds3 = util.addTableSource[(Int, Long, String)]("Table4",'j, 'k, 'l)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.join(ds2)
      .where(true)
      .join(ds3)
      .where('a === 'd && 'e === 'k)
      .select('a, 'f, 'l)
    val t2 = ds1.join(ds2)
      .where("true")
      .join(ds3)
      .where("a === d && e === k")
      .select("a, f, l")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testJoinWithDisjunctivePred(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.join(ds2).filter('a === 'd && ('b === 'e || 'b === 'e - 10)).select('c, 'g)
    val t2 = ds1.join(ds2).filter("a = d && (b = e || b = e - 10)").select("c, g")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testJoinWithExpressionPreds(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.join(ds2).filter('b === 'h + 1 && 'a - 1 === 'd + 2).select('c, 'g)
    val t2 = ds1.join(ds2).filter("b = h + 1 && a - 1 = d + 2").select("c, g")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testLeftJoinWithMultipleKeys(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.leftOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)
    val t2 = ds1.leftOuterJoin(ds2, "a = d && b = h").select("c, g")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testRightJoinWithMultipleKeys(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.rightOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)
    val t2 = ds1.rightOuterJoin(ds2, "a = d && b = h").select("c, g")

    verifyTableEquals(t1, t2)
  }

  @Test
  def testFullOuterJoinWithMultipleKeys(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val t1 = ds1.fullOuterJoin(ds2, 'a === 'd && 'b === 'h).select('c, 'g)
    val t2 = ds1.fullOuterJoin(ds2, "a = d && b = h").select("c, g")

    verifyTableEquals(t1, t2)
  }
}
