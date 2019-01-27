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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.CountDistinct
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class RetractionRulesTest extends TableTestBase {

  @Test
  def testSelect(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(String, Int)]('word, 'number)

    val resultTable = table.select('word, 'number)

    util.verifyPlanAndTrait(resultTable)
  }

  // one level unbounded groupBy
  @Test
  def testGroupBy(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(String, Int)]('word, 'number)

    val resultTable = table
      .groupBy('word)
      .select('number.count)

    util.verifyPlanAndTrait(resultTable)
  }

  // two level unbounded groupBy
  @Test
  def testTwoGroupBy(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(String, Int)]('word, 'number)

    val resultTable = table
      .groupBy('word)
      .select('word, 'number.count as 'count)
      .groupBy('count)
      .select('count, 'count.count as 'frequency)

    util.verifyPlanAndTrait(resultTable)
  }

  // group window
  @Test
  def testGroupWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(String, Int)]('word, 'number, 'rowtime.rowtime)

    val resultTable = table
      .window(Tumble over 50.milli on 'rowtime as 'w)
      .groupBy('w, 'word)
      .select('word, 'number.count as 'count)

    util.verifyPlanAndTrait(resultTable)
  }

  // group window
  @Test
  def testTwoGroupWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(String, Int)]('word, 'number, 'rowtime.rowtime)

    val resultTable = table
      .window(Tumble over 50.milli on 'rowtime as 'w)
      .groupBy('w, 'word)
      .select('word, 'number.count as 'count, 'w.rowtime as 'w_rowtime)
      .window(Tumble over 1.second on 'w_rowtime as 'w2)
      .groupBy('w2, 'word)
      .select('word, 'count.sum as 'sum)

    util.verifyPlanAndTrait(resultTable)
  }

  // group window with emit
  @Test
  def testGroupWindowWithEmitStrategy(): Unit = {
    val util = streamTestUtil()
    util.tableEnv.getConfig
      .withIdleStateRetentionTime(Time.hours(1L))
      .withEarlyFireInterval(Time.milliseconds(10L))
      .withLateFireInterval(Time.milliseconds(0L))

    val table = util.addTable[(String, Int)]('word, 'number, 'rowtime.rowtime)

    val resultTable = table
      .window(Tumble over 50.milli on 'rowtime as 'w)
      .groupBy('w, 'word)
      .select('word, 'number.count as 'count)

    util.verifyPlanAndTrait(resultTable)
  }

  // two group window with emit
  @Test
  def testTwoGroupWindowWithEmitStrategy(): Unit = {
    val util = streamTestUtil()
    util.tableEnv.getConfig
      .withIdleStateRetentionTime(Time.hours(1L))
      .withEarlyFireInterval(Time.milliseconds(10L))
      .withLateFireInterval(Time.milliseconds(0L))

    val table = util.addTable[(String, Int)]('word, 'number, 'rowtime.rowtime)

    val resultTable = table
      .window(Tumble over 50.milli on 'rowtime as 'w)
      .groupBy('w, 'word)
      .select('word, 'number.count as 'count, 'w.rowtime as 'w_rowtime)
      .window(Tumble over 1.second on 'w_rowtime as 'w2)
      .groupBy('w2, 'word)
      .select('word, 'count.sum as 'sum)

    util.verifyPlanAndTrait(resultTable)
  }

  // over window
  @Test
  def testOverWindow(): Unit = {
    val util = streamTestUtil()
    util.addTable[(String, Int)]("T1", 'word, 'number, 'proctime.proctime)

    val sqlQuery =
      "SELECT " +
        "word, count(number) " +
        "OVER (PARTITION BY word ORDER BY proctime " +
        "ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW)" +
        "FROM T1"

    util.verifyPlanAndTrait(sqlQuery)
  }

  // test binaryNode
  @Test
  def testBinaryNode(): Unit = {
    val util = streamTestUtil()
    val lTable = util.addTable[(String, Int)]('word, 'number)
    val rTable = util.addTable[(String, Long)]('word_r, 'count_r)

    val resultTable = lTable
      .groupBy('word)
      .select('word, 'number.count as 'count)
      .unionAll(rTable)
      .groupBy('count)
      .select('count, 'count.count as 'frequency)

    util.verifyPlanAndTrait(resultTable)
  }

  // test join keys equal unique keys of children
  @Test
  def testJoinKeysEqualUniqueKeys(): Unit = {
    val util = streamTestUtil()
    val lTable = util.addTable[(String, Int)]('pk_l, 'a_l)
    val rTable = util.addTable[(String, Long)]('pk_r, 'a_r)

    val lTableWithPk = lTable
      .groupBy('pk_l)
      .select('pk_l, 'a_l.max as 'a_l)

    val rTableWithPk = rTable
      .groupBy('pk_r)
      .select('pk_r, 'a_r.max as 'a_r)

    val resultTable = lTableWithPk
      .join(rTableWithPk, 'pk_l === 'pk_r)

    util.verifyPlanAndTrait(resultTable)
  }

  // test join keys don't equal unique keys of children
  @Test
  def testJoinKeysDontEqualUniqueKeys(): Unit = {
    val util = streamTestUtil()
    val lTable = util.addTable[(String, String)]('pk_l, 'a_l)
    val rTable = util.addTable[(String, String)]('pk_r, 'a_r)

    val lTableWithPk = lTable
      .groupBy('pk_l)
      .select('pk_l, 'a_l.max as 'a_l)

    val rTableWithPk = rTable
      .groupBy('pk_r)
      .select('pk_r, 'a_r.max as 'a_r)

    val resultTable = lTableWithPk
      .join(rTableWithPk, 'pk_l === 'a_r)

    util.verifyPlanAndTrait(resultTable)
  }

  // test left and right without unique keys
  @Test
  def testJoinLeftRightWithoutUniqueKeys(): Unit = {
    val util = streamTestUtil()
    val lTable = util.addTable[(String, String)]('pk_l, 'a_l)
    val rTable = util.addTable[(String, String)]('pk_r, 'a_r)

    val resultTable = lTable
      .join(rTable, 'pk_l === 'a_r)

    util.verifyPlanAndTrait(resultTable)
  }


  @Test
  def testLeftJoin(): Unit = {
    val util = streamTestUtil()
    val lTable = util.addTable[(Int, Int)]('a, 'b)
    val rTable = util.addTable[(Int, String)]('bb, 'c)

    val resultTable = lTable
      .leftOuterJoin(rTable, 'b === 'bb)
      .select('a, 'b, 'c)

    util.verifyPlanAndTrait(resultTable)
  }

  @Test
  def testAggFollowedWithLeftJoin(): Unit = {
    val util = streamTestUtil()
    val lTable = util.addTable[(Int, Int)]('a, 'b)
    val rTable = util.addTable[(Int, String)]('bb, 'c)

    val countDistinct = new CountDistinct
    val resultTable = lTable
      .leftOuterJoin(rTable, 'b === 'bb)
      .select('a, 'b, 'c)
      .groupBy('a)
      .select('a, countDistinct('c))

    util.verifyPlanAndTrait(resultTable)
  }

  @Test
  def testRightJoin(): Unit = {
    val util = streamTestUtil()
    val lTable = util.addTable[(Int, Int)]('a, 'b)
    val rTable = util.addTable[(Int, String)]('bb, 'c)

    val resultTable = lTable
      .rightOuterJoin(rTable, 'b === 'bb)
      .select('a, 'b, 'c)

    util.verifyPlanAndTrait(resultTable)
  }

  @Test
  def testAggFollowedWithRightJoin(): Unit = {
    val util = streamTestUtil()
    val lTable = util.addTable[(Int, Int)]('a, 'b)
    val rTable = util.addTable[(Int, String)]('bb, 'c)

    val countDistinct = new CountDistinct
    val resultTable = lTable
      .rightOuterJoin(rTable, 'b === 'bb)
      .select('a, 'b, 'c)
      .groupBy('a)
      .select('a, countDistinct('c))

    util.verifyPlanAndTrait(resultTable)
  }

  @Test
  def testFullJoin(): Unit = {
    val util = streamTestUtil()
    val lTable = util.addTable[(Int, Int)]('a, 'b)
    val rTable = util.addTable[(Int, String)]('bb, 'c)

    val resultTable = lTable
      .fullOuterJoin(rTable, 'b === 'bb)
      .select('a, 'b, 'c)

    util.verifyPlanAndTrait(resultTable)
  }

  @Test
  def testAggFollowedWithFullJoin(): Unit = {
    val util = streamTestUtil()
    val lTable = util.addTable[(Int, Int)]('a, 'b)
    val rTable = util.addTable[(Int, String)]('bb, 'c)

    val countDistinct = new CountDistinct
    val resultTable = lTable
      .fullOuterJoin(rTable, 'b === 'bb)
      .select('a, 'b, 'c)
      .groupBy('a)
      .select('a, countDistinct('c))

    util.verifyPlanAndTrait(resultTable)
  }
}
