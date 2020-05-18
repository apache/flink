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

package org.apache.flink.table.plan

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.table.utils.StreamTableTestUtil

import org.junit.Assert._
import org.junit.Test

class UpdatingPlanCheckerTest {

  @Test
  def testSelect(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(String, Int)]('a, 'b)
    val resultTable = table.select('a, 'b)

    util.verifyTableUniqueKey(resultTable, Nil)
  }

  @Test
  def testGroupByWithoutKey(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(String, Int)]('a, 'b)

    val resultTable = table
      .groupBy('a)
      .select('b.count)

    util.verifyTableUniqueKey(resultTable, Nil)
  }

  @Test
  def testGroupByWithoutKey2(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(String, Int, Int)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('a, 'b)
      .select('a, 'c.count)

    util.verifyTableUniqueKey(resultTable, Nil)
  }

  @Test
  def testGroupBy(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(String, Int)]('a, 'b)

    val resultTable = table
      .groupBy('a)
      .select('a, 'b.count)

    util.verifyTableUniqueKey(resultTable, Seq("a"))
  }

  @Test
  def testGroupByWithDuplicateKey(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(String, Int)]('a, 'b)

    val resultTable = table
      .groupBy('a)
      .select('a as 'a1, 'a as 'a2, 'b.count)

    util.verifyTableUniqueKey(resultTable, Seq("a1", "a2"))
    // both a1 and a2 belong to the same group, i.e., a1. We use the lexicographic smallest
    // attribute as the common group id
    util.verifyTableKeyGroups(resultTable, Seq(("a1", "a1"), ("a2", "a1")))
  }

  @Test
  def testGroupWindow(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(String, Int)]('a, 'b, 'proctime.proctime)

    val resultTable = table
      .window(Tumble over 5.milli on 'proctime as 'w)
      .groupBy('w, 'a)
      .select('a, 'b.count, 'w.proctime as 'p, 'w.start as 's, 'w.end as 'e)

    util.verifyTableUniqueKey(resultTable, Seq("a", "s", "e"))
  }

  @Test
  def testForwardBothKeysForJoin1(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(Int, Int)]('pk, 'a)

    val lTableWithPk = table
      .groupBy('pk)
      .select('pk as 'l1, 'pk as 'l2, 'pk as 'l3, 'a.max as 'l4, 'a.min as 'l5)

    val rTableWithPk = table
      .groupBy('pk)
      .select('pk as 'r2, 'pk as 'r3, 'a.max as 'r1, 'a.min as 'r4, 'a.count as 'r5)

    val resultTable = lTableWithPk
      .join(rTableWithPk)
      .where('l2 === 'r2 && 'l4 === 'r3 && 'l4 === 'r5 && 'l5 === 'r4)
      .select('l1, 'l2, 'l3, 'l4, 'l5, 'r1, 'r2, 'r3, 'r4, 'r5)

    util.verifyTableUniqueKey(resultTable, Seq("l1", "l2", "l3", "l4", "r2", "r3", "r5"))
  }

  @Test
  def testForwardBothKeysForLeftJoin1(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(Int, Int)]('pk, 'a)

    val lTableWithPk = table
      .groupBy('pk)
      .select('pk as 'l1, 'pk as 'l2, 'pk as 'l3, 'a.max as 'l4, 'a.min as 'l5)

    val rTableWithPk = table
      .groupBy('pk)
      .select('pk as 'r2, 'pk as 'r3, 'a.max as 'r1, 'a.min as 'r4, 'a.min as 'r5)

    val resultTable = lTableWithPk
      .leftOuterJoin(rTableWithPk)
      .where('l2 === 'r2 && 'l4 === 'r3 && 'l4 === 'r5 && 'l5 === 'r4)
      .select('l1, 'l2, 'l3, 'l4, 'l5, 'r1, 'r2, 'r3, 'r4, 'r5)

    util.verifyTableUniqueKey(resultTable, Seq("l1", "l2", "l3", "l4", "r2", "r3", "r5"))
  }

  @Test
  def testForwardBothKeysForJoin2(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(Int, Int)]('pk, 'a)

    val lTableWithPk = table
      .groupBy('pk)
      .select('pk as 'l1, 'pk as 'l2, 'pk as 'l3, 'a.max as 'l4, 'a.min as 'l5)

    val rTableWithPk = table
      .groupBy('pk)
      .select('pk as 'r2, 'pk as 'r3, 'a.max as 'r1, 'a.min as 'r4, 'a.count as 'r5)

    val resultTable = lTableWithPk
      .join(rTableWithPk)
      .where('l5 === 'r4)
      .select('l1, 'l2, 'l3, 'l4, 'l5, 'r1, 'r2, 'r3, 'r4, 'r5)

    util.verifyTableUniqueKey(resultTable, Seq("l1", "l2", "l3", "r2", "r3"))
  }

  @Test
  def testForwardBothKeysForLeftJoin2(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(Int, Int)]('pk, 'a)

    val lTableWithPk = table
      .groupBy('pk)
      .select('pk as 'l1, 'pk as 'l2, 'pk as 'l3, 'a.max as 'l4, 'a.min as 'l5)

    val rTableWithPk = table
      .groupBy('pk)
      .select('pk as 'r2, 'pk as 'r3, 'a.max as 'r1, 'a.min as 'r4, 'a.count as 'r5)

    val resultTable = lTableWithPk
      .leftOuterJoin(rTableWithPk)
      .where('l5 === 'r4)
      .select('l1, 'l2, 'l3, 'l4, 'l5, 'r1, 'r2, 'r3, 'r4, 'r5)

    util.verifyTableUniqueKey(resultTable, Seq("l1", "l2", "l3", "r2", "r3"))
  }

  @Test
  def testJoinKeysEqualsLeftKeys(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(Int, Int)]('pk, 'a)

    val lTableWithPk = table
      .groupBy('pk)
      .select('pk as 'leftpk, 'a.max as 'lefta)

    val rTableWithPk = table
      .groupBy('pk)
      .select('pk as 'rightpk, 'a.max as 'righta)

    val resultTable = lTableWithPk
      .join(rTableWithPk)
      .where('leftpk === 'righta)
      .select('rightpk, 'lefta, 'righta)

    util.verifyTableUniqueKey(resultTable, Seq("rightpk", "righta"))
  }

  @Test
  def testJoinKeysEqualsLeftKeysForLeftJoin(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(Int, Int)]('pk, 'a)

    val lTableWithPk = table
      .groupBy('pk)
      .select('pk as 'leftpk, 'a.max as 'lefta)

    val rTableWithPk = table
      .groupBy('pk)
      .select('pk as 'rightpk, 'a.max as 'righta)

    val resultTable = lTableWithPk
      .leftOuterJoin(rTableWithPk)
      .where('leftpk === 'righta)
      .select('rightpk, 'lefta, 'righta)

    util.verifyTableUniqueKey(resultTable, Seq("rightpk", "righta"))
  }

  @Test
  def testJoinKeysEqualsRightKeys(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(Int, Int)]('pk, 'a)

    val lTableWithPk = table
      .groupBy('pk)
      .select('pk as 'leftpk, 'a.max as 'lefta)

    val rTableWithPk = table
      .groupBy('pk)
      .select('pk as 'rightpk, 'a.max as 'righta)

    val resultTable = lTableWithPk
      .join(rTableWithPk)
      .where('lefta === 'rightpk)
      .select('leftpk, 'lefta, 'righta)

    util.verifyTableUniqueKey(resultTable, Seq("leftpk", "lefta"))
  }

  @Test
  def testJoinKeysEqualsRightKeysForLeftJoin(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(Int, Int)]('pk, 'a)

    val lTableWithPk = table
      .groupBy('pk)
      .select('pk as 'leftpk, 'a.max as 'lefta)

    val rTableWithPk = table
      .groupBy('pk)
      .select('pk as 'rightpk, 'a.max as 'righta)

    val resultTable = lTableWithPk
      .join(rTableWithPk)
      .where('lefta === 'rightpk)
      .select('leftpk, 'lefta, 'righta)

    util.verifyTableUniqueKey(resultTable, Seq("leftpk", "lefta"))
  }


  @Test
  def testNonKeysJoin(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(Int, Int)]('a, 'b)

    val lTable = table
      .select('a as 'a, 'b as 'b)

    val rTable = table
      .select('a as 'aa, 'b as 'bb)

    val resultTable = lTable
      .join(rTable)
      .where('a === 'aa)
      .select('a, 'aa, 'b, 'bb)

    util.verifyTableUniqueKey(resultTable, Nil)
  }

  @Test
  def testNonKeysJoin2(): Unit = {
    val util = new UpdatePlanCheckerUtil()
    val table = util.addTable[(Int, Int)]('a, 'b)

    val lTable = table
      .select('a as 'a, 'b as 'b)

    val rTable = table
      .select('a as 'aa, 'b as 'bb)

    val resultTable = lTable
      .leftOuterJoin(rTable, 'a === 'aa)
      .select('a, 'aa, 'b, 'bb)

    util.verifyTableUniqueKey(resultTable, Nil)
  }
}


class UpdatePlanCheckerUtil extends StreamTableTestUtil {

  def verifySqlUniqueKey(query: String, expected: Seq[String]): Unit = {
    verifyTableUniqueKey(tableEnv.sqlQuery(query), expected)
  }

  def getKeyGroups(resultTable: Table): Option[Seq[(String, String)]] = {
    val optimized = optimize(resultTable)
    UpdatingPlanChecker.getUniqueKeyGroups(optimized)
  }

  def verifyTableKeyGroups(resultTable: Table, expected: Seq[(String, String)]): Unit = {
    val actual = getKeyGroups(resultTable)
    if (actual.isDefined) {
      assertEquals(expected.sorted, actual.get.sorted)
    } else {
      assertEquals(expected.sorted, Nil)
    }
  }

  def verifyTableUniqueKey(resultTable: Table, expected: Seq[String]): Unit = {
    val actual = getKeyGroups(resultTable).map(_.map(_._1))
    if (actual.isDefined) {
      assertEquals(expected.sorted, actual.get.sorted)
    } else {
      assertEquals(expected.sorted, Nil)
    }
  }
}
