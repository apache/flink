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

import org.apache.flink.table.api.Table
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.table.utils.StreamTableTestUtil
import org.junit.Assert._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.junit.Test


class UpdatingPlanCheckerTest {

  @Test
  def testSelect(): Unit = {
    val util = new UpdatePlanChecnkerUtil()
    val table = util.addTable[(String, Int)]('word, 'number)
    val resultTable = table.select('word, 'number)

    util.verifyTableUniqueKey(resultTable, Nil)
  }

  @Test
  def testGroupByWithoutKey(): Unit = {
    val util = new UpdatePlanChecnkerUtil()
    val table = util.addTable[(String, Int)]('word, 'number)

    val resultTable = table
      .groupBy('word)
      .select('number.count)

    util.verifyTableUniqueKey(resultTable, Nil)
  }

  @Test
  def testGroupBy(): Unit = {
    val util = new UpdatePlanChecnkerUtil()
    val table = util.addTable[(String, Int)]('word, 'number)

    val resultTable = table
      .groupBy('word)
      .select('word, 'number.count)

    util.verifyTableUniqueKey(resultTable, Seq("word"))
  }

  @Test
  def testGroupByWithDumplicateKey(): Unit = {
    val util = new UpdatePlanChecnkerUtil()
    val table = util.addTable[(String, Int)]('word, 'number)

    val resultTable = table
      .groupBy('word)
      .select('word as 'word1, 'word as 'word2, 'number.count)

    util.verifyTableUniqueKey(resultTable, Seq("word1", "word2"))
  }

  //1. join key = left key = right key
  @Test
  def testJoinKeysEqualsleftAndRightKeys(): Unit = {
    val util = new UpdatePlanChecnkerUtil()
    val table = util.addTable[(Int, Int)]('pk, 'a)

    val leftTableWithPk = table
      .groupBy('pk)
      .select('pk as 'leftpk, 'a.max as 'lefta)

    val rightTableWithPk = table
      .groupBy('pk)
      .select('pk as 'rightpk, 'a.max as 'righta)

    val resultTable = leftTableWithPk
      .join(rightTableWithPk)
      .where('leftpk === 'rightpk)
      .select('leftpk, 'lefta, 'righta)

    util.verifyTableUniqueKey(resultTable, Seq("leftpk"))
  }

  //2. join key = left key
  @Test
  def testJoinKeysEqualsLeftKeys(): Unit = {
    val util = new UpdatePlanChecnkerUtil()
    val table = util.addTable[(Int, Int)]('pk, 'a)

    val leftTableWithPk = table
      .groupBy('pk)
      .select('pk as 'leftpk, 'a.max as 'lefta)

    val rightTableWithPk = table
      .groupBy('pk)
      .select('pk as 'rightpk, 'a.max as 'righta)

    val resultTable = leftTableWithPk
      .join(rightTableWithPk)
      .where('leftpk === 'righta)
      .select('rightpk, 'lefta, 'righta)

    util.verifyTableUniqueKey(resultTable, Seq("rightpk"))
  }

  //3. join key = right key
  @Test
  def testJoinKeysEqualsRightKeys(): Unit = {
    val util = new UpdatePlanChecnkerUtil()
    val table = util.addTable[(Int, Int)]('pk, 'a)

    val leftTableWithPk = table
      .groupBy('pk)
      .select('pk as 'leftpk, 'a.max as 'lefta)

    val rightTableWithPk = table
      .groupBy('pk)
      .select('pk as 'rightpk, 'a.max as 'righta)

    val resultTable = leftTableWithPk
      .join(rightTableWithPk)
      .where('lefta === 'rightpk)
      .select('leftpk, 'lefta, 'righta)

    util.verifyTableUniqueKey(resultTable, Seq("leftpk"))
  }

  //4. join key not left or right key
  @Test
  def testJoinKeysWithoutLeftRightKeys(): Unit = {
    val util = new UpdatePlanChecnkerUtil()
    val table = util.addTable[(Int, Int)]('pk, 'a)

    val leftTableWithPk = table
      .groupBy('pk)
      .select('pk as 'leftpk, 'a.max as 'lefta)

    val rightTableWithPk = table
      .groupBy('pk)
      .select('pk as 'rightpk, 'a.max as 'righta)

    val resultTable = leftTableWithPk
      .join(rightTableWithPk)
      .where('lefta === 'righta)
      .select('leftpk, 'rightpk, 'lefta, 'righta)

    util.verifyTableUniqueKey(resultTable, Seq("leftpk", "rightpk"))
  }

  @Test
  def testNonKeysJoin(): Unit = {
    val util = new UpdatePlanChecnkerUtil()
    val table = util.addTable[(Int, Int)]('a, 'b)

    val leftTable = table
      .select('a as 'a, 'b as 'b)

    val rightTable = table
      .select('a as 'aa, 'b as 'bb)

    val resultTable = leftTable
      .join(rightTable)
      .where('a === 'aa)
      .select('a, 'aa, 'b, 'bb)

    util.verifyTableUniqueKey(resultTable, Nil)
  }
}


class UpdatePlanChecnkerUtil extends StreamTableTestUtil {

  def verifySqlUniqueKey(query: String, expected: Seq[String]): Unit = {
    verifyTableUniqueKey(tableEnv.sql(query), expected)
  }

  def verifyTableUniqueKey(resultTable: Table, expected: Seq[String]): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = tableEnv.optimize(relNode, updatesAsRetraction = false)
    val actual = UpdatingPlanChecker.getUniqueKeyFields(optimized)

    if (actual.isDefined) {
      assertEquals(expected.sorted, actual.get.toList.sorted)
    } else {
      assertEquals(None, actual)
    }
  }
}
