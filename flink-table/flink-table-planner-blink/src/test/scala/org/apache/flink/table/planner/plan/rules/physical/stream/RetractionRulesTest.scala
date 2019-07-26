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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class RetractionRulesTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addTableSource[(String, Int)]("MyTable", 'word, 'number)

  @Test
  def testSelect(): Unit = {
    util.verifyPlanWithTrait("SELECT word, number FROM MyTable")
  }

  @Test
  def testOneLevelGroupBy(): Unit = {
    // one level unbounded groupBy
    util.verifyPlanWithTrait("SELECT COUNT(number) FROM MyTable GROUP BY word")
  }

  @Test
  def testTwoLevelGroupBy(): Unit = {
    // two level unbounded groupBy
    val sql =
      """
        |SELECT cnt, COUNT(cnt) AS frequency FROM (
        |  SELECT word, COUNT(number) as cnt FROM MyTable GROUP BY word
        |) GROUP BY cnt
      """.stripMargin
    util.verifyPlanWithTrait(sql)
  }

  @Test
  def testGroupByWithUnion(): Unit = {
    util.addTableSource[(String, Long)]("MyTable2", 'word, 'cnt)

    val sql =
      """
        |SELECT cnt, COUNT(cnt) AS frequency FROM (
        |   SELECT word, COUNT(number) AS cnt FROM MyTable GROUP BY word
        |   UNION ALL
        |   SELECT word, cnt FROM MyTable2
        |) GROUP BY cnt
      """.stripMargin
    util.verifyPlanWithTrait(sql)
  }

}
