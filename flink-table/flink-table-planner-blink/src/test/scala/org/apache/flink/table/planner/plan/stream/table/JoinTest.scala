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

package org.apache.flink.table.planner.plan.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

import java.sql.Timestamp

/**
  * Tests for both windowed and non-windowed joins.
  */
class JoinTest extends TableTestBase {

  // Tests for inner join
  @Test
  def testRowTimeWindowInnerJoin(): Unit = {
    val util = streamTestUtil()
    val left = util.addDataStream[(Long, Int, String)]("T1", 'a, 'b, 'c, 'lrtime.rowtime)

    val right = util.addDataStream[(Long, Int, String)]("T2",'d, 'e, 'f, 'rrtime.rowtime)

    val resultTable = left.join(right)
      .where('a === 'd && 'lrtime >= 'rrtime - 5.minutes && 'lrtime < 'rrtime + 3.seconds)
      .select('a, 'e, 'lrtime)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testProcTimeWindowInnerJoin(): Unit = {
    val util = streamTestUtil()
    val left = util.addDataStream[(Long, Int, String)]("T1", 'a, 'b, 'c, 'lptime.proctime)

    val right = util.addDataStream[(Long, Int, String)]("T2",'d, 'e, 'f, 'rptime.proctime)

    val resultTable = left.join(right)
      .where('a === 'd && 'lptime >= 'rptime - 1.second && 'lptime < 'rptime)
      .select('a, 'e, 'lptime)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testProcTimeWindowInnerJoinWithEquiTimeAttrs(): Unit = {
    val util = streamTestUtil()
    val left = util.addDataStream[(Long, Int, String)]("T1",'a, 'b, 'c, 'lptime.proctime)

    val right = util.addDataStream[(Long, Int, String)]("T2",'d, 'e, 'f, 'rptime.proctime)

    val resultTable = left.join(right)
      .where('a === 'd && 'lptime === 'rptime)
      .select('a, 'e, 'lptime)

    util.verifyExecPlan(resultTable)
  }

  /**
    * The time indicator can be accessed from non-time predicates now.
    */
  @Test
  def testRowTimeInnerJoinWithTimeAccessed(): Unit = {
    val util = streamTestUtil()
    val left = util.addDataStream[(Long, Int, Timestamp)]("T1",'a, 'b, 'c, 'lrtime.rowtime)
    val right = util.addDataStream[(Long, Int, Timestamp)]("T2",'d, 'e, 'f, 'rrtime.rowtime)
    val resultTable = left.join(right)
      .where('a ==='d && 'lrtime >= 'rrtime - 5.minutes && 'lrtime < 'rrtime && 'lrtime > 'f)

    util.verifyExecPlan(resultTable)
  }

  // Tests for left outer join
  @Test
  def testRowTimeWindowLeftOuterJoin(): Unit = {
    val util = streamTestUtil()
    val left = util.addDataStream[(Long, Int, String)]("T1",'a, 'b, 'c, 'lrtime.rowtime)
    val right = util.addDataStream[(Long, Int, String)]("T2",'d, 'e, 'f, 'rrtime.rowtime)

    val resultTable = left
      .leftOuterJoin(
        right,
        'a === 'd && 'lrtime >= 'rrtime - 5.minutes && 'lrtime < 'rrtime + 3.seconds)
      .select('a, 'e, 'lrtime)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testProcTimeWindowLeftOuterJoin(): Unit = {
    val util = streamTestUtil()
    val left = util.addDataStream[(Long, Int, String)]("T1",'a, 'b, 'c, 'lptime.proctime)

    val right = util.addDataStream[(Long, Int, String)]("T2",'d, 'e, 'f, 'rptime.proctime)

    val resultTable = left
      .leftOuterJoin(right, 'a === 'd && 'lptime >= 'rptime - 1.second && 'lptime < 'rptime)
      .select('a, 'e, 'lptime)

    util.verifyExecPlan(resultTable)
  }

  // Tests for right outer join
  @Test
  def testRowTimeWindowRightOuterJoin(): Unit = {
    val util = streamTestUtil()
    val left = util.addDataStream[(Long, Int, String)]("T1",'a, 'b, 'c, 'lrtime.rowtime)

    val right = util.addDataStream[(Long, Int, String)]("T2",'d, 'e, 'f, 'rrtime.rowtime)

    val resultTable = left
      .rightOuterJoin(
        right,
        'a === 'd && 'lrtime >= 'rrtime - 5.minutes && 'lrtime < 'rrtime + 3.seconds)
      .select('a, 'e, 'lrtime)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testProcTimeWindowRightOuterJoin(): Unit = {
    val util = streamTestUtil()
    val left = util.addDataStream[(Long, Int, String)]("T1",'a, 'b, 'c, 'lptime.proctime)

    val right = util.addDataStream[(Long, Int, String)]("T2",'d, 'e, 'f, 'rptime.proctime)

    val resultTable = left
      .rightOuterJoin(right, 'a === 'd && 'lptime >= 'rptime - 1.second && 'lptime < 'rptime)
      .select('a, 'e, 'lptime)

    util.verifyExecPlan(resultTable)
  }

  // Tests for full outer join
  @Test
  def testRowTimeWindowFullOuterJoin(): Unit = {
    val util = streamTestUtil()
    val left = util.addDataStream[(Long, Int, String)]("T1",'a, 'b, 'c, 'lrtime.rowtime)

    val right = util.addDataStream[(Long, Int, String)]("T2",'d, 'e, 'f, 'rrtime.rowtime)

    val resultTable = left
      .fullOuterJoin(
        right,
        'a === 'd && 'lrtime >= 'rrtime - 5.minutes && 'lrtime < 'rrtime + 3.seconds)
      .select('a, 'e, 'lrtime)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testProcTimeWindowFullOuterJoin(): Unit = {
    val util = streamTestUtil()
    val left = util.addDataStream[(Long, Int, String)]("T1",'a, 'b, 'c, 'lptime.proctime)

    val right = util.addDataStream[(Long, Int, String)]("T2", 'd, 'e, 'f, 'rptime.proctime)

    val resultTable = left
      .fullOuterJoin(right, 'a === 'd && 'lptime >= 'rptime - 1.second && 'lptime < 'rptime)
      .select('a, 'e, 'lptime)

    util.verifyExecPlan(resultTable)
  }

  // Test for outer join optimization
  @Test
  def testRowTimeWindowOuterJoinOpt(): Unit = {
    val util = streamTestUtil()
    val left = util.addDataStream[(Long, Int, String)]("T1",'a, 'b, 'c, 'lrtime.rowtime)

    val right = util.addDataStream[(Long, Int, String)]("T2", 'd, 'e, 'f, 'rrtime.rowtime)

    val resultTable = left.leftOuterJoin(right)
      .where('a === 'd && 'lrtime >= 'rrtime - 5.minutes && 'lrtime < 'rrtime + 3.seconds)
      .select('a, 'e, 'lrtime)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testLeftOuterJoinEquiPred(): Unit = {
    val util = streamTestUtil()
    val t = util.addDataStream[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addDataStream[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.leftOuterJoin(s, 'a === 'z).select('b, 'y)

    util.verifyExecPlan(joined)
  }

  @Test
  def testLeftOuterJoinEquiAndLocalPred(): Unit = {
    val util = streamTestUtil()
    val t = util.addDataStream[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addDataStream[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.leftOuterJoin(s, 'a === 'z && 'b < 2).select('b, 'y)

    util.verifyExecPlan(joined)
  }

  @Test
  def testLeftOuterJoinEquiAndNonEquiPred(): Unit = {
    val util = streamTestUtil()
    val t = util.addDataStream[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addDataStream[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.leftOuterJoin(s, 'a === 'z && 'b < 'x).select('b, 'y)

    util.verifyExecPlan(joined)
  }

  @Test
  def testRightOuterJoinEquiPred(): Unit = {
    val util = streamTestUtil()
    val t = util.addDataStream[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addDataStream[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.rightOuterJoin(s, 'a === 'z).select('b, 'y)

    util.verifyExecPlan(joined)
  }

  @Test
  def testRightOuterJoinEquiAndLocalPred(): Unit = {
    val util = streamTestUtil()
    val t = util.addDataStream[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addDataStream[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.rightOuterJoin(s, 'a === 'z && 'x < 2).select('b, 'x)

    util.verifyExecPlan(joined)
  }

  @Test
  def testRightOuterJoinEquiAndNonEquiPred(): Unit = {
    val util = streamTestUtil()
    val t = util.addDataStream[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addDataStream[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.rightOuterJoin(s, 'a === 'z && 'b < 'x).select('b, 'y)

    util.verifyExecPlan(joined)
  }
}
