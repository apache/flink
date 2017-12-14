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

package org.apache.flink.table.api.stream.table

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

/**
  * Currently only time-windowed inner joins can be processed in a streaming fashion.
  */
class JoinTest extends TableTestBase {

  @Test
  def testRowTimeWindowInnerJoin(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, String)]('a, 'b, 'c, 'ltime.rowtime)
    val right = util.addTable[(Long, Int, String)]('d, 'e, 'f, 'rtime.rowtime)

    val resultTable = left.join(right)
      .where('a === 'd && 'ltime >= 'rtime - 5.minutes && 'ltime < 'rtime + 3.seconds)
      .select('a, 'e, 'ltime)

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamWindowJoin",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "ltime")
          ),
          unaryNode(
            "DataStreamCalc",
            streamTableNode(1),
            term("select", "d", "e", "rtime")
          ),
          term("where", "AND(=(a, d), >=(ltime, -(rtime, 300000))," +
            " <(ltime, DATETIME_PLUS(rtime, 3000)))"),
          term("join", "a", "ltime", "d", "e", "rtime"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "e", "ltime")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testProcTimeWindowInnerJoin(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, String)]('a, 'b, 'c, 'ltime.proctime)
    val right = util.addTable[(Long, Int, String)]('d, 'e, 'f, 'rtime.proctime)

    val resultTable = left.join(right)
      .where('a === 'd && 'ltime >= 'rtime - 1.second && 'ltime < 'rtime)
      .select('a, 'e, 'ltime)

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamWindowJoin",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "ltime")
          ),
          unaryNode(
            "DataStreamCalc",
            streamTableNode(1),
            term("select", "d", "e", "rtime")
          ),
          term("where", "AND(=(a, d), >=(ltime, -(rtime, 1000)), <(ltime, rtime))"),
          term("join", "a", "ltime", "d", "e", "rtime"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "e", "PROCTIME(ltime) AS ltime")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testProcTimeWindowInnerJoinWithEquiTimeAttrs(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, String)]('a, 'b, 'c, 'ltime.proctime)
    val right = util.addTable[(Long, Int, String)]('d, 'e, 'f, 'rtime.proctime)

    val resultTable = left.join(right)
      .where('a === 'd && 'ltime === 'rtime)
      .select('a, 'e, 'ltime)

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamWindowJoin",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "ltime")
          ),
          unaryNode(
            "DataStreamCalc",
            streamTableNode(1),
            term("select", "d", "e", "rtime")
          ),
          term("where", "AND(=(a, d), =(ltime, rtime))"),
          term("join", "a", "ltime", "d", "e", "rtime"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "e", "PROCTIME(ltime) AS ltime")
      )
    util.verifyTable(resultTable, expected)
  }

  /**
    * The time indicator can be accessed from non-time predicates now.
    */
  @Test
  def testInnerJoinWithTimeIndicatorAccessed(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, Timestamp)]('a, 'b, 'c, 'ltime.rowtime)
    val right = util.addTable[(Long, Int, Timestamp)]('d, 'e, 'f, 'rtime.rowtime)

    val resultTable = left.join(right)
      .where('a ==='d && 'ltime >= 'rtime - 5.minutes && 'ltime < 'rtime && 'ltime > 'f)

    val expected =
      binaryNode(
        "DataStreamWindowJoin",
        streamTableNode(0),
        streamTableNode(1),
        term("where", "AND(=(a, d), >=(ltime, -(rtime, 300000)), <(ltime, rtime), >(ltime, f))"),
        term("join", "a", "b", "c", "ltime", "d", "e", "f", "rtime"),
        term("joinType", "InnerJoin")
      )
    util.verifyTable(resultTable, expected)
  }

}
