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

package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.logical.{SessionGroupWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.{Ignore, Test}

class DistinctAggregateTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)](
    "MyTable",
    'a, 'b, 'c,
    'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testDistinct(): Unit = {
    val sql = "SELECT DISTINCT a, b, c FROM MyTable"

    val expected =
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a, b, c")
        ),
        term("groupBy", "a, b, c"),
        term("select", "a, b, c")
      )
    streamUtil.verifySql(sql, expected)
  }

  // TODO: this query should be optimized to only have a single DataStreamGroupAggregate
  // TODO: reopen this until FLINK-7144 fixed
  @Ignore
  @Test
  def testDistinctAfterAggregate(): Unit = {
    val sql = "SELECT DISTINCT a FROM MyTable GROUP BY a, b, c"

    val expected =
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a")
        ),
        term("groupBy", "a"),
        term("select", "a")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testDistinctAggregate(): Unit = {
    val sqlQuery = "SELECT " +
      "  c, SUM(DISTINCT a), SUM(a), COUNT(DISTINCT b) " +
      "FROM MyTable " +
      "GROUP BY c "

    val expected =
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "c", "a", "b")
        ),
        term("groupBy", "c"),
        term("select", "c",
          "SUM(DISTINCT a) AS EXPR$1", "SUM(a) AS EXPR$2", "COUNT(DISTINCT b) AS EXPR$3")
      )
    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testDistinctAggregateOnTumbleWindow(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), " +
      "  SUM(a) " +
      "FROM MyTable " +
      "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE) "

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "rowtime", "a")
      ),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS EXPR$0", "SUM(a) AS EXPR$1")
    )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testMultiDistinctAggregateSameFieldOnHopWindow(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), " +
      "  SUM(DISTINCT a), " +
      "  MAX(DISTINCT a) " +
      "FROM MyTable " +
      "GROUP BY HOP(rowtime, INTERVAL '15' MINUTE, INTERVAL '1' HOUR) "

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "rowtime", "a")
      ),
      term("window", SlidingGroupWindow('w$, 'rowtime, 3600000.millis, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS EXPR$0", "SUM(DISTINCT a) AS EXPR$1",
        "MAX(DISTINCT a) AS EXPR$2")
    )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testDistinctAggregateWithGroupingOnSessionWindow(): Unit = {
    val sqlQuery = "SELECT a, " +
      "  COUNT(a), " +
      "  SUM(DISTINCT c) " +
      "FROM MyTable " +
      "GROUP BY a, SESSION(rowtime, INTERVAL '15' MINUTE) "

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "a", "rowtime", "c")
      ),
      term("groupBy", "a"),
      term("window", SessionGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "a", "COUNT(a) AS EXPR$1", "SUM(DISTINCT c) AS EXPR$2")
    )

    streamUtil.verifySql(sqlQuery, expected)
  }
}
