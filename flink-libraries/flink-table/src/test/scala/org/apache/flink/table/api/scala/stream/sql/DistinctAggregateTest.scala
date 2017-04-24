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

package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.logical.TumblingGroupWindow
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class DistinctAggregateTest extends TableTestBase {

  @Test
  def testSingleDistinctAggregate(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'rowtime.rowtime)

    val sqlQuery = "SELECT COUNT(DISTINCT a) FROM MyTable " +
      "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "1970-01-01 00:00:00 AS $f0", "a")
      ),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS EXPR$0")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testMultiDistinctAggregateOnSameColumn(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'rowtime.rowtime)

    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT a), MAX(DISTINCT a) FROM MyTable " +
      "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "1970-01-01 00:00:00 AS $f0", "a")
      ),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS EXPR$0", "SUM(DISTINCT a) AS EXPR$1",
        "MAX(DISTINCT a) AS EXPR$2")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testSingleDistinctAggregateAndOneOrMultiNonDistinctAggregate(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'rowtime.rowtime)

    // case 0x00: DISTINCT on COUNT and Non-DISTINCT on others
    val sqlQuery0 = "SELECT COUNT(DISTINCT a), SUM(c) FROM MyTable " +
      "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    val expected0 = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "1970-01-01 00:00:00 AS $f0", "a", "c")
      ),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS EXPR$0", "SUM(c) AS EXPR$1")
    )

    util.verifySql(sqlQuery0, expected0)

    // case 0x01: Non-DISTINCT on COUNT and DISTINCT on others
    val sqlQuery1 = "SELECT COUNT(a), SUM(DISTINCT c) FROM MyTable" +
      " GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    val expected1 = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "1970-01-01 00:00:00 AS $f0", "a", "c")
      ),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "COUNT(a) AS EXPR$0", "SUM(DISTINCT c) AS EXPR$1")
    )

    util.verifySql(sqlQuery1, expected1)
  }

  @Test
  def testMultiDistinctAggregateOnDifferentColumn(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'rowtime.rowtime)

    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT c) FROM MyTable " +
      "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "1970-01-01 00:00:00 AS $f0", "a", "c")
      ),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS EXPR$0", "SUM(DISTINCT c) AS EXPR$1")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testMultiDistinctAndNonDistinctAggregateOnDifferentColumn(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'rowtime.rowtime)

    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT c), COUNT(b) FROM MyTable " +
      "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(0),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS EXPR$0", "SUM(DISTINCT c) AS EXPR$1",
        "COUNT(b) AS EXPR$2")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testSingleDistinctAggregateWithGrouping(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'rowtime.rowtime)

    val sqlQuery = "SELECT a, COUNT(a), SUM(DISTINCT c) FROM MyTable " +
      "GROUP BY a, TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "a", "1970-01-01 00:00:00 AS $f1", "c")
      ),
      term("groupBy", "a"),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "a", "COUNT(a) AS EXPR$1", "SUM(DISTINCT c) AS EXPR$2")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testSingleDistinctAggregateWithGroupingAndCountStar(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'rowtime.rowtime)

    val sqlQuery = "SELECT a, COUNT(*), SUM(DISTINCT c) FROM MyTable " +
      "GROUP BY a, TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "a", "1970-01-01 00:00:00 AS $f1", "c")
      ),
      term("groupBy", "a"),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "a", "COUNT(*) AS EXPR$1", "SUM(DISTINCT c) AS EXPR$2")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testTwoDistinctAggregateWithGroupingAndCountStar(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'rowtime.rowtime)

    val sqlQuery = "SELECT a, COUNT(*), SUM(DISTINCT c), COUNT(DISTINCT c) FROM MyTable " +
      "GROUP BY a, TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "a", "1970-01-01 00:00:00 AS $f1", "c")
      ),
      term("groupBy", "a"),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "a", "COUNT(*) AS EXPR$1", "SUM(DISTINCT c) AS EXPR$2",
        "COUNT(DISTINCT c) AS EXPR$3")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testTwoDifferentDistinctAggregateWithGroupingAndCountStar(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'rowtime.rowtime)

    val sqlQuery = "SELECT a, COUNT(*), SUM(DISTINCT c), COUNT(DISTINCT b) FROM MyTable " +
      "GROUP BY a, TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      streamTableNode(0),
      term("groupBy", "a"),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "a", "COUNT(*) AS EXPR$1", "SUM(DISTINCT c) AS EXPR$2",
        "COUNT(DISTINCT b) AS EXPR$3")
    )

    util.verifySql(sqlQuery, expected)
  }
}
