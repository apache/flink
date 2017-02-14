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
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.logical.EventTimeTumblingGroupWindow
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class WindowAggregateTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c)

  @Test
  def testNonPartitionedTumbleWindow() = {
    val sql = "SELECT COUNT(*) FROM MyTable GROUP BY FLOOR(rowtime() TO HOUR)"
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "1970-01-01 00:00:00 AS $f0")
          ),
          term("window", EventTimeTumblingGroupWindow(None, 'rowtime, 3600000.millis)),
          term("select", "COUNT(*) AS EXPR$0")
        ),
        term("select", "EXPR$0")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testPartitionedTumbleWindow1() = {
    val sql = "SELECT a, COUNT(*) FROM MyTable GROUP BY a, FLOOR(rowtime() TO MINUTE)"
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "1970-01-01 00:00:00 AS $f1")
          ),
          term("groupBy", "a"),
          term("window", EventTimeTumblingGroupWindow(None, 'rowtime, 60000.millis)),
          term("select", "a", "COUNT(*) AS EXPR$1")
        ),
        term("select", "a", "EXPR$1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testPartitionedTumbleWindow2() = {
    val sql = "SELECT a, SUM(c), b FROM MyTable GROUP BY a, FLOOR(rowtime() TO SECOND), b"
    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "1970-01-01 00:00:00 AS $f1, b, c")
          ),
          term("groupBy", "a, b"),
          term("window", EventTimeTumblingGroupWindow(None, 'rowtime, 1000.millis)),
          term("select", "a", "b", "SUM(c) AS EXPR$1")
        ),
        term("select", "a", "EXPR$1", "b")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test(expected = classOf[TableException])
  def testMultiWindow() = {
    val sql = "SELECT COUNT(*) FROM MyTable GROUP BY " +
      "FLOOR(rowtime() TO HOUR), FLOOR(rowtime() TO MINUTE)"
    val expected = ""
    streamUtil.verifySql(sql, expected)
  }

  @Test(expected = classOf[TableException])
  def testInvalidWindowExpression() = {
    val sql = "SELECT COUNT(*) FROM MyTable GROUP BY FLOOR(localTimestamp TO HOUR)"
    val expected = ""
    streamUtil.verifySql(sql, expected)
  }
}
