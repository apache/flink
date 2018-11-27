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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.logical.{SessionGroupWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.utils.TableTestUtil.{streamTableNode, term, unaryNode}
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class DistinctAggregateTest extends TableTestBase {
  val util = streamTestUtil()
  val table = util.addTable[(Int, Long, String)](
    "MyTable",
    'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testDistinctAggregate(): Unit = {
    val result = table
      .groupBy('c)
      .select('c, 'a.sum.distinct, 'a.sum, 'b.count.distinct)

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
             "SUM(DISTINCT a) AS TMP_0", "SUM(a) AS TMP_1", "COUNT(DISTINCT b) AS TMP_2")
      )
    util.verifyTable(result, expected)
  }

  @Test
  def testDistinctAggregateOnTumbleWindow(): Unit = {
    val result = table
      .window(Tumble over 15.minute on 'rowtime as 'w)
      .groupBy('w)
      .select('a.count.distinct, 'a.sum)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "a", "rowtime")
      ),
      term("window", TumblingGroupWindow('w, 'rowtime, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS TMP_0", "SUM(a) AS TMP_1")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testMultiDistinctAggregateSameFieldOnHopWindow(): Unit = {
    val result = table
      .window(Slide over 1.hour every 15.minute on 'rowtime as 'w)
      .groupBy('w)
      .select('a.count.distinct, 'a.sum.distinct, 'a.max.distinct)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "a", "rowtime")
      ),
      term("window", SlidingGroupWindow('w, 'rowtime, 3600000.millis, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS TMP_0", "SUM(DISTINCT a) AS TMP_1",
           "MAX(DISTINCT a) AS TMP_2")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testDistinctAggregateWithGroupingOnSessionWindow(): Unit = {
    val result = table
      .window(Session withGap 15.minute on 'rowtime as 'w)
      .groupBy('a, 'w)
      .select('a, 'a.count, 'c.count.distinct)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "a", "c", "rowtime")
      ),
      term("groupBy", "a"),
      term("window", SessionGroupWindow('w, 'rowtime, 900000.millis)),
      term("select", "a", "COUNT(a) AS TMP_0", "COUNT(DISTINCT c) AS TMP_1")
    )

    util.verifyTable(result, expected)
  }
}
