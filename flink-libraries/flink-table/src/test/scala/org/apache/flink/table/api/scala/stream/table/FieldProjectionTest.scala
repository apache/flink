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
package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{Upper, WindowReference}
import org.apache.flink.table.plan.logical.TumblingGroupWindow
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{TableTestBase, _}
import org.junit.Test

/**
  * Tests for all the situations when we can do fields projection. Like selecting few fields
  * from a large field count source.
  */
class FieldProjectionTest extends TableTestBase {

  val streamUtil: StreamTableTestUtil = streamTestUtil()

  @Test
  def testSelectFromWindow(): Unit = {
    val sourceTable = streamUtil
      .addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd, 'rowtime.rowtime)
    val resultTable = sourceTable
        .window(Tumble over 5.millis on 'rowtime as 'w)
        .groupBy('w)
        .select(Upper('c).count, 'a.sum)

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "c", "a", "rowtime", "UPPER(c) AS $f3")
        ),
        term("window",
          TumblingGroupWindow(
            WindowReference("w"),
            'rowtime,
            5.millis)),
        term("select", "COUNT($f3) AS TMP_0", "SUM(a) AS TMP_1")
      )

    streamUtil.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFromGroupedWindow(): Unit = {
    val sourceTable = streamUtil
      .addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd, 'rowtime.rowtime)
    val resultTable = sourceTable
        .window(Tumble over 5.millis on 'rowtime as 'w)
        .groupBy('w, 'b)
        .select(Upper('c).count, 'a.sum, 'b)

    val expected = unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "c", "a", "b", "rowtime", "UPPER(c) AS $f4")
          ),
          term("groupBy", "b"),
          term("window",
            TumblingGroupWindow(
              WindowReference("w"),
              'rowtime,
              5.millis)),
          term("select", "b", "COUNT($f4) AS TMP_0", "SUM(a) AS TMP_1")
        ),
        term("select", "TMP_0", "TMP_1", "b")
    )

    streamUtil.verifyTable(resultTable, expected)
  }

}


