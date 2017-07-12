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

package org.apache.flink.table.calcite.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.WindowReference
import org.apache.flink.table.plan.logical.TumblingGroupWindow
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

/**
  * Tests for [[org.apache.flink.table.calcite.RelTimeIndicatorConverter]].
  */
class RelTimeIndicatorConverterTest extends TableTestBase {

  @Test
  def testGroupingOnProctime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int)]("MyTable" , 'long, 'int, 'proctime.proctime)

    val result = util.tableEnv.sql("SELECT COUNT(long) FROM MyTable GROUP BY proctime")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "TIME_MATERIALIZATION(proctime) AS proctime", "long")
        ),
        term("groupBy", "proctime"),
        term("select", "proctime", "COUNT(long) AS EXPR$0")
      ),
      term("select", "EXPR$0")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testAggregationOnProctime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int)]("MyTable" , 'long, 'int, 'proctime.proctime)

    val result = util.tableEnv.sql("SELECT MIN(proctime) FROM MyTable GROUP BY long")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "long", "TIME_MATERIALIZATION(proctime) AS proctime")
        ),
        term("groupBy", "long"),
        term("select", "long", "MIN(proctime) AS EXPR$0")
      ),
      term("select", "EXPR$0")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testWindow(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Long, Int)]("MyTable", 'rowtime.rowtime, 'long, 'int)

    val result = util.tableEnv.sql(
      "SELECT TUMBLE_END(rowtime, INTERVAL '0.1' SECOND) AS `rowtime`, `long`, " +
        "SUM(`int`) FROM MyTable " +
        "GROUP BY `long`, TUMBLE(rowtime, INTERVAL '0.1' SECOND)")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        streamTableNode(0),
        term("groupBy", "long"),
        term(
          "window",
          TumblingGroupWindow(
            WindowReference("w$"),
            'rowtime,
            100.millis)),
        term("select", "long", "SUM(int) AS EXPR$2", "start('w$) AS w$start", "end('w$) AS w$end")
      ),
      term("select", "w$end", "long", "EXPR$2")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testWindowWithAggregationOnRowtime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Long, Int)]("MyTable", 'rowtime.rowtime, 'long, 'int)

    val result = util.tableEnv.sql("SELECT MIN(rowtime), long FROM MyTable " +
      "GROUP BY long, TUMBLE(rowtime, INTERVAL '0.1' SECOND)")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "long", "rowtime", "TIME_MATERIALIZATION(rowtime) AS $f2")
        ),
        term("groupBy", "long"),
        term(
          "window",
          TumblingGroupWindow(
            'w$,
            'rowtime,
            100.millis)),
        term("select", "long", "MIN($f2) AS EXPR$0")
      ),
      term("select", "EXPR$0", "long")
    )

    util.verifyTable(result, expected)
  }

}
