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

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{TimeIntervalUnit, WindowReference}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.plan.TimeIndicatorConversionTest.TableFunc
import org.apache.flink.table.plan.logical.TumblingGroupWindow
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

/**
  * Tests for [[org.apache.flink.table.calcite.RelTimeIndicatorConverter]].
  */
class TimeIndicatorConversionTest extends TableTestBase {

  @Test
  def testSimpleMaterialization(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int, 'proctime.proctime)

    val result = t
      .select('rowtime.floor(TimeIntervalUnit.DAY) as 'rowtime, 'long)
      .filter('long > 0)
      .select('rowtime)

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select", "FLOOR(CAST(rowtime)", "FLAG(DAY)) AS rowtime"),
      term("where", ">(long, 0)")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testSelectAll(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int, 'proctime.proctime)

    val result = t.select('*)

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select", "rowtime", "long", "int",
        "PROCTIME(proctime) AS proctime")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testFilteringOnRowtime(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int)

    val result = t
      .filter('rowtime > "1990-12-02 12:11:11".toTimestamp)
      .select('rowtime)

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select", "rowtime"),
      term("where", ">(rowtime, 1990-12-02 12:11:11)")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testGroupingOnRowtime(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int, 'proctime.proctime)

    val result = t
      .groupBy('rowtime)
      .select('long.count)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "long", "CAST(rowtime) AS rowtime")
        ),
        term("groupBy", "rowtime"),
        term("select", "rowtime", "COUNT(long) AS TMP_0")
      ),
      term("select", "TMP_0")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testAggregationOnRowtime(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int)

    val result = t
      .groupBy('long)
      .select('rowtime.min)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "CAST(rowtime) AS rowtime", "long")
        ),
        term("groupBy", "long"),
        term("select", "long", "MIN(rowtime) AS TMP_0")
      ),
      term("select", "TMP_0")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testTableFunction(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int, 'proctime.proctime)
    val func = new TableFunc

    val result = t.join(func('rowtime, 'proctime, "") as 's).select('rowtime, 'proctime, 's)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
        term("invocation",
          s"${func.functionIdentifier}(CAST($$0):TIMESTAMP(3) NOT NULL, PROCTIME($$3), '')"),
        term("correlate", s"table(TableFunc(CAST(rowtime), PROCTIME(proctime), ''))"),
        term("select", "rowtime", "long", "int", "proctime", "s"),
        term("rowType", "RecordType(TIME ATTRIBUTE(ROWTIME) rowtime, BIGINT long, INTEGER int, " +
          "TIME ATTRIBUTE(PROCTIME) proctime, VARCHAR(65536) s)"),
        term("joinType", "INNER")
      ),
      term("select", "rowtime", "PROCTIME(proctime) AS proctime", "s")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testWindow(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int)

    val result = t
      .window(Tumble over 100.millis on 'rowtime as 'w)
      .groupBy('w, 'long)
      .select('w.end as 'rowtime, 'long, 'int.sum)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        streamTableNode(0),
        term("groupBy", "long"),
        term(
          "window",
          TumblingGroupWindow(
            'w,
            'rowtime,
            100.millis)),
        term("select", "long", "SUM(int) AS TMP_1", "end('w) AS TMP_0")
      ),
      term("select", "TMP_0 AS rowtime", "long", "TMP_1")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testUnion(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]("MyTable", 'rowtime.rowtime, 'long, 'int)

    val result = t.unionAll(t).select('rowtime)

    val expected = binaryNode(
      "DataStreamUnion",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "rowtime")
      ),
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "rowtime")
      ),
      term("all", "true"),
      term("union all", "rowtime")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testMultiWindow(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Long, Long, Int)]('rowtime.rowtime, 'long, 'int)

    val result = t
      .window(Tumble over 100.millis on 'rowtime as 'w)
      .groupBy('w, 'long)
      .select('w.rowtime as 'newrowtime, 'long, 'int.sum as 'int)
      .window(Tumble over 1.second on 'newrowtime as 'w2)
      .groupBy('w2, 'long)
      .select('w2.end, 'long, 'int.sum)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          unaryNode(
            "DataStreamGroupWindowAggregate",
            streamTableNode(0),
            term("groupBy", "long"),
            term(
              "window",
              TumblingGroupWindow(
                'w,
                'rowtime,
                100.millis)),
            term("select", "long", "SUM(int) AS TMP_1", "rowtime('w) AS TMP_0")
          ),
          term("select", "TMP_0 AS newrowtime", "long", "TMP_1 AS int")
        ),
        term("groupBy", "long"),
        term(
          "window",
          TumblingGroupWindow(
            'w2,
            'newrowtime,
            1000.millis)),
        term("select", "long", "SUM(int) AS TMP_3", "end('w2) AS TMP_2")
      ),
      term("select", "TMP_2", "long", "TMP_3")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testGroupingOnProctime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int)]("MyTable" , 'long, 'int, 'proctime.proctime)

    val result = util.tableEnv.sqlQuery("SELECT COUNT(long) FROM MyTable GROUP BY proctime")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "PROCTIME(proctime) AS proctime", "long")
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

    val result = util.tableEnv.sqlQuery("SELECT MIN(proctime) FROM MyTable GROUP BY long")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "long", "PROCTIME(proctime) AS proctime")
        ),
        term("groupBy", "long"),
        term("select", "long", "MIN(proctime) AS EXPR$0")
      ),
      term("select", "EXPR$0")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testWindowSql(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Long, Int)]("MyTable", 'rowtime.rowtime, 'long, 'int)

    val result = util.tableEnv.sqlQuery(
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
        term("select",
          "long",
          "SUM(int) AS EXPR$2",
          "start('w$) AS w$start",
          "end('w$) AS w$end",
          "rowtime('w$) AS w$rowtime",
          "proctime('w$) AS w$proctime")
      ),
      term("select", "w$end AS rowtime", "long", "EXPR$2")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testWindowWithAggregationOnRowtime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Long, Int)]("MyTable", 'rowtime.rowtime, 'long, 'int)

    val result = util.tableEnv.sqlQuery("SELECT MIN(rowtime), long FROM MyTable " +
      "GROUP BY long, TUMBLE(rowtime, INTERVAL '0.1' SECOND)")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "long", "rowtime", "CAST(rowtime) AS rowtime0")
        ),
        term("groupBy", "long"),
        term(
          "window",
          TumblingGroupWindow(
            'w$,
            'rowtime,
            100.millis)),
        term("select", "long", "MIN(rowtime0) AS EXPR$0")
      ),
      term("select", "EXPR$0", "long")
    )

    util.verifyTable(result, expected)
  }

}

object TimeIndicatorConversionTest {

  class TableFunc extends TableFunction[String] {
    val t = new Timestamp(0L)
    def eval(time1: Long, time2: Timestamp, string: String): Unit = {
      collect(time1.toString + time2.after(t) + string)
    }
  }
}
