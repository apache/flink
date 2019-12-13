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
package org.apache.flink.table.planner.runtime.batch.sql.agg

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.config.ExecutionConfigOptions.{TABLE_EXEC_DISABLED_OPERATORS, TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM}
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.utils.DateTimeTestUtil.localDateTime
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils.unixTimestampToLocalDateTime

import org.junit.{Before, Test}

import java.sql.Date
import java.time.LocalDateTime

import scala.collection.JavaConverters._
import scala.collection.Seq

class AggregateReduceGroupingITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("T1",
      Seq(row(2, 1, "A", null),
        row(3, 2, "A", "Hi"),
        row(5, 2, "B", "Hello"),
        row(6, 3, "C", "Hello world")),
      new RowTypeInfo(Types.INT, Types.INT, Types.STRING, Types.STRING),
      "a1, b1, c1, d1",
      Array(true, true, true, true),
      FlinkStatistic.builder().uniqueKeys(Set(Set("a1").asJava).asJava).build()
    )

    registerCollection("T2",
      Seq(row(1, 1, "X"),
        row(1, 2, "Y"),
        row(2, 3, null),
        row(2, 4, "Z")),
      new RowTypeInfo(Types.INT, Types.INT, Types.STRING),
      "a2, b2, c2",
      Array(true, true, true),
      FlinkStatistic.builder()
        .uniqueKeys(Set(Set("b2").asJava, Set("a2", "b2").asJava).asJava).build()
    )

    registerCollection("T3",
      Seq(row(1, 10, "Hi", 1L),
        row(2, 20, "Hello", 1L),
        row(2, 20, "Hello world", 2L),
        row(3, 10, "Hello world, how are you?", 1L),
        row(4, 20, "I am fine.", 2L),
        row(4, null, "Luke Skywalker", 2L)),
      new RowTypeInfo(Types.INT, Types.INT, Types.STRING, Types.LONG),
      "a3, b3, c3, d3",
      Array(true, true, true, true),
      FlinkStatistic.builder().uniqueKeys(Set(Set("a1").asJava).asJava).build()
    )

    registerCollection("T4",
      Seq(row(1, 1, "A", localDateTime("2018-06-01 10:05:30"), "Hi"),
        row(2, 1, "B", localDateTime("2018-06-01 10:10:10"), "Hello"),
        row(3, 2, "B", localDateTime("2018-06-01 10:15:25"), "Hello world"),
        row(4, 3, "C", localDateTime("2018-06-01 10:36:49"), "I am fine.")),
      new RowTypeInfo(Types.INT, Types.INT, Types.STRING, Types.LOCAL_DATE_TIME, Types.STRING),
      "a4, b4, c4, d4, e4",
      Array(true, true, true, true, true),
      FlinkStatistic.builder().uniqueKeys(Set(Set("a4").asJava).asJava).build()
    )

    registerCollection("T5",
      Seq(row(2, 1, "A", null),
        row(3, 2, "B", "Hi"),
        row(1, null, "C", "Hello"),
        row(4, 3, "D", "Hello world"),
        row(3, 1, "E", "Hello world, how are you?"),
        row(5, null, "F", null),
        row(7, 2, "I", "hahaha"),
        row(6, 1, "J", "I am fine.")),
      new RowTypeInfo(Types.INT, Types.INT, Types.STRING, Types.STRING),
      "a5, b5, c5, d5",
      Array(true, true, true, true),
      FlinkStatistic.builder().uniqueKeys(Set(Set("c5").asJava).asJava).build()
    )

    registerCollection("T6",
      (0 until 50000).map(
        i => row(i, 1L, if (i % 500 == 0) null else s"Hello$i", "Hello world", 10,
          unixTimestampToLocalDateTime(i + 1531820000000L).toLocalDate)),
      new RowTypeInfo(Types.INT, Types.LONG, Types.STRING,
        Types.STRING, Types.INT, Types.LOCAL_DATE),
      "a6, b6, c6, d6, e6, f6",
      Array(true, true, true, true, true, true),
      FlinkStatistic.builder().uniqueKeys(Set(Set("a6").asJava).asJava).build()
    )
    // HashJoin is disabled due to translateToPlanInternal method is not implemented yet
    tEnv.getConfig.getConfiguration.setString(TABLE_EXEC_DISABLED_OPERATORS, "HashJoin")
  }

  @Test
  def testSingleAggOnTable_SortAgg(): Unit = {
    tEnv.getConfig.getConfiguration.setString(TABLE_EXEC_DISABLED_OPERATORS, "HashAgg")
    testSingleAggOnTable()
    checkResult("SELECT a6, b6, max(c6), count(d6), sum(e6) FROM T6 GROUP BY a6, b6",
      (0 until 50000).map(i => row(i, 1L, if (i % 500 == 0) null else s"Hello$i", 1L, 10))
    )
  }

  @Test
  def testSingleAggOnTable_HashAgg_WithLocalAgg(): Unit = {
    tEnv.getConfig.getConfiguration.setString(TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")
    tEnv.getConfig.getConfiguration.setString(
      OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE")
    // set smaller parallelism to avoid MemoryAllocationException
    tEnv.getConfig.getConfiguration.setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2)
    testSingleAggOnTable()
  }

  @Test
  def testSingleAggOnTable_HashAgg_WithoutLocalAgg(): Unit = {
    tEnv.getConfig.getConfiguration.setString(TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")
    tEnv.getConfig.getConfiguration.setString(
      OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "ONE_PHASE")
    testSingleAggOnTable()
  }

  private def testSingleAggOnTable(): Unit = {
    // group by fix length
    checkResult("SELECT a1, b1, count(c1) FROM T1 GROUP BY a1, b1",
      Seq(row(2, 1, 1), row(3, 2, 1), row(5, 2, 1), row(6, 3, 1)))
    // group by string
    checkResult("SELECT a1, c1, count(d1), avg(b1) FROM T1 GROUP BY a1, c1",
      Seq(row(2, "A", 0, 1), row(3, "A", 1, 2), row(5, "B", 1, 2), row(6, "C", 1, 3)))
    checkResult("SELECT c5, d5, avg(b5), avg(a5) FROM T5 WHERE d5 IS NOT NULL GROUP BY c5, d5",
      Seq(row("B", "Hi", 2, 3), row("C", "Hello", null, 1),
        row("D", "Hello world", 3, 4), row("E", "Hello world, how are you?", 1, 3),
        row("I", "hahaha", 2, 7), row("J", "I am fine.", 1, 6)))
    // group by string with null
    checkResult("SELECT a1, d1, count(d1) FROM T1 GROUP BY a1, d1",
      Seq(row(2, null, 0), row(3, "Hi", 1), row(5, "Hello", 1), row(6, "Hello world", 1)))
    checkResult("SELECT c5, d5, avg(b5), avg(a5) FROM T5 GROUP BY c5, d5",
      Seq(row("A", null, 1, 2), row("B", "Hi", 2, 3), row("C", "Hello", null, 1),
        row("D", "Hello world", 3, 4), row("E", "Hello world, how are you?", 1, 3),
        row("F", null, null, 5), row("I", "hahaha", 2, 7), row("J", "I am fine.", 1, 6)))

    checkResult("SELECT a3, b3, count(c3) FROM T3 GROUP BY a3, b3",
      Seq(row(1, 10, 1), row(2, 20, 2), row(3, 10, 1), row(4, 20, 1), row(4, null, 1)))
    checkResult("SELECT a2, b2, count(c2) FROM T2 GROUP BY a2, b2",
      Seq(row(1, 1, 1), row(1, 2, 1), row(2, 3, 0), row(2, 4, 1)))

    // group by constants
    checkResult("SELECT a1, b1, count(c1) FROM T1 GROUP BY a1, b1, 1, true",
      Seq(row(2, 1, 1), row(3, 2, 1), row(5, 2, 1), row(6, 3, 1)))
    checkResult("SELECT count(c1) FROM T1 GROUP BY 1, true", Seq(row(4)))

    // large data, for hash agg mode it will fallback
    checkResult("SELECT a6, c6, avg(b6), count(d6), avg(e6) FROM T6 GROUP BY a6, c6",
      (0 until 50000).map(i => row(i, if (i % 500 == 0) null else s"Hello$i", 1, 1L, 10))
    )
    checkResult("SELECT a6, d6, avg(b6), count(c6), avg(e6) FROM T6 GROUP BY a6, d6",
      (0 until 50000).map(i => row(i, "Hello world", 1, if (i % 500 == 0) 0L else 1L, 10))
    )
    checkResult("SELECT a6, f6, avg(b6), count(c6), avg(e6) FROM T6 GROUP BY a6, f6",
      (0 until 50000).map(i => row(i, new Date(i + 1531820000000L), 1,
        if (i % 500 == 0) 0L else 1L, 10))
    )
  }

  @Test
  def testMultiAggs(): Unit = {
    checkResult("SELECT a1, b1, c1, d1, m, COUNT(*) FROM " +
      "(SELECT a1, b1, c1, COUNT(d1) AS d1, MAX(d1) AS m FROM T1 GROUP BY a1, b1, c1) t " +
      "GROUP BY a1, b1, c1, d1, m",
      Seq(row(2, 1, "A", 0, null, 1), row(3, 2, "A", 1, "Hi", 1),
        row(5, 2, "B", 1, "Hello", 1), row(6, 3, "C", 1, "Hello world", 1)))

    checkResult("SELECT a3, b3, c, s, COUNT(*) FROM " +
      "(SELECT a3, b3, COUNT(d3) AS c, SUM(d3) AS s, MAX(d3) AS m FROM T3 GROUP BY a3, b3) t " +
      "GROUP BY a3, b3, c, s",
      Seq(row(1, 10, 1, 1, 1), row(2, 20, 2, 3, 1), row(3, 10, 1, 1, 1),
        row(4, 20, 1, 2, 1), row(4, null, 1, 2, 1)))
  }

  @Test
  def testAggOnInnerJoin(): Unit = {
    checkResult("SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
      "(SELECT * FROM T1, T2 WHERE a1 = b2) t GROUP BY a1, b1, a2, b2",
      Seq(row(2, 1, 1, 2, 1), row(3, 2, 2, 3, 1)))

    checkResult("SELECT a2, b2, a3, b3, COUNT(c2) FROM " +
      "(SELECT * FROM T2, T3 WHERE b2 = a3) t GROUP BY a2, b2, a3, b3",
      Seq(row(1, 1, 1, 10, 1), row(1, 2, 2, 20, 2), row(2, 3, 3, 10, 0),
        row(2, 4, 4, 20, 1), row(2, 4, 4, null, 1)))

    checkResult("SELECT a1, b1, a2, b2, a3, b3, COUNT(c1) FROM " +
      "(SELECT * FROM T1, T2, T3 WHERE a1 = b2 AND a1 = a3) t GROUP BY a1, b1, a2, b2, a3, b3",
      Seq(row(2, 1, 1, 2, 2, 20, 2), row(3, 2, 2, 3, 3, 10, 1)))
  }

  @Test
  def testAggOnLeftJoin(): Unit = {
    checkResult("SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
      "(SELECT * FROM T1 LEFT JOIN T2 ON a1 = b2) t GROUP BY a1, b1, a2, b2",
      Seq(row(2, 1, 1, 2, 1), row(3, 2, 2, 3, 1),
        row(5, 2, null, null, 1), row(6, 3, null, null, 1)))

    checkResult("SELECT a1, b1, a3, b3, COUNT(c1) FROM " +
      "(SELECT * FROM T1 LEFT JOIN T3 ON a1 = a3) t GROUP BY a1, b1, a3, b3",
      Seq(row(2, 1, 2, 20, 2), row(3, 2, 3, 10, 1),
        row(5, 2, null, null, 1), row(6, 3, null, null, 1)))

    checkResult("SELECT a3, b3, a1, b1, COUNT(c1) FROM " +
      "(SELECT * FROM T3 LEFT JOIN T1 ON a1 = a3) t GROUP BY a3, b3, a1, b1",
      Seq(row(1, 10, null, null, 0), row(2, 20, 2, 1, 2), row(3, 10, 3, 2, 1),
        row(4, 20, null, null, 0), row(4, null, null, null, 0)))
  }

  @Test
  def testAggOnRightJoin(): Unit = {
    checkResult("SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
      "(SELECT * FROM T1 RIGHT JOIN T2 ON a1 = b2) t GROUP BY a1, b1, a2, b2",
      Seq(row(2, 1, 1, 2, 1), row(3, 2, 2, 3, 1),
        row(null, null, 1, 1, 0), row(null, null, 2, 4, 0)))

    checkResult("SELECT a1, b1, a3, b3, COUNT(c1) FROM " +
      "(SELECT * FROM T1 RIGHT JOIN T3 ON a1 = a3) t GROUP BY a1, b1, a3, b3",
      Seq(row(2, 1, 2, 20, 2), row(3, 2, 3, 10, 1), row(null, null, 1, 10, 0),
        row(null, null, 4, 20, 0), row(null, null, 4, null, 0)))

    checkResult("SELECT a3, b3, a1, b1, COUNT(c1) FROM " +
      "(SELECT * FROM T3 RIGHT JOIN T1 ON a1 = a3) t GROUP BY a3, b3, a1, b1",
      Seq(row(2, 20, 2, 1, 2), row(3, 10, 3, 2, 1),
        row(null, null, 5, 2, 1), row(null, null, 6, 3, 1)))
  }

  @Test
  def testAggOnFullJoin(): Unit = {
    checkResult("SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
      "(SELECT * FROM T1 FULL OUTER JOIN T2 ON a1 = b2) t GROUP BY a1, b1, a2, b2",
      Seq(row(2, 1, 1, 2, 1), row(3, 2, 2, 3, 1), row(5, 2, null, null, 1),
        row(6, 3, null, null, 1), row(null, null, 1, 1, 0), row(null, null, 2, 4, 0)))

    checkResult("SELECT a1, b1, a3, b3, COUNT(c1) FROM " +
      "(SELECT * FROM T1 FULL OUTER JOIN T3 ON a1 = a3) t GROUP BY a1, b1, a3, b3",
      Seq(row(2, 1, 2, 20, 2), row(3, 2, 3, 10, 1), row(5, 2, null, null, 1),
        row(6, 3, null, null, 1), row(null, null, 1, 10, 0), row(null, null, 4, 20, 0),
        row(null, null, 4, null, 0)))
  }

  @Test
  def testAggOnOver(): Unit = {
    checkResult("SELECT a1, b1, c, COUNT(d1) FROM " +
      "(SELECT a1, b1, d1, COUNT(*) OVER (PARTITION BY c1) AS c FROM T1) t GROUP BY a1, b1, c",
      Seq(row(2, 1, 2, 0), row(3, 2, 2, 1), row(5, 2, 1, 1), row(6, 3, 1, 1)))
  }

  @Test
  def testAggOnWindow(): Unit = {
    checkResult("SELECT a4, b4, COUNT(c4) FROM T4 " +
      "GROUP BY a4, b4, TUMBLE(d4, INTERVAL '15' MINUTE)",
      Seq(row(1, 1, 1), row(2, 1, 1), row(3, 2, 1), row(4, 3, 1)))

    checkResult("SELECT a4, c4, COUNT(b4), AVG(b4) FROM T4 " +
      "GROUP BY a4, c4, TUMBLE(d4, INTERVAL '15' MINUTE)",
      Seq(row(1, "A", 1, 1), row(2, "B", 1, 1), row(3, "B", 1, 2), row(4, "C", 1, 3)))

    checkResult("SELECT a4, e4, s, avg(ab), count(cb) FROM " +
      "(SELECT a4, e4, avg(b4) as ab, count(b4) AS cb, " +
      "TUMBLE_START(d4, INTERVAL '15' MINUTE) AS s, " +
      "TUMBLE_END(d4, INTERVAL '15' MINUTE) AS e FROM T4 " +
      "GROUP BY a4, e4, TUMBLE(d4, INTERVAL '15' MINUTE)) t GROUP BY a4, e4, s",
      Seq(row(1, "Hi", LocalDateTime.of(2018, 6, 1, 10, 0, 0), 1, 1),
        row(2, "Hello", LocalDateTime.of(2018, 6, 1, 10, 0, 0), 1, 1),
        row(3, "Hello world", LocalDateTime.of(2018, 6, 1, 10, 15, 0), 2, 1),
        row(4, "I am fine.", LocalDateTime.of(2018, 6, 1, 10, 30, 0), 3, 1)))

    checkResult("SELECT a4, c4, s, COUNT(b4) FROM " +
      "(SELECT a4, c4, avg(b4) AS b4, " +
      "TUMBLE_START(d4, INTERVAL '15' MINUTE) AS s, " +
      "TUMBLE_END(d4, INTERVAL '15' MINUTE) AS e FROM T4 " +
      "GROUP BY a4, c4, TUMBLE(d4, INTERVAL '15' MINUTE)) t GROUP BY a4, c4, s",
      Seq(row(1, "A", LocalDateTime.of(2018, 6, 1, 10, 0, 0), 1),
        row(2, "B", LocalDateTime.of(2018, 6, 1, 10, 0, 0), 1),
        row(3, "B", LocalDateTime.of(2018, 6, 1, 10, 15, 0), 1),
        row(4, "C", LocalDateTime.of(2018, 6, 1, 10, 30, 0), 1)))

    checkResult("SELECT a4, c4, e, COUNT(b4) FROM " +
      "(SELECT a4, c4, VAR_POP(b4) AS b4, " +
      "TUMBLE_START(d4, INTERVAL '15' MINUTE) AS s, " +
      "TUMBLE_END(d4, INTERVAL '15' MINUTE) AS e FROM T4 " +
      "GROUP BY a4, c4, TUMBLE(d4, INTERVAL '15' MINUTE)) t GROUP BY a4, c4, e",
      Seq(row(1, "A", LocalDateTime.of(2018, 6, 1, 10, 15, 0), 1),
        row(2, "B", LocalDateTime.of(2018, 6, 1, 10, 15, 0), 1),
        row(3, "B", LocalDateTime.of(2018, 6, 1, 10, 30, 0), 1),
        row(4, "C", LocalDateTime.of(2018, 6, 1, 10, 45, 0), 1)))

    checkResult("SELECT a4, b4, c4, COUNT(*) FROM " +
      "(SELECT a4, c4, SUM(b4) AS b4, " +
      "TUMBLE_START(d4, INTERVAL '15' MINUTE) AS s, " +
      "TUMBLE_END(d4, INTERVAL '15' MINUTE) AS e FROM T4 " +
      "GROUP BY a4, c4, TUMBLE(d4, INTERVAL '15' MINUTE)) t GROUP BY a4, b4, c4",
      Seq(row(1, 1, "A", 1), row(2, 1, "B", 1), row(3, 2, "B", 1), row(4, 3, "C", 1)))
  }

  @Test
  def testAggWithGroupingSets(): Unit = {
    checkResult("SELECT a1, b1, c1, COUNT(d1) FROM T1 " +
      "GROUP BY GROUPING SETS ((a1, b1), (a1, c1))",
      Seq(row(2, 1, null, 0), row(2, null, "A", 0), row(3, 2, null, 1),
        row(3, null, "A", 1), row(5, 2, null, 1), row(5, null, "B", 1),
        row(6, 3, null, 1), row(6, null, "C", 1)))

    checkResult("SELECT a1, c1, COUNT(d1) FROM T1 " +
      "GROUP BY GROUPING SETS ((a1, c1), (a1), ())",
      Seq(row(2, "A", 0), row(2, null, 0), row(3, "A", 1), row(3, null, 1), row(5, "B", 1),
        row(5, null, 1), row(6, "C", 1), row(6, null, 1), row(null, null, 3)))

    checkResult("SELECT a1, b1, c1, COUNT(d1) FROM T1 " +
      "GROUP BY GROUPING SETS ((a1, b1, c1), (a1, b1, d1))",
      Seq(row(2, 1, "A", 0), row(2, 1, null, 0), row(3, 2, "A", 1), row(3, 2, null, 1),
        row(5, 2, "B", 1), row(5, 2, null, 1), row(6, 3, "C", 1), row(6, 3, null, 1)))
  }

  @Test
  def testAggWithRollup(): Unit = {
    checkResult("SELECT a1, b1, c1, COUNT(d1) FROM T1 GROUP BY ROLLUP (a1, b1, c1)",
      Seq(row(2, 1, "A", 0), row(2, 1, null, 0), row(2, null, null, 0), row(3, 2, "A", 1),
        row(3, 2, null, 1), row(3, null, null, 1), row(5, 2, "B", 1), row(5, 2, null, 1),
        row(5, null, null, 1), row(6, 3, "C", 1), row(6, 3, null, 1), row(6, null, null, 1),
        row(null, null, null, 3)))
  }

  @Test
  def testAggWithCube(): Unit = {
    checkResult("SELECT a1, b1, c1, COUNT(d1) FROM T1 GROUP BY CUBE (a1, b1, c1)",
      Seq(row(2, 1, "A", 0), row(2, 1, null, 0), row(2, null, "A", 0), row(2, null, null, 0),
        row(3, 2, "A", 1), row(3, 2, null, 1), row(3, null, "A", 1), row(3, null, null, 1),
        row(5, 2, "B", 1), row(5, 2, null, 1), row(5, null, "B", 1), row(5, null, null, 1),
        row(6, 3, "C", 1), row(6, 3, null, 1), row(6, null, "C", 1), row(6, null, null, 1),
        row(null, 1, "A", 0), row(null, 1, null, 0), row(null, 2, "A", 1), row(null, 2, "B", 1),
        row(null, 2, null, 2), row(null, 3, "C", 1), row(null, 3, null, 1), row(null, null, "A", 1),
        row(null, null, "B", 1), row(null, null, "C", 1), row(null, null, null, 3)))
  }

  @Test
  def testSingleDistinctAgg(): Unit = {
    checkResult("SELECT a1, COUNT(DISTINCT c1) FROM T1 GROUP BY a1",
      Seq(row(2, 1), row(3, 1), row(5, 1), row(6, 1)))

    checkResult("SELECT a1, b1, COUNT(DISTINCT c1) FROM T1 GROUP BY a1, b1",
      Seq(row(2, 1, 1), row(3, 2, 1), row(5, 2, 1), row(6, 3, 1)))
  }

  @Test
  def testSingleDistinctAgg_WithNonDistinctAgg(): Unit = {
    checkResult("SELECT a1, COUNT(DISTINCT c1), SUM(b1) FROM T1 GROUP BY a1",
      Seq(row(2, 1, 1), row(3, 1, 2), row(5, 1, 2), row(6, 1, 3)))

    checkResult("SELECT a1, c1, COUNT(DISTINCT c1), SUM(b1) FROM T1 GROUP BY a1, c1",
      Seq(row(2, "A", 1, 1), row(3, "A", 1, 2), row(5, "B", 1, 2), row(6, "C", 1, 3)))

    checkResult("SELECT a1, COUNT(DISTINCT c1), SUM(b1) FROM T1 GROUP BY a1",
      Seq(row(2, 1, 1), row(3, 1, 2), row(5, 1, 2), row(6, 1, 3)))

    checkResult("SELECT a1, d1, COUNT(DISTINCT c1), SUM(b1) FROM T1 GROUP BY a1, d1",
      Seq(row(2, null, 1, 1), row(3, "Hi", 1, 2),
        row(5, "Hello", 1, 2), row(6, "Hello world", 1, 3)))
  }

  @Test
  def testMultiDistinctAggs(): Unit = {
    checkResult("SELECT a1, COUNT(DISTINCT b1), SUM(DISTINCT b1) FROM T1 GROUP BY a1", Seq(row(2,
      1, 1), row(3, 1, 2), row(5, 1, 2), row(6, 1, 3)))

    checkResult("SELECT a1, d1, COUNT(DISTINCT c1), SUM(DISTINCT b1) FROM T1 GROUP BY a1, d1",
      Seq(row(2, null, 1, 1), row(3, "Hi", 1, 2),
        row(5, "Hello", 1, 2), row(6, "Hello world", 1, 3)))

    checkResult(
      "SELECT a1, SUM(DISTINCT b1), MAX(DISTINCT b1), MIN(DISTINCT c1) FROM T1 GROUP BY a1",
      Seq(row(2, 1, 1, "A"), row(3, 2, 2, "A"), row(5, 2, 2, "B"), row(6, 3, 3, "C")))

    checkResult(
      "SELECT a1, d1, COUNT(DISTINCT c1), MAX(DISTINCT b1), SUM(b1) FROM T1 GROUP BY a1, d1",
      Seq(row(2, null, 1, 1, 1), row(3, "Hi", 1, 2, 2),
        row(5, "Hello", 1, 2, 2), row(6, "Hello world", 1, 3, 3)))

    checkResult("SELECT a1, b1, COUNT(DISTINCT c1), COUNT(DISTINCT d1) FROM T1 GROUP BY a1, b1",
      Seq(row(2, 1, 1, 0), row(3, 2, 1, 1), row(5, 2, 1, 1), row(6, 3, 1, 1)))
  }

}
