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
package org.apache.flink.table.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.utils.StreamingWithAggTestBase.AggMode
import org.apache.flink.table.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils.{CommonTestData, StreamingWithAggTestBase, TestingRetractSink}
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._
import scala.collection.{Seq, mutable}

@RunWith(classOf[Parameterized])
class FlinkAggregateRemoveRuleITCase(
    aggMode: AggMode,
    minibatch: MiniBatchMode,
    backend: StateBackendMode)
  extends StreamingWithAggTestBase(aggMode, minibatch, backend) {

  @Test
  def testSimple(): Unit = {
    checkResult("SELECT a, b FROM T GROUP BY a, b",
      Seq(row(2, 1), row(3, 2), row(5, 2), row(6, 3)))

    checkResult("SELECT a, b + 1, c, s FROM (" +
      "SELECT a, MIN(b) AS b, SUM(b) AS s, MAX(c) AS c FROM MyTable2 GROUP BY a)",
      Seq(row(1, 2, 0, 1), row(2, 3, 2, 5)))

    checkResult("SELECT a, SUM(b) AS s FROM MyTable2 GROUP BY a",
      Seq(row(1, 1), row(2, 5)))

    checkResult(
      "SELECT a, b + 1, c, s FROM (" +
        "SELECT a, MIN(b) AS b, SUM(b) AS s, MAX(c) AS c FROM MyTable GROUP BY a)",
      Seq(
        row(1, 2L, "Hi", 1L),
        row(2, 3L, "Hello", 2L),
        row(3, 3L, "Hello world", 2L)
      ))
  }

  @Test
  def testWithGroupingSets(): Unit = {
    checkResult("SELECT a, b, c, COUNT(d) FROM T " +
      "GROUP BY GROUPING SETS ((a, b), (a, c))",
      Seq(row(2, 1, null, 0), row(2, null, "A", 0), row(3, 2, null, 1),
        row(3, null, "A", 1), row(5, 2, null, 1), row(5, null, "B", 1),
        row(6, 3, null, 1), row(6, null, "C", 1)))

    checkResult("SELECT a, c, COUNT(d) FROM T " +
      "GROUP BY GROUPING SETS ((a, c), (a), ())",
      Seq(row(2, "A", 0), row(2, null, 0), row(3, "A", 1), row(3, null, 1), row(5, "B", 1),
        row(5, null, 1), row(6, "C", 1), row(6, null, 1), row(null, null, 3)))

    checkResult("SELECT a, b, c, COUNT(d) FROM T " +
      "GROUP BY GROUPING SETS ((a, b, c), (a, b, d))",
      Seq(row(2, 1, "A", 0), row(2, 1, null, 0), row(3, 2, "A", 1), row(3, 2, null, 1),
        row(5, 2, "B", 1), row(5, 2, null, 1), row(6, 3, "C", 1), row(6, 3, null, 1)))
  }

  @Test
  def testWithRollup(): Unit = {
    checkResult("SELECT a, b, c, COUNT(d) FROM T GROUP BY ROLLUP (a, b, c)",
      Seq(row(2, 1, "A", 0), row(2, 1, null, 0), row(2, null, null, 0), row(3, 2, "A", 1),
        row(3, 2, null, 1), row(3, null, null, 1), row(5, 2, "B", 1), row(5, 2, null, 1),
        row(5, null, null, 1), row(6, 3, "C", 1), row(6, 3, null, 1), row(6, null, null, 1),
        row(null, null, null, 3)))
  }

  @Test
  def testWithCube(): Unit = {
    checkResult("SELECT a, b, c, COUNT(d) FROM T GROUP BY CUBE (a, b, c)",
      Seq(row(2, 1, "A", 0), row(2, 1, null, 0), row(2, null, "A", 0), row(2, null, null, 0),
        row(3, 2, "A", 1), row(3, 2, null, 1), row(3, null, "A", 1), row(3, null, null, 1),
        row(5, 2, "B", 1), row(5, 2, null, 1), row(5, null, "B", 1), row(5, null, null, 1),
        row(6, 3, "C", 1), row(6, 3, null, 1), row(6, null, "C", 1), row(6, null, null, 1),
        row(null, 1, "A", 0), row(null, 1, null, 0), row(null, 2, "A", 1), row(null, 2, "B", 1),
        row(null, 2, null, 2), row(null, 3, "C", 1), row(null, 3, null, 1), row(null, null, "A", 1),
        row(null, null, "B", 1), row(null, null, "C", 1), row(null, null, null, 3)))

    checkResult(
      "SELECT b, c, e, SUM(a), MAX(d) FROM MyTable2 GROUP BY CUBE (b, c, e)",
      Seq(
        row(null, null, null, 5, "Hallo Welt wie"),
        row(null, null, 1, 3, "Hallo Welt wie"),
        row(null, null, 2, 2, "Hallo Welt"),
        row(null, 0, null, 1, "Hallo"),
        row(null, 0, 1, 1, "Hallo"),
        row(null, 1, null, 2, "Hallo Welt"),
        row(null, 1, 2, 2, "Hallo Welt"),
        row(null, 2, null, 2, "Hallo Welt wie"),
        row(null, 2, 1, 2, "Hallo Welt wie"),
        row(1, null, null, 1, "Hallo"),
        row(1, null, 1, 1, "Hallo"),
        row(1, 0, null, 1, "Hallo"),
        row(1, 0, 1, 1, "Hallo"),
        row(2, null, null, 2, "Hallo Welt"),
        row(2, null, 2, 2, "Hallo Welt"),
        row(2, 1, null, 2, "Hallo Welt"),
        row(2, 1, 2, 2, "Hallo Welt"),
        row(3, null, null, 2, "Hallo Welt wie"),
        row(3, null, 1, 2, "Hallo Welt wie"),
        row(3, 2, null, 2, "Hallo Welt wie"),
        row(3, 2, 1, 2, "Hallo Welt wie")
      ))
  }

  @Test
  def testSingleDistinctAgg(): Unit = {
    checkResult("SELECT a, COUNT(DISTINCT c) FROM T GROUP BY a",
      Seq(row(2, 1), row(3, 1), row(5, 1), row(6, 1)))

    checkResult("SELECT a, b, COUNT(DISTINCT c) FROM T GROUP BY a, b",
      Seq(row(2, 1, 1), row(3, 2, 1), row(5, 2, 1), row(6, 3, 1)))

    checkResult("SELECT a, b, COUNT(DISTINCT c), COUNT(DISTINCT d) FROM T GROUP BY a, b",
      Seq(row(2, 1, 1, 0), row(3, 2, 1, 1), row(5, 2, 1, 1), row(6, 3, 1, 1)))
  }

  @Test
  def testSingleDistinctAgg_WithNonDistinctAgg(): Unit = {
    checkResult("SELECT a, COUNT(DISTINCT c), SUM(b) FROM T GROUP BY a",
      Seq(row(2, 1, 1), row(3, 1, 2), row(5, 1, 2), row(6, 1, 3)))

    checkResult("SELECT a, c, COUNT(DISTINCT c), SUM(b) FROM T GROUP BY a, c",
      Seq(row(2, "A", 1, 1), row(3, "A", 1, 2), row(5, "B", 1, 2), row(6, "C", 1, 3)))

    checkResult("SELECT a, COUNT(DISTINCT c), SUM(b) FROM T GROUP BY a",
      Seq(row(2, 1, 1), row(3, 1, 2), row(5, 1, 2), row(6, 1, 3)))

    checkResult("SELECT a, d, COUNT(DISTINCT c), SUM(b) FROM T GROUP BY a, d",
      Seq(row(2, null, 1, 1), row(3, "Hi", 1, 2),
        row(5, "Hello", 1, 2), row(6, "Hello world", 1, 3)))
  }

  @Test
  def testMultiDistinctAggs(): Unit = {
    checkResult("SELECT a, COUNT(DISTINCT b), SUM(DISTINCT b) FROM T GROUP BY a", Seq(row(2,
      1, 1), row(3, 1, 2), row(5, 1, 2), row(6, 1, 3)))

    checkResult("SELECT a, d, COUNT(DISTINCT c), SUM(DISTINCT b) FROM T GROUP BY a, d",
      Seq(row(2, null, 1, 1), row(3, "Hi", 1, 2),
        row(5, "Hello", 1, 2), row(6, "Hello world", 1, 3)))

    checkResult(
      "SELECT a, SUM(DISTINCT b), MAX(DISTINCT b), MIN(DISTINCT c) FROM T GROUP BY a",
      Seq(row(2, 1, 1, "A"), row(3, 2, 2, "A"), row(5, 2, 2, "B"), row(6, 3, 3, "C")))

    checkResult(
      "SELECT a, d, COUNT(DISTINCT c), MAX(DISTINCT b), SUM(b) FROM T GROUP BY a, d",
      Seq(row(2, null, 1, 1, 1), row(3, "Hi", 1, 2, 2),
        row(5, "Hello", 1, 2, 2), row(6, "Hello world", 1, 3, 3)))
  }

  @Test
  def testAggregateRemove(): Unit = {
    val data = new mutable.MutableList[(Int, Int)]
    data .+= ((1, 1))
    data .+= ((2, 2))
    data .+= ((3, 3))
    data .+= ((4, 2))
    data .+= ((4, 4))
    data .+= ((6, 2))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T1", t)

    val t1 = tEnv.sqlQuery(
      """
        |select sum(b) from
        | (select b from
        |   (select b, sum(a) from
        |     (select b, sum(a) as a from T1 group by b) t1
        |   group by b) t2
        | ) t3
      """.stripMargin)
    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = List("10")
    assertEquals(expected, sink.getRetractResults)
  }

  private def checkResult(str: String, rows: Seq[Row]): Unit = {
    super.before()
    val data = new mutable.MutableList[(Int, Int, String, String)]
    data.+=((2, 1, "A", null))
    data.+=((3, 2, "A", "Hi"))
    data.+=((5, 2, "B", "Hello"))
    data.+=((6, 3, "C", "Hello world"))

    tEnv.registerTableWithPk("T", failingDataSource(data).toTable(tEnv, 'a, 'b, 'c, 'd), List("a"))

    val t1 = failingDataSource(CommonTestData.getSmall3Data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTableWithPk("MyTable", t1, List("a"))

    val t2 = failingDataSource(CommonTestData.getSmall5Data).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    tEnv.registerTableWithPk("MyTable2", t2, List("b"))

    val t = tEnv.sqlQuery(str)
    val sink = new TestingRetractSink
    env.setMaxParallelism(1)
    env.setParallelism(1)
    t.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = rows.map(_.toString)
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

}
