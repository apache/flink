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

import scala.collection.Seq

@RunWith(classOf[Parameterized])
class PruneAggregateCallRuleITCase(
    aggMode: AggMode,
    minibatch: MiniBatchMode,
    backend: StateBackendMode)
  extends StreamingWithAggTestBase(aggMode, minibatch, backend) {

  @Test
  def testNoneEmptyGroupKey(): Unit = {
    checkResult(
      "SELECT a FROM (SELECT b, MAX(a) AS a, COUNT(*), MAX(c) FROM MyTable GROUP BY b) t",
      Seq(row(1), row(3))
    )
    checkResult(
      """
        |SELECT c, a FROM
        | (SELECT a, c, COUNT(b) as b, SUM(b) as s FROM MyTable GROUP BY a, c) t
        |WHERE s > 1
      """.stripMargin,
      Seq(row("Hello world", 3), row("Hello", 2))
    )
  }

  @Test
  def testEmptyGroupKey(): Unit = {
    checkResult(
      "SELECT 1 FROM (SELECT SUM(a) FROM MyTable) t",
      Seq(row(1))
    )

    checkResult(
      "SELECT * FROM MyTable WHERE EXISTS (SELECT COUNT(*) FROM MyTable2)",
      Seq(row(1, 1, "Hi"), row(2, 2, "Hello"), row(3, 2, "Hello world"))
    )

    checkResult(
      "SELECT 1 FROM (SELECT SUM(a), COUNT(*) FROM MyTable) t",
      Seq(row(1))
    )

    checkResult(
      "SELECT 1 FROM (SELECT COUNT(*), SUM(a) FROM MyTable) t",
      Seq(row(1))
    )

    checkResult(
      "SELECT * FROM MyTable WHERE EXISTS (SELECT SUM(a), COUNT(*) FROM MyTable2)",
      Seq(row(1, 1, "Hi"), row(2, 2, "Hello"), row(3, 2, "Hello world"))
    )

    checkResult(
      "SELECT * FROM MyTable WHERE EXISTS (SELECT COUNT(*), SUM(a) FROM MyTable2)",
      Seq(row(1, 1, "Hi"), row(2, 2, "Hello"), row(3, 2, "Hello world"))
    )
  }

  private def checkResult(str: String, rows: Seq[Row]): Unit = {
    super.before()
    val t1 = failingDataSource(CommonTestData.getSmall3Data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", t1)

    val t2 = failingDataSource(CommonTestData.getSmall5Data).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable2", t2)

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
