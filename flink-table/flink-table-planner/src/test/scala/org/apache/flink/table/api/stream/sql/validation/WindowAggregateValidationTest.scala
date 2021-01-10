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

package org.apache.flink.table.api.stream.sql.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}

import org.junit.Test

class WindowAggregateValidationTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testTumbleWindowNoOffset(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("TUMBLE window with alignment is not supported yet")

    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM MyTable " +
        "GROUP BY TUMBLE(proctime, INTERVAL '2' HOUR, TIME '10:00:00')"

    streamUtil.verifySql(sqlQuery, "n/a")
  }

  @Test
  def testHopWindowNoOffset(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("HOP window with alignment is not supported yet")

    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM MyTable " +
        "GROUP BY HOP(proctime, INTERVAL '1' HOUR, INTERVAL '2' HOUR, TIME '10:00:00')"

    streamUtil.verifySql(sqlQuery, "n/a")
  }

  @Test
  def testSessionWindowNoOffset(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("SESSION window with alignment is not supported yet")

    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM MyTable " +
        "GROUP BY SESSION(proctime, INTERVAL '2' HOUR, TIME '10:00:00')"

    streamUtil.verifySql(sqlQuery, "n/a")
  }

  @Test
  def testVariableWindowSize(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Only constant window intervals with millisecond resolution are supported")

    val sql = "SELECT COUNT(*) FROM MyTable GROUP BY TUMBLE(proctime, c * INTERVAL '1' MINUTE)"
    streamUtil.verifySql(sql, "n/a")
  }

  @Test
  def testWindowUdAggInvalidArgs(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Given parameters of function do not match any signature")

    streamUtil.tableEnv.registerFunction("weightedAvg", new WeightedAvgWithMerge)

    val sqlQuery =
      "SELECT SUM(a) AS sumA, weightedAvg(a, b) AS wAvg " +
        "FROM MyTable " +
        "GROUP BY TUMBLE(proctime, INTERVAL '2' HOUR, TIME '10:00:00')"

    streamUtil.verifySql(sqlQuery, "n/a")
  }

  @Test
  def testWindowWrongWindowParameter(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Only constant window intervals with millisecond resolution are supported")

    val sqlQuery =
      "SELECT COUNT(*) FROM MyTable " +
        "GROUP BY TUMBLE(proctime, INTERVAL '2-10' YEAR TO MONTH)"

    streamUtil.verifySql(sqlQuery, "n/a")
  }
}
