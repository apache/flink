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

package org.apache.flink.table.api.batch.sql.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.utils.TableTestBase

import org.junit.Test

import java.sql.Timestamp

class GroupWindowValidationTest extends TableTestBase {

  @Test(expected = classOf[TableException])
  def testHopWindowNoOffset(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM T " +
        "GROUP BY HOP(ts, INTERVAL '1' HOUR, INTERVAL '2' HOUR, TIME '10:00:00')"

    util.verifySql(sqlQuery, "n/a")
  }

  @Test(expected = classOf[TableException])
  def testSessionWindowNoOffset(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM T " +
        "GROUP BY SESSION(ts, INTERVAL '2' HOUR, TIME '10:00:00')"

    util.verifySql(sqlQuery, "n/a")
  }

  @Test(expected = classOf[TableException])
  def testVariableWindowSize() = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sql = "SELECT COUNT(*) " +
      "FROM T " +
      "GROUP BY TUMBLE(ts, b * INTERVAL '1' MINUTE)"
    util.verifySql(sql, "n/a")
  }

  @Test(expected = classOf[ValidationException])
  def testTumbleWindowWithInvalidUdAggArgs() = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val weightedAvg = new WeightedAvgWithMerge
    util.tableEnv.registerFunction("weightedAvg", weightedAvg)

    val sql = "SELECT weightedAvg(c, a) AS wAvg " +
      "FROM T " +
      "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE)"
    util.verifySql(sql, "n/a")
  }

  @Test(expected = classOf[ValidationException])
  def testWindowProctime(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT " +
        "  TUMBLE_PROCTIME(ts, INTERVAL '4' MINUTE)" +
        "FROM T " +
        "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE), c"

    // should fail because PROCTIME properties are not yet supported in batch
    util.verifySql(sqlQuery, "FAIL")
  }
}
