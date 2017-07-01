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

import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.runtime.join.JoinUtil
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Assert._
import org.junit.Test

class JoinTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c.rowtime, 'proctime.proctime)
  streamUtil.addTable[(Int, String, Long)]("MyTable2", 'a, 'b, 'c.rowtime, 'proctime.proctime)

  @Test
  def testProcessingTimeInnerJoin() = {

    val sqlQuery = "SELECT t1.a, t2.b " +
      "FROM MyTable as t1 join MyTable2 as t2 on t1.a = t2.a and " +
      "t1.proctime between t2.proctime - interval '1' hour and t2.proctime + interval '1' hour"
    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamRowStreamJoin",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "proctime")
          ),
          unaryNode(
            "DataStreamCalc",
            streamTableNode(1),
            term("select", "a", "b", "proctime")
          ),
          term("condition",
            "AND(=(a, a0), >=(TIME_MATERIALIZATION(proctime), " +
              "-(TIME_MATERIALIZATION(proctime0), 3600000)), " +
              "<=(TIME_MATERIALIZATION(proctime), " +
              "DATETIME_PLUS(TIME_MATERIALIZATION(proctime0), 3600000)))"),
          term("select", "a, proctime, a0, b, proctime0"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "b")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }


  @Test
  def testJoinTimeBoundary(): Unit = {
    verifyTimeBoundary(
      "t1.proctime between t2.proctime - interval '1' hour " +
        "and t2.proctime + interval '1' hour",
      3600000,
      3600000,
      "proctime")

    verifyTimeBoundary(
      "t1.proctime > t2.proctime - interval '1' second and " +
        "t1.proctime < t2.proctime + interval '1' second",
      999,
      999,
      "proctime")

    verifyTimeBoundary(
      "t1.c >= t2.c - interval '1' second and " +
        "t1.c <= t2.c + interval '1' second",
      1000,
      1000,
      "rowtime")

    verifyTimeBoundary(
      "t1.c >= t2.c and " +
        "t1.c <= t2.c + interval '1' second",
      0,
      1000,
      "rowtime")

    verifyTimeBoundary(
      "t1.c >= t2.c + interval '1' second and " +
        "t1.c <= t2.c + interval '10' second",
      0,
      10000,
      "rowtime")

    verifyTimeBoundary(
      "t2.c - interval '1' second <= t1.c and " +
        "t2.c + interval '10' second >= t1.c",
      1000,
      10000,
      "rowtime")

    verifyTimeBoundary(
      "t1.c - interval '2' second >= t2.c + interval '1' second -" +
        "interval '10' second and " +
        "t1.c <= t2.c + interval '10' second",
      7000,
      10000,
      "rowtime")
  }

  def verifyTimeBoundary(
    timeSql: String,
    expLeftSize: Long,
    expRightSize: Long,
    expTimeType: String) = {
    val query =
      "SELECT t1.a, t2.b FROM MyTable as t1 join MyTable2 as t2 on t1.a = t2.a and " + timeSql

    val resultTable = streamUtil.tEnv.sql(query)
    val relNode = resultTable.getRelNode
    val joinNode = relNode.getInput(0).asInstanceOf[LogicalJoin]
    val rexNode = joinNode.getCondition
    val (timeType, leftSize, rightSize, conditionWithoutTime) =
    JoinUtil.analyzeTimeBoundary(rexNode, 4, 2, joinNode.getRowType,
      joinNode.getCluster.getRexBuilder, streamUtil.tEnv.getConfig)

    val timeTypeStr =
      if (FlinkTypeFactory.isProctimeIndicatorType(timeType)) "proctime"
      else if (FlinkTypeFactory.isRowtimeIndicatorType(timeType)) "rowtime"
      else "NA"
    assertEquals(expLeftSize, leftSize)
    assertEquals(expRightSize, rightSize)
    assertEquals(expTimeType, timeTypeStr)
  }

}
