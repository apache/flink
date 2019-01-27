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

package org.apache.flink.table.plan.rules.logical

import java.util
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{AggPhaseEnforcer, TableConfigOptions}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.StreamingWithAggTestBase.{AggMode, LocalGlobalOff, LocalGlobalOn}
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}

import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class SplitAggregateTest(aggMode: AggMode) extends TableTestBase {

  private var streamUtil: StreamTableTestUtil = _

  @Before
  def before(): Unit = {
    streamUtil= streamTestUtil()
    streamUtil.addTable[(Long, Int, String)](
      "MyTable", 'a, 'b, 'c)
    val tableConfig = streamUtil.tableEnv.getConfig
    tableConfig.getConf.setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    tableConfig.getConf.setBoolean(TableConfigOptions.SQL_OPTIMIZER_DATA_SKEW_DISTINCT_AGG, true)

    aggMode match {
      case LocalGlobalOn => tableConfig.getConf.setString(
        TableConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER, AggPhaseEnforcer.TWO_PHASE.toString)
      case LocalGlobalOff => tableConfig.getConf.setString(
        TableConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER, AggPhaseEnforcer.ONE_PHASE.toString)
    }
  }

  @Test
  def testSingleDistinctAgg(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT c) FROM MyTable"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleMinAgg(): Unit = {
    val sqlQuery = "SELECT MIN(c) FROM MyTable"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleFirstValueAgg(): Unit = {
    val sqlQuery = "SELECT FIRST_VALUE(c) FROM MyTable GROUP BY a"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctAgg(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT b) FROM MyTable"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleMaxWithDistinctAgg(): Unit = {
     val sqlQuery =
      """
        |SELECT a, COUNT(DISTINCT b), MAX(c)
        |FROM MyTable
        |GROUP BY a
      """.stripMargin
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleFirstValueWithDistinctAgg(): Unit = {
    val sqlQuery = "SELECT a, FIRST_VALUE(c), COUNT(DISTINCT b) FROM MyTable GROUP BY a"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggWithAllNonDistinctAgg(): Unit = {
    val sqlQuery =
      """
        |SELECT a, COUNT(DISTINCT c), SUM(b), AVG(b), MAX(b), MIN(b), COUNT(b), COUNT(*)
        |FROM MyTable
        |GROUP BY a
      """.stripMargin
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggWithGroupBy(): Unit = {
    val sqlQuery = "SELECT a, COUNT(DISTINCT c) FROM MyTable GROUP BY a"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggWithAndNonDistinctAggOnSameColumn(): Unit = {
    val sqlQuery = "SELECT a, COUNT(DISTINCT b), SUM(b), AVG(b) FROM MyTable GROUP BY a"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSomeColumnsBothInDistinctAggAndGroupBy(): Unit = {
    // TODO: the COUNT(DISTINCT a) can be optimized to literal 1
    val sqlQuery = "SELECT a, COUNT(DISTINCT a), COUNT(b) FROM MyTable GROUP BY a"
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testAggWithFilterClause(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  a,
         |  COUNT(DISTINCT b) filter (where not b = 2),
         |  SUM(b) filter (where not b = 5),
         |  SUM(b) filter (where not b = 2)
         |FROM MyTable
         |GROUP BY a
       """.stripMargin
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctWithRetraction(): Unit = {
    val sql =
      """
        |SELECT a, COUNT(DISTINCT b), COUNT(1)
        |FROM (
        |  SELECT c, AVG(a) as a, AVG(b) as b
        |  FROM MyTable
        |  GROUP BY c
        |) GROUP BY a
      """.stripMargin
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testMinMaxWithRetraction(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  c, MIN(b), MAX(b), SUM(b), COUNT(*), COUNT(DISTINCT a)
         |FROM(
         |  SELECT
         |    a, AVG(c) as b, MAX(c) as c
         |  FROM MyTable
         |  GROUP BY a
         |) GROUP BY c
       """.stripMargin
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testFirstValueLastValueWithRetraction(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  b, FIRST_VALUE(c), LAST_VALUE(c), COUNT(DISTINCT c)
         |FROM(
         |  SELECT
         |    a, COUNT(DISTINCT b) as b, MAX(b) as c
         |  FROM MyTable
         |  GROUP BY a
         |) GROUP BY b
       """.stripMargin
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testAggWithJoin(): Unit = {
    val sqlQuery =
      s"""
         |SELECT *
         |FROM(
         |  SELECT
         |    c, SUM(b) as b, SUM(b) as d, COUNT(DISTINCT a) as a
         |  FROM(
         |    SELECT
         |      a, COUNT(DISTINCT b) as b, SUM(b) as c, SUM(b) as d
         |    FROM MyTable
         |    GROUP BY a
         |  ) GROUP BY c
         |) as MyTable1 JOIN MyTable ON MyTable1.b + 2 = MyTable.a
       """.stripMargin
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testBucketsConfiguration(): Unit = {
    streamUtil.tableEnv.getConfig.getConf.setInteger(
      TableConfigOptions.SQL_OPTIMIZER_DATA_SKEW_DISTINCT_AGG_BUCKET, 100)
    val sqlQuery = "SELECT COUNT(DISTINCT c) FROM MyTable"
    streamUtil.verifyPlan(sqlQuery)
  }
}

object SplitAggregateTest {

  @Parameterized.Parameters(name = "LocalGlobal={0}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(LocalGlobalOff),
      Array(LocalGlobalOn))
  }
}
