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

package org.apache.flink.table.plan.batch.sql

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.table.api.AggPhaseEnforcer.AggPhaseEnforcer
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{AggPhaseEnforcer, OperatorType, TableConfigOptions}
import org.apache.flink.table.functions.aggregate.CountAggFunction
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.VarSum1AggFunction
import org.apache.flink.table.util.TableTestBase
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

/**
  * Test for testing aggregate plans.
  */
@RunWith(classOf[Parameterized])
class SortAggregateTest(
    aggStrategy: AggPhaseEnforcer,
    withNdv: Boolean)
  extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, OperatorType.HashAgg.toString)
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER, aggStrategy.toString)
    util.addTable("MyTable", CommonTestData.get3Source(Array("a", "b", "c")))
    val tableStats = if (withNdv) {
      TableStats(100000000L, Map[String, ColumnStats](
        "a" -> ColumnStats(100000000L, null, null, null, null, null),
        "b" -> ColumnStats(3L, null, null, null, null, null),
        "c" -> ColumnStats(3L, null, null, null, null, null)
      ))
    } else {
      TableStats(100000000L)
    }
    util.tableEnv.alterTableStats("MyTable", Some(tableStats))
  }

  @Test
  def testAggregate(): Unit = {
    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAggregateWithFilter(): Unit = {
    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable WHERE a = 1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAggregateWithFilterOnNestedFields(): Unit = {
    util.addTable[(Int, Long, (Int, Long))]("MyTable3", 'a, 'b, 'c)
    val sqlQuery = "SELECT avg(a), sum(b), count(c), sum(c._1) FROM MyTable3 WHERE a = 1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupAggregate(): Unit = {
    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable GROUP BY a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupAggregateWithFilter(): Unit = {
    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable WHERE a = 1 GROUP BY a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingSets(): Unit = {
    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g FROM MyTable " +
      "GROUP BY GROUPING SETS (b, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCube(): Unit = {
    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g, " +
      "GROUPING(b) as gb, GROUPING(c) as gc, " +
      "GROUPING_ID(b) as gib, GROUPING_ID(c) as gic, " +
      "GROUPING_ID(b, c) as gid " +
      "FROM MyTable " +
      "GROUP BY CUBE (b, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRollup(): Unit = {
    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g, " +
      "GROUPING(b) as gb, GROUPING(c) as gc, " +
      "GROUPING_ID(b) as gib, GROUPING_ID(c) as gic, " +
      "GROUPING_ID(b, c) as gid " + " FROM MyTable " +
      "GROUP BY ROLLUP (b, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testUDAGG(): Unit = {
    util.tableEnv.registerFunction("countFun", new CountAggFunction())
    val sqlQuery = "SELECT countFun(a), countFun(b), countFun(c) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAggNotSupportMerge(): Unit = {
    util.tableEnv.registerFunction("var_sum", new VarSum1AggFunction)
    val sqlQuery = "SELECT b, var_sum(a) from MyTable group by b"
    util.verifyPlan(sqlQuery)
  }
}

object SortAggregateTest {

  @Parameterized.Parameters(name = "aggStrategy={0}, withNdv={1}")
  def parameters(): util.Collection[Array[Any]] = {
    Seq[Array[Any]](
      Array(AggPhaseEnforcer.NONE, true),
      Array(AggPhaseEnforcer.NONE, false),
      Array(AggPhaseEnforcer.ONE_PHASE, true),
      Array(AggPhaseEnforcer.ONE_PHASE, false),
      Array(AggPhaseEnforcer.TWO_PHASE, true),
      Array(AggPhaseEnforcer.TWO_PHASE, false))
  }
}
