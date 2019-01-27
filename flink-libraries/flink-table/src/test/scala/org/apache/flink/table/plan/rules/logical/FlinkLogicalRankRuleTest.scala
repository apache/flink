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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.util.TableTestBase

import org.junit.{Before, Test}

import scala.collection.JavaConversions._

class FlinkLogicalRankRuleTest extends TableTestBase {
  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    var startRemove = false
    val programs = FlinkStreamPrograms.buildPrograms(util.tableEnv.getConfig.getConf)
    programs.getProgramNames.foreach { name =>
      if (name.equals(FlinkBatchPrograms.PHYSICAL)) {
        startRemove = true
      }
      if (startRemove) {
        programs.remove(name)
      }
    }

    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceStreamPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)
    util.addTable[(Int, Long, String)]("x", 'a, 'b, 'c)
  }

  @Test
  def testRowNumberFunc(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, ROW_NUMBER() OVER (PARTITION BY b ORDER BY a) rn FROM x) t " +
      "WHERE rn <= 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testWithoutFilter(): Unit = {
    val sqlQuery = "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM x"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithUpperValue(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM x) t " +
      "WHERE rk <= 2 AND a > 10"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithRange(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY b, c ORDER BY a) rk FROM x) t " +
      "WHERE rk <= 2 AND rk > -2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithLowerValue(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a, c) rk FROM x) t " +
      "WHERE rk > 2"
    thrown.expectMessage("Rank end is not specified.")
    thrown.expect(classOf[TableException])
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithEquals(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a, c) rk FROM x) t " +
      "WHERE rk = 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithVariableField1(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY c) rk FROM x) t " +
      "WHERE rk < a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithVariableField2(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY c) rk FROM x) t " +
      "WHERE rk > a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithVariableField3(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY c) rk FROM x) t " +
      "WHERE rk < a and b > 5"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithVariableField4(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY c) rk FROM x) t " +
      "WHERE rk = b"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testWithoutPartitionBy(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, RANK() OVER (ORDER BY a) rk FROM x) t " +
      "WHERE rk < 10"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiSameRankFunctionsWithSameGroup(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, " +
      "RANK() OVER (PARTITION BY b ORDER BY a) rk1, " +
      "RANK() OVER (PARTITION BY b ORDER BY a) rk2 FROM x) t " +
      "WHERE rk1 < 10"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiSameRankFunctionsWithDiffGroup(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, " +
      "RANK() OVER (PARTITION BY b ORDER BY a) rk1, " +
      "RANK() OVER (PARTITION BY c ORDER BY a) rk2 FROM x) t " +
      "WHERE rk1 < 10"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDiffRankFunctions(): Unit = {
    val sqlQuery = "SELECT * FROM (" +
      "SELECT a, b, " +
      "RANK() OVER (PARTITION BY b ORDER BY a) rk, " +
      "ROW_NUMBER() OVER (PARTITION BY b ORDER BY a) rn FROM x) t " +
      "WHERE rk < 10"
    util.verifyPlan(sqlQuery)
  }
}


