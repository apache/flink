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
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.util.TableTestBase

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.util.Random

@RunWith(classOf[Parameterized])
class FlinkFilterJoinRuleTest(fieldsNullable: Boolean) extends TableTestBase {
  private val util = nullableBatchTestUtil(fieldsNullable)

  @Before
  def setup(): Unit = {
    val programs = FlinkBatchPrograms.buildPrograms(util.getTableEnv.getConfig.getConf)
    programs.remove(FlinkBatchPrograms.PHYSICAL)
    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.addTable[(Int, Long)]("leftT", 'a, 'b)
    util.addTable[(Int, Long)]("rightT", 'c, 'd)
  }

  @Test
  def testFilterPushDownLeftSemi1(): Unit = {
    val sqlQuery = "SELECT * FROM " +
      "(SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT)) T " +
      "WHERE T.b > 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownLeftSemi2(): Unit = {
    val sqlQuery = "SELECT * FROM " +
      "(SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT)) T" +
      " WHERE T.b > 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownLeftSemi3(): Unit = {
    val sqlQuery = "SELECT * FROM " +
      "(SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE a = c)) T " +
      "WHERE T.b > 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinConditionPushDownLeftSemi1(): Unit = {
    val sqlQuery = "SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT WHERE b > 2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinConditionPushDownLeftSemi2(): Unit = {
    val sqlQuery = "SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE b > 2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinConditionPushDownLeftSemi3(): Unit = {
    val sqlQuery = "SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE a = c AND b > 2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownLeftAnti1(): Unit = {
    val sqlQuery = "SELECT * FROM " +
      "(SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE c < 3)) T " +
      "WHERE T.b > 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownLeftAnti2(): Unit = {
    val sqlQuery = "SELECT * FROM " +
      "(SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT where c > 10)) T " +
      "WHERE T.b > 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownLeftAnti3(): Unit = {
    val sqlQuery = "SELECT * FROM " +
      "(SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b = d AND c < 3)) T " +
      "WHERE T.b > 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownLeftAnti4(): Unit = {
    val sqlQuery = "SELECT * FROM " +
      "(SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a = c)) T " +
      "WHERE T.b > 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinConditionPushDownLeftAnti1(): Unit = {
    val sqlQuery = "SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b > 2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinConditionPushDownLeftAnti2(): Unit = {
    val sqlQuery = "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE b > 2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinConditionPushDownLeftAnti3(): Unit = {
    val sqlQuery = "SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b = d AND b > 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinConditionPushDownLeftAnti4(): Unit = {
    val sqlQuery = "SELECT * FROM leftT WHERE NOT EXISTS " +
      "(SELECT * FROM rightT WHERE a = c AND b > 2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPushFilterPastJoinWithNondeterministicFilter(): Unit = {
    util.tableEnv.registerFunction("rand_add", RandAdd)
    val sqlQuery = "SELECT a, b FROM leftT, rightT WHERE b = d AND rand_add(a) > 10"
    util.verifyPlan(sqlQuery)
  }

}

object FlinkFilterJoinRuleTest {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}

object RandAdd extends ScalarFunction {
  def eval(value: Int): Int = {
    val r = new Random()
    value + r.nextInt()
  }

  override def isDeterministic = false
}
