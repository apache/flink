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

import java.lang.{Long => JLong}

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.TableTestBase
import org.junit.{Before, Test}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class JoinTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.getTableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, true)
    util.addTable[(Int, Long)]("x", 'a, 'b)
    util.addTable[(Int, Long)]("y", 'c, 'd)

    util.addTable("T1", CommonTestData.get3Source(Array("f1", "f2", "f3")))
    util.addTable("T2", CommonTestData.get3Source(Array("f4", "f5", "f6")))
    // Prevent CSV sample random values.
    util.tableEnv.alterTableStats("T1", Some(TableStats(100000000L)))
    util.tableEnv.alterTableStats("T2", Some(TableStats(100000000L)))
    util.tableEnv.alterSkewInfo("T1", Map("f1" -> List[AnyRef](new Integer(1)).asJava))
    util.disableBroadcastHashJoin()
  }

  @Test
  def testNonEquiJoinCondition(): Unit = {
    val query = "SELECT a, d FROM x, y WHERE a + 1 = c"
    util.verifyPlan(query)
  }

  // skew join match

  @Test
  def testSkewJoin(): Unit = {
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4"
    util.verifyPlan(query)
  }

  @Test
  def testSkewJoinNotEqual(): Unit = {
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4 and f1 <> 10086"
    util.verifyPlan(query)
  }

  @Test
  def testSkewJoinLess(): Unit = {
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4 and f1 < 10086"
    util.verifyPlan(query)
  }

  @Test
  def testSkewJoinLessOrEqual(): Unit = {
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4 and f1 <= 1"
    util.verifyPlan(query)
  }

  @Test
  def testSkewJoinGreater(): Unit = {
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4 and f1 > -500"
    util.verifyPlan(query)
  }

  @Test
  def testSkewJoinGreaterOrEqual(): Unit = {
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4 and f1 >= 1"
    util.verifyPlan(query)
  }

  @Test
  def testSkewJoinTwoValue(): Unit = {
    util.tableEnv.alterSkewInfo("T1", Map("f1" ->
        List[AnyRef](new Integer(1), new Integer(2)).asJava))
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4"
    util.verifyPlan(query)
  }

  @Test
  def testSkewJoinThreeValue(): Unit = {
    util.tableEnv.alterSkewInfo("T1", Map("f1" ->
        List[AnyRef](new Integer(1), new Integer(2), new Integer(3)).asJava))
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4"
    util.verifyPlan(query)
  }

  @Test
  def testSkewJoinTwoField(): Unit = {
    util.tableEnv.alterSkewInfo("T1", Map(
      "f1" -> List[AnyRef](new Integer(1)).asJava,
      "f2" -> List[AnyRef](new JLong(1)).asJava))
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4 AND f2 = f5"
    util.verifyPlan(query)
  }

  @Test
  def testSkewJoinTwoTable(): Unit = {
    util.tableEnv.alterSkewInfo("T1", Map("f1" -> List[AnyRef](new Integer(1)).asJava))
    util.tableEnv.alterSkewInfo("T2", Map("f4" -> List[AnyRef](new Integer(5)).asJava))
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4"
    util.verifyPlan(query)
  }

  // skew join not match

  @Test
  def testSkewJoinEqual(): Unit = {
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4 and f1 = 10086"
    util.verifyPlan(query)
  }

  @Test
  def testSkewJoinLessNotMatch(): Unit = {
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4 and f1 < -1"
    util.verifyPlan(query)
  }

  @Test
  def testSkewJoinGreaterNotMatch(): Unit = {
    val query = "SELECT * FROM T1, T2 WHERE f1 = f4 and f1 > 5"
    util.verifyPlan(query)
  }

  @Test
  def testSkewJoinEqualToField(): Unit = {
    util.addTable("T5", CommonTestData.get5Source(Array("f5_1", "f5_2", "f5_3", "f5_4", "f5_5")))
    util.tableEnv.alterTableStats("T5", Some(TableStats(100000000L)))
    util.tableEnv.alterSkewInfo("T5", Map("f5_1" -> List[AnyRef](new Integer(1)).asJava))
    val query = "SELECT * FROM T5, T2 WHERE f5_1 = f4 and f5_1 = f5_3"
    util.verifyPlan(query)
  }
}
