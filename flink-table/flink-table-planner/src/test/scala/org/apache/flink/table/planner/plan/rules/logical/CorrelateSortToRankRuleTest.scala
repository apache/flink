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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.plan.optimize.program.{FlinkChainedProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE, StreamOptimizeContext}
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
  * Test for [[CalcRankTransposeRule]].
  */
class CorrelateSortToRankRuleTest extends TableTestBase {
  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[StreamOptimizeContext]()
    programs.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(RuleSets.ofList(CorrelateSortToRankRule.INSTANCE))
          .build())
    util.replaceStreamProgram(programs)

    val createTable =
      s"""
         |create table t1(
         |  f0 int,
         |  f1 bigint,
         |  f2 varchar(20)
         |) with (
         |  'connector' = 'values',
         |  'bounded' = 'false'
         |)
         |""".stripMargin
    util.tableEnv.executeSql(createTable)
  }

  @Test
  def testCorrelateSortToRank(): Unit = {
    val query =
      s"""
         |SELECT f0, f1
         |FROM
         |  (SELECT DISTINCT f0 FROM t1) t2,
         |  LATERAL (
         |    SELECT f1, f2
         |    FROM t1
         |    WHERE f0 = t2.f0
         |    ORDER BY f2
         |    DESC LIMIT 3
         |  )
      """.stripMargin
    util.verifyRelPlan(query)
  }

  @Test
  def testNonInnerJoinNotSupported(): Unit = {
    val query =
      s"""
         |SELECT f0, f1
         |FROM
         |  (SELECT DISTINCT f0 FROM t1) t2
         |  NATURAL LEFT JOIN
         |  LATERAL (
         |    SELECT f1, f2
         |    FROM t1
         |    WHERE f0 = t2.f0
         |    ORDER BY f2
         |    DESC LIMIT 3
         |  )
      """.stripMargin
    util.verifyRelPlan(query)
  }

  @Test
  def testAggCallNotSupported(): Unit = {
    val query =
      s"""
         |SELECT mf0, f1
         |FROM
         |  (SELECT max(f0) as mf0 FROM t1) t2,
         |  LATERAL (
         |    SELECT f1, f2
         |    FROM t1
         |    WHERE f0 = t2.mf0
         |    ORDER BY f2
         |    DESC LIMIT 3
         |  )
      """.stripMargin
    util.verifyRelPlan(query)
  }

  @Test
  def testCorrelateSortToRankWithMultipleGroupKeys(): Unit = {
    val query =
      s"""
         |SELECT f0, f2
         |FROM
         |  (SELECT DISTINCT f0, f1 FROM t1) t2,
         |  LATERAL (
         |    SELECT f2
         |    FROM t1
         |    WHERE f0 = t2.f0 AND f1 = t2.f1
         |    ORDER BY f2
         |    DESC LIMIT 3
         |  )
      """.stripMargin
    util.verifyRelPlan(query)
  }

  @Test
  def testAggInputNonMappingNotSupported(): Unit = {
    val query =
      s"""
         |SELECT f0, f1
         |FROM
         |  (SELECT DISTINCT f0 FROM (SELECT f0 + f1 as f0 from t1)) t2,
         |  LATERAL (
         |    SELECT f1, f2
         |    FROM t1
         |    WHERE f0 = t2.f0
         |    ORDER BY f2
         |    DESC LIMIT 3
         |  )
      """.stripMargin
    util.verifyRelPlan(query)
  }

  @Test
  def testSortWithOffsetNotSupported(): Unit = {
    val query =
      s"""
         |SELECT f0, f1
         |FROM
         |  (SELECT DISTINCT f0 FROM t1) t2,
         |  LATERAL (
         |    SELECT f1, f2
         |    FROM t1
         |    WHERE f0 = t2.f0
         |    ORDER BY f2 DESC
         |    OFFSET 2 ROWS
         |    FETCH NEXT 3 ROWS ONLY
         |  )
      """.stripMargin
    util.verifyRelPlan(query)
  }

  @Test
  def testNonEqualConditionNotSupported(): Unit = {
    val query =
      s"""
         |SELECT f0, f1
         |FROM
         |  (SELECT DISTINCT f0 FROM t1) t2,
         |  LATERAL (
         |    SELECT f1, f2
         |    FROM t1
         |    WHERE f0 > t2.f0
         |    ORDER BY f2
         |    DESC LIMIT 3
         |  )
      """.stripMargin
    util.verifyRelPlan(query)
  }

  @Test
  def testFilterConditionNotCorrelationID(): Unit = {
    val query =
      s"""
         |SELECT f0, f1
         |FROM
         |  (SELECT DISTINCT f0 FROM t1) t2,
         |  LATERAL (
         |    SELECT f1, f2
         |    FROM t1
         |    WHERE t2.f0 = f0 + 1
         |    ORDER BY f2
         |    DESC LIMIT 3
         |  )
      """.stripMargin
    util.verifyRelPlan(query)
  }

  @Test
  def testMultipleGroupingsWithConstantNotSupported1(): Unit = {
    val query =
      s"""
         |SELECT f0, f2
         |FROM
         |  (SELECT DISTINCT f0, f1 FROM t1) t2,
         |  LATERAL (
         |    SELECT f2
         |    FROM t1
         |    WHERE f0 = 1 AND f1 = t2.f1
         |    ORDER BY f2
         |    DESC LIMIT 3
         |  )
      """.stripMargin
    util.verifyRelPlan(query)
  }

  @Test
  def testMultipleGroupingsWithConstantNotSupported2(): Unit = {
    val query =
      s"""
         |SELECT f0, f2
         |FROM
         |  (SELECT DISTINCT f0, f1 FROM t1) t2,
         |  LATERAL (
         |    SELECT f2
         |    FROM t1
         |    WHERE 1 = t2.f0 AND f1 = t2.f1
         |    ORDER BY f2
         |    DESC LIMIT 3
         |  )
      """.stripMargin
    util.verifyRelPlan(query)
  }
}
