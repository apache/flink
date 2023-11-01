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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.api.ExplainDetail
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}

import org.junit.{Before, Test}

/** Tests for [[WatermarkAssignerChangelogNormalizeTransposeRule]] */
class WatermarkAssignerChangelogNormalizeTransposeRuleTest extends TableTestBase {
  private val util: StreamTableTestUtil = streamTestUtil()

  @Before
  def setup(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE simple_src (
                     |  currency STRING,
                     |  currency_no STRING,
                     |  rate  BIGINT,
                     |  currency_time TIMESTAMP(3),
                     |  WATERMARK FOR currency_time AS currency_time - interval '5' SECOND,
                     |  PRIMARY KEY(currency) NOT ENFORCED
                     |) WITH (
                     |  'connector' = 'values',
                     |  'changelog-mode' = 'UA,D',
                     |  'enable-watermark-push-down' = 'true'
                     |)
                     |""".stripMargin)

    util.addTable(s"""
                     |CREATE TABLE src_with_computed_column (
                     |  currency STRING,
                     |  currency_no STRING,
                     |  rate  BIGINT,
                     |  c STRING,
                     |  currency_time as to_timestamp(c),
                     |  WATERMARK FOR currency_time AS currency_time - interval '5' SECOND,
                     |  PRIMARY KEY(currency) NOT ENFORCED
                     |) WITH (
                     |  'connector' = 'values',
                     |  'changelog-mode' = 'UA,D',
                     |  'enable-watermark-push-down' = 'true'
                     |)
                     |""".stripMargin)

    util.addTable(s"""
                     |CREATE TABLE src_with_computed_column2 (
                     | currency int,
                     | currency2 as currency + 2,
                     | currency_no STRING,
                     | rate BIGINT,
                     | c STRING,
                     | currency_time as to_timestamp(c),
                     | WATERMARK FOR currency_time AS currency_time - interval '5' SECOND,
                     | PRIMARY KEY(currency) NOT ENFORCED
                     |) WITH (
                     | 'connector' = 'values',
                     | 'changelog-mode' = 'UA,D',
                     | 'enable-watermark-push-down' = 'true'
                     |)
                     |""".stripMargin)
  }

  // ----------------------------------------------------------------------------------------
  // Tests for queries matches WITHOUT_CALC patten
  // Rewrite always happens in the case
  // ----------------------------------------------------------------------------------------
  @Test
  def testPushdownWatermarkWithoutCalc(): Unit = {
    val sql =
      """
        |SELECT
        |  currency,
        |  COUNT(1) AS cnt,
        |  TUMBLE_START(currency_time, INTERVAL '5' SECOND) as w_start,
        |  TUMBLE_END(currency_time, INTERVAL '5' SECOND) as w_end
        |FROM simple_src
        |GROUP BY currency, TUMBLE(currency_time, INTERVAL '5' SECOND)
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  // ----------------------------------------------------------------------------------------
  // Tests for queries matches WITH_CALC patten
  // ----------------------------------------------------------------------------------------

  /** push down calc and watermark assigner as a whole if shuffle keys are kept after Calc. */
  @Test
  def testPushdownCalcAndWatermarkAssignerWithCalc(): Unit = {
    val sql =
      """
        |SELECT
        |  currency,
        |  COUNT(1) AS cnt,
        |  TUMBLE_START(currency_time, INTERVAL '5' SECOND) as w_start,
        |  TUMBLE_END(currency_time, INTERVAL '5' SECOND) as w_end
        |FROM src_with_computed_column
        |GROUP BY currency, TUMBLE(currency_time, INTERVAL '5' SECOND)
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  /**
   * only push down watermark assigner if satisfy all the following condition:
   *   1. shuffle keys are not kept after Calc 2. row time field does not depend on computed column
   */
  @Test
  def testPushdownWatermarkAssignerWithCalc(): Unit = {
    val sql =
      """
        |SELECT
        |  TUMBLE_START(currency_time, INTERVAL '5' SECOND) as w_start,
        |  TUMBLE_END(currency_time, INTERVAL '5' SECOND) as w_end,
        |  MAX(rate) AS max_rate
        |FROM simple_src
        |GROUP BY TUMBLE(currency_time, INTERVAL '5' SECOND)
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  /**
   * push down new calc with all shuffle keys and new watermarkAssigner, then add a top calc to
   * remove add new added shuffle keys
   */
  @Test
  def testPushdownNewCalcAndWatermarkAssignerWithCalc(): Unit = {
    val sql =
      """
        |SELECT
        |  TUMBLE_START(currency_time, INTERVAL '5' SECOND) as w_start,
        |  TUMBLE_END(currency_time, INTERVAL '5' SECOND) as w_end,
        |  MAX(rate) AS max_rate
        |FROM src_with_computed_column
        |GROUP BY TUMBLE(currency_time, INTERVAL '5' SECOND)
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testGroupKeyIsComputedColumn(): Unit = {
    val sql =
      """
        |SELECT
        |  currency2,
        |  COUNT(1) AS cnt,
        |  TUMBLE_START(currency_time, INTERVAL '5' SECOND) as w_start,
        |  TUMBLE_END(currency_time, INTERVAL '5' SECOND) as w_end
        |FROM src_with_computed_column2
        |GROUP BY currency2, TUMBLE(currency_time, INTERVAL '5' SECOND)
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testPushdownCalcNotAffectChangelogNormalizeKey(): Unit = {
    util.addTable("""
                    |CREATE TABLE t1 (
                    |  ingestion_time TIMESTAMP(3) METADATA FROM 'ts',
                    |  a VARCHAR NOT NULL,
                    |  b VARCHAR NOT NULL,
                    |  WATERMARK FOR ingestion_time AS ingestion_time
                    |) WITH (
                    | 'connector' = 'values',
                    | 'readable-metadata' = 'ts:TIMESTAMP(3)'
                    |)
      """.stripMargin)
    util.addTable("""
                    |CREATE TABLE t2 (
                    |  k VARBINARY,
                    |  ingestion_time TIMESTAMP(3) METADATA FROM 'ts',
                    |  a VARCHAR NOT NULL,
                    |  f BOOLEAN NOT NULL,
                    |  WATERMARK FOR `ingestion_time` AS `ingestion_time`,
                    |  PRIMARY KEY (`a`) NOT ENFORCED
                    |) WITH (
                    | 'connector' = 'values',
                    | 'readable-metadata' = 'ts:TIMESTAMP(3)',
                    | 'changelog-mode' = 'I,UA,D'
                    |)
      """.stripMargin)
    // After FLINK-28988 applied, the filter will not be pushed down into left input of join and get
    // a more optimal plan (upsert mode without ChangelogNormalize).
    val sql =
      """
        |SELECT t1.a, t1.b, t2.f
        |FROM t1 INNER JOIN t2 FOR SYSTEM_TIME AS OF t1.ingestion_time
        | ON t1.a = t2.a WHERE t2.f = true
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }
}
