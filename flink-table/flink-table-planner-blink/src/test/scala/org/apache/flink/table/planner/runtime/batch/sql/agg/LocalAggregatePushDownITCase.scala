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

package org.apache.flink.table.planner.runtime.batch.sql.agg

import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.{BatchTestBase, TestData}
import org.junit.{Before, Test}

class LocalAggregatePushDownITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    env.setParallelism(1) // set sink parallelism to 1
    conf.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED, true)
    val testDataId = TestValuesTableFactory.registerData(TestData.personData)
    val ddl =
      s"""
         |CREATE TABLE AggregatableTable (
         |  id int,
         |  age int,
         |  name string,
         |  height int,
         |  gender string,
         |  deposit bigint
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$testDataId',
         |  'bounded' = 'true'
         |)
       """.stripMargin
    tEnv.executeSql(ddl)

  }

  @Test
  def testAggregateWithGroupBy(): Unit = {
    checkResult(
      """
        |SELECT
        |  min(age) as min_age,
        |  max(height),
        |  avg(deposit),
        |  sum(deposit),
        |  count(1),
        |  gender
        |FROM
        |  AggregatableTable
        |GROUP BY gender
        |ORDER BY min_age
        |""".stripMargin,
      Seq(
        row(19, 180, 126, 630, 5, "f"),
        row(23, 182, 220, 1320, 6, "m"))
    )
  }

  @Test
  def testAggregateWithMultiGroupBy(): Unit = {
    checkResult(
      """
        |SELECT
        |  min(age),
        |  max(height),
        |  avg(deposit),
        |  sum(deposit),
        |  count(1),
        |  gender,
        |  age
        |FROM
        |  AggregatableTable
        |GROUP BY gender, age
        |""".stripMargin,
      Seq(
        row(19, 172, 50, 50, 1, "f", 19),
        row(20, 180, 200, 200, 1, "f", 20),
        row(23, 182, 250, 750, 3, "m", 23),
        row(25, 171, 126, 380, 3, "f", 25),
        row(27, 175, 300, 300, 1, "m", 27),
        row(28, 165, 170, 170, 1, "m", 28),
        row(34, 170, 100, 100, 1, "m", 34))
    )
  }

  @Test
  def testAggregateWithoutGroupBy(): Unit = {
    checkResult(
      """
        |SELECT
        |  min(age),
        |  max(height),
        |  avg(deposit),
        |  sum(deposit),
        |  count(*)
        |FROM
        |  AggregatableTable
        |""".stripMargin,
      Seq(
        row(19, 182, 177, 1950, 11))
    )
  }

  @Test
  def testAggregateCanNotPushDown(): Unit = {
    checkResult(
      """
        |SELECT
        |  min(age),
        |  max(height),
        |  avg(deposit),
        |  sum(deposit),
        |  count(distinct age),
        |  gender
        |FROM
        |  AggregatableTable
        |GROUP BY gender
        |""".stripMargin,
      Seq(
        row(19, 180, 126, 630, 3, "f"),
        row(23, 182, 220, 1320, 4, "m"))
    )
  }
}
