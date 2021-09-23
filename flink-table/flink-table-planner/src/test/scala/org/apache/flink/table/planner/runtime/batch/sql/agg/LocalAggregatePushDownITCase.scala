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

package org.apache.flink.table.planner.plan.batch.sql.agg

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.functions.aggfunctions.CollectAggFunction
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
         |  deposit bigint,
         |  points bigint
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$testDataId',
         |  'filterable-fields' = 'id',
         |  'bounded' = 'true'
         |)
       """.stripMargin
    tEnv.executeSql(ddl)

  }

  @Test
  def testPushDownLocalHashAggWithGroup(): Unit = {
    checkResult(
      """
        |SELECT
        |  avg(deposit) as avg_dep,
        |  sum(deposit),
        |  count(1),
        |  gender
        |FROM
        |  AggregatableTable
        |GROUP BY gender
        |ORDER BY avg_dep
        |""".stripMargin,
      Seq(
        row(126, 630, 5, "f"),
        row(220, 1320, 6, "m"))
    )
  }

  @Test
  def testDisablePushDownLocalAgg(): Unit = {
    // disable push down local agg
    tEnv.getConfig.getConfiguration.setBoolean(
        OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED,
        false)

    checkResult(
      """
        |SELECT
        |  avg(deposit) as avg_dep,
        |  sum(deposit),
        |  count(1),
        |  gender
        |FROM
        |  AggregatableTable
        |GROUP BY gender
        |ORDER BY avg_dep
        |""".stripMargin,
      Seq(
        row(126, 630, 5, "f"),
        row(220, 1320, 6, "m"))
    )

    // reset config
    tEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED,
      true)
  }

  @Test
  def testPushDownLocalHashAggWithOutGroup(): Unit = {
    checkResult(
      """
        |SELECT
        |  avg(deposit),
        |  sum(deposit),
        |  count(*)
        |FROM
        |  AggregatableTable
        |""".stripMargin,
      Seq(
        row(177, 1950, 11))
    )
  }

  @Test
  def testPushDownLocalSortAggWithoutSort(): Unit = {
    // enable sort agg
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg")

    checkResult(
      """
        |SELECT
        |  avg(deposit),
        |  sum(deposit),
        |  count(*)
        |FROM
        |  AggregatableTable
        |""".stripMargin,
      Seq(
        row(177, 1950, 11))
    )

    // reset config
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "")
  }

  @Test
  def testPushDownLocalSortAggWithSort(): Unit = {
    // enable sort agg
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg")

    checkResult(
      """
        |SELECT
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
        row(50, 50, 1, "f", 19),
        row(200, 200, 1, "f", 20),
        row(250, 750, 3, "m", 23),
        row(126, 380, 3, "f", 25),
        row(300, 300, 1, "m", 27),
        row(170, 170, 1, "m", 28),
        row(100, 100, 1, "m", 34))
    )

    // reset config
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "")
  }

  @Test
  def testAggWithAuxGrouping(): Unit = {
    checkResult(
      """
        |SELECT
        |  name,
        |  d,
        |  p,
        |  count(*)
        |FROM (
        |  SELECT
        |    name,
        |    sum(deposit) as d,
        |    max(points) as p
        |  FROM AggregatableTable
        |  GROUP BY name
        |) t
        |GROUP BY name, d, p
        |""".stripMargin,
      Seq(
        row("tom", 200, 1000, 1),
        row("mary", 100, 1000, 1),
        row("jack", 150, 1300, 1),
        row("rose", 100, 500, 1),
        row("danny", 300, 300, 1),
        row("tommas", 400, 4000, 1),
        row("olivia", 50, 9000, 1),
        row("stef", 100, 1900, 1),
        row("emma", 180, 800, 1),
        row("benji", 170, 11000, 1),
        row("eva", 200, 1000, 1))
    )
  }

  @Test
  def testPushDownLocalAggAfterFilterPushDown(): Unit = {
    checkResult(
      """
        |SELECT
        |  avg(deposit),
        |  sum(deposit),
        |  count(1),
        |  gender,
        |  age
        |FROM
        |  AggregatableTable
        |WHERE age <= 20
        |GROUP BY gender, age
        |""".stripMargin,
      Seq(
        row(50, 50, 1, "f", 19),
        row(200, 200, 1, "f", 20))
    )
  }

  @Test
  def testLocalAggWithLimit(): Unit = {
    checkResult(
      """
        |SELECT
        |  avg(deposit) as avg_dep,
        |  sum(deposit),
        |  count(1),
        |  gender
        |FROM
        |  (
        |    SELECT * FROM AggregatableTable
        |    LIMIT 10
        |  ) t
        |GROUP BY gender
        |ORDER BY avg_dep
        |""".stripMargin,
      Seq(
        row(107, 430, 4, "f"),
        row(220, 1320, 6, "m"))
    )
  }

  @Test
  def testLocalAggWithUDAF(): Unit = {
    // add UDAF
    tEnv.createTemporarySystemFunction(
      "udaf_collect",
      new CollectAggFunction(DataTypes.BIGINT().getLogicalType))

    checkResult(
      """
        |SELECT
        |  udaf_collect(deposit),
        |  count(1),
        |  gender,
        |  age
        |FROM
        |  AggregatableTable
        |GROUP BY gender, age
        |""".stripMargin,
      Seq(
        row("{100=1}", 1, "m", 34),
        row("{100=2, 180=1}", 3, "f", 25),
        row("{170=1}", 1, "m", 28),
        row("{200=1}", 1, "f", 20),
        row("{300=1}", 1, "m", 27),
        row("{400=1, 150=1, 200=1}", 3, "m", 23),
        row("{50=1}", 1, "f", 19))
    )
  }

  @Test
  def testLocalAggWithUnsupportedDataTypes(): Unit = {
    // only agg on Long columns and count are supported to be pushed down
    // in {@link TestValuesTableFactory}

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
  def testLocalAggWithColumnExpression1(): Unit = {
    checkResult(
      """
        |SELECT
        |  avg(deposit),
        |  sum(deposit + points),
        |  count(1),
        |  gender,
        |  age
        |FROM
        |  AggregatableTable
        |GROUP BY gender, age
        |""".stripMargin,
      Seq(
        row(250, 7050, 3, "m", 23),
        row(126, 2680, 3, "f", 25),
        row(300, 600, 1, "m", 27),
        row(50, 9050, 1, "f", 19),
        row(100, 2000, 1, "m", 34),
        row(170, 11170, 1, "m", 28),
        row(200, 1200, 1, "f", 20))
    )
  }

  @Test
  def testLocalAggWithColumnExpression2(): Unit = {

    checkResult(
      """
        |SELECT
        |  avg(distinct deposit),
        |  sum(deposit),
        |  count(1),
        |  gender,
        |  age
        |FROM
        |  AggregatableTable
        |GROUP BY gender, age
        |""".stripMargin,
      Seq(
        row(50, 50, 1, "f", 19),
        row(200, 200, 1, "f", 20),
        row(140, 380, 3, "f", 25),
        row(250, 750, 3, "m", 23),
        row(300, 300, 1, "m", 27),
        row(170, 170, 1, "m", 28),
        row(100, 100, 1, "m", 34))
    )
  }

  @Test
  def testLocalAggWithWindow(): Unit = {

    checkResult(
      """
        |SELECT
        |  avg(deposit) over (partition by gender),
        |  gender,
        |  age
        |FROM
        |  AggregatableTable
        |""".stripMargin,
      Seq(
        row(126, "f", 19),
        row(126, "f", 20),
        row(220, "m", 23),
        row(220, "m", 23),
        row(220, "m", 23),
        row(126, "f", 25),
        row(126, "f", 25),
        row(126, "f", 25),
        row(220, "m", 27),
        row(220, "m", 28),
        row(220, "m", 34)
      )
    )
  }


  @Test
  def testLocalAggWithFilter(): Unit = {

    checkResult(
      """
        |SELECT
        |  avg(deposit),
        |  sum(deposit) FILTER(WHERE deposit > 100),
        |  count(1),
        |  gender,
        |  age
        |FROM
        |  AggregatableTable
        |GROUP BY gender, age
        |""".stripMargin,
      Seq(
        row(100, null, 1, "m", 34),
        row(126, 180, 3, "f", 25),
        row(170, 170, 1, "m", 28),
        row(200, 200, 1, "f", 20),
        row(250, 750, 3, "m", 23),
        row(300, 300, 1, "m", 27),
        row(50, null, 1, "f", 19))
    )
  }

}
