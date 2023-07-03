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
package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Test

class RankTest extends TableTestBase {

  private val util = batchTestUtil()
  util.addTableSource[(Int, String, Long)]("MyTable", 'a, 'b, 'c)

  def testRowNumberWithoutOrderBy(): Unit = {
    val sqlQuery =
      """
        |SELECT ROW_NUMBER() over (partition by a) FROM MyTable
      """.stripMargin
    assertThatThrownBy(() => util.tableEnv.executeSql(sqlQuery))
      .hasRootCauseMessage(
        "Over Agg: The window rank function requires order by clause with non-constant fields. " +
          "please re-check the over window statement.")
  }

  @Test
  def testRowNumberWithOrderByConstant(): Unit = {
    val sqlQuery =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b,
        |  ROW_NUMBER() OVER (PARTITION BY b ORDER BY '2023-03-29') AS row_num
        |  FROM MyTable
        |)
        |WHERE row_num <= 10
      """.stripMargin

    assertThatThrownBy(() => util.tableEnv.executeSql(sqlQuery))
      .hasRootCauseMessage(
        "Over Agg: The window rank function requires order by clause with non-constant fields. " +
          "please re-check the over window statement.")
  }

  @Test(expected = classOf[RuntimeException])
  def testRowNumberWithMultiGroups(): Unit = {
    val sqlQuery =
      """
        |SELECT ROW_NUMBER() over (partition by a order by b) as a,
        |       ROW_NUMBER() over (partition by b) as b
        |       FROM MyTable
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testRankWithoutOrderBy(): Unit = {
    val sqlQuery =
      """
        |SELECT RANK() over (partition by a) FROM MyTable
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testRankWithMultiGroups(): Unit = {
    val sqlQuery =
      """
        |SELECT RANK() over (partition by a order by b) as a,
        |       RANK() over (partition by b) as b
        |       FROM MyTable
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testDenseRankWithoutOrderBy(): Unit = {
    val sqlQuery =
      """
        |SELECT dense_rank() over (partition by a) FROM MyTable
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testDenseRankWithMultiGroups(): Unit = {
    val sqlQuery =
      """
        |SELECT DENSE_RANK() over (partition by a order by b) as a,
        |       DENSE_RANK() over (partition by b) as b
        |       FROM MyTable
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithUpperValue(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM MyTable) t
        |WHERE rk <= 2 AND a > 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithRange(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY b, c ORDER BY a) rk FROM MyTable) t
        |WHERE rk <= 2 AND rk > -2
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithEquals(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a, c) rk FROM MyTable) t
        |WHERE rk = 2
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testWithoutPartitionBy(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (ORDER BY a) rk FROM MyTable) t
        |WHERE rk < 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultiSameRankFunctionsWithSameGroup(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b,
        |        RANK() OVER (PARTITION BY b ORDER BY a) rk1,
        |        RANK() OVER (PARTITION BY b ORDER BY a) rk2 FROM MyTable) t
        |WHERE rk1 < 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testDuplicateRankFunctionColumnName(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable2", 'a, 'b, 'rk)
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM MyTable2) t
        |WHERE rk < 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRankFunctionInMiddle(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, RANK() OVER (PARTITION BY a ORDER BY a) rk, b, c FROM MyTable) t
        |WHERE rk < 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testCreateViewWithRowNumber(): Unit = {
    util.addTable("""
                    |CREATE TABLE test_source (
                    |  name STRING,
                    |  eat STRING,
                    |  age BIGINT
                    |) WITH (
                    |  'connector' = 'values',
                    |  'bounded' = 'true'
                    |)
      """.stripMargin)
    util.tableEnv.executeSql(
      "create view view1 as select name, eat ,sum(age) as cnt\n"
        + "from test_source group by name, eat")
    util.tableEnv.executeSql(
      "create view view2 as\n"
        + "select *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY cnt DESC) as row_num\n"
        + "from view1")
    util.addTable(
      s"""
         |create table sink (
         |  name varchar,
         |  eat varchar,
         |  cnt bigint
         |)
         |with(
         |  'connector' = 'print'
         |)
         |""".stripMargin
    )
    util.verifyExecPlanInsert(
      "insert into sink select name, eat, cnt\n"
        + "from view2 where row_num <= 3")
  }

  @Test
  def testRankWithAnotherRankAsInput(): Unit = {
    val sql =
      """
        |SELECT CAST(rna AS INT) AS rn1, CAST(rnb AS INT) AS rn2 FROM (
        |  SELECT *, row_number() over (partition by a order by b desc) AS rnb
        |  FROM (
        |    SELECT *, row_number() over (partition by a, c order by b desc) AS rna
        |    FROM MyTable
        |  )
        |  WHERE rna <= 100
        |)
        |WHERE rnb <= 200
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testRedundantRankNumberColumnRemove(): Unit = {
    util.addDataStream[(String, Long, Long, Long)](
      "MyTable1",
      'uri,
      'reqcount,
      'start_time,
      'bucket_id)
    val sql =
      """
        |SELECT
        |  CONCAT('http://txmov2.a.yximgs.com', uri) AS url,
        |  reqcount AS download_count,
        |  start_time AS `timestamp`
        |FROM
        |  (
        |    SELECT
        |      uri,
        |      reqcount,
        |      rownum_2,
        |      start_time
        |    FROM
        |      (
        |        SELECT
        |          uri,
        |          reqcount,
        |          start_time,
        |          RANK() OVER (
        |            PARTITION BY start_time
        |            ORDER BY
        |              reqcount DESC
        |          ) AS rownum_2
        |        FROM
        |          (
        |            SELECT
        |            uri,
        |            reqcount,
        |            start_time,
        |            RANK() OVER (
        |                PARTITION BY start_time, bucket_id
        |                ORDER BY
        |                reqcount DESC
        |            ) AS rownum_1
        |            FROM MyTable1
        |          )
        |        WHERE
        |          rownum_1 <= 100000
        |      )
        |    WHERE
        |      rownum_2 <= 100000
        |  )
        |""".stripMargin
    util.verifyExecPlan(sql)
  }
}
