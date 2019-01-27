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

package org.apache.flink.table.api.stream.sql.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class RankValidationTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String)]("MyTable", 'a, 'b)

  @Test(expected = classOf[RuntimeException])
  def testRowNumberWithOutOrderBy(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, ROW_NUMBER() OVER (PARTITION BY b) as rank_num
        |  FROM MyTable)
        |WHERE rank_num <= a""".stripMargin
    streamUtil.explainSql(sql)
  }

  @Test(expected = classOf[ValidationException])
  def testRankWithOutOrderBy(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, RANK() OVER (PARTITION BY b) as rank_num
        |  FROM MyTable)
        |WHERE rank_num <= a""".stripMargin
    streamUtil.explainSql(sql)
  }

  @Test(expected = classOf[ValidationException])
  def testDenseRankWithOutOrderBy(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, DENSE_RANK() OVER (PARTITION BY b) as rank_num
        |  FROM MyTable)
        |WHERE rank_num <= a""".stripMargin
    streamUtil.explainSql(sql)
  }

  @Test(expected = classOf[RuntimeException])
  def testRowNumberWithMultiGroups(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, ROW_NUMBER() OVER (PARTITION BY b order by a) as rank_num,
        |         ROW_NUMBER() OVER (PARTITION BY a) as rank_num1
        |  FROM MyTable)
        |WHERE rank_num <= a""".stripMargin
    streamUtil.explainSql(sql)
  }

  @Test(expected = classOf[ValidationException])
  def testRankWithMultiGroups(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, RANK() OVER (PARTITION BY b order by a) as rank_num,
        |         RANK() OVER (PARTITION BY a) as rank_num1
        |  FROM MyTable)
        |WHERE rank_num <= a""".stripMargin
    streamUtil.explainSql(sql)
  }

  @Test(expected = classOf[ValidationException])
  def testDenseRankWithMultiGroups(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, DENSE_RANK() OVER (PARTITION BY b order by a) as rank_num,
        |         DENSE_RANK() OVER (PARTITION BY a) as rank_num1
        |  FROM MyTable)
        |WHERE rank_num <= a""".stripMargin
    streamUtil.explainSql(sql)
  }
}
