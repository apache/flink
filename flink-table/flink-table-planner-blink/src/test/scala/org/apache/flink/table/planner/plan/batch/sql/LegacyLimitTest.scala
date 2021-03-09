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
import org.apache.flink.table.api.{SqlParserException, _}
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.{Before, Test}

class LegacyLimitTest extends TableTestBase {

  protected val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val ddl =
      s"""
         |CREATE TABLE LimitTable (
         |  a int,
         |  b bigint,
         |  c string
         |) WITH (
         |  'connector.type' = 'TestLimitableTableSource',
         |  'is-bounded' = 'true'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)
  }

  @Test
  def testLimitWithoutOffset(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable LIMIT 5")
  }

  @Test
  def testLimit0WithoutOffset(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable LIMIT 0")
  }

  @Test(expected = classOf[SqlParserException])
  def testNegativeLimitWithoutOffset(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable LIMIT -1")
  }

  @Test
  def testLimitWithOffset(): Unit = {
    util.verifyExecPlan("SELECT a, c FROM MyTable LIMIT 10 OFFSET 1")
  }

  @Test
  def testLimitWithOffset0(): Unit = {
    util.verifyExecPlan("SELECT a, c FROM MyTable LIMIT 10 OFFSET 0")
  }

  @Test
  def testLimit0WithOffset0(): Unit = {
    util.verifyExecPlan("SELECT a, c FROM MyTable LIMIT 0 OFFSET 0")
  }

  @Test
  def testLimit0WithOffset(): Unit = {
    util.verifyExecPlan("SELECT a, c FROM MyTable LIMIT 0 OFFSET 10")
  }

  @Test(expected = classOf[SqlParserException])
  def testLimitWithNegativeOffset(): Unit = {
    util.verifyExecPlan("SELECT a, c FROM MyTable LIMIT 10 OFFSET -1")
  }

  @Test
  def testFetchWithOffset(): Unit = {
    util.verifyExecPlan("SELECT a, c FROM MyTable OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY")
  }

  @Test
  def testFetchWithoutOffset(): Unit = {
    util.verifyExecPlan("SELECT a, c FROM MyTable FETCH FIRST 10 ROWS ONLY")
  }

  @Test
  def testFetch0WithoutOffset(): Unit = {
    util.verifyExecPlan("SELECT a, c FROM MyTable FETCH FIRST 0 ROWS ONLY")
  }

  @Test
  def testOnlyOffset(): Unit = {
    util.verifyExecPlan("SELECT a, c FROM MyTable OFFSET 10 ROWS")
  }

  @Test
  def testFetchWithLimitSource(): Unit = {
    val sqlQuery = "SELECT a, c FROM LimitTable FETCH FIRST 10 ROWS ONLY"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testOrderByWithLimitSource(): Unit = {
    val sqlQuery = "SELECT a, c FROM LimitTable ORDER BY c LIMIT 10"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testLimitWithLimitSource(): Unit = {
    val sqlQuery = "SELECT a, c FROM LimitTable LIMIT 10"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testLimitWithOffsetAndLimitSource(): Unit = {
    val sqlQuery = "SELECT a, c FROM LimitTable LIMIT 10 OFFSET 1"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testFetchWithOffsetAndLimitSource(): Unit = {
    val sqlQuery = "SELECT a, c FROM LimitTable OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY"
    util.verifyExecPlan(sqlQuery)
  }

}
