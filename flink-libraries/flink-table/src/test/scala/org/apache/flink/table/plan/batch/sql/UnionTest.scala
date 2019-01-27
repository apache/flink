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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBase
import org.junit.{Before, Test}

class UnionTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.addTable[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("MyTable2", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("MyTable3", 'a, 'b, 'c)
  }

  @Test
  def testUnionAll(): Unit = {
    val sqlQuery = "SELECT a, c FROM (" +
      "SELECT a, c FROM MyTable1 UNION ALL (SELECT a, c FROM MyTable2))" +
      "WHERE a > 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testUnion(): Unit = {
    val sqlQuery = "SELECT a FROM (SELECT * FROM MyTable1 UNION (SELECT * FROM MyTable2))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTernaryUnion(): Unit = {
    val sqlQuery = "SELECT a FROM (SELECT a FROM MyTable1 WHERE b > 2" +
      "UNION (SELECT a FROM MyTable2 UNION (SELECT a FROM MyTable3)))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testUnionAllDiffType(): Unit = {
    val sqlQuery = "SELECT * FROM (select a, b from MyTable1 union all select a, " +
      "cast(0 as decimal(2, 1)) from MyTable2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testUnionEmptyPrune(): Unit = {
    val sqlQuery = "WITH tmp AS (" +
      "SELECT a, b, c, 's' sale_type FROM MyTable1 " +
      "UNION ALL " +
      "SELECT a, b, c, 'w' sale_type FROM MyTable2)" +
      "SELECT first_t1.a,  sec_t1.b / first_t1.b,  sec_t2.b / first_t2.b " +
      "FROM tmp first_t1, tmp sec_t1, tmp first_t2, tmp sec_t2 " +
      "WHERE first_t1.a = sec_t1.a AND " +
      "first_t2.a = sec_t2.a AND " +
      "first_t1.a = first_t2.a AND " +
      "first_t1.sale_type = 's' AND " +
      "first_t1.c = '2001' AND " +
      "sec_t1.sale_type = 's' AND " +
      "sec_t1.c = '2002' AND " +
      "first_t2.sale_type = 'w' AND " +
      "first_t2.c = '2001' AND " +
      "sec_t2.sale_type = 'w' AND " +
      "sec_t2.c = '2002'"
    util.verifyPlan(sqlQuery)
  }
}
