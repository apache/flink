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
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBase
import org.junit.{Before, Test}

class OverWindowAggregateTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
  }

  @Test
  def testOverWindowWithoutFrame(): Unit = {
    val sqlQuery = "SELECT c, count(*) over (partition by c) FROM Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOverWindow0PrecedingAnd0Following(): Unit = {
    val sqlQuery = "SELECT c, count(*) " +
        "over (partition by c order by a rows between 0 PRECEDING and 0 FOLLOWING) " +
        "FROM Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOverWindowCurrentRowAnd0Following(): Unit = {
    val sqlQuery = "SELECT c, count(*) " +
        "over (partition by c order by a rows between CURRENT ROW and 0 FOLLOWING) " +
        "FROM Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOverWindow0PrecedingAndCurrentRow(): Unit = {
    val sqlQuery = "SELECT c, count(*) " +
        "over (partition by c order by a rows between 0 PRECEDING and CURRENT ROW) " +
        "FROM Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testOverWindowRangeProhibitType(): Unit = {
    val sqlQuery = "SELECT count(*) " +
        "over (partition by c order by c range between -1 PRECEDING and 10 FOLLOWING) " +
        "FROM Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOverWindowRangeType(): Unit = {
    val sqlQuery = "SELECT count(*) " +
        "over (partition by c order by a range between -1 PRECEDING and 10 FOLLOWING) FROM Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiOverWindowRangeType(): Unit = {
    val sqlQuery = "SELECT count(*) over (partition by c order by a range between -1 PRECEDING " +
        "and 10 FOLLOWING)," +
        "sum(a) over (partition by c order by a)," +
        "rank() over (partition by c order by a, c)," +
        "sum(a) over (partition by c order by a range between 1 PRECEDING and 10 FOLLOWING)," +
        "count(*) over (partition by c order by c rows between 1 PRECEDING and 10 FOLLOWING) FROM" +
        " Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testRowsWindowWithNegative(): Unit = {
    val sqlQuery = "SELECT count(*) over (partition by c order by a rows between -1 PRECEDING and" +
        " 10 FOLLOWING) FROM Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRangeWindowWithNegative1(): Unit = {
    val sqlQuery = "SELECT count(*) over (partition by c order by a range between -1 PRECEDING " +
        "and 10 FOLLOWING) FROM Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRangeWindowWithNegative2(): Unit = {
    val sqlQuery = "SELECT count(*) over (partition by c order by a range between -1 FOLLOWING " +
        "and 10 FOLLOWING) FROM Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankRangeWindowWithCompositeOrders(): Unit = {
    val sqlQuery = "SELECT rank() over (partition by c order by a, c) FROM Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankRangeWindowWithConstants1(): Unit = {
    val sqlQuery = "SELECT count(1) over () FROM Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankRangeWindowWithConstants2(): Unit = {
    val sqlQuery = "SELECT sum(a) over (partition by c order by a range between -1 FOLLOWING " +
        "and 10 FOLLOWING), count(1) over (partition by c order by a range between -1 FOLLOWING " +
        "and 10 FOLLOWING)  FROM Table3"
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testDistinct(): Unit = {
    val sqlQuery = "SELECT sum(Distinct a) over (partition by c order by a range " +
        "between -1 FOLLOWING and 10 FOLLOWING) FROM Table3"
    util.verifyPlan(sqlQuery)
  }
}
