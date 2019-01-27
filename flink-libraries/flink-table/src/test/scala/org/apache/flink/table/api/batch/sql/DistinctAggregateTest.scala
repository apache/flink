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

package org.apache.flink.table.api.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class DistinctAggregateTest extends TableTestBase {

  @Test
  def testSingleDistinctAggregate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT COUNT(DISTINCT a) FROM MyTable"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctAggregateOnSameColumn(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT a), MAX(DISTINCT a) FROM MyTable"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggregateAndOneOrMultiNonDistinctAggregate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    // case 0x00: DISTINCT on COUNT and Non-DISTINCT on others
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(b) FROM MyTable"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggregateAndOneOrMultiNonDistinctAggregate2(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    // case 0x01: Non-DISTINCT on COUNT and DISTINCT on others
    val sqlQuery = "SELECT COUNT(a), SUM(DISTINCT b) FROM MyTable"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctAggregateOnDifferentColumn(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT b) FROM MyTable"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctAndNonDistinctAggregateOnDifferentColumn(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT b), COUNT(c) FROM MyTable"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggregateWithGrouping(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT a, COUNT(a), SUM(DISTINCT b) FROM MyTable GROUP BY a"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggregateWithGroupingAndCountStar(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT a, COUNT(*), SUM(DISTINCT b) FROM MyTable GROUP BY a"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTwoDistinctAggregateWithGroupingAndCountStar(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT a, COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT b) FROM MyTable GROUP BY a"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTwoDifferentDistinctAggregateWithGroupingAndCountStar(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT a, COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT c) FROM MyTable GROUP BY a"

    util.verifyPlan(sqlQuery)
  }
}
