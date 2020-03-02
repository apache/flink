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

package org.apache.flink.table.planner.plan.common

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableTestBase}
import org.junit.{Before, Test}

abstract class DistinctAggregateTestBase extends TableTestBase {
  protected val util: BatchTableTestUtil = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String, String, String)]("MyTable2", 'a, 'b, 'c, 'd, 'e)
  }
  @Test
  def testSingleDistinctAgg(): Unit = {
    util.verifyPlan("SELECT COUNT(DISTINCT a) FROM MyTable")
  }

  @Test
  def testMultiDistinctAggOnSameColumn(): Unit = {
    util.verifyPlan("SELECT COUNT(DISTINCT a), SUM(DISTINCT a) FILTER (WHERE b > 0),\n" +
      "MAX(DISTINCT a) FROM MyTable")
  }

  @Test
  def testSingleDistinctAggAndOneOrMultiNonDistinctAgg1(): Unit = {
    // case 0x00: DISTINCT on COUNT and Non-DISTINCT on others
    util.verifyPlan("SELECT COUNT(DISTINCT a) FILTER (WHERE a > 0), SUM(b) FROM MyTable")
  }

  @Test
  def testSingleDistinctAggAndOneOrMultiNonDistinctAgg2(): Unit = {
    // case 0x01: Non-DISTINCT on COUNT and DISTINCT on others
    // when field `a` is non-nullable, count(a) = count(*)
    util.verifyPlan("SELECT COUNT(a) filter (WHERE a > 0), SUM(DISTINCT b) FROM MyTable")
  }

  @Test
  def testMultiDistinctAggOnDifferentColumn(): Unit = {
    util.verifyPlan("SELECT COUNT(DISTINCT a), SUM(DISTINCT b),\n" +
      "COUNT(DISTINCT c) FILTER (WHERE a > 5) FROM MyTable")
  }

  @Test
  def testMultiDistinctAndNonDistinctAggOnDifferentColumn(): Unit = {
    util.verifyPlan("SELECT COUNT(DISTINCT a) FILTER (WHERE c > 0),\n" +
      "SUM(DISTINCT b), COUNT(c) FROM MyTable")
  }

  @Test
  def testSingleDistinctAggWithGroupBy(): Unit = {
    // when field `a` is non-nullable, count(a) = count(*)
    util.verifyPlan("SELECT a, COUNT(a), SUM(DISTINCT b) FROM MyTable GROUP BY a")
  }

  @Test
  def testSingleDistinctAggWithGroupByAndCountStar(): Unit = {
    util.verifyPlan("SELECT a, COUNT(*), SUM(DISTINCT b) FROM MyTable GROUP BY a")
  }

  @Test
  def testTwoDistinctAggWithGroupByAndCountStar(): Unit = {
    val sqlQuery = "SELECT a, COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT b) FROM MyTable GROUP BY a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTwoDifferentDistinctAggWithGroupByAndCountStar(): Unit = {
    val sqlQuery = "SELECT a, COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT c) FROM MyTable GROUP BY a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDifferentDistinctAggWithNonDistinctAggOnSameColumn(): Unit = {
    util.verifyPlan("SELECT COUNT(DISTINCT a), SUM(DISTINCT b), MAX(a), MIN(a) FROM MyTable")
  }

  @Test
  def testMultiDifferentDistinctAggWithNonDistinctAggOnSameColumnAndGroupBy(): Unit = {
    val sqlQuery =
      "SELECT COUNT(DISTINCT a), SUM(DISTINCT b), MAX(a), MIN(a) FROM MyTable GROUP BY c"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDifferentDistinctAggWithNonDistinctAggOnDifferentColumnAndGroupBy(): Unit = {
    util.verifyPlan("SELECT SUM(DISTINCT a), COUNT(DISTINCT c) FROM MyTable GROUP BY b")
  }

  @Test
  def testDistinctAggWithDuplicateField(): Unit = {
    // when field `a` is non-nullable, count(a) = count(*)
    util.verifyPlan("SELECT a, COUNT(a), SUM(b), SUM(DISTINCT b) FROM MyTable GROUP BY a")
  }

  @Test
  def testMultiDistinctAggOnSameColumnWithGroupingSets(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT a), MAX(DISTINCT a) " +
      "FROM MyTable2 GROUP BY GROUPING SETS (b, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggAndOneOrMultiNonDistinctAggWithGroupingSets1(): Unit = {
    // case 0x00: DISTINCT on COUNT and Non-DISTINCT on others
    util.verifyPlan("SELECT COUNT(DISTINCT a), SUM(b) FROM MyTable2 GROUP BY GROUPING SETS (b, c)")
  }

  @Test
  def testSingleDistinctAggAndOneOrMultiNonDistinctAggWithGroupingSets2(): Unit = {
    // case 0x01: Non-DISTINCT on COUNT and DISTINCT on others
    util.verifyPlan("SELECT COUNT(a), SUM(DISTINCT b) FROM MyTable2 GROUP BY GROUPING SETS (c, d)")
  }

  @Test
  def testMultiDistinctAggOnDifferentColumnWithGroupingSets(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT b) FROM MyTable2 " +
      "GROUP BY GROUPING SETS (c, d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctAndNonDistinctAggOnDifferentColumnWithGroupingSets(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT b), COUNT(c) FROM MyTable2 " +
      "GROUP BY GROUPING SETS (d, e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctWithFilter(): Unit = {
    val sqlQuery = "SELECT d, COUNT(DISTINCT c) FILTER (WHERE a > 0) FROM MyTable2 GROUP BY d"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctOnSameColumnWithFilter(): Unit = {
    val sqlQuery = "SELECT d, COUNT(DISTINCT c), COUNT(DISTINCT c) FILTER (WHERE a > 10),\n" +
      "COUNT(DISTINCT c) FILTER (WHERE a < 10) FROM MyTable2 GROUP BY d"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def TestMultiDistinctOnDifferentColumnWithFilter(): Unit = {
    val sqlQuery = "SELECT d, COUNT(DISTINCT c), COUNT(DISTINCT c) FILTER (WHERE a > 0),\n" +
      "COUNT(DISTINCT b) FILTER (WHERE b > 1) FROM MyTable2 GROUP BY d"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def TestMultiDistinctWithFilterAndNonDistinctAgg(): Unit = {
    val sqlQuery = "SELECT d, COUNT(DISTINCT c), COUNT(DISTINCT c) FILTER (WHERE a > 0),\n" +
      "MAX(e), MIN(e) FROM MyTable2 GROUP BY d"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctAndNonDistinctAggWithFilter(): Unit = {
    val sqlQuery = "SELECT d, MAX(e), MAX(e) FILTER (WHERE a < 10), COUNT(DISTINCT c),\n" +
      "COUNT(DISTINCT c) FILTER (WHERE a > 5), COUNT(DISTINCT b) FILTER (WHERE b > 3)\n" +
      "FROM MyTable2 GROUP BY d"
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testTooManyDistinctAggOnDifferentColumn(): Unit = {
    // max group count must be less than 64
    val fieldNames = (0 until 64).map(i => s"f$i").toArray
    val fieldTypes: Array[TypeInformation[_]] = Array.fill(fieldNames.length)(Types.INT)
    util.addTableSource("MyTable64", fieldTypes, fieldNames)

    val distinctList = fieldNames.map(f => s"COUNT(DISTINCT $f)").mkString(", ")
    val maxList = fieldNames.map(f => s"MAX($f)").mkString(", ")
    val sqlQuery = s"SELECT $distinctList, $maxList FROM MyTable64"

    util.verifyPlan(sqlQuery)
  }
}
