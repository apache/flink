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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize.FlinkBatchPrograms
import org.apache.flink.table.util.{TableTestBase, TestTableSourceWithFieldNullables}

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

@RunWith(classOf[Parameterized])
class DistinctAggregateTest(fieldsNullable: Boolean) extends TableTestBase {

  private val util = nullableBatchTestUtil(fieldsNullable)

  @Before
  def before(): Unit = {
    val programs = FlinkBatchPrograms.buildPrograms(util.getTableEnv.getConfig.getConf)
    programs.remove(FlinkBatchPrograms.PHYSICAL)
    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(programs).build()
    util.getTableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addTable[(Int, Long, String, String, String)]("MyTable5", 'a, 'b, 'c, 'd, 'e)
  }

  @Test
  def testSingleDistinctAgg(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggOnMultiColumns(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a, b) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctAggOnSameColumn(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT a), MAX(DISTINCT a) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggAndOneOrMultiNonDistinctAgg1(): Unit = {
    // case 0x00: DISTINCT on COUNT and Non-DISTINCT on others
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(b) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggAndOneOrMultiNonDistinctAgg2(): Unit = {
    // case 0x01: Non-DISTINCT on COUNT and DISTINCT on others
    val sqlQuery = "SELECT COUNT(a), SUM(DISTINCT b) FROM MyTable"
    // when field `a` is non-nullable, count(a) = count(*)
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctAggOnDifferentColumn(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT b) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctAndNonDistinctAggOnDifferentColumn(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT b), COUNT(c) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggWithGroupBy(): Unit = {
    val sqlQuery = "SELECT a, COUNT(a), SUM(DISTINCT b) FROM MyTable GROUP BY a"
    // when field `a` is non-nullable, count(a) = count(*)
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggWithGroupByAndCountStar(): Unit = {
    val sqlQuery = "SELECT a, COUNT(*), SUM(DISTINCT b) FROM MyTable GROUP BY a"
    util.verifyPlan(sqlQuery)
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
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT b), MAX(a), MIN(a) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDifferentDistinctAggWithNonDistinctAggOnSameColumnAndGroupBy(): Unit = {
    val sqlQuery =
      "SELECT COUNT(DISTINCT a), SUM(DISTINCT b), MAX(a), MIN(a) FROM MyTable GROUP BY c"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDifferentDistinctAggWithNonDistinctAggOnDifferentColumnAndGroupBy(): Unit = {
    val sqlQuery = "SELECT SUM(DISTINCT a), COUNT(DISTINCT c) FROM MyTable GROUP BY b"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDistinctAggWithDuplicateField(): Unit = {
    val sqlQuery = "SELECT a, COUNT(a), SUM(b), SUM(DISTINCT b) FROM MyTable GROUP BY a"
    // when field `a` is non-nullable, count(a) = count(*)
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggOnMultiColumnsWithGroupingSets(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a, b) FROM MyTable5 GROUP BY GROUPING SETS (c, d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctAggOnSameColumnWithGroupingSets(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT a), MAX(DISTINCT a) " +
      "FROM MyTable5 GROUP BY GROUPING SETS (b, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggAndOneOrMultiNonDistinctAggWithGroupingSets1(): Unit = {
    // case 0x00: DISTINCT on COUNT and Non-DISTINCT on others
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(b) FROM MyTable5 GROUP BY GROUPING SETS (b, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggAndOneOrMultiNonDistinctAggWithGroupingSets2(): Unit = {
    // case 0x01: Non-DISTINCT on COUNT and DISTINCT on others
    val sqlQuery = "SELECT COUNT(a), SUM(DISTINCT b) FROM MyTable5 GROUP BY GROUPING SETS (c, d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctAggOnDifferentColumnWithGroupingSets(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT b) FROM MyTable5 " +
      "GROUP BY GROUPING SETS (c, d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDistinctAndNonDistinctAggOnDifferentColumnWithGroupingSets(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT b), COUNT(c) FROM MyTable5 " +
      "GROUP BY GROUPING SETS (d, e)"
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testTooManyDistinctAggOnDifferentColumn(): Unit = {
    // max group count must be less than 64
    val fieldNames = (0 until 64).map(i => s"f$i").toArray
    val fieldTypes: Array[TypeInformation[_]] = Array.fill(fieldNames.length)(Types.INT)
    val fieldNullables = Array.fill(fieldNames.length)(false)
    val ts = new TestTableSourceWithFieldNullables(fieldNames, fieldTypes, fieldNullables)
    util.tableEnv.registerTableSource("MyTable64", ts)

    val distinctList = fieldNames.map(f => s"COUNT(DISTINCT $f)").mkString(", ")
    val maxList = fieldNames.map(f => s"MAX($f)").mkString(", ")
    val sqlQuery = s"SELECT $distinctList, $maxList FROM MyTable64"

    util.printSql(sqlQuery)
  }

}

object DistinctAggregateTest {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}
