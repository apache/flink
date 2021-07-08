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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

/**
  * Test for [[DecomposeGroupingSetsRule]].
  */
class DecomposeGroupingSetsRuleTest extends TableTestBase {
  private val util = batchTestUtil()
  util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
  util.buildBatchProgram(FlinkBatchProgram.PHYSICAL)

  @Test
  def testGroupingSets(): Unit = {
    val sqlQuery =
      """
        |SELECT b, c, avg(a) AS a, GROUP_ID() AS g FROM MyTable
        |GROUP BY GROUPING SETS (b, c)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testCube(): Unit = {
    val sqlQuery =
      """
        |SELECT b, c,
        |    AVG(a) AS a,
        |    GROUP_ID() AS g,
        |    GROUPING(b) AS gb,
        |    GROUPING(c) AS gc,
        |    GROUPING_ID(b) AS gib,
        |    GROUPING_ID(c) AS gic,
        |    GROUPING_ID(b, c) AS gid
        |FROM MyTable
        |    GROUP BY CUBE (b, c)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRollup(): Unit = {
    val sqlQuery =
      """
        |SELECT b, c,
        |    AVG(a) AS a,
        |    GROUP_ID() AS g,
        |    GROUPING(b) AS gb,
        |    GROUPING(c) AS gc,
        |    GROUPING_ID(b) AS gib,
        |    GROUPING_ID(c) AS gic,
        |    GROUPING_ID(b, c) as gid
        |FROM MyTable
        |     GROUP BY ROLLUP (b, c)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testGroupingSetsWithOneGrouping(): Unit = {
    val sqlQuery =
      """
        |SELECT b,
        |    AVG(a) AS a,
        |    GROUP_ID() as g,
        |    GROUPING(b) as gb,
        |    GROUPING_ID(b) as gib
        |FROM MyTable
        |    GROUP BY GROUPING SETS (b)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testGroupingSetsWithDuplicateFields1(): Unit = {
    // field 'b' and 'c' need be outputted as duplicate fields
    val sqlQuery =
      """
        |SELECT count(b) as b, count(c) as c FROM MyTable GROUP BY GROUPING SETS (b, c)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testGroupingSetsWithDuplicateFields2(): Unit = {
    // field 'a' need not be outputted as duplicate field
    val sqlQuery =
      """
        |SELECT count(a) as a, count(b) as b, count(c) as c FROM MyTable
        |GROUP BY GROUPING SETS ((a, b), (a, c))
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testCALCITE1824(): Unit = {
    val sqlQuery =
      """
        |SELECT a, GROUP_ID() AS g, COUNT(*) as c FROM MyTable GROUP BY GROUPING SETS (a, (), ())
      """.stripMargin
    // TODO:
    // When "[CALCITE-1824] GROUP_ID returns wrong result" is fixed,
    // "Calc(select=[a, 0 AS g, c])" will be changed to
    // "Calc(select=[a, CASE(=($e, 0), 0, =($e, 1), 0, 1) AS g, c])".
    util.verifyRelPlan(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testTooManyGroupingFields(): Unit = {
    // max group count must be less than 64
    val fieldNames = (0 until 64).map(i => s"f$i").toArray
    val fieldTypes: Array[TypeInformation[_]] = Array.fill(fieldNames.length)(Types.INT)
    util.addTableSource("MyTable64", fieldTypes, fieldNames)

    val fields = fieldNames.mkString(",")
    val sqlQuery = s"SELECT $fields FROM MyTable64 GROUP BY GROUPING SETS ($fields)"

    util.verifyRelPlan(sqlQuery)
  }
}
