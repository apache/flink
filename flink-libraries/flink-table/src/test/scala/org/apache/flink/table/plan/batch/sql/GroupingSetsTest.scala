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

import org.junit.{Before, Test}

class GroupingSetsTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    val programs = FlinkBatchPrograms.buildPrograms(util.tableEnv.getConfig.getConf)
    programs.remove(FlinkBatchPrograms.PHYSICAL)
    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
  }

  @Test
  def testGroupingSets(): Unit = {
    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g FROM MyTable " +
      "GROUP BY GROUPING SETS (b, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCube(): Unit = {
    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g, " +
      "GROUPING(b) as gb, GROUPING(c) as gc, " +
      "GROUPING_ID(b) as gib, GROUPING_ID(c) as gic, " +
      "GROUPING_ID(b, c) as gid " +
      "FROM MyTable " +
      "GROUP BY CUBE (b, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRollup(): Unit = {
    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g, " +
      "GROUPING(b) as gb, GROUPING(c) as gc, " +
      "GROUPING_ID(b) as gib, GROUPING_ID(c) as gic, " +
      "GROUPING_ID(b, c) as gid " + " FROM MyTable " +
      "GROUP BY ROLLUP (b, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingSetsWithOneGrouping(): Unit = {
    val sqlQuery = "SELECT b, avg(a) as a, GROUP_ID() as g, " +
      " GROUPING(b) as gb, GROUPING_ID(b) as gib " +
      " FROM MyTable GROUP BY GROUPING SETS (b)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingSetsWithDuplicateFields1(): Unit = {
    // field 'b' and 'c' need be outputted as duplicate fields
    val sqlQuery = "SELECT count(b) as b, count(c) as c FROM MyTable " +
      "GROUP BY GROUPING SETS (b, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingSetsWithDuplicateFields2(): Unit = {
    // field 'a' need not be outputted as duplicate field
    val sqlQuery = "SELECT count(a) as a, count(b) as b, count(c) as c FROM MyTable " +
      "GROUP BY GROUPING SETS ((a, b), (a, c))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCALCITE1824(): Unit = {
    val sqlQuery = "SELECT a, GROUP_ID() as g, COUNT(*) as c " +
      "from MyTable GROUP BY GROUPING SETS (a, (), ())"
    // TODO:
    // When "[CALCITE-1824] GROUP_ID returns wrong result" is fixed,
    // "Calc(select=[a, 0 AS g, c])" will be changed to
    // "Calc(select=[a, CASE(=($e, 0), 0, =($e, 1), 0, 1) AS g, c])".
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testTooManyGroupingFields(): Unit = {
    // max group count must be less than 64
    val fieldNames = (0 until 64).map(i => s"f$i").toArray
    val fieldTypes: Array[TypeInformation[_]] = Array.fill(fieldNames.length)(Types.INT)
    val fieldNullables = Array.fill(fieldNames.length)(false)
    val ts = new TestTableSourceWithFieldNullables(fieldNames, fieldTypes, fieldNullables)
    util.tableEnv.registerTableSource("MyTable64", ts)

    val fields = fieldNames.mkString(",")
    val sqlQuery = s"SELECT $fields FROM MyTable64 GROUP BY GROUPING SETS ($fields)"

    util.printSql(sqlQuery)
  }
}
