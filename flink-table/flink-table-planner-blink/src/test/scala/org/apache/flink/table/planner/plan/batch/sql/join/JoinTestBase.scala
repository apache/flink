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
package org.apache.flink.table.planner.plan.batch.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableTestBase}

import org.junit.Test

abstract class JoinTestBase extends TableTestBase {

  protected val util: BatchTableTestUtil = batchTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
  util.addTableSource[(Int, Long, Int, String, Long)]("MyTable2", 'd, 'e, 'f, 'g, 'h)

  @Test(expected = classOf[ValidationException])
  def testJoinNonExistingKey(): Unit = {
    util.verifyPlan("SELECT c, g FROM MyTable1, MyTable2 WHERE foo = e")
  }

  @Test(expected = classOf[TableException])
  def testJoinNonMatchingKeyTypes(): Unit = {
    // INTEGER and VARCHAR(65536) does not have common type now
    util.verifyPlan("SELECT c, g FROM MyTable1, MyTable2 WHERE a = g")
  }

  @Test(expected = classOf[ValidationException])
  def testJoinWithAmbiguousFields(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable0", 'a0, 'b0, 'c)
    util.verifyPlan("SELECT a, c FROM MyTable1, MyTable0 WHERE a = a0")
  }

  @Test
  def testInnerJoinWithEquiPred(): Unit = {
    util.verifyPlan("SELECT c, g FROM MyTable1, MyTable2 WHERE a = d")
  }

  @Test
  def testInnerJoinWithFilter(): Unit = {
    util.verifyPlan("SELECT c, g FROM MyTable2, MyTable1 WHERE a = d AND d < 2")
  }

  @Test
  def testInnerJoinWithEquiAndLocalPred(): Unit = {
    util.verifyPlan("SELECT c, g FROM MyTable2 INNER JOIN MyTable1 ON a = d AND d < 2")
  }

  @Test
  def testInnerJoinWithEquiAndNonEquiPred(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable2 INNER JOIN MyTable1 ON a = d AND d < 2 AND b < h")
  }

  @Test
  def testInnerJoinWithoutJoinPred(): Unit = {
    val query = "SELECT a, d FROM MyTable1, MyTable2"
    util.verifyPlan(query)
  }

  @Test
  def testInnerJoinWithNonEquiPred(): Unit = {
    val query = "SELECT a, d FROM MyTable1, MyTable2 WHERE a + 1 = d"
    util.verifyPlan(query)
  }

  @Test
  def testInnerJoinWithMultipleKeys(): Unit = {
    util.verifyPlan("SELECT c, g FROM MyTable2 INNER JOIN MyTable1 ON a = d AND b = e")
  }

  @Test
  def testInnerJoinWithInvertedField(): Unit = {
    util.verifyPlan("SELECT c, g FROM MyTable1, MyTable2 WHERE b = e AND a = d")
  }

  @Test
  def testLeftOuterJoinWithEquiPred(): Unit = {
    util.verifyPlan("SELECT c, g FROM MyTable1 LEFT OUTER JOIN MyTable2 ON b = e")
  }

  @Test
  def testLeftOuterJoinWithEquiAndLocalPred(): Unit = {
    util.verifyPlan("SELECT c, g FROM MyTable2 LEFT OUTER JOIN  MyTable1 ON a = d AND d < 2")
  }

  @Test
  def testLeftOuterJoinWithEquiAndNonEquiPred(): Unit = {
    val sql = "SELECT * FROM MyTable2 LEFT OUTER JOIN  MyTable1 ON a = d AND d < 2 AND b < h"
    util.verifyPlan(sql)
  }

  @Test
  def testLeftOuterJoinNoEquiPred(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable2 LEFT OUTER JOIN MyTable1 ON a <> d")
  }

  @Test
  def testLeftOuterJoinOnTrue(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable2 LEFT OUTER JOIN MyTable1 ON true")
  }

  @Test
  def testLeftOuterJoinOnFalse(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable2 LEFT OUTER JOIN MyTable1 ON false")
  }

  @Test
  def testRightOuterJoinWithEquiPred(): Unit = {
    util.verifyPlan("SELECT c, g FROM MyTable1 RIGHT OUTER JOIN MyTable2 ON b = e")
  }

  @Test
  def testRightOuterJoinWithEquiAndLocalPred(): Unit = {
    util.verifyPlan("SELECT c, g FROM MyTable2 RIGHT OUTER JOIN  MyTable1 ON a = d AND d < 2")
  }

  @Test
  def testRightOuterJoinWithEquiAndNonEquiPred(): Unit = {
    val sql = "SELECT * FROM MyTable2 RIGHT OUTER JOIN  MyTable1 ON a = d AND d < 2 AND b < h"
    util.verifyPlan(sql)
  }

  @Test
  def testRightOuterJoinWithNonEquiPred(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable2 RIGHT OUTER JOIN MyTable1 ON a <> d")
  }

  @Test
  def testRightOuterJoinOnTrue(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable2 RIGHT OUTER JOIN MyTable1 ON true")
  }

  @Test
  def testRightOuterJoinOnFalse(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable2 RIGHT OUTER JOIN MyTable1 ON false")
  }

  @Test
  def testFullOuterJoinWithEquiPred(): Unit = {
    util.verifyPlan("SELECT c, g FROM MyTable1 FULL OUTER JOIN MyTable2 ON b = e")
  }

  @Test
  def testFullOuterJoinWithEquiAndLocalPred(): Unit = {
    util.verifyPlan("SELECT c, g FROM MyTable2 FULL OUTER JOIN  MyTable1 ON a = d AND d < 2")
  }

  @Test
  def testFullOuterJoinWithEquiAndNonEquiPred(): Unit = {
    val sql = "SELECT * FROM MyTable2 FULL OUTER JOIN MyTable1 ON a = d AND d < 2 AND b < h"
    util.verifyPlan(sql)
  }

  @Test
  def testFullOuterJoinWithNonEquiPred(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable2 FULL OUTER JOIN MyTable1 ON a <> d")
  }

  @Test
  def testFullOuterJoinOnTrue(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable2 FULL OUTER JOIN MyTable1 ON true")
  }

  @Test
  def testFullOuterJoinOnFalse(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable2 FULL OUTER JOIN MyTable1 ON false")
  }

  @Test
  def testFullOuterWithUsing(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable3", 'a, 'b, 'c)
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM MyTable1) FULL JOIN (SELECT * FROM MyTable3) USING (a)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCrossJoin(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable2 CROSS JOIN MyTable1")
  }

  @Test
  def testSelfJoin(): Unit = {
    util.addTableSource[(Long, String)]("src", 'k, 'v)
    val sql =
      s"""SELECT * FROM
         |  (SELECT * FROM src WHERE k = 0) src1
         |LEFT OUTER JOIN
         |  (SELECT * from src WHERE k = 0) src2
         |ON (src1.k = src2.k AND src2.k > 10)
         """.stripMargin
    util.verifyPlan(sql)
  }
}
