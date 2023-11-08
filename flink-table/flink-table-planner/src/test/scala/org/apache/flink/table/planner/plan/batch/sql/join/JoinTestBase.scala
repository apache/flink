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
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableTestBase}

import org.junit.Test

abstract class JoinTestBase extends TableTestBase {

  protected val util: BatchTableTestUtil = batchTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
  util.addTableSource[(Int, Long, Int, String, Long)]("MyTable2", 'd, 'e, 'f, 'g, 'h)

  @Test(expected = classOf[ValidationException])
  def testJoinNonExistingKey(): Unit = {
    util.verifyExecPlan("SELECT c, g FROM MyTable1, MyTable2 WHERE foo = e")
  }

  @Test
  def testLeftOuterJoinWithFilter2(): Unit = {
    // For left/right join, we will only push equal filter condition into
    // other side by derived from join condition and filter condition. So,
    // d IS NULL cannot be push into left side.
    util.verifyExecPlan(
      "SELECT d, e, f FROM MyTable1 LEFT JOIN MyTable2 ON a = d where d IS NULL AND a < 12")
  }

  @Test
  def testLeftOuterJoinWithFilter3(): Unit = {
    // For left/right join, we will only push equal filter condition into
    // other side by derived from join condition and filter condition. So,
    // d < 10 cannot be push into left side.
    util.verifyExecPlan(
      "SELECT d, e, f FROM MyTable1 LEFT JOIN MyTable2 ON a = d where d < 10 AND a < 12")
  }

  @Test
  def testLeftOuterJoinWithFilter4(): Unit = {
    // For left/right join, we will only push equal filter condition into
    // other side by derived from join condition and filter condition. So,
    // d = null cannot be push into left side.
    util.verifyExecPlan("SELECT d, e, f FROM MyTable1 LEFT JOIN MyTable2 ON a = d where d = null")
  }

  @Test(expected = classOf[TableException])
  def testJoinNonMatchingKeyTypes(): Unit = {
    // INTEGER and VARCHAR(65536) does not have common type now
    util.verifyExecPlan("SELECT c, g FROM MyTable1, MyTable2 WHERE a = g")
  }

  @Test(expected = classOf[ValidationException])
  def testJoinWithAmbiguousFields(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable0", 'a0, 'b0, 'c)
    util.verifyExecPlan("SELECT a, c FROM MyTable1, MyTable0 WHERE a = a0")
  }

  @Test
  def testInnerJoinWithEquiPred(): Unit = {
    util.verifyExecPlan("SELECT c, g FROM MyTable1, MyTable2 WHERE a = d")
  }

  @Test
  def testInnerJoinWithFilter(): Unit = {
    util.verifyExecPlan("SELECT c, g FROM MyTable2, MyTable1 WHERE a = d AND d < 2")
  }

  @Test
  def testInnerJoinWithEquiAndLocalPred(): Unit = {
    util.verifyExecPlan("SELECT c, g FROM MyTable2 INNER JOIN MyTable1 ON a = d AND d < 2")
  }

  @Test
  def testInnerJoinWithEquiAndNonEquiPred(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable2 INNER JOIN MyTable1 ON a = d AND d < 2 AND b < h")
  }

  @Test
  def testInnerJoinWithoutJoinPred(): Unit = {
    val query = "SELECT a, d FROM MyTable1, MyTable2"
    util.verifyExecPlan(query)
  }

  @Test
  def testInnerJoinWithNonEquiPred(): Unit = {
    val query = "SELECT a, d FROM MyTable1, MyTable2 WHERE a + 1 = d"
    util.verifyExecPlan(query)
  }

  @Test
  def testInnerJoinWithMultipleKeys(): Unit = {
    util.verifyExecPlan("SELECT c, g FROM MyTable2 INNER JOIN MyTable1 ON a = d AND b = e")
  }

  @Test
  def testInnerJoinWithInvertedField(): Unit = {
    util.verifyExecPlan("SELECT c, g FROM MyTable1, MyTable2 WHERE b = e AND a = d")
  }

  @Test
  def testLeftOuterJoinWithEquiPred(): Unit = {
    util.verifyExecPlan("SELECT c, g FROM MyTable1 LEFT OUTER JOIN MyTable2 ON b = e")
  }

  @Test
  def testLeftOuterJoinWithEquiAndLocalPred(): Unit = {
    util.verifyExecPlan("SELECT c, g FROM MyTable2 LEFT OUTER JOIN  MyTable1 ON a = d AND d < 2")
  }

  @Test
  def testLeftOuterJoinWithEquiAndNonEquiPred(): Unit = {
    val sql = "SELECT * FROM MyTable2 LEFT OUTER JOIN  MyTable1 ON a = d AND d < 2 AND b < h"
    util.verifyExecPlan(sql)
  }

  @Test
  def testLeftOuterJoinNoEquiPred(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable2 LEFT OUTER JOIN MyTable1 ON a <> d")
  }

  @Test
  def testLeftOuterJoinOnTrue(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable2 LEFT OUTER JOIN MyTable1 ON true")
  }

  @Test
  def testLeftOuterJoinOnFalse(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable2 LEFT OUTER JOIN MyTable1 ON false")
  }

  @Test
  def testRightOuterJoinWithEquiPred(): Unit = {
    util.verifyExecPlan("SELECT c, g FROM MyTable1 RIGHT OUTER JOIN MyTable2 ON b = e")
  }

  @Test
  def testRightOuterJoinWithEquiAndLocalPred(): Unit = {
    util.verifyExecPlan("SELECT c, g FROM MyTable2 RIGHT OUTER JOIN  MyTable1 ON a = d AND d < 2")
  }

  @Test
  def testRightOuterJoinWithEquiAndNonEquiPred(): Unit = {
    val sql = "SELECT * FROM MyTable2 RIGHT OUTER JOIN  MyTable1 ON a = d AND d < 2 AND b < h"
    util.verifyExecPlan(sql)
  }

  @Test
  def testRightOuterJoinWithNonEquiPred(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable2 RIGHT OUTER JOIN MyTable1 ON a <> d")
  }

  @Test
  def testRightOuterJoinOnTrue(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable2 RIGHT OUTER JOIN MyTable1 ON true")
  }

  @Test
  def testRightOuterJoinOnFalse(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable2 RIGHT OUTER JOIN MyTable1 ON false")
  }

  @Test
  def testFullOuterJoinWithEquiPred(): Unit = {
    util.verifyExecPlan("SELECT c, g FROM MyTable1 FULL OUTER JOIN MyTable2 ON b = e")
  }

  @Test
  def testFullOuterJoinWithEquiAndLocalPred(): Unit = {
    util.verifyExecPlan("SELECT c, g FROM MyTable2 FULL OUTER JOIN  MyTable1 ON a = d AND d < 2")
  }

  @Test
  def testFullOuterJoinWithEquiAndNonEquiPred(): Unit = {
    val sql = "SELECT * FROM MyTable2 FULL OUTER JOIN MyTable1 ON a = d AND d < 2 AND b < h"
    util.verifyExecPlan(sql)
  }

  @Test
  def testFullOuterJoinWithNonEquiPred(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable2 FULL OUTER JOIN MyTable1 ON a <> d")
  }

  @Test
  def testFullOuterJoinOnTrue(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable2 FULL OUTER JOIN MyTable1 ON true")
  }

  @Test
  def testFullOuterJoinOnFalse(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable2 FULL OUTER JOIN MyTable1 ON false")
  }

  @Test
  def testFullOuterWithUsing(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable3", 'a, 'b, 'c)
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM MyTable1) FULL JOIN (SELECT * FROM MyTable3) USING (a)
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testCrossJoin(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable2 CROSS JOIN MyTable1")
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
    util.verifyExecPlan(sql)
  }

  @Test
  def testInnerJoinWithFilterPushDown(): Unit = {
    util.verifyExecPlan("""
                          |SELECT * FROM
                          |   (select a, count(b) as b from MyTable1 group by a)
                          |   join
                          |   (select d, count(e) as e from MyTable2 group by d)
                          |   on true where a = d and b = e and d = 2
                          |""".stripMargin)
  }

  @Test
  def testInnerJoinWithJoinConditionPushDown(): Unit = {
    util.verifyExecPlan("""
                          |SELECT * FROM
                          |   (select a, count(b) as b from MyTable1 group by a)
                          |   join
                          |   (select d, count(e) as e from MyTable2 group by d)
                          |   on a = d and b = e and d = 2 and b = 1
                          |""".stripMargin)
  }

  @Test
  def testLeftJoinWithFilterPushDown(): Unit = {
    util.verifyExecPlan("""
                          |SELECT * FROM
                          |   (select a, count(b) as b from MyTable1 group by a)
                          |   left join
                          |   (select d, count(e) as e from MyTable2 group by d)
                          |   on true where a = d and b = e and a = 2
                          |""".stripMargin)
  }

  @Test
  def testLeftJoinWithJoinConditionPushDown(): Unit = {
    util.verifyExecPlan("""
                          |SELECT * FROM
                          |   (select a, count(b) as b from MyTable1 group by a)
                          |   left join
                          |   (select d, count(e) as e from MyTable2 group by d)
                          |   on a = d and b = e and a = 2 and e = 1
                          |""".stripMargin)
  }

  @Test
  def testRightJoinWithFilterPushDown(): Unit = {
    util.verifyExecPlan("""
                          |SELECT * FROM
                          |   (select a, count(b) as b from MyTable1 group by a)
                          |   right join
                          |   (select d, count(e) as e from MyTable2 group by d)
                          |   on true where a = d and b = e and d = 2
                          |""".stripMargin)
  }

  @Test
  def testRightJoinWithJoinConditionPushDown(): Unit = {
    util.verifyExecPlan("""
                          |SELECT * FROM
                          |   (select a, count(b) as b from MyTable1 group by a)
                          |   right join
                          |   (select d, count(e) as e from MyTable2 group by d)
                          |   on a = d and b = e and d = 2 and b = 1
                          |""".stripMargin)
  }

  @Test
  def testJoinPartitionTableWithNonExistentPartition(): Unit = {
    util.tableEnv.executeSql("""
                               |create table leftPartitionTable (
                               | a1 varchar,
                               | b1 int)
                               | partitioned by (b1) 
                               | with (
                               | 'connector' = 'values',
                               | 'bounded' = 'true',
                               | 'partition-list' = 'b1:1'
                               |)
                               |""".stripMargin)
    util.tableEnv.executeSql("""
                               |create table rightPartitionTable (
                               | a2 varchar,
                               | b2 int)
                               | partitioned by (b2) 
                               | with (
                               | 'connector' = 'values',
                               | 'bounded' = 'true',
                               | 'partition-list' = 'b2:2'
                               |)
                               |""".stripMargin)
    // partition 'b2 = 3' not exists.
    util.verifyExecPlan(
      """
        |SELECT * FROM leftPartitionTable, rightPartitionTable WHERE b1 = 1 AND b2 = 3 AND a1 = a2
        |""".stripMargin)
  }
}
