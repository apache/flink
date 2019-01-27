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

class JoinTest extends TableTestBase {

  @Test
  def testLeftOuterJoinEquiPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t LEFT OUTER JOIN s ON a = z"

    util.verifyPlan(query)
  }

  @Test
  def testLeftOuterJoinEquiAndLocalPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t LEFT OUTER JOIN s ON a = z AND b < 2"

    util.verifyPlan(query)
  }

  @Test
  def testLeftOuterJoinEquiAndNonEquiPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t LEFT OUTER JOIN s ON a = z AND b < x"

    util.verifyPlan(query)
  }

  @Test
  def testRightOuterJoinEquiPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t RIGHT OUTER JOIN s ON a = z"

    util.verifyPlan(query)
  }

  @Test
  def testRightOuterJoinEquiAndLocalPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, x FROM t RIGHT OUTER JOIN s ON a = z AND x < 2"

    util.verifyPlan(query)
  }

  @Test
  def testRightOuterJoinEquiAndNonEquiPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t RIGHT OUTER JOIN s ON a = z AND b < x"

    util.verifyPlan(query)
  }

  @Test
  def testFullOuterJoinEquiPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t FULL OUTER JOIN s ON a = z"

    util.verifyPlan(query)
  }

  @Test
  def testFullOuterJoinEquiAndLocalPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t FULL OUTER JOIN s ON a = z AND b < 2 AND z > 5"

    util.verifyPlan(query)
  }

  @Test
  def testFullOuterJoinEquiAndNonEquiPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t FULL OUTER JOIN s ON a = z AND b < x"

    util.verifyPlan(query)
  }

  @Test
  def testJoinNonMatchingKeyTypes(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE a = g"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFullOuterJoinNoEquiJoinPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3 FULL OUTER JOIN Table5 ON b <> e"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinNoEquiJoinPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3 RIGHT OUTER JOIN Table5 ON b < e"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinNoEquiJoinPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3 LEFT OUTER JOIN Table5 ON b > e"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinNoEqualityPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE d = f"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCrossJoin(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("Table4", 'a1, 'b1, 'c1)

    val sqlQuery = "SELECT a, a1 FROM Table3 CROSS JOIN Table4"

    util.verifyPlan(sqlQuery)
  }

}
