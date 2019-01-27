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
package org.apache.flink.table.api.stream.sql

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableConfigOptions

import org.junit.{Before, Test}

class JoinStreamPlanTest extends StreamPlanTestBase {

  @Before
  def setUp(): Unit = {
    streamUtil.tableEnv.getConfig.setConf(new Configuration())
  }

  @Test
  def testInnerJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A JOIN B ON a1 = b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testInnerJoinWithMiniBatch(): Unit = {
    streamUtil.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    val query = "SELECT a1, b1 FROM A JOIN B ON a1 = b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testInnerJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) JOIN ($query2) ON a1 = b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testInnerJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) JOIN ($query2) ON a2 = b2"
    verifyPlanAndTrait(query)
  }

  @Test
  def testLeftJoinNonEqui(): Unit = {
    val query = "SELECT a1, b1 FROM A LEFT JOIN B ON a1 = b1 AND a2 > b2"
    verifyPlanAndTrait(query)
  }

  @Test
  def testLeftJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN ($query2) ON a1 = b1 AND a2 > b2"
    verifyPlanAndTrait(query)
  }

  @Test
  def testLeftJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN B ON a1 = b1 AND a2 > b2"
    verifyPlanAndTrait(query)
  }

  @Test
  def testLeftJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) LEFT JOIN ($query2) ON a2 = b2 AND a1 > b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testLeftJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A LEFT JOIN B ON a1 = b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testLeftJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN ($query2) ON a1 = b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testLeftJoinWithRightNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN B ON a1 = b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testLeftJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) LEFT JOIN ($query2) ON a2 = b2"
    verifyPlanAndTrait(query)
  }

  @Test
  def testRightJoinNonEqui(): Unit = {
    val query = "SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1 AND a2 > b2"
    verifyPlanAndTrait(query)
  }

  @Test
  def testRightJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN ($query2) ON a1 = b1 AND a2 > b2"
    verifyPlanAndTrait(query)
  }

  @Test
  def testRightJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN B ON a1 = b1 AND a2 > b2"
    verifyPlanAndTrait(query)
  }

  @Test
  def testRightJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) RIGHT JOIN ($query2) ON a2 = b2 AND a1 > b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testRightJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testRightJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN ($query2) ON a1 = b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testRightJoinWithRightNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN B ON a1 = b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testRightJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) RIGHT JOIN ($query2) ON a2 = b2"
    verifyPlanAndTrait(query)
  }

  @Test
  def testFullJoinNonEqui(): Unit = {
    val query = "SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1 AND a2 > b2"
    verifyPlanAndTrait(query)
  }

  @Test
  def testFullJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN ($query2) ON a1 = b1 AND a2 > b2"
    verifyPlanAndTrait(query)
  }

  @Test
  def testFullJoinWithFullNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN B ON a1 = b1 AND a2 > b2"
    verifyPlanAndTrait(query)
  }

  @Test
  def testFullJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) FULL JOIN ($query2) ON a2 = b2 AND a1 > b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testFullJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testFullJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN ($query2) ON a1 = b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testFullJoinWithFullNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN B ON a1 = b1"
    verifyPlanAndTrait(query)
  }

  @Test
  def testFullJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) FULL JOIN ($query2) ON a2 = b2"
    verifyPlanAndTrait(query)
  }
}
