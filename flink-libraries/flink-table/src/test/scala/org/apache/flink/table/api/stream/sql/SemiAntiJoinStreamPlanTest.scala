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

import org.junit.Test

class SemiAntiJoinStreamPlanTest extends StreamPlanTestBase {

  @Test
  def testSemiJoin(): Unit = {
    val query = "SELECT * FROM A WHERE a1 in (SELECT b1 from B)"
    verifyPlanAndTrait(query)
  }

  @Test
  def testSemiJoinNonEqui(): Unit = {
    val query = "SELECT * FROM A WHERE a1 in (SELECT b1 from B WHERE a2 > b2)"
    verifyPlanAndTrait(query)
  }

  @Test
  def testSemiJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT * FROM ($query1) WHERE a1 in (SELECT b1 from ($query2) WHERE a2 > b2)"
    verifyPlanAndTrait(query)
  }

  @Test
  def testSemiJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT * FROM ($query1) WHERE a1 in (SELECT b1 from B WHERE a2 > b2)"
    verifyPlanAndTrait(query)
  }

  @Test
  def testSemiJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT * FROM ($query1) WHERE a2 in (SELECT b2 from ($query2) WHERE a1 > b1)"
    verifyPlanAndTrait(query)
  }

  @Test
  def testAntiJoin(): Unit = {
    val query = "SELECT * FROM A WHERE NOT EXISTS (SELECT b1 from B WHERE a1 = b1)"
    verifyPlanAndTrait(query)
  }

  @Test
  def testAntiJoinNonEqui(): Unit = {
    val query = "SELECT * FROM A WHERE NOT EXISTS (SELECT b1 from B WHERE a1 = b1 AND a2 > b2)"
    verifyPlanAndTrait(query)
  }

  @Test
  def testAntiJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT * FROM ($query1) WHERE NOT EXISTS (SELECT b1 from ($query2) WHERE a1 = " +
        s"b1 AND a2 > b2)"
    verifyPlanAndTrait(query)
  }

  @Test
  def testAntiJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT * FROM ($query1) WHERE NOT EXISTS (SELECT b1 from B WHERE a1 = b1 AND a2" +
        s" > b2)"
    verifyPlanAndTrait(query)
  }

  @Test
  def testAntiJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT * FROM ($query1) WHERE NOT EXISTS (SELECT b2 from ($query2) WHERE a2 = " +
        s"b2 AND a1 > b1)"
    verifyPlanAndTrait(query)
  }
}
