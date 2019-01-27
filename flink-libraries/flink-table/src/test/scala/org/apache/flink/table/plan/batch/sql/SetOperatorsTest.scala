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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableConfigOptions, TableException}
import org.apache.flink.table.util.TableTestBase
import org.junit.{Before, Test}

class SetOperatorsTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "SortAgg")
    util.addTable[(Int, Long, String)]("T1", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("T2", 'd, 'e, 'f)
  }

  @Test(expected = classOf[TableException])
  def testIntersectAll(): Unit = {
    util.printSql("SELECT c FROM T1 INTERSECT ALL SELECT f FROM T2")
  }

  @Test(expected = classOf[TableException])
  def testExceptAll(): Unit = {
    util.printSql("SELECT c FROM T1 EXCEPT ALL SELECT f FROM T2")
  }

  @Test
  def testIntersect(): Unit = {
    util.verifyPlan("SELECT c FROM T1 INTERSECT SELECT f FROM T2")
  }

  @Test
  def testIntersectLeftIsEmpty(): Unit = {
    util.verifyPlan("SELECT c FROM T1 WHERE 1=0 INTERSECT SELECT f FROM T2")
  }

  @Test
  def testIntersectRightIsEmpty(): Unit = {
    util.verifyPlan("SELECT c FROM T1 INTERSECT SELECT f FROM T2 WHERE 1=0")
  }

  @Test
  def testExcept(): Unit = {
    util.verifyPlan("SELECT c FROM T1 EXCEPT SELECT f FROM T2")
  }

  @Test
  def testExceptLeftIsEmpty(): Unit = {
    util.verifyPlan("SELECT c FROM T1 WHERE 1=0 EXCEPT SELECT f FROM T2")
  }

  @Test
  def testExceptRightIsEmpty(): Unit = {
    util.verifyPlan("SELECT c FROM T1 EXCEPT SELECT f FROM T2 WHERE 1=0")
  }
}
