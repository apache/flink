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

package org.apache.flink.table.runtime.batch.sql.joins

import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.junit.{Before, Test}

import scala.collection.Seq

class JoinDependentFilterPushdownITCase extends BatchTestBase with JoinITCaseBase {

  @Before
  def before(): Unit = {
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)
    registerCollection("Table5", data5, type5, "d, e, f, g, h", nullablesOfData5)
    registerCollection("SmallTable3", smallData3, type3, "a, b, c", nullablesOfSmallData3)
  }

  @Test
  def testSimple(): Unit = {
    val sql = "SELECT a, d FROM Table3, Table5 WHERE " +
        "(a = 1 and d = 2) or (a = 2 and d = 1)"
    verifyPlanAndCheckResult(
      sql,
      Seq(
        row(1, 2),
        row(1, 2),
        row(2, 1)
      ))
  }

  @Test
  def testAnd(): Unit = {
    val sql = "SELECT a, d FROM Table3, Table5 WHERE " +
        "b = e and ((a = 1 and d = 2) or (a = 2 and d = 1))"
    verifyPlanAndCheckResult(
      sql,
      Seq())
  }

  @Test
  def testNotDo(): Unit = {
    val sql = "SELECT a, d FROM SmallTable3, Table5 WHERE " +
        "b = e or ((a = 1 and d = 2) or (a = 2 and d = 1))"
    verifyPlanAndCheckResult(
      sql,
      Seq(
        row(1, 1),
        row(1, 2),
        row(1, 2),
        row(2, 1),
        row(2, 2),
        row(3, 2)
      ))
  }

  @Test
  def testThreeOr(): Unit = {
    val sql = "SELECT a, d FROM Table3, Table5 WHERE " +
        "(b = e and a = 0) or ((a = 1 and d = 2) or (a = 2 and d = 1))"
    verifyPlanAndCheckResult(
      sql,
      Seq(
        row(1, 2),
        row(1, 2),
        row(2, 1)
      ))
  }

  @Test
  def testAndOr(): Unit = {
    val sql = "SELECT a, d FROM Table3, Table5 WHERE " +
        "((a = 1 and d = 2) or (a = 2 and d = 1)) and ((a = 3 and d = 4) or (a = 4 and d = 3))"
    verifyPlanAndCheckResult(
      sql,
      Seq())
  }

  @Test
  def testMultiFields(): Unit = {
    val sql = "SELECT a, d FROM Table3, Table5 WHERE " +
        "(a = 1 and b = 1 and d = 2 and e = 2) or (a = 2 and b = 2 and d = 1 and e = 1)"
    verifyPlanAndCheckResult(
      sql,
      Seq(
        row(1, 2),
        row(2, 1)))
  }

  @Test
  def testMultiSingleSideFields(): Unit = {
    val sql = "SELECT a, d FROM Table3, Table5 WHERE " +
      "(a = 1 and b = 1 and d = 2 and e = 2) or (d = 1 and e = 1)"
    verifyPlanAndCheckResult(
      sql,
      Seq(
        row(1, 1), row(1, 2), row(10, 1), row(11, 1), row(12, 1),
        row(13, 1), row(14, 1), row(15, 1), row(16, 1), row(17, 1),
        row(18, 1), row(19, 1), row(2, 1), row(20, 1), row(21, 1),
        row(3, 1), row(4, 1), row(5, 1), row(6, 1), row(7, 1),
        row(8, 1), row(9, 1)
      ))
  }

  @Test
  def testMultiJoins(): Unit = {
    val sql = "SELECT T1.a, T2.d FROM SmallTable3 T1, " +
        "(SELECT * FROM Table3, Table5 WHERE a = d) T2 WHERE " +
        "(T1.a = 1 and T1.b = 1 and T2.a = 2 and T2.e = 2) or " +
        "(T1.a = 2 and T2.b = 2 and T2.d = 1 and T2.e = 1)"
    verifyPlanAndCheckResult(
      sql,
      Seq(row(1, 2)))
  }
}
