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
package org.apache.flink.table.planner.runtime.batch.sql.join

import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._

import org.junit.jupiter.api.{BeforeEach, Test}

class JoinConditionTypeCoerceITCase extends BatchTestBase {

  @BeforeEach
  override def before(): Unit = {
    super.before()
    registerCollection("t1", numericData, numericType, "a, b, c, d, e", nullablesOfNumericData)
    registerCollection("t2", numericData, numericType, "a, b, c, d, e", nullablesOfNumericData)
    // Disable NestedLoopJoin.
    JoinITCaseHelper.disableOtherJoinOpForJoin(tEnv, JoinType.SortMergeJoin)
  }

  @Test
  def testInnerJoinIntEqualsLong(): Unit = {
    val sqlQuery = "select t1.* from t1, t2 where t1.a = t2.b"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInnerJoinIntEqualsFloat(): Unit = {
    val sqlQuery = "select t1.* from t1, t2 where t1.a = t2.c"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInnerJoinIntEqualsDouble(): Unit = {
    val sqlQuery = "select t1.* from t1, t2 where t1.a = t2.d"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInnerJoinIntEqualsDecimal(): Unit = {
    val sqlQuery = "select t1.* from t1, t2 where t1.a = t2.e"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInnerJoinFloatEqualsDouble(): Unit = {
    val sqlQuery = "select t1.* from t1, t2 where t1.c = t2.d"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInnerJoinFloatEqualsDecimal(): Unit = {
    val sqlQuery = "select t1.* from t1, t2 where t1.c = t2.e"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInnerJoinDoubleEqualsDecimal(): Unit = {
    val sqlQuery = "select t1.* from t1, t2 where t1.d = t2.e"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInToSemiJoinIntEqualsLong(): Unit = {
    val sqlQuery = "select * from t1 where t1.a in (select b from t2)"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInToSemiJoinIntEqualsFloat(): Unit = {
    val sqlQuery = "select * from t1 where t1.a in (select c from t2)"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInToSemiJoinIntEqualsDouble(): Unit = {
    val sqlQuery = "select * from t1 where t1.a in (select d from t2)"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInToSemiJoinIntEqualsDecimal(): Unit = {
    val sqlQuery = "select * from t1 where t1.a in (select e from t2)"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInToSemiJoinFloatEqualsDouble(): Unit = {
    val sqlQuery = "select * from t1 where t1.c in (select d from t2)"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInToSemiJoinFloatEqualsDecimal(): Unit = {
    val sqlQuery = "select * from t1 where t1.c in (select e from t2)"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }

  @Test
  def testInToSemiJoinDoubleEqualsDecimal(): Unit = {
    val sqlQuery = "select * from t1 where t1.d in (select e from t2)"
    checkResult(
      sqlQuery,
      Seq(
        row(1, 1, 1.0, 1.0, "1.000000000000000000"),
        row(2, 2, 2.0, 2.0, "2.000000000000000000"),
        row(3, 3, 3.0, 3.0, "3.000000000000000000")))
  }
}
