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
package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.planner.runtime.batch.sql.join.JoinITCaseHelper
import org.apache.flink.table.planner.runtime.batch.sql.join.JoinType._
import org.apache.flink.table.planner.runtime.utils.{BatchTableEnvUtil, BatchTestBase}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.testutils.junit.extensions.parameterized.{Parameter, ParameterizedTestExtension, Parameters}

import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.util

import scala.util.Random

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class SetOperatorsITCase extends BatchTestBase {

  @Parameter var joinType: JoinType = _

  @BeforeEach
  override def before(): Unit = {
    super.before()
    registerCollection("AllNullTable3", allNullData3, type3, "a, b, c")
    registerCollection("SmallTable3", smallData3, type3, "a, b, c")
    registerCollection("Table3", data3, type3, "a, b, c")
    registerCollection("Table5", data5, type5, "a, b, c, d, e")
    JoinITCaseHelper.disableOtherJoinOpForJoin(tEnv, joinType)
  }

  @TestTemplate
  def testIntersect(): Unit = {
    val data = List(
      row(1, 1L, "Hi"),
      row(2, 2L, "Hello"),
      row(2, 2L, "Hello"),
      row(3, 2L, "Hello world!")
    )
    val shuffleData = Random.shuffle(data)
    BatchTableEnvUtil.registerCollection(
      tEnv,
      "T",
      shuffleData,
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO),
      "a, b, c")

    checkResult("SELECT c FROM SmallTable3 INTERSECT SELECT c FROM T", Seq(row("Hi"), row("Hello")))
  }

  @TestTemplate
  def testIntersectWithFilter(): Unit = {
    checkResult(
      "SELECT c FROM ((SELECT * FROM SmallTable3) INTERSECT (SELECT * FROM Table3)) WHERE a > 1",
      Seq(row("Hello"), row("Hello world")))
  }

  @TestTemplate
  def testExcept(): Unit = {
    val data = List(row(1, 1L, "Hi"))
    BatchTableEnvUtil.registerCollection(
      tEnv,
      "T",
      data,
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO),
      "a, b, c")

    checkResult(
      "SELECT c FROM SmallTable3 EXCEPT (SELECT c FROM T)",
      Seq(row("Hello"), row("Hello world")))
  }

  @TestTemplate
  def testExceptWithFilter(): Unit = {
    checkResult(
      "SELECT c FROM (" +
        "SELECT * FROM SmallTable3 EXCEPT (SELECT a, b, d FROM Table5))" +
        "WHERE b < 2",
      Seq(row("Hi")))
  }

  @TestTemplate
  def testIntersectWithNulls(): Unit = {
    checkResult("SELECT c FROM AllNullTable3 INTERSECT SELECT c FROM AllNullTable3", Seq(row(null)))
  }

  @TestTemplate
  def testExceptWithNulls(): Unit = {
    checkResult("SELECT c FROM AllNullTable3 EXCEPT SELECT c FROM AllNullTable3", Seq())
  }

  @TestTemplate
  def testIntersectAll(): Unit = {
    BatchTableEnvUtil.registerCollection(tEnv, "T1", Seq(1, 1, 1, 2, 2), "c")
    BatchTableEnvUtil.registerCollection(tEnv, "T2", Seq(1, 2, 2, 2, 3), "c")
    checkResult("SELECT c FROM T1 INTERSECT ALL SELECT c FROM T2", Seq(row(1), row(2), row(2)))
  }

  @TestTemplate
  def testMinusAll(): Unit = {
    BatchTableEnvUtil.registerCollection(tEnv, "T2", Seq((1, 1L, "Hi")), "a, b, c")
    val t1 = "SELECT * FROM SmallTable3"
    val t2 = "SELECT * FROM T2"
    checkResult(
      s"SELECT c FROM (($t1 UNION ALL $t1 UNION ALL $t1) EXCEPT ALL ($t2 UNION ALL $t2))",
      Seq(
        row("Hi"),
        row("Hello"),
        row("Hello"),
        row("Hello"),
        row("Hello world"),
        row("Hello world"),
        row("Hello world"))
    )
  }
}

object SetOperatorsITCase {
  @Parameters(name = "{0}")
  def parameters(): util.Collection[Array[_]] = {
    util.Arrays.asList(
      // TODO
      //      Array(BroadcastHashJoin),
      //      Array(HashJoin),
      //      Array(NestedLoopJoin),
      Array(SortMergeJoin))
  }
}
