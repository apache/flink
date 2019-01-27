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

package org.apache.flink.table.runtime.batch.sql

import java.util

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.joins.JoinType.{BroadcastHashJoin, HashJoin, JoinType, NestedLoopJoin, SortMergeJoin}
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.apache.flink.table.runtime.batch.sql.joins.JoinITCaseBase
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import scala.util.Random

@RunWith(classOf[Parameterized])
class SetOperatorsITCase(joinType: JoinType) extends BatchTestBase with JoinITCaseBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection("AllNullTable3", allNullData3, type3, allNullablesOfNullData3, 'a, 'b, 'c)
    registerCollection("SmallTable3", smallData3, type3, nullablesOfSmallData3, 'a, 'b, 'c)
    registerCollection("Table3", data3, type3, nullablesOfData3, 'a, 'b, 'c)
    registerCollection("Table5", data5, type5, nullablesOfData5, 'a, 'b, 'c, 'd, 'e)
    disableOtherJoinOpForJoin(tEnv, joinType)
  }

  @Test
  def testIntersect(): Unit = {
    val data = List(
      row(1, 1L, "Hi"),
      row(2, 2L, "Hello"),
      row(2, 2L, "Hello"),
      row(3, 2L, "Hello world!")
    )
    val shuffleData = Random.shuffle(data)
    tEnv.registerCollection(
      "T",
      shuffleData,
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO),
      Seq(false, false, false),
      'a, 'b, 'c)

    checkResult(
      "SELECT c FROM SmallTable3 INTERSECT SELECT c FROM T",
      Seq(row("Hi"), row("Hello")))
  }

  @Test
  def testIntersectWithFilter(): Unit = {
    checkResult(
      "SELECT c FROM ((SELECT * FROM SmallTable3) INTERSECT (SELECT * FROM Table3)) WHERE a > 1",
      Seq(row("Hello"), row("Hello world")))
  }

  @Test
  def testExcept(): Unit = {
    val data = List(row(1, 1L, "Hi"))
    tEnv.registerCollection("T", data,
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO),
      Seq(false, false, false),
      'a, 'b, 'c)

    checkResult(
      "SELECT c FROM SmallTable3 EXCEPT (SELECT c FROM T)",
      Seq(row("Hello"), row("Hello world")))
  }

  @Test
  def testExceptWithFilter(): Unit = {
    checkResult(
      "SELECT c FROM (" +
          "SELECT * FROM SmallTable3 EXCEPT (SELECT a, b, d FROM Table5))" +
          "WHERE b < 2",
      Seq(row("Hi")))
  }

  @Test
  def testIntersectWithNulls(): Unit = {
    checkResult(
      "SELECT c FROM AllNullTable3 INTERSECT SELECT c FROM AllNullTable3",
      Seq(row(null)))
  }

  @Test
  def testExceptWithNulls(): Unit = {
    checkResult(
      "SELECT c FROM AllNullTable3 EXCEPT SELECT c FROM AllNullTable3",
      Seq())
  }
}

object SetOperatorsITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): util.Collection[Array[_]] = {
    util.Arrays.asList(
      Array(BroadcastHashJoin), Array(HashJoin), Array(SortMergeJoin), Array(NestedLoopJoin))
  }
}
