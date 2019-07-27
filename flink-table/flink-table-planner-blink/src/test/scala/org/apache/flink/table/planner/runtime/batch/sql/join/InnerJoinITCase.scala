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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.config.ExecutionConfigOptions._
import org.apache.flink.table.planner.runtime.batch.sql.join.JoinITCaseHelper.disableOtherJoinOpForJoin
import org.apache.flink.table.planner.runtime.batch.sql.join.JoinType.{BroadcastHashJoin, HashJoin, JoinType, NestedLoopJoin, SortMergeJoin}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.math.{BigDecimal => JBigDecimal}
import java.util

import scala.collection.Seq
import scala.util.Random

@RunWith(classOf[Parameterized])
class InnerJoinITCase(expectedJoinType: JoinType) extends BatchTestBase {

  private lazy val myUpperCaseData = Seq(
    row(1, "A"),
    row(2, "B"),
    row(3, "C"),
    row(4, "D"),
    row(5, "E"),
    row(6, "F"),
    row(null, "G"))

  private lazy val myLowerCaseData = Seq(
    row(1, "a"),
    row(2, "b"),
    row(3, "c"),
    row(4, "d"),
    row(null, "e")
  )

  private lazy val myTestData1 = Seq(
    row(1, 1),
    row(1, 2),
    row(2, 1),
    row(2, 2),
    row(3, 1),
    row(3, 2)
  )

  private lazy val myTestData2 = Seq(
    row(1, 1),
    row(1, 2),
    row(2, 1),
    row(2, 2),
    row(3, 1),
    row(3, 2)
  )

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("myUpperCaseData", myUpperCaseData, INT_STRING, "N, L", Array(true, false))
    registerCollection("myLowerCaseData", myLowerCaseData, INT_STRING, "n, l", Array(true, false))
    registerCollection("myTestData1", myTestData1, INT_INT, "a, b", Array(false, false))
    registerCollection("myTestData2", myTestData2, INT_INT, "a, b", Array(false, false))
    disableOtherJoinOpForJoin(tEnv, expectedJoinType)
  }

  @Test
  def testOneMatchPerRow(): Unit = {
    checkResult(
      "SELECT * FROM myUpperCaseData u, myLowerCaseData l WHERE u.N = l.n",
      Seq(
        row(1, "A", 1, "a"),
        row(2, "B", 2, "b"),
        row(3, "C", 3, "c"),
        row(4, "D", 4, "d")
      ))
  }

  @Test
  def testMultipleMatches(): Unit = {
    expectedJoinType match {
      case NestedLoopJoin =>
        checkResult(
          "SELECT * FROM myTestData1 A, myTestData2 B WHERE A.a = B.a and A.a = 1",
          Seq(
            row(1, 1, 1, 1),
            row(1, 1, 1, 2),
            row(1, 2, 1, 1),
            row(1, 2, 1, 2)
          ))
      case _ =>
      // A.a = B.a and A.a = 1 => A.a = 1 and B.a = 1, so after ftd and join condition simplified,
      // join condition is TRUE. Only NestedLoopJoin can handle join without any equi-condition.
    }
  }

  @Test
  def testNoMatches(): Unit = {
    checkResult(
      "SELECT * FROM myTestData1 A, myTestData2 B WHERE A.a = B.a and A.a = 1 and B.a = 2",
      Seq())
  }

  @Test
  def testDecimalAsKey(): Unit = {
    val DEC_INT = new RowTypeInfo(BigDecimalTypeInfo.of(9, 0), INT_TYPE_INFO)
    registerCollection(
      "leftTable",
      Seq(
        row(new JBigDecimal(0), 0),
        row(new JBigDecimal(1), 1),
        row(new JBigDecimal(2), 2)
      ),
      DEC_INT,
      "a, b")
    registerCollection(
      "rightTable",
      Seq(
        row(new JBigDecimal(0), 0),
        row(new JBigDecimal(1), 1)
      ),
      DEC_INT,
      "c, d")
    checkResult(
      "SELECT * FROM leftTable, rightTable WHERE a = c",
      Seq(
        row(0, 0, 0, 0),
        row(1, 1, 1, 1)
      ))
  }

  @Test
  def testBigForSpill(): Unit = {

    conf.getConfiguration.setString(TABLE_EXEC_RESOURCE_SORT_MEMORY, "1mb")
    conf.getConfiguration.setString(TABLE_EXEC_RESOURCE_HASH_JOIN_MEMORY, "2mb")
    conf.getConfiguration.setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1)

    val bigData = Random.shuffle(
      bigIntStringData.union(bigIntStringData).union(bigIntStringData).union(bigIntStringData))
    registerCollection("bigData1", bigData, INT_STRING, "a, b")
    registerCollection("bigData2", bigData, INT_STRING, "c, d")

    checkResult(
      "SELECT a, b FROM bigData1, bigData2 WHERE a = c",
      bigIntStringData.flatMap(row => Seq.fill(16)(row)))
  }

  @Test
  def testSortMergeJoinOutputOrder(): Unit = {
    if (expectedJoinType == SortMergeJoin) {
      conf.getConfiguration.setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1)
      env.getConfig.setParallelism(1)

      conf.getConfiguration.setString(TABLE_EXEC_RESOURCE_SORT_MEMORY, "1mb")

      val bigData = Random.shuffle(
        bigIntStringData.union(bigIntStringData).union(bigIntStringData).union(bigIntStringData))
      registerCollection("bigData1", bigData, INT_STRING, "a, b")
      registerCollection("bigData2", bigData, INT_STRING, "c, d")

      checkResult(
        "SELECT a, b FROM bigData1, bigData2 WHERE a = c",
        bigIntStringData.flatMap(row => Seq.fill(16)(row)),
        isSorted = true)
    }
  }
}

object InnerJoinITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): util.Collection[Array[_]] = {
    util.Arrays.asList(
      Array(BroadcastHashJoin), Array(HashJoin), Array(SortMergeJoin), Array(NestedLoopJoin))
  }
}

