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

import java.math.{BigDecimal => JBigDecimal}
import java.util
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import JoinType.{BroadcastHashJoin, HashJoin, JoinType, NestedLoopJoin, SortMergeJoin}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO
import org.apache.flink.api.common.typeinfo.BigDecimalTypeInfo
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.InsertRuntimeFilterRule
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.util.NodeResourceUtil
import org.apache.flink.table.util.NodeResourceUtil.InferMode

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import scala.collection.Seq
import scala.util.Random
import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class InnerJoinITCase(expectedJoinType: JoinType) extends BatchTestBase with JoinITCaseBase {

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

  override def getConfiguration: Configuration = {
    val conf = new Configuration()
    conf.setString("akka.ask.timeout", "5 min")
    conf
  }

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection("myUpperCaseData", myUpperCaseData, INT_STRING, "N, L", Seq(true, false))
    registerCollection("myLowerCaseData", myLowerCaseData, INT_STRING, "n, l", Seq(true, false))
    registerCollection("myTestData1", myTestData1, INT_INT, "a, b", Seq(false, false))
    registerCollection("myTestData2", myTestData2, INT_INT, "a, b", Seq(false, false))
    disableOtherJoinOpForJoin(tEnv, expectedJoinType)
  }

  @Test
  def testOneMatchPerRow(): Unit = {
    checkResult(
      "SELECT * FROM myUpperCaseData, myLowerCaseData WHERE N = n",
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

    conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MEM, 1)
    conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_HASH_JOIN_TABLE_MEM, 2)
    tEnv.getConfig.getConf.setInteger(NodeResourceUtil.SQL_RESOURCE_PER_REQUEST_MEM, 2)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1)

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
      tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1)

      conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MEM, 1)

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

  @Test
  def testSpillForRuntimeFilter(): Unit = {
    if (expectedJoinType == HashJoin) {
      testSpillForRuntimeFilterInner()
    }
  }

  @Test
  def testSpillForRuntimeFilterWaitRf(): Unit = {
    if (expectedJoinType == HashJoin) {
      testSpillForRuntimeFilterInner(true)
    }
  }

  private def testSpillForRuntimeFilterInner(waitRf: Boolean = false): Unit = {

    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_ENABLED, true)
    if (waitRf) {
      tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_WAIT, true)
    }

    conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MEM, 1)
    conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_HASH_JOIN_TABLE_MEM, 2)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1)

    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 10)
    tEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_RESOURCE_INFER_MODE, InferMode.NONE.toString)

    val bigData = Random.shuffle(
      bigIntStringData.union(bigIntStringData).union(bigIntStringData).union(bigIntStringData))
    registerCollection("bigData1", bigData, INT_STRING, "a, b")
    registerCollection("bigData2", bigData, INT_STRING, "c, d")

    tEnv.alterTableStats("bigData1", Some(TableStats(1000L,
      Map[String, ColumnStats]("a" -> ColumnStats(50L, 0L, 4D, 4, 10000, 1)).asJava)))
    tEnv.alterTableStats("bigData2", Some(TableStats(10000L,
      Map[String, ColumnStats]("c" -> ColumnStats(200L, 0L, 4D, 4, 10000, 1)).asJava)))

    checkResult(
      "SELECT a, b FROM bigData1, bigData2 WHERE a = c",
      bigIntStringData.flatMap(row => Seq.fill(16)(row)))
  }

  @Test
  def testRuntimeFilterPushDownToAgg(): Unit = {
    if (expectedJoinType == HashJoin) {

      tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_ENABLED, true)
      tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_WAIT, true)
      InsertRuntimeFilterRule.resetBroadcastIdCounter()
      tEnv.getConfig.getConf.setLong(
        TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_PROBE_ROW_COUNT_MIN, 5)
      tEnv.getConfig.getConf.setInteger(
        TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_ROW_COUNT_NUM_BITS_RATIO, 1)

      conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MEM, 1)
      conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_HASH_JOIN_TABLE_MEM, 2)

      tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 10)
      tEnv.getConfig.getConf.setString(
        TableConfigOptions.SQL_RESOURCE_INFER_MODE, InferMode.NONE.toString)

      val bigData = Random.shuffle(
        bigIntStringData.union(bigIntStringData).union(bigIntStringData).union(bigIntStringData))
      registerCollection("bigData1", bigData, INT_STRING, "a, b")
      registerCollection("bigData2", bigData, INT_STRING, "c, d")

      tEnv.alterTableStats("bigData1", Some(TableStats(1000L,
        Map[String, ColumnStats]("a" -> ColumnStats(1000L, 0L, 4D, 4, 10000, 1)).asJava)))
      tEnv.alterTableStats("bigData2", Some(TableStats(10000L,
        Map[String, ColumnStats]("c" -> ColumnStats(10000L, 0L, 4D, 4, 10000, 1)).asJava)))

      checkResult(
        "SELECT count(*) FROM (SELECT a, b, cnt FROM bigData1, " +
            "(select c, count(*) as cnt from bigData2 group by c) as T2 WHERE a = c)",
        Seq(row(40000)))
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
