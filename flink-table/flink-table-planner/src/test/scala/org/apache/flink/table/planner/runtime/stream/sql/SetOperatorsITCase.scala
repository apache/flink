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
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestData, TestingRetractSink}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.TestTemplate
import org.junit.jupiter.api.extension.ExtendWith

import scala.collection.mutable

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class SetOperatorsITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @TestTemplate
  def testIntersect(): Unit = {
    val tableA = failingDataSource(TestData.smallTupleData3)
      .toTable(tEnv, 'a1, 'a2, 'a3)
    val tableB = failingDataSource(TestData.tupleData3)
      .toTable(tEnv, 'b1, 'b2, 'b3)
    tEnv.createTemporaryView("A", tableA)
    tEnv.createTemporaryView("B", tableB)

    val sqlQuery = "SELECT a1, a2, a3 from A INTERSECT SELECT b1, b2, b3 from B"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testExcept(): Unit = {
    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))
    data1.+=((3, 8L, "Hi9"))

    val data2 = new mutable.MutableList[(Int, Long, String)]
    data2.+=((1, 1L, "Hi1"))
    data2.+=((2, 2L, "Hi2"))
    data2.+=((3, 2L, "Hi3"))

    val t1 = failingDataSource(data1).toTable(tEnv, 'a1, 'a2, 'a3)
    val t2 = failingDataSource(data2).toTable(tEnv, 'b1, 'b2, 'b3)
    tEnv.createTemporaryView("T1", t1)
    tEnv.createTemporaryView("T2", t2)

    val sqlQuery = "SELECT a3 from T1 EXCEPT SELECT b3 from T2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList(
      "Hi5",
      "Hi6",
      "Hi8",
      "Hi9"
    )
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testIntersectAll(): Unit = {
    val t1 = failingDataSource(Seq(1, 1, 1, 2, 2)).toTable(tEnv, 'c)
    val t2 = failingDataSource(Seq(1, 2, 2, 2, 3)).toTable(tEnv, 'c)
    tEnv.createTemporaryView("T1", t1)
    tEnv.createTemporaryView("T2", t2)

    val sqlQuery = "SELECT c FROM T1 INTERSECT ALL SELECT c FROM T2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList("1", "2", "2")
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testMinusAll(): Unit = {
    val tableA = failingDataSource(TestData.smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("tableA", tableA)
    val tableB = failingDataSource(Seq((1, 1L, "Hi"), (1, 1L, "Hi"))).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("tableB", tableB)

    val t1 = "SELECT * FROM tableA"
    val t2 = "SELECT * FROM tableB"
    val sqlQuery =
      s"SELECT c FROM (($t1 UNION ALL $t1 UNION ALL $t1) EXCEPT ALL $t2)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = mutable.MutableList(
      "Hi",
      "Hello",
      "Hello",
      "Hello",
      "Hello world",
      "Hello world",
      "Hello world"
    )
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

}
