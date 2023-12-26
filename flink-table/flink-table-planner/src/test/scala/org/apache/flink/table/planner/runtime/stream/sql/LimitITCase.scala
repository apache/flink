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
import org.apache.flink.table.api.{TableException, _}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.TestTemplate
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class LimitITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @TestTemplate
  def testLimit(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.createTemporaryView("T", ds)

    val sql = "SELECT * FROM T LIMIT 4"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("book,1,12", "book,2,19", "book,4,11", "fruit,4,33")
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testOffsetAndFetch(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val ds = failingDataSource(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.createTemporaryView("T", ds)

    val sql = "SELECT * FROM T LIMIT 4 OFFSET 2"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("book,4,11", "fruit,4,33", "fruit,3,44", "fruit,5,22")
    assertThat(sink.getRetractResults.sorted).isEqualTo(expected.sorted)
  }

  /** Limit could not handle order by without fetch or limit */
  @TestTemplate
  def testWithoutFetch(): Unit = {
    val data = List(
      ("book", 1, 12),
      ("book", 2, 19),
      ("book", 4, 11),
      ("fruit", 4, 33),
      ("fruit", 3, 44),
      ("fruit", 5, 22))

    val t = env.fromCollection(data).toTable(tEnv, 'category, 'shopId, 'num)
    tEnv.createTemporaryView("T", t)

    val sql = "SELECT * FROM T OFFSET 2"

    assertThatThrownBy(() => tEnv.sqlQuery(sql).toRetractStream[Row])
      .hasMessage("FETCH is missed, which on streaming table is not supported currently.")
      .isInstanceOf[TableException]
  }
}
