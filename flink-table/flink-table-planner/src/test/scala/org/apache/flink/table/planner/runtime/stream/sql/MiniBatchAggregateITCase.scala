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
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingRetractSink}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.types.{Row, RowKind}

import org.junit._
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.time.Duration

@RunWith(classOf[Parameterized])
class MiniBatchAggregateITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Before
  def setupEnv(): Unit = {
    tEnv.getConfig.getConfiguration
      .setBoolean(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    tEnv.getConfig.getConfiguration
      .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofMillis(1))
    tEnv.getConfig.getConfiguration.setLong(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 10L)
  }

  def row(kind: RowKind, args: Any*): Row = {
    val values = args.toArray
    val row = new Row(values.length)
    row.setKind(kind)
    var i = 0
    while (i < values.length) {
      row.setField(i, values(i))
      i += 1
    }
    row
  }

  @Test
  def testLagFunction(): Unit = {
    val data: Seq[Row] = Seq(
      row(RowKind.UPDATE_BEFORE, 1, 1L, "HI"),
      row(RowKind.UPDATE_AFTER, 1, 2L, "Hello world")
    )
    val dataId1: String = TestValuesTableFactory.registerData(data)
    tEnv.executeSql(
      s"""
         |CREATE TABLE Table1(
         |`a` INT,
         |`b` BIGINT,
         |`c` STRING
         |) WITH(
         |'connector' = 'values',
         |'data-id' = '$dataId1',
         |'changelog-mode'='I,UA,UB,D',
         |'bounded' = 'false'
         |)
         |""".stripMargin
    )
    val result = tEnv
      .sqlQuery(
        s"""
           |SELECT `a`, max(`b`), max(`c`) FROM Table1 GROUP BY `a`
           |""".stripMargin
      )
      .toRetractStream[Row]
    val sink = new TestingRetractSink
    result.addSink(sink)
    env.execute()
    val expected = Seq("1,2,Hello world")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

}
