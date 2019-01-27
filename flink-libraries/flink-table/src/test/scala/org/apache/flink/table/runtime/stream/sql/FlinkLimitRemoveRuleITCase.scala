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

package org.apache.flink.table.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfigOptions, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TestingUpsertTableSink
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

class FlinkLimitRemoveRuleITCase {

  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _

  @Before
  def setup(): Unit = {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    tEnv = TableEnvironment.getTableEnvironment(env)
  }

  @Test
  def testSimpleLimitRemove(): Unit = {
    val data = List(
      1,2,3,4,5,6
    )

    env.setParallelism(1)
    tEnv.registerTable("T",
      env.fromCollection(data).toTable(tEnv).as('key))

    val sql =
      """
        |select key2 from (select cast(key as int) key2 from T) t limit 0
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 1))
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED, true)
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    assertEquals(0, tableSink.getRawResults.size)
  }

  @Test
  def testLimitRemoveWithExists(): Unit = {
    val data = List(
      1,2,3,4,5,6
    )

    env.setParallelism(1)
    tEnv.registerTable("T",
      env.fromCollection(data).toTable(tEnv).as('key))

    val sql =
      """
        |select key2 from (select cast(key as int) key2 from T)
        |where exists (select key from T limit 0)
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 1))
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED, true)
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    assertEquals(0, tableSink.getRawResults.size)
  }

  @Test
  def testLimitRemoveWithSelect(): Unit = {
    val data = List(
      1,2,3,4,5,6
    )

    env.setParallelism(1)
    tEnv.registerTable("T",
      env.fromCollection(data).toTable(tEnv).as('key))

    val sql =
      """
        |select key2 from (select cast(key as int) key2 from T limit 0)
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 1))
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED, true)
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    assertEquals(0, tableSink.getRawResults.size)
  }

  @Test
  def testLimitRemoveWithOrderBy(): Unit = {
    val data = List(
      1,2,3,4,5,6
    )

    env.setParallelism(1)
    tEnv.registerTable("T",
      env.fromCollection(data).toTable(tEnv).as('key))

    val sql =
      """
        |select key2 from (select cast(key as int) key2 from T) t
        | order by key2 limit 0
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 1))
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED, true)
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    assertEquals(0, tableSink.getRawResults.size)
  }

  @Test
  def testLimitRemoveWithIn(): Unit = {
    val data = List(
      1,2,3,4,5,6
    )

    env.setParallelism(1)
    tEnv.registerTable("T",
      env.fromCollection(data).toTable(tEnv).as('key))

    val sql =
      """
        |select key2 from (select cast(key as int) key2 from T)
        |where key2 in (select key from T limit 0)
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 1))
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED, true)
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    assertEquals(0, tableSink.getRawResults.size)
  }

  @Test
  def testLimitRemoveWithJoin(): Unit = {
    val data = List(
      1,2,3,4,5,6
    )

    val data2 = List(
      1,2,3,4,5,6
    )

    env.setParallelism(1)
    tEnv.registerTable("T",
      env.fromCollection(data).toTable(tEnv).as('key))

    tEnv.registerTable("T2",
      env.fromCollection(data2).toTable(tEnv).as('key2))

    val sql =
      """
        |select key from T inner join
        |(select key2 from T2 limit 0) on true
      """.stripMargin

    val tableSink = new TestingUpsertTableSink(Array(0, 1))
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED, true)
    tEnv.sqlQuery(sql).writeToSink(tableSink)
    env.execute()

    assertEquals(0, tableSink.getRawResults.size)
  }
}
