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

package org.apache.flink.table.planner.runtime.utils

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, ImplicitExpressionConversions}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.api.{EnvironmentSettings, Table}

import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row

import org.junit.rules.{ExpectedException, TemporaryFolder}
import org.junit.{After, Before, Rule}

class StreamingTestBase extends AbstractTestBase {

  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _
  val _tempFolder = new TemporaryFolder
  var enableObjectReuse = true
  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  @Before
  def before(): Unit = {
    this.env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    if (enableObjectReuse) {
      this.env.getConfig.enableObjectReuse()
    }
    val setting = EnvironmentSettings.newInstance().inStreamingMode().build()
    this.tEnv = StreamTableEnvironment.create(env, setting)
  }

  @After
  def after(): Unit = {
    StreamTestSink.clear()
    TestValuesTableFactory.clearAllData()
  }

  /**
   * Creates a new Row and assigns the given values to the Row's fields.
   * We use [[rowOf()]] here to avoid conflicts with [[ImplicitExpressionConversions.row]].
   */
  protected def rowOf(args: Any*): Row = {
    val row = new Row(args.length)
    0 until args.length foreach {
      i => row.setField(i, args(i))
    }
    row
  }

  def execInsertSqlAndWaitResult(insert: String): JobExecutionResult = {
    TableEnvUtil.execInsertSqlAndWaitResult(tEnv, insert)
  }

  def execInsertTableAndWaitResult(table: Table, targetPath: String): JobExecutionResult = {
    TableEnvUtil.execInsertTableAndWaitResult(table, targetPath)
  }
}
