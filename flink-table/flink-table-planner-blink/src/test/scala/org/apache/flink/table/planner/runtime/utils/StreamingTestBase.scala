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

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableException}
import org.apache.flink.table.planner.operations.PlannerQueryOperation
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner
import org.apache.flink.table.planner.utils.TableTestUtil
import org.apache.flink.test.util.AbstractTestBase

import org.junit.rules.{ExpectedException, TemporaryFolder}
import org.junit.{Before, Rule}

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
    StreamTestSink.clear()
    this.env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    if (enableObjectReuse) {
      this.env.getConfig.enableObjectReuse()
    }
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    this.tEnv = StreamTableEnvironment.create(env, setting)
  }

  def addTableWithWatermark(
      tableName: String,
      sourceTable: Table,
      rowtimeField: String,
      offset: Long): Unit = {
    val sourceRel = TableTestUtil.toRelNode(sourceTable)
    val rowtimeFieldIdx = sourceRel.getRowType.getFieldNames.indexOf(rowtimeField)
    if (rowtimeFieldIdx < 0) {
      throw new TableException(s"$rowtimeField does not exist, please check it")
    }
    val watermarkAssigner = new LogicalWatermarkAssigner(
      sourceRel.getCluster,
      sourceRel.getTraitSet,
      sourceRel,
      Some(rowtimeFieldIdx),
      Option(offset)
    )
    val queryOperation = new PlannerQueryOperation(watermarkAssigner)
    tEnv.registerTable(tableName, TableTestUtil.createTable(tEnv, queryOperation))
  }
}
