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

package org.apache.flink.table.runtime.dataset

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class DataSetWindowAggregateITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  val data = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hallo"),
    (3L, 2, "Hello"),
    (6L, 3, "Hello"),
    (4L, 5, "Hello"),
    (16L, 4, "Hello world"),
    (8L, 3, "Hello world"))

  @Test(expected = classOf[UnsupportedOperationException])
  def testAllEventTimeTumblingWindowOverCount(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = env.fromCollection(data).toTable(tEnv, 'long, 'int, 'string)

    // Count tumbling non-grouping window on event-time are currently not supported
    table
      .window(Tumble over 2.rows on 'long)
      .select('int.count)
      .toDataSet[Row]
  }

  @Test
  def testEventTimeTumblingGroupWindowOverCount(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = env.fromCollection(data).toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Tumble over 2.rows on 'long)
      .select('string, 'int.sum)

    val expected = "Hello,7\n" + "Hello world,7\n"
    val results = windowedTable.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = env.fromCollection(data).toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .groupBy('string)
      .window(Tumble over 5.milli on 'long as 'w)
      .select('string, 'int.sum, 'w.start, 'w.end)

    val expected = "Hello world,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01\n" +
      "Hello world,4,1970-01-01 00:00:00.015,1970-01-01 00:00:00.02\n" +
      "Hello,7,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005\n" +
      "Hello,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01\n" +
      "Hallo,2,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005\n" +
      "Hi,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005\n"

    val results = windowedTable.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAllEventTimeTumblingWindowOverTime(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = env.fromCollection(data).toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .select('int.sum, 'w.start, 'w.end)

    val expected = "10,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005\n" +
      "6,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01\n" +
      "4,1970-01-01 00:00:00.015,1970-01-01 00:00:00.02\n"

    val results = windowedTable.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testEventTimeSessionGroupWindow(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = env.fromCollection(data).toTable(tEnv, 'long, 'int, 'string)
    val windowedTable = table
      .groupBy('string)
      .window(Session withGap 7.milli on 'long as 'w)
      .select('string, 'string.count, 'w.start, 'w.end)

    val results = windowedTable.toDataSet[Row].collect()

    val expected = "Hallo,1,1970-01-01 00:00:00.002,1970-01-01 00:00:00.009\n" +
      "Hello world,1,1970-01-01 00:00:00.008,1970-01-01 00:00:00.015\n" +
      "Hello world,1,1970-01-01 00:00:00.016,1970-01-01 00:00:00.023\n" +
      "Hello,3,1970-01-01 00:00:00.003,1970-01-01 00:00:00.013\n" +
      "Hi,1,1970-01-01 00:00:00.001,1970-01-01 00:00:00.008"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[UnsupportedOperationException])
  def testAlldEventTimeSessionGroupWindow(): Unit = {
    // Non-grouping Session window on event-time are currently not supported
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    val table = env.fromCollection(data).toTable(tEnv, 'long, 'int, 'string)
    val windowedTable =table
      .window(Session withGap 7.milli on 'long as 'w)
      .select('string.count).toDataSet[Row].collect()
  }
}
