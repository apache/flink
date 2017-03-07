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

import java.math.BigDecimal

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.TableProgramsClusterTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.apache.flink.table.api.ValidationException
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class DataSetWindowAggregateITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
    extends TableProgramsClusterTestBase(mode, configMode) {

  val data = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"))

  @Test(expected = classOf[UnsupportedOperationException])
  def testAllEventTimeTumblingWindowOverCount(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = env
      .fromCollection(data)
      .toTable(tEnv, 'long, 'int, 'double, 'float, 'bigdec, 'string)

    // Count tumbling non-grouping window on event-time are currently not supported
    table
      .window(Tumble over 2.rows on 'long as 'w)
      .groupBy('w)
      .select('int.count)
      .toDataSet[Row]
  }

  @Test
  def testEventTimeTumblingGroupWindowOverCount(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = env
      .fromCollection(data)
      .toTable(tEnv, 'long, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.sum, 'int.count, 'int.max, 'int.min, 'int.avg,
              'double.sum, 'double.count, 'double.max, 'double.min, 'double.avg,
              'float.sum, 'float.count, 'float.max, 'float.min, 'float.avg,
              'bigdec.sum, 'bigdec.count, 'bigdec.max, 'bigdec.min, 'bigdec.avg)

    val expected = "Hello,7,2,5,2,3,7.0,2,5.0,2.0,3.5,7.0,2,5.0,2.0,3.5,7,2,5,2,3.5\n" +
      "Hello world,7,2,4,3,3,7.0,2,4.0,3.0,3.5,7.0,2,4.0,3.0,3.5,7,2,4,3,3.5\n"
    val results = windowedTable.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testEventTimeTumblingGroupWindowOverTime(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = env
      .fromCollection(data)
      .toTable(tEnv, 'long, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w, 'string)
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

    val table = env
      .fromCollection(data)
      .toTable(tEnv, 'long, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w)
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

    val table = env
      .fromCollection(data)
      .toTable(tEnv, 'long, 'int, 'double, 'float, 'bigdec, 'string)
    val windowedTable = table
      .window(Session withGap 7.milli on 'long as 'w)
      .groupBy('string, 'w)
      .select('string, 'string.count, 'w.start, 'w.end)

    val results = windowedTable.toDataSet[Row].collect()

    val expected = "Hallo,1,1970-01-01 00:00:00.002,1970-01-01 00:00:00.009\n" +
      "Hello world,1,1970-01-01 00:00:00.008,1970-01-01 00:00:00.015\n" +
      "Hello world,1,1970-01-01 00:00:00.016,1970-01-01 00:00:00.023\n" +
      "Hello,3,1970-01-01 00:00:00.003,1970-01-01 00:00:00.014\n" +
      "Hi,1,1970-01-01 00:00:00.001,1970-01-01 00:00:00.008"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAllEventTimeSessionGroupWindow(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    val table = env
      .fromCollection(data)
      .toTable(tEnv, 'long, 'int, 'double, 'float, 'bigdec, 'string)

    val results =table
      .window(Session withGap 2.milli on 'long as 'w)
      .groupBy('w)
      .select('string.count, 'w.start, 'w.end).toDataSet[Row].collect()

    val expected = "4,1970-01-01 00:00:00.001,1970-01-01 00:00:00.006\n" +
      "2,1970-01-01 00:00:00.007,1970-01-01 00:00:00.01\n" +
      "1,1970-01-01 00:00:00.016,1970-01-01 00:00:00.018"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[ValidationException])
  def testMultiGroupWindow(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = env
      .fromCollection(data)
      .toTable(tEnv, 'long, 'int, 'double, 'float, 'bigdec, 'string)
    table
      .window(Tumble over 5.milli on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count)
      .window( Slide over 5.milli every 1.milli on 'int as 'w2)
      .groupBy('w2)
      .select('string)
      .toDataSet[Row]
  }

}
