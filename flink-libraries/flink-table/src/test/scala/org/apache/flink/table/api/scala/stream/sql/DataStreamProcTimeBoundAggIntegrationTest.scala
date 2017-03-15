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

package org.apache.flink.table.runtime.datastream

import java.math.BigDecimal

import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.runtime.datastream.DataStreamAggregateITCase.TimestampWithEqualWatermark
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable
import test.scala.org.apache.flink.table.api.scala.stream.utils.StreamITCase
import test.scala.org.apache.flink.table.api.scala.stream.utils.StreamTestData
import test.scala.org.apache.flink.table.api.scala.stream.utils.StreamITCase
import test.scala.org.apache.flink.table.api.scala.stream.utils.StreamTestData
import test.scala.org.apache.flink.table.api.scala.stream.utils.StreamITCase
import test.scala.org.apache.flink.table.api.scala.stream.utils.StreamTestData
import test.scala.org.apache.flink.table.api.scala.stream.utils.StreamITCase
import test.scala.org.apache.flink.table.api.scala.stream.utils.StreamTestData

class DataStreamAggregateITCase extends StreamingMultipleProgramsTestBase {

  val data = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"))

  // ----------------------------------------------------------------------------------------------
  // Sliding windows
  // ----------------------------------------------------------------------------------------------

  @Test
  def testSelectExpressionFromTable(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, MAX(c) OVER (PARTITION BY a ORDER BY procTime() "
    +"RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS maxC FROM MyTable"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("2,0", "4,1", "6,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

}

object DataStreamAggregateITCase {
  class TimestampWithEqualWatermark
      extends AssignerWithPunctuatedWatermarks[(Long, Int, Double, Float, BigDecimal, String)] {

    override def checkAndGetNextWatermark(
      lastElement: (Long, Int, Double, Float, BigDecimal, String),
      extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp)
    }

    override def extractTimestamp(
      element: (Long, Int, Double, Float, BigDecimal, String),
      previousElementTimestamp: Long): Long = {
      element._1
    }
  }
}
