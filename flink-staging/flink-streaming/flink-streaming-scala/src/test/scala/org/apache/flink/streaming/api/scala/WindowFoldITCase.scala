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

package org.apache.flink.streaming.api.scala

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.TimestampExtractor
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test
import org.junit.Assert._

import scala.collection.mutable

/**
 * Tests for Folds over windows. These also test whether OutputTypeConfigurable functions
 * work for windows, because FoldWindowFunction is OutputTypeConfigurable.
 */
class WindowFoldITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testFoldWindow(): Unit = {
    WindowFoldITCase.testResults = mutable.MutableList()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source1 = env.addSource(new SourceFunction[(String, Int)]() {
      def run(ctx: SourceFunction.SourceContext[(String, Int)]) {
        ctx.collect(("a", 0))
        ctx.collect(("a", 1))
        ctx.collect(("a", 2))
        ctx.collect(("b", 3))
        ctx.collect(("b", 4))
        ctx.collect(("b", 5))
        ctx.collect(("a", 6))
        ctx.collect(("a", 7))
        ctx.collect(("a", 8))
      }

      def cancel() {
      }
    }).assignTimestamps(new WindowFoldITCase.Tuple2TimestampExtractor)

    source1
      .keyBy(0)
      .window(TumblingTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
      .fold(("R:", 0), { (acc: (String, Int), v: (String, Int)) => (acc._1 + v._1, acc._2 + v._2) })
      .addSink(new SinkFunction[(String, Int)]() {
        def invoke(value: (String, Int)) {
        WindowFoldITCase.testResults += value.toString
        }
      })

    env.execute("Fold Window Test")

    val expectedResult = mutable.MutableList(
      "(R:aaa,3)",
      "(R:aaa,21)",
      "(R:bbb,12)")

    assertEquals(expectedResult.sorted, WindowFoldITCase.testResults.sorted)
  }

  @Test
  def testFoldAllWindow(): Unit = {
    WindowFoldITCase.testResults = mutable.MutableList()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source1 = env.addSource(new SourceFunction[(String, Int)]() {
      def run(ctx: SourceFunction.SourceContext[(String, Int)]) {
        ctx.collect(("a", 0))
        ctx.collect(("a", 1))
        ctx.collect(("a", 2))
        ctx.collect(("b", 3))
        ctx.collect(("a", 3))
        ctx.collect(("b", 4))
        ctx.collect(("a", 4))
        ctx.collect(("b", 5))
        ctx.collect(("a", 5))
      }

      def cancel() {
      }
    }).assignTimestamps(new WindowFoldITCase.Tuple2TimestampExtractor)

    source1
      .windowAll(TumblingTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
      .fold(("R:", 0), { (acc: (String, Int), v: (String, Int)) => (acc._1 + v._1, acc._2 + v._2) })
      .addSink(new SinkFunction[(String, Int)]() {
      def invoke(value: (String, Int)) {
        WindowFoldITCase.testResults += value.toString
      }
    })

    env.execute("Fold All-Window Test")

    val expectedResult = mutable.MutableList(
      "(R:aaa,3)",
      "(R:bababa,24)")

    assertEquals(expectedResult.sorted, WindowFoldITCase.testResults.sorted)
  }

}


object WindowFoldITCase {
  private var testResults: mutable.MutableList[String] = null

  private class Tuple2TimestampExtractor extends TimestampExtractor[(String, Int)] {
    def extractTimestamp(element: (String, Int), currentTimestamp: Long): Long = {
      element._2
    }

    def extractWatermark(element: (String, Int), currentTimestamp: Long): Long = {
      element._2 - 1
    }

    def getCurrentWatermark: Long = {
      Long.MinValue
    }
  }
}
