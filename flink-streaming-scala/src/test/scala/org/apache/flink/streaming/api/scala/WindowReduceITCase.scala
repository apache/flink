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

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.testutils.{CheckingIdentityRichAllWindowFunction, CheckingIdentityRichProcessAllWindowFunction, CheckingIdentityRichProcessWindowFunction, CheckingIdentityRichWindowFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

/**
 * Tests for Folds over windows. These also test whether OutputTypeConfigurable functions
 * work for windows, because FoldWindowFunction is OutputTypeConfigurable.
 */
class WindowReduceITCase extends AbstractTestBase {

  @Test
  def testReduceWindow(): Unit = {
    WindowReduceITCase.testResults = mutable.MutableList()

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

        // source is finite, so it will have an implicit MAX watermark when it finishes
      }

      def cancel() {
      }
    }).assignTimestampsAndWatermarks(new WindowReduceITCase.Tuple2TimestampExtractor)

    source1
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
      .reduce( (a, b) => (a._1 + b._1, a._2 + b._2) )
      .addSink(new SinkFunction[(String, Int)]() {
        def invoke(value: (String, Int)) {
          WindowReduceITCase.testResults += value.toString
        }
      })

    env.execute("Reduce Window Test")

    val expectedResult = mutable.MutableList(
      "(aaa,3)",
      "(aaa,21)",
      "(bbb,12)")

    assertEquals(expectedResult.sorted, WindowReduceITCase.testResults.sorted)
  }

  @Test
  def testReduceWithWindowFunction(): Unit = {
    WindowReduceITCase.testResults = mutable.MutableList()
    CheckingIdentityRichWindowFunction.reset()

    val reduceFunc = new ReduceFunction[(String, Int)] {
      override def reduce(a: (String, Int), b: (String, Int)): (String, Int) = {
        (a._1 + b._1, a._2 + b._2)
      }
    }
    
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

        // source is finite, so it will have an implicit MAX watermark when it finishes
      }

      def cancel() {
      }
    }).assignTimestampsAndWatermarks(new WindowReduceITCase.Tuple2TimestampExtractor)
    
    source1
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
      .reduce(
        reduceFunc,
        new CheckingIdentityRichWindowFunction[(String, Int), Tuple, TimeWindow]())
      .addSink(new SinkFunction[(String, Int)]() {
        def invoke(value: (String, Int)) {
          WindowReduceITCase.testResults += value.toString
        }
      })

    env.execute("Reduce Window Test")

    val expectedResult = mutable.MutableList(
      "(aaa,3)",
      "(aaa,21)",
      "(bbb,12)")

    assertEquals(expectedResult.sorted, WindowReduceITCase.testResults.sorted)

    CheckingIdentityRichWindowFunction.checkRichMethodCalls()
  }

  @Test
  def testReduceWithProcessWindowFunction(): Unit = {
    WindowReduceITCase.testResults = mutable.MutableList()
    CheckingIdentityRichProcessWindowFunction.reset()

    val reduceFunc = new ReduceFunction[(String, Int)] {
      override def reduce(a: (String, Int), b: (String, Int)): (String, Int) = {
        (a._1 + b._1, a._2 + b._2)
      }
    }

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

        // source is finite, so it will have an implicit MAX watermark when it finishes
      }

      def cancel() {
      }
    }).assignTimestampsAndWatermarks(new WindowReduceITCase.Tuple2TimestampExtractor)

    source1
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
      .reduce(
        reduceFunc,
        new CheckingIdentityRichProcessWindowFunction[(String, Int), Tuple, TimeWindow]())
      .addSink(new SinkFunction[(String, Int)]() {
        def invoke(value: (String, Int)) {
          WindowReduceITCase.testResults += value.toString
        }
      })

    env.execute("Reduce Process Window Test")

    val expectedResult = mutable.MutableList(
      "(aaa,3)",
      "(aaa,21)",
      "(bbb,12)")

    assertEquals(expectedResult.sorted, WindowReduceITCase.testResults.sorted)

    CheckingIdentityRichProcessWindowFunction.checkRichMethodCalls()
  }

  @Test
  def testReduceAllWindow(): Unit = {
    WindowReduceITCase.testResults = mutable.MutableList()
    
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

        // source is finite, so it will have an implicit MAX watermark when it finishes
      }

      def cancel() {
      }
    }).assignTimestampsAndWatermarks(new WindowReduceITCase.Tuple2TimestampExtractor)

    source1
      .windowAll(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
      .reduce( (a, b) => (a._1 + b._1, a._2 + b._2) )
      .addSink(new SinkFunction[(String, Int)]() {
      def invoke(value: (String, Int)) {
        WindowReduceITCase.testResults += value.toString
      }
    })

    env.execute("Fold All-Window Test")

    val expectedResult = mutable.MutableList(
      "(aaa,3)",
      "(bababa,24)")

    assertEquals(expectedResult.sorted, WindowReduceITCase.testResults.sorted)
  }

  @Test
  def testReduceAllWithWindowFunction(): Unit = {
    WindowReduceITCase.testResults = mutable.MutableList()
    CheckingIdentityRichAllWindowFunction.reset()

    val reduceFunc = new ReduceFunction[(String, Int)] {
      override def reduce(a: (String, Int), b: (String, Int)): (String, Int) = {
        (a._1 + b._1, a._2 + b._2)
      }
    }
    
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

        // source is finite, so it will have an implicit MAX watermark when it finishes
      }

      def cancel() {
      }
    }).assignTimestampsAndWatermarks(new WindowReduceITCase.Tuple2TimestampExtractor)

    source1
      .windowAll(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
      .reduce(
        reduceFunc,
        new CheckingIdentityRichAllWindowFunction[(String, Int), TimeWindow]())
      .addSink(new SinkFunction[(String, Int)]() {
        def invoke(value: (String, Int)) {
          WindowReduceITCase.testResults += value.toString
        }
      })

    env.execute("Fold All-Window Test")

    val expectedResult = mutable.MutableList(
      "(aaa,3)",
      "(bababa,24)")

    assertEquals(expectedResult.sorted, WindowReduceITCase.testResults.sorted)

    CheckingIdentityRichAllWindowFunction.checkRichMethodCalls()
  }

  @Test
  def testReduceAllWithProcessWindowFunction(): Unit = {
    WindowReduceITCase.testResults = mutable.MutableList()
    CheckingIdentityRichProcessAllWindowFunction.reset()

    val reduceFunc = new ReduceFunction[(String, Int)] {
      override def reduce(a: (String, Int), b: (String, Int)): (String, Int) = {
        (a._1 + b._1, a._2 + b._2)
      }
    }

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

        // source is finite, so it will have an implicit MAX watermark when it finishes
      }

      def cancel() {
      }
    }).assignTimestampsAndWatermarks(new WindowReduceITCase.Tuple2TimestampExtractor)

    source1
      .windowAll(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
      .reduce(
        reduceFunc,
        new CheckingIdentityRichProcessAllWindowFunction[(String, Int), TimeWindow]())
      .addSink(new SinkFunction[(String, Int)]() {
        def invoke(value: (String, Int)) {
          WindowReduceITCase.testResults += value.toString
        }
      })

    env.execute("Fold All-Window Test")

    val expectedResult = mutable.MutableList(
      "(aaa,3)",
      "(bababa,24)")

    assertEquals(expectedResult.sorted, WindowReduceITCase.testResults.sorted)

    CheckingIdentityRichProcessAllWindowFunction.checkRichMethodCalls()
  }
}

object WindowReduceITCase {
  
  private var testResults: mutable.MutableList[String] = null

  private class Tuple2TimestampExtractor extends AssignerWithPunctuatedWatermarks[(String, Int)] {

    private var currentTimestamp = -1L

    override def extractTimestamp(element: (String, Int), previousTimestamp: Long): Long = {
      currentTimestamp = element._2
      currentTimestamp
    }

    def checkAndGetNextWatermark(
        lastElement: (String, Int),
        extractedTimestamp: Long): Watermark = {
      new Watermark(lastElement._2 - 1)
    }
  }
}



