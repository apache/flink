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

import java.util

import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.test.streaming.runtime.util.TestListResultSink
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.util.Collector
import org.junit.Assert._
import org.junit.Test

/**
 * Integration test for streaming programs using side outputs.
 */
class SideOutputITCase extends AbstractTestBase {

  /**
    * Test ProcessFunction side output.
    */
  @Test
  def testProcessFunctionSideOutput() {
    val sideOutputResultSink = new TestListResultSink[String]
    val resultSink = new TestListResultSink[Int]

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    val dataStream = env.fromElements(1, 2, 5, 3, 4)

    val outputTag = OutputTag[String]("side")

    val passThroughtStream = dataStream
      .process(new ProcessFunction[Int, Int] {
        override def processElement(
            value: Int, ctx: ProcessFunction[Int, Int]#Context, out: Collector[Int]): Unit = {
          out.collect(value)
          ctx.output(outputTag, "sideout-" + String.valueOf(value))
        }
      })

    passThroughtStream.getSideOutput(outputTag).addSink(sideOutputResultSink)
    passThroughtStream.addSink(resultSink)

    env.execute()
    
    assertEquals(
      util.Arrays.asList("sideout-1", "sideout-2", "sideout-3", "sideout-4", "sideout-5"),
      sideOutputResultSink.getSortedResult)
    
    assertEquals(util.Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult)
  }

  /**
   * Test keyed ProcessFunction side output.
   */
  @Test
  def testKeyedProcessFunctionSideOutput() {
    val sideOutputResultSink = new TestListResultSink[String]
    val resultSink = new TestListResultSink[Int]

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    val dataStream = env.fromElements(1, 2, 5, 3, 4)

    val outputTag = OutputTag[String]("side")

    val passThroughtStream = dataStream
      .keyBy(x => x)
      .process(new ProcessFunction[Int, Int] {
        override def processElement(
            value: Int, ctx: ProcessFunction[Int, Int]#Context, out: Collector[Int]): Unit = {
          out.collect(value)
          ctx.output(outputTag, "sideout-" + String.valueOf(value))
        }
      })

    passThroughtStream.getSideOutput(outputTag).addSink(sideOutputResultSink)
    passThroughtStream.addSink(resultSink)

    env.execute()

    assertEquals(
      util.Arrays.asList("sideout-1", "sideout-2", "sideout-3", "sideout-4", "sideout-5"),
      sideOutputResultSink.getSortedResult)

    assertEquals(util.Arrays.asList(1, 2, 3, 4, 5), resultSink.getSortedResult)
  }

  /**
   * Test ProcessFunction side outputs with wrong [[OutputTag]].
   */
  @Test
  def testProcessFunctionSideOutputWithWrongTag() {
    val sideOutputResultSink = new TestListResultSink[String]
    val resultSink = new TestListResultSink[Int]

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    val dataStream = env.fromElements(1, 2, 5, 3, 4)

    val outputTag = OutputTag[String]("side")
    val otherOutputTag = OutputTag[String]("other-side")

    val passThroughtStream = dataStream
      .process(new ProcessFunction[Int, Int] {
        override def processElement(
            value: Int,
            ctx: ProcessFunction[Int, Int]#Context,
            out: Collector[Int]): Unit = {
          ctx.output(otherOutputTag, "sideout-" + String.valueOf(value))
        }
      })

    passThroughtStream.getSideOutput(outputTag).addSink(sideOutputResultSink)

    env.execute()

    assertTrue(sideOutputResultSink.getSortedResult.isEmpty)
  }

  /**
   * Test window late arriving events stream
   */
  @Test
  def testAllWindowLateArrivingEvents() {
    val resultSink = new TestListResultSink[String]
    val lateResultSink = new TestListResultSink[(String, Int)]

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.fromElements(("1", 1), ("2", 2), ("5", 5), ("3", 3), ("4", 4))


    val lateDataTag = OutputTag[(String, Int)]("late")

    val windowOperator = dataStream
      .assignTimestampsAndWatermarks(new TestAssigner)
      .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(1)))
      .sideOutputLateData(lateDataTag)
      .process(new ProcessAllWindowFunction[(String, Int), String, TimeWindow] {
        override def process(
            context: Context,
            elements: Iterable[(String, Int)],
            out: Collector[String]): Unit = {
          for (in <- elements) {
            out.collect(in._1)
          }
        }
      })

    windowOperator
      .getSideOutput(lateDataTag)
      .addSink(lateResultSink)

    windowOperator.addSink(resultSink)

    env.execute()
    
    assertEquals(util.Arrays.asList("1", "2", "5"), resultSink.getResult)
    assertEquals(util.Arrays.asList(("3", 3), ("4", 4)), lateResultSink.getResult)
  }

  /**
   * Test window late arriving events stream
   */
  @Test
  def testKeyedWindowLateArrivingEvents() {
    val resultSink = new TestListResultSink[String]
    val lateResultSink = new TestListResultSink[(String, Int)]

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.fromElements(("1", 1), ("2", 2), ("5", 5), ("3", 3), ("4", 4))


    val lateDataTag = OutputTag[(String, Int)]("late")

    val windowOperator = dataStream
      .assignTimestampsAndWatermarks(new TestAssigner)
      .keyBy(i => i._1)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(1)))
      .sideOutputLateData(lateDataTag)
      .process(new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
        override def process(
            key:String,
            context: Context,
            elements: Iterable[(String, Int)],
            out: Collector[String]): Unit = {
          for (in <- elements) {
            out.collect(in._1)
          }
        }
      })

    windowOperator
      .getSideOutput(lateDataTag)
      .addSink(lateResultSink)

    windowOperator.addSink(resultSink)

    env.execute()

    assertEquals(util.Arrays.asList("1", "2", "5"), resultSink.getResult)
    assertEquals(util.Arrays.asList(("3", 3), ("4", 4)), lateResultSink.getResult)
  }

  /**
    * Test ProcessWindowFunction side output.
    */
  @Test
  def testProcessWindowFunctionSideOutput() {
    val resultSink = new TestListResultSink[String]
    val sideOutputResultSink = new TestListResultSink[String]

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.fromElements(("1", 1), ("2", 2), ("5", 5), ("3", 3), ("4", 4))


    val sideOutputTag = OutputTag[String]("side")

    val windowOperator = dataStream
      .assignTimestampsAndWatermarks(new TestAssigner)
      .keyBy(i => i._1)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(1)))
      .process(new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
        override def process(
            key: String,
            context: Context,
            elements: Iterable[(String, Int)],
            out: Collector[String]): Unit = {
          for (in <- elements) {
            out.collect(in._1)
            context.output(sideOutputTag, "sideout-" + in._1)
          }
        }
      })

    windowOperator
      .getSideOutput(sideOutputTag)
      .addSink(sideOutputResultSink)

    windowOperator.addSink(resultSink)

    env.execute()

    assertEquals(util.Arrays.asList("1", "2", "5"), resultSink.getResult)
    assertEquals(util.Arrays.asList("sideout-1", "sideout-2", "sideout-5"),
                  sideOutputResultSink.getResult)
  }

  /**
    * Test ProcessAllWindowFunction side output.
    */
  @Test
  def testProcessAllWindowFunctionSideOutput() {
    val resultSink = new TestListResultSink[String]
    val sideOutputResultSink = new TestListResultSink[String]

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.fromElements(("1", 1), ("2", 2), ("5", 5), ("3", 3), ("4", 4))


    val sideOutputTag = OutputTag[String]("side")

    val windowOperator = dataStream
      .assignTimestampsAndWatermarks(new TestAssigner)
      .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(1)))
      .process(new ProcessAllWindowFunction[(String, Int), String, TimeWindow] {
        override def process(
                              context: Context,
                              elements: Iterable[(String, Int)],
                              out: Collector[String]): Unit = {
          for (in <- elements) {
            out.collect(in._1)
            context.output(sideOutputTag, "sideout-" + in._1)
          }
        }
      })

    windowOperator
      .getSideOutput(sideOutputTag)
      .addSink(sideOutputResultSink)

    windowOperator.addSink(resultSink)

    env.execute()

    assertEquals(util.Arrays.asList("1", "2", "5"), resultSink.getResult)
    assertEquals(util.Arrays.asList("sideout-1", "sideout-2", "sideout-5"),
      sideOutputResultSink.getResult)
  }

  /**
   * Test the union of two side outputs.
   */
  @Test
  def testUnionOfTwoSideOutputs() {
    val evensResultSink = new TestListResultSink[Int]
    val oddsResultSink = new TestListResultSink[Int]
    val oddsUEvensResultSink = new TestListResultSink[Int]
    val evensUOddsResultSink = new TestListResultSink[Int]
    val oddsUOddsResultSink = new TestListResultSink[Int]
    val evensUEvensResultSink = new TestListResultSink[Int]
    val oddsUEvensExternalResultSink = new TestListResultSink[Int]

    val resultSink = new TestListResultSink[Int]

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    val input = env.fromElements(1, 2, 3, 4)

    val oddTag = OutputTag[Int]("odds")
    val evenTag = OutputTag[Int]("even")

    val passThroughStream =
      input.process(new ProcessFunction[Int, Int] {
        override def processElement(value: Int,
                                    ctx: ProcessFunction[Int, Int]#Context,
                                    out: Collector[Int]): Unit = {
          if (value % 2 != 0) {
            ctx.output(oddTag, value)
          } else {
            ctx.output(evenTag, value)
          }
          out.collect(value)
        }
      })

    val evens = passThroughStream.getSideOutput(evenTag)
    val odds = passThroughStream.getSideOutput(oddTag)

    evens.addSink(evensResultSink)
    odds.addSink(oddsResultSink)
    passThroughStream.addSink(resultSink)

    odds.union(evens).addSink(oddsUEvensResultSink)
    evens.union(odds).addSink(evensUOddsResultSink)

    odds.union(odds).addSink(oddsUOddsResultSink)
    evens.union(evens).addSink(evensUEvensResultSink)

    odds.union(env.fromElements(2, 4)).addSink(oddsUEvensExternalResultSink)

    env.execute()

    assertEquals(
      util.Arrays.asList(1, 3),
      oddsResultSink.getSortedResult)

    assertEquals(
      util.Arrays.asList(2, 4),
      evensResultSink.getSortedResult)

    assertEquals(
      util.Arrays.asList(1, 2, 3, 4),
      resultSink.getSortedResult)

    assertEquals(
      util.Arrays.asList(1, 2, 3, 4),
      oddsUEvensResultSink.getSortedResult)

    assertEquals(
      util.Arrays.asList(1, 2, 3, 4),
      evensUOddsResultSink.getSortedResult)

    assertEquals(
      util.Arrays.asList(1, 1, 3, 3),
      oddsUOddsResultSink.getSortedResult)

    assertEquals(
      util.Arrays.asList(2, 2, 4, 4),
      evensUEvensResultSink.getSortedResult)

    assertEquals(
      util.Arrays.asList(1, 2, 3, 4),
      oddsUEvensExternalResultSink.getSortedResult)
  }

}

class TestAssigner extends AssignerWithPunctuatedWatermarks[(String, Int)] {
  override def checkAndGetNextWatermark(
  lastElement: (String, Int),
  extractedTimestamp: Long): Watermark = new Watermark(extractedTimestamp)

  override def extractTimestamp(
  element: (String, Int),
  previousElementTimestamp: Long): Long = element._2.toLong
}
