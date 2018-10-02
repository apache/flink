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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.state.{AggregatingStateDescriptor, FoldingStateDescriptor, ListStateDescriptor, ReducingStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.operators.{OneInputStreamOperator, OutputTypeConfigurable}
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, EventTimeTrigger, ProcessingTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.streaming.runtime.operators.windowing._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.util.Collector
import org.junit.Assert._
import org.junit.Test

/**
  * These tests verify that the api calls on [[WindowedStream]] instantiate the correct
  * window operator.
  *
  * We also create a test harness and push one element into the operator to verify
  * that we get some output.
  */
class WindowTranslationTest {

  // --------------------------------------------------------------------------
  //  rich function tests
  // --------------------------------------------------------------------------

  /**
    * .reduce() does not support [[RichReduceFunction]], since the reduce function is used
    * internally in a [[org.apache.flink.api.common.state.ReducingState]].
    */
  @Test(expected = classOf[UnsupportedOperationException])
  def testReduceWithRichReducerFails() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.fromElements(("hello", 1), ("hello", 2))

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    source
      .keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .reduce(new RichReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)) = null
      })

    fail("exception was not thrown")
  }

  /**
   * .reduce() does not support [[RichReduceFunction]], since the reduce function is used
   * internally in a [[org.apache.flink.api.common.state.ReducingState]].
   */
  @Test(expected = classOf[UnsupportedOperationException])
  def testAggregateWithRichFunctionFails() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.fromElements(("hello", 1), ("hello", 2))

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    source
      .keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .aggregate(new DummyRichAggregator())

    fail("exception was not thrown")
  }

  /**
    * .fold() does not support [[RichFoldFunction]], since the reduce function is used internally
    * in a [[org.apache.flink.api.common.state.FoldingState]].
    */
  @Test(expected = classOf[UnsupportedOperationException])
  def testFoldWithRichFolderFails() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.fromElements(("hello", 1), ("hello", 2))

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    source
      .keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(("", 0), new RichFoldFunction[(String, Int), (String, Int)] {
        override def fold(accumulator: (String, Int), value: (String, Int)) = null
      })

    fail("exception was not thrown")
  }

  // --------------------------------------------------------------------------
  //  merging window checks
  // --------------------------------------------------------------------------

  @Test
  def testSessionWithFoldFails() {
    // verify that fold does not work with merging windows
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val windowedStream = env.fromElements("Hello", "Ciao")
      .keyBy(x => x)
      .window(EventTimeSessionWindows.withGap(Time.seconds(5)))

    try
      windowedStream.fold("", new FoldFunction[String, String]() {
        @throws[Exception]
        def fold(accumulator: String, value: String): String = accumulator
      })

    catch {
      case _: UnsupportedOperationException =>
        // expected
        // use a catch to ensure that the exception is thrown by the fold
        return
    }

    fail("The fold call should fail.")
  }

  @Test
  def testMergingAssignerWithNonMergingTriggerFails() {
    // verify that we check for trigger compatibility
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val windowedStream = env.fromElements("Hello", "Ciao")
      .keyBy(x => x)
      .window(EventTimeSessionWindows.withGap(Time.seconds(5)))

    try
      windowedStream.trigger(new Trigger[String, TimeWindow]() {
        def onElement(
            element: String,
            timestamp: Long,
            window: TimeWindow,
            ctx: Trigger.TriggerContext) = null

        def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = null

        def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = null

        override def canMerge = false

        def clear(window: TimeWindow, ctx: Trigger.TriggerContext) {}
      })

    catch {
      case _: UnsupportedOperationException =>
        // expected
        // use a catch to ensure that the exception is thrown by the fold
        return
    }

    fail("The trigger call should fail.")
  }

  @Test
  def testMergingWindowsWithEvictor() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(EventTimeSessionWindows.withGap(Time.seconds(1)))
      .evictor(CountEvictor.of(2))
      .process(new TestProcessWindowFunction)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[EventTimeSessionWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  // --------------------------------------------------------------------------
  //  reduce() tests
  // --------------------------------------------------------------------------

  @Test
  def testReduceEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .reduce(new DummyReducer)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testReduceProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .reduce(new DummyReducer)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testReduceEventTimeWithScalaFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .reduce( (x, _) => x )

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testReduceWithWindowFunctionEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      .reduce(
        new DummyReducer, new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach ( x => out.collect(x))
      })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testReduceWithWindowFunctionProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .reduce(
        new DummyReducer, new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach ( x => out.collect(x))
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testReduceWithProcessWindowFunctionEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      .reduce(
        new DummyReducer,
        new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def process(
              key: String,
              window: Context,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach ( x => out.collect(x))
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testReduceWithProcessWindowFunctionProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .reduce(
        new DummyReducer,
        new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def process(
              key: String,
              window: Context,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach ( x => out.collect(x))
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testApplyWithPreReducerEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      .apply(
        new DummyReducer, new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach ( x => out.collect(x))
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testApplyWithPreReducerAndEvictor() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      .evictor(CountEvictor.of(100))
      .apply(
        new DummyReducer,
        new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach ( x => out.collect(x))
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }


  @Test
  def testReduceWithWindowFunctionEventTimeWithScalaFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      .reduce(
        { (x, _) => x },
        {
          (_: String, _: TimeWindow, in: Iterable[(String, Int)], out: Collector[(String, Int)]) =>
            in foreach { x => out.collect(x)}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  // --------------------------------------------------------------------------
  //  aggregate() tests
  // --------------------------------------------------------------------------

  @Test
  def testAggregateEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .aggregate(new DummyAggregator())

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[AggregatingStateDescriptor[_, _, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testAggregateProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .aggregate(new DummyAggregator())

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[AggregatingStateDescriptor[_, _, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testAggregateWithWindowFunctionEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      .aggregate(new DummyAggregator(), new TestWindowFunction())

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[AggregatingStateDescriptor[_, _, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testAggregateWithWindowFunctionProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .aggregate(new DummyAggregator(), new TestWindowFunction())

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[AggregatingStateDescriptor[_, _, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testAggregateWithProcessWindowFunctionEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      .aggregate(new DummyAggregator(), new TestProcessWindowFunction())

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[AggregatingStateDescriptor[_, _, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testAggregateWithProcessWindowFunctionProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .aggregate(new DummyAggregator(), new TestProcessWindowFunction())

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[AggregatingStateDescriptor[_, _, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testAggregateWithWindowFunctionEventTimeWithScalaFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      .aggregate(
        new DummyAggregator(),
        { (_: String, _: TimeWindow, in: Iterable[(String, Int)], out: Collector[(String, Int)]) =>
          in foreach { x => out.collect(x)}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[AggregatingStateDescriptor[_, _, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  // --------------------------------------------------------------------------
  //  fold() tests
  // --------------------------------------------------------------------------

  @Test
  def testFoldEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(("", "", 1), new DummyFolder)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testFoldProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(("", "", 1), new DummyFolder)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testFoldEventTimeWithScalaFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(("", "", 1)) { (acc, _) => acc }

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }


  @Test
  def testFoldWithWindowFunctionEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(
        ("", "", 1),
        new DummyFolder,
        new WindowFunction[(String, String, Int), (String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._3))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testFoldWithWindowFunctionProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(
        ("", "", 1),
        new DummyFolder,
        new WindowFunction[(String, String, Int), (String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._3))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testFoldWithProcessWindowFunctionEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(
        ("", "", 1),
        new DummyFolder,
        new ProcessWindowFunction[(String, String, Int), (String, Int), String, TimeWindow] {
          override def process(
              key: String,
              window: Context,
              input: Iterable[(String, String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._3))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testFoldWithProcessWindowFunctionProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(
        ("", "", 1),
        new DummyFolder,
        new ProcessWindowFunction[(String, String, Int), (String, Int), String, TimeWindow] {
          override def process(
              key: String,
              window: Context,
              input: Iterable[(String, String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._3))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testApplyWithPreFolderEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .apply(
        ("", "", 1),
        new DummyFolder,
        new WindowFunction[(String, String, Int), (String, String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, String, Int)],
              out: Collector[(String, String, Int)]): Unit =
            input foreach {x => out.collect((x._1, x._2, x._3))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testApplyWithPreFolderAndEvictor() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .evictor(CountEvictor.of(100))
      .apply(
        ("", "", 1),
        new DummyFolder,
        new WindowFunction[(String, String, Int), (String, String, Int), String, TimeWindow] {
          override def apply(
                              key: String,
                              window: TimeWindow,
                              input: Iterable[(String, String, Int)],
                              out: Collector[(String, String, Int)]): Unit =
            input foreach {x => out.collect((x._1, x._2, x._3))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testFoldWithWindowFunctionEventTimeWithScalaFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .fold(
        ("", "", 1),
        { (acc: (String, String, Int), _) => acc },
        { (
            _: String,
            _: TimeWindow,
            in: Iterable[(String, String, Int)],
            out: Collector[(String, Int)]) =>
              in foreach { x => out.collect((x._1, x._3)) }
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  // --------------------------------------------------------------------------
  //  apply() tests
  // --------------------------------------------------------------------------

  @Test
  def testApplyEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .apply(
        new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._2))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testApplyProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .apply(
        new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._2))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testProcessEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .process(
        new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def process(
              key: String,
              window: Context,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._2))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testProcessProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .process(
        new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def process(
              key: String,
              window: Context,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._2))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testApplyEventTimeWithScalaFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .apply { (key, window, in, out: Collector[(String, Int)]) =>
        in foreach { x => out.collect(x)}
      }

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }


  @Test
  def testReduceWithCustomTrigger() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .trigger(CountTrigger.of(1))
      .reduce(new DummyReducer)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[CountTrigger[_]])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testFoldWithCustomTrigger() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .trigger(CountTrigger.of(1))
      .fold(("", "", 1), new DummyFolder)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[CountTrigger[_]])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[FoldingStateDescriptor[_, _]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testApplyWithCustomTrigger() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .trigger(CountTrigger.of(1))
      .apply(
        new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._2))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[CountTrigger[_]])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testProcessWithCustomTrigger() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .trigger(CountTrigger.of(1))
      .process(
        new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def process(
              key: String,
              window: Context,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._2))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[CountTrigger[_]])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }


  @Test
  def testReduceWithEvictor() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .evictor(CountEvictor.of(100))
      .reduce(new DummyReducer)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[EvictingWindowOperator[_, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[
      EvictingWindowOperator[String, (String, Int), (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getEvictor.isInstanceOf[CountEvictor[_]])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testReduceWithEvictorAndProcessFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .evictor(CountEvictor.of(100))
      .reduce(new DummyReducer, new TestProcessWindowFunction)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[EvictingWindowOperator[_, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[
      EvictingWindowOperator[String, (String, Int), (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getEvictor.isInstanceOf[CountEvictor[_]])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testAggregateWithEvictor() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .evictor(CountEvictor.of(100))
      .aggregate(new DummyAggregator())

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testAggregateWithEvictorAndProcessFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .evictor(CountEvictor.of(100))
      .aggregate(new DummyAggregator(), new TestProcessWindowFunction)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[WindowOperator[_, _, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[WindowOperator[String, (String, Int), _, (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testFoldWithEvictor() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .evictor(CountEvictor.of(100))
      .fold(("", "", 1), new DummyFolder)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[EvictingWindowOperator[_, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[
      EvictingWindowOperator[String, (String, Int), (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getEvictor.isInstanceOf[CountEvictor[_]])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    winOperator.setOutputType(
      window1.javaStream.getType.asInstanceOf[TypeInformation[(String, Int)]],
      new ExecutionConfig)

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testFoldWithEvictorAndProcessFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .evictor(CountEvictor.of(100))
      .fold(("", "", 1), new DummyFolder, new TestFoldProcessWindowFunction)

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[EvictingWindowOperator[_, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[
      EvictingWindowOperator[String, (String, Int), (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getEvictor.isInstanceOf[CountEvictor[_]])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    winOperator.setOutputType(
      window1.javaStream.getType.asInstanceOf[TypeInformation[(String, Int)]],
      new ExecutionConfig)

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }


  @Test
  def testApplyWithEvictor() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .evictor(CountEvictor.of(100))
      .apply(
        new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def apply(
              key: String,
              window: TimeWindow,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._2))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[EvictingWindowOperator[_, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[EvictingWindowOperator[String, (String, Int), (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getEvictor.isInstanceOf[CountEvictor[_]])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testProcessWithEvictor() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .evictor(CountEvictor.of(100))
      .process(
        new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def process(
              key: String,
              window: Context,
              input: Iterable[(String, Int)],
              out: Collector[(String, Int)]): Unit = input foreach {x => out.collect((x._1, x._2))}
        })

    val transform = window1
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[EvictingWindowOperator[_, _, _, _ <: Window]])

    val winOperator = operator
      .asInstanceOf[EvictingWindowOperator[String, (String, Int), (String, Int), _ <: Window]]

    assertTrue(winOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator.getEvictor.isInstanceOf[CountEvictor[_]])
    assertTrue(winOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeWindows])
    assertTrue(winOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), (String, Int)](
      winOperator,
      winOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  /**
    * Ensure that we get some output from the given operator when pushing in an element and
    * setting watermark and processing time to `Long.MaxValue`.
    */
  @throws[Exception]
  private def processElementAndEnsureOutput[K, IN, OUT](
      operator: OneInputStreamOperator[IN, OUT],
      keySelector: KeySelector[IN, K],
      keyType: TypeInformation[K],
      element: IN) {
    val testHarness =
      new KeyedOneInputStreamOperatorTestHarness[K, IN, OUT](operator, keySelector, keyType)

    if (operator.isInstanceOf[OutputTypeConfigurable[String]]) {
      // use a dummy type since window functions just need the ExecutionConfig
      // this is also only needed for Fold, which we're getting rid off soon.
      operator.asInstanceOf[OutputTypeConfigurable[String]]
        .setOutputType(BasicTypeInfo.STRING_TYPE_INFO, new ExecutionConfig)
    }
    testHarness.open()

    testHarness.setProcessingTime(0)
    testHarness.processWatermark(Long.MinValue)

    testHarness.processElement(new StreamRecord[IN](element, 0))

    // provoke any processing-time/event-time triggers
    testHarness.setProcessingTime(Long.MaxValue)
    testHarness.processWatermark(Long.MaxValue)

    // we at least get the two watermarks and should also see an output element
    assertTrue(testHarness.getOutput.size >= 3)

    testHarness.close()
  }
}

class DummyReducer extends ReduceFunction[(String, Int)] {
  override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
    value1
  }
}

class DummyFolder extends FoldFunction[(String, Int), (String, String, Int)] {
  override def fold(acc: (String, String, Int), in: (String, Int)): (String, String, Int) = {
    acc
  }
}

class DummyAggregator extends AggregateFunction[(String, Int), (String, Int), (String, Int)] {

  override def createAccumulator(): (String, Int) = ("", 0)

  override def merge(a: (String, Int), b: (String, Int)): (String, Int) = a

  override def getResult(accumulator: (String, Int)): (String, Int) = accumulator

  override def add(value: (String, Int), accumulator: (String, Int)): (String, Int) = accumulator
}

class DummyRichAggregator extends RichAggregateFunction[(String, Int), (String, Int), (String, Int)]
{

  override def createAccumulator(): (String, Int) = ("", 0)

  override def merge(a: (String, Int), b: (String, Int)): (String, Int) = a

  override def getResult(accumulator: (String, Int)): (String, Int) = accumulator

  override def add(value: (String, Int), accumulator: (String, Int)): (String, Int) = accumulator
}

class TestWindowFunction
  extends WindowFunction[(String, Int), (String, String, Int), String, TimeWindow] {

  override def apply(
      key: String,
      window: TimeWindow,
      input: Iterable[(String, Int)],
      out: Collector[(String, String, Int)]): Unit = {

    input.foreach(e => out.collect((e._1, e._1, e._2)))
  }
}

class TestProcessWindowFunction
  extends ProcessWindowFunction[(String, Int), (String, String, Int), String, TimeWindow] {

  override def process(
      key: String,
      window: Context,
      input: Iterable[(String, Int)],
      out: Collector[(String, String, Int)]): Unit = {

    input.foreach(e => out.collect((e._1, e._1, e._2)))
  }
}

class TestFoldProcessWindowFunction
  extends ProcessWindowFunction[(String, String, Int), (String, Int), String, TimeWindow] {

  override def process(
                        key: String,
                        window: Context,
                        input: Iterable[(String, String, Int)],
                        out: Collector[(String, Int)]): Unit = {

    input.foreach(e => out.collect((e._1, e._3)))
  }
}




