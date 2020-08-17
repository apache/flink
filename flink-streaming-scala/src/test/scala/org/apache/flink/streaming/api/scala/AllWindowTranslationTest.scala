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


import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.state.{AggregatingStateDescriptor, ListStateDescriptor, ReducingStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction}
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
import org.junit.rules.ExpectedException
import org.junit.{Rule, Test}

/**
  * These tests verify that the api calls on [[AllWindowedStream]] instantiate the correct
  * window operator.
  *
  * We also create a test harness and push one element into the operator to verify
  * that we get some output.
  */
class AllWindowTranslationTest {

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  // ------------------------------------------------------------------------
  //  rich function tests
  // ------------------------------------------------------------------------
  
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
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .reduce(new RichReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)) = null
      })

    fail("exception was not thrown")
  }

  /**
   * .aggregate() does not support [[RichAggregateFunction]], since the reduce function is used
   * internally in a [[org.apache.flink.api.common.state.AggregatingState]].
   */
  @Test(expected = classOf[UnsupportedOperationException])
  def testAggregateWithRichFunctionFails() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.fromElements(("hello", 1), ("hello", 2))

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    source
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .aggregate(new DummyRichAggregator())

    fail("exception was not thrown")
  }

  // ------------------------------------------------------------------------
  //  merging window precondition
  // ------------------------------------------------------------------------

  @Test
  def testMergingAssignerWithNonMergingTriggerFails() {
    expectedException.expect(classOf[UnsupportedOperationException])
    expectedException.expectMessage(
      "A merging window assigner cannot be used with a trigger"
        + " that does not support merging")
    // verify that we check for trigger compatibility
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromElements("Hello", "Ciao")
      .windowAll(EventTimeSessionWindows.withGap(Time.seconds(5)))
      .trigger(new Trigger[String, TimeWindow]() {
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
  }

  @Test
  def testMergingWindowsWithEvictor() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .windowAll(EventTimeSessionWindows.withGap(Time.seconds(1)))
      .evictor(CountEvictor.of(2))
      .process(new TestProcessAllWindowFunction)

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


  // ------------------------------------------------------------------------
  //  reduce() translation tests
  // ------------------------------------------------------------------------

  @Test
  def testReduceEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
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
      .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
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
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
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
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
      .reduce(
        new DummyReducer,
        new ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def process(
              context: Context,
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
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .reduce(
        new DummyReducer, new ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def process(
              context: Context,
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
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
      .reduce(
        new DummyReducer, new ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def process(context: Context,
                               elements: Iterable[(String, Int)],
                               out: Collector[(String, Int)]): Unit = {
            elements foreach ( x => out.collect(x))
          }
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
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .reduce(
        new DummyReducer, new ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def process(
              context: Context,
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
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
      .apply(
        new DummyReducer, new AllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def apply(
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
  def testReduceWithWindowFunctionEventTimeWithScalaFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
      .reduce(
        { (x, _) => x },
        {
          (_: TimeWindow, in: Iterable[(String, Int)], out: Collector[(String, Int)]) =>
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

  // ------------------------------------------------------------------------
  //  aggregate() translation tests
  // ------------------------------------------------------------------------

  @Test
  def testAggregateEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
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
      .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
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
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
      .aggregate(new DummyAggregator(), new TestAllWindowFunction())

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
        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
        .aggregate(new DummyAggregator(), new TestAllWindowFunction())

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
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
      .aggregate(new DummyAggregator(), new TestProcessAllWindowFunction())

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
        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
        .aggregate(new DummyAggregator(), new TestProcessAllWindowFunction())

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
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
      .aggregate(
        new DummyAggregator(),
        { (_: TimeWindow, in: Iterable[(String, Int)], out: Collector[(String, Int)]) => {
          in foreach { x => out.collect(x)}
        } })

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

  // ------------------------------------------------------------------------
  //  apply() translation tests
  // ------------------------------------------------------------------------

  @Test
  def testApplyEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .apply(
        new AllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def apply(
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
  def testApplyProcessingTimeTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .apply(
        new AllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def apply(
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
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .process(
        new ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def process(
              context: Context,
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
  def testProcessProcessingTimeTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .process(
        new ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def process(
              context: Context,
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
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .apply { (window, in, out: Collector[(String, Int)]) =>
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
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
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
  def testApplyWithCustomTrigger() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .trigger(CountTrigger.of(1))
      .apply(
        new AllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def apply(
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
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .trigger(CountTrigger.of(1))
      .process(
        new ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def process(
              context: Context,
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
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
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
  def testApplyWithEvictor() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .evictor(CountEvictor.of(100))
      .apply(
        new AllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def apply(
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
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(100)))
      .evictor(CountEvictor.of(100))
      .process(
        new ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {
          override def process(
              context: Context,
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

class TestAllWindowFunction
    extends AllWindowFunction[(String, Int), (String, Int), TimeWindow] {

  override def apply(
      window: TimeWindow,
      input: Iterable[(String, Int)],
      out: Collector[(String, Int)]): Unit = {

    input.foreach(out.collect)
  }
}

class TestProcessAllWindowFunction
    extends ProcessAllWindowFunction[(String, Int), (String, Int), TimeWindow] {

  override def process(
      context: Context,
      input: Iterable[(String, Int)],
      out: Collector[(String, Int)]): Unit = {

    input.foreach(out.collect)
  }
}
