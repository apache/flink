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

import java.util.{List => JList}

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.state.{AggregatingStateDescriptor, ListStateDescriptor, ReducingStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.operators.{OneInputStreamOperator, OutputTypeConfigurable}
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.slicing.Slice
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, EventTimeTrigger, ProcessingTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.streaming.runtime.operators.windowing._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.junit.Assert._
import org.junit.Test

/**
  * These tests verify that the api calls on [[SlicedStream]] instantiate the correct
  * [[SliceOperator]].
  */
class SliceTranslationTest {

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
      .slice(TumblingEventTimeSlices.of(Time.seconds(1), Time.milliseconds(100)))
      .reduceToSlice(new RichReduceFunction[(String, Int)] {
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
      .slice(TumblingEventTimeSlices.of(Time.seconds(1), Time.milliseconds(100)))
      .aggregateToSlice(new DummyRichAggregator())

    fail("exception was not thrown")
  }

  // --------------------------------------------------------------------------
  //  reduceToSlice() tests
  // --------------------------------------------------------------------------

  @Test
  def testReduceEventTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val slice: DataStream[Slice[(String, Int), String, TimeWindow]] = source
      .keyBy(_._1)
      .slice(TumblingEventTimeSlices.of(Time.seconds(1), Time.milliseconds(100)))
      .reduceToSlice(new DummyReducer)

    val transform = slice
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), Slice[(String, Int), String, TimeWindow]]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[SliceOperator[_, _, _, _ <: Window]])

    val sliceOperator = operator
      .asInstanceOf[SliceOperator[String, (String, Int), (String, Int), TimeWindow]]

    assertTrue(sliceOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(sliceOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeSlices])
    assertTrue(sliceOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), Slice[(String, Int), String, TimeWindow]](
      sliceOperator,
      sliceOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testReduceProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val slice = source
      .keyBy(_._1)
      .slice(TumblingProcessingTimeSlices.of(Time.seconds(1), Time.milliseconds(100)))
      .reduceToSlice(new DummyReducer)

    val transform = slice
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), Slice[(String, Int), String, TimeWindow]]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[SliceOperator[_, _, _, TimeWindow]])

    val sliceOperator = operator
      .asInstanceOf[SliceOperator[String, (String, Int), (String, Int), TimeWindow]]

    assertTrue(sliceOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(sliceOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeSlices])
    assertTrue(sliceOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), Slice[(String, Int), String, TimeWindow]](
      sliceOperator,
      sliceOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testReduceEventTimeWithScalaFunction() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val slice = source
      .keyBy(_._1)
      .slice(TumblingEventTimeSlices.of(Time.seconds(1), Time.milliseconds(100)))
      .reduceToSlice((x, _) => x)

    val transform = slice
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), Slice[(String, Int), String, TimeWindow]]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[SliceOperator[_, _, _, TimeWindow]])

    val sliceOperator = operator
      .asInstanceOf[SliceOperator[String, (String, Int), (String, Int), TimeWindow]]

    assertTrue(sliceOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(sliceOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeSlices])
    assertTrue(sliceOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), Slice[(String, Int), String, TimeWindow]](
      sliceOperator,
      sliceOperator.getKeySelector,
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

    val slice = source
      .keyBy(_._1)
      .slice(TumblingEventTimeSlices.of(Time.seconds(1), Time.milliseconds(100)))
      .aggregateToSlice(new DummyAggregator())

    val transform = slice
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), Slice[(String, Int), String, TimeWindow]]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[SliceOperator[_, _, _, TimeWindow]])

    val sliceOperator = operator
      .asInstanceOf[SliceOperator[String, (String, Int), (String, Int), TimeWindow]]

    assertTrue(sliceOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(sliceOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeSlices])
    assertTrue(sliceOperator.getStateDescriptor.isInstanceOf[AggregatingStateDescriptor[_, _, _]])

    processElementAndEnsureOutput[String, (String, Int), Slice[(String, Int), String, TimeWindow]](
      sliceOperator,
      sliceOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testAggregateProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val slice = source
      .keyBy(_._1)
      .slice(TumblingProcessingTimeSlices.of(Time.seconds(1), Time.milliseconds(100)))
      .aggregateToSlice(new DummyAggregator())

    val transform = slice
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), Slice[(String, Int), String, TimeWindow]]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[SliceOperator[_, _, _, TimeWindow]])

    val sliceOperator = operator
      .asInstanceOf[SliceOperator[String, (String, Int), (String, Int), TimeWindow]]

    assertTrue(sliceOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(sliceOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeSlices])
    assertTrue(sliceOperator.getStateDescriptor.isInstanceOf[AggregatingStateDescriptor[_, _, _]])

    processElementAndEnsureOutput[String, (String, Int), Slice[(String, Int), String, TimeWindow]](
      sliceOperator,
      sliceOperator.getKeySelector,
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

    val slice = source
      .keyBy(_._1)
      .slice(TumblingEventTimeSlices.of(Time.seconds(1), Time.milliseconds(100)))
      .applyToSlice()

    val transform = slice
      .javaStream
      .getTransformation
      .asInstanceOf[
          OneInputTransformation[(String, Int),
          Slice[JList[(String, Int)],
          String,
          TimeWindow]]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[IterableSliceOperator[_, _, TimeWindow]])

    val sliceOperator = operator
      .asInstanceOf[IterableSliceOperator[String, (String, Int), TimeWindow]]

    assertTrue(sliceOperator.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(sliceOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeSlices])
    assertTrue(sliceOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[
        String,
        (String, Int),
        Slice[JList[(String, Int)],
        String,
        TimeWindow]](
      sliceOperator,
      sliceOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testApplyProcessingTime() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val slice = source
      .keyBy(_._1)
      .slice(TumblingProcessingTimeSlices.of(Time.seconds(1), Time.milliseconds(100)))
      .applyToSlice()

    val transform = slice
      .javaStream
      .getTransformation
      .asInstanceOf[
          OneInputTransformation[(String, Int),
          Slice[JList[(String, Int)],
          String,
          TimeWindow]]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[IterableSliceOperator[_, _, TimeWindow]])

    val sliceOperator = operator
      .asInstanceOf[IterableSliceOperator[String, (String, Int), TimeWindow]]

    assertTrue(sliceOperator.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(sliceOperator.getWindowAssigner.isInstanceOf[TumblingProcessingTimeSlices])
    assertTrue(sliceOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[
        String,
        (String, Int),
        Slice[JList[(String, Int)],
        String,
        TimeWindow]](
      sliceOperator,
      sliceOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  // --------------------------------------------------------------------------
  //  custom trigger test
  // --------------------------------------------------------------------------

  @Test
  def testReduceWithCustomTrigger() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val slice = source
      .keyBy(_._1)
      .slice(TumblingEventTimeSlices.of(Time.seconds(1), Time.milliseconds(100)))
      .trigger(CountTrigger.of(1))
      .reduceToSlice(new DummyReducer)

    val transform = slice
      .javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), Slice[(String, Int), String, TimeWindow]]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[SliceOperator[_, _, _, TimeWindow]])

    val sliceOperator = operator
      .asInstanceOf[SliceOperator[String, (String, Int), (String, Int), TimeWindow]]

    assertTrue(sliceOperator.getTrigger.isInstanceOf[CountTrigger[_]])
    assertTrue(sliceOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeSlices])
    assertTrue(sliceOperator.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])

    processElementAndEnsureOutput[String, (String, Int), Slice[(String, Int), String, TimeWindow]](
      sliceOperator,
      sliceOperator.getKeySelector,
      BasicTypeInfo.STRING_TYPE_INFO,
      ("hello", 1))
  }

  @Test
  def testApplyWithCustomTrigger() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val slice = source
      .keyBy(_._1)
      .slice(TumblingEventTimeSlices.of(Time.seconds(1), Time.milliseconds(100)))
      .trigger(CountTrigger.of(1))
      .applyToSlice()

    val transform = slice
      .javaStream
      .getTransformation
      .asInstanceOf[
          OneInputTransformation[(String, Int),
          Slice[JList[(String, Int)],
          String,
          TimeWindow]]]

    val operator = transform.getOperator
    assertTrue(operator.isInstanceOf[IterableSliceOperator[_, _, TimeWindow]])

    val sliceOperator = operator
      .asInstanceOf[IterableSliceOperator[String, (String, Int), TimeWindow]]

    assertTrue(sliceOperator.getTrigger.isInstanceOf[CountTrigger[_]])
    assertTrue(sliceOperator.getWindowAssigner.isInstanceOf[TumblingEventTimeSlices])
    assertTrue(sliceOperator.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])

    processElementAndEnsureOutput[
        String,
        (String, Int),
        Slice[JList[(String, Int)],
        String,
        TimeWindow]](
      sliceOperator,
      sliceOperator.getKeySelector,
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
