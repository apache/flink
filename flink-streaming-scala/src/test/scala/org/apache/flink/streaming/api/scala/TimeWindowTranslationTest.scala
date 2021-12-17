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

import org.apache.flink.api.common.state.{ListStateDescriptor, ReducingStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.util.Collector

import org.junit.Assert._
import org.junit.Test

import java.util.concurrent.TimeUnit

/**
  * These tests verify that the api calls on [[WindowedStream]] that use the "time" shortcut
  * instantiate the correct window operator.
  */
class TimeWindowTranslationTest extends AbstractTestBase {

  /**
    * Verifies that calls to timeWindow() instantiate a regular
    * windowOperator instead of an aligned one.
    */
  @Test
  def testAlignedWindowDeprecation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val reducer = new DummyReducer

    val window1 = source
      .keyBy(0)
      .timeWindow(Time.seconds(1), Time.milliseconds(100))
      .reduce(reducer)

    val transform1 = window1.javaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator1 = transform1.getOperator

    assertTrue(operator1.isInstanceOf[WindowOperator[_, _, _, _, _]])

    val window2 = source
      .keyBy(0)
      .timeWindow(Time.minutes(1))
      .apply(new WindowFunction[(String, Int), (String, Int), Tuple, TimeWindow]() {
        def apply(
                   key: Tuple,
                   window: TimeWindow,
                   values: Iterable[(String, Int)],
                   out: Collector[(String, Int)]) { }
      })

    val transform2 = window2.javaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator2 = transform2.getOperator

    assertTrue(operator2.isInstanceOf[WindowOperator[_, _, _, _, _]])
  }

  @Test
  def testReduceEventTimeWindows(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(0)
      .timeWindow(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS))
      .reduce(new DummyReducer())

    val transform1 = window1.javaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator1 = transform1.getOperator

    assertTrue(operator1.isInstanceOf[WindowOperator[_, _, _, _, _]])

    val winOperator1 = operator1.asInstanceOf[WindowOperator[_, _, _, _, _]]

    assertTrue(winOperator1.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator1.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator1.getStateDescriptor.isInstanceOf[ReducingStateDescriptor[_]])
  }

  @Test
  def testApplyEventTimeWindows(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val window1 = source
      .keyBy(0)
      .timeWindow(Time.of(1, TimeUnit.SECONDS), Time.of(100, TimeUnit.MILLISECONDS))
      .apply(new WindowFunction[(String, Int), (String, Int), Tuple, TimeWindow] {
        override def apply(
            key: Tuple,
            window: TimeWindow,
            input: Iterable[(String, Int)],
            out: Collector[(String, Int)]): Unit = ???
      })

    val transform1 = window1.javaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator1 = transform1.getOperator

    assertTrue(operator1.isInstanceOf[WindowOperator[_, _, _, _, _]])

    val winOperator1 = operator1.asInstanceOf[WindowOperator[_, _, _, _, _]]

    assertTrue(winOperator1.getTrigger.isInstanceOf[EventTimeTrigger])
    assertTrue(winOperator1.getWindowAssigner.isInstanceOf[SlidingEventTimeWindows])
    assertTrue(winOperator1.getStateDescriptor.isInstanceOf[ListStateDescriptor[_]])
  }
}
