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

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.streaming.api.windowing.assigners.{TumblingTimeWindows, SlidingTimeWindows}
import org.apache.flink.streaming.api.windowing.evictors.{CountEvictor, TimeEvictor}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ProcessingTimeTrigger, CountTrigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.buffers.{HeapWindowBuffer, PreAggregatingHeapWindowBuffer}
import org.apache.flink.streaming.runtime.operators.windowing.{EvictingWindowOperator, WindowOperator, AccumulatingProcessingTimeWindowOperator, AggregatingProcessingTimeWindowOperator}
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.util.Collector

import org.junit.Assert._
import org.junit.Test

class WindowTranslationTest extends StreamingMultipleProgramsTestBase {

  /**
   * These tests ensure that the fast aligned time windows operator is used if the
   * conditions are right.
   */
  @Test
  def testFastTimeWindows(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val reducer = new DummyReducer

    val window1 = source
      .keyBy(0)
      .window(SlidingTimeWindows.of(
        Time.of(1, TimeUnit.SECONDS),
        Time.of(100, TimeUnit.MILLISECONDS)))
      .reduce(reducer)

    val transform1 = window1.getJavaStream.getTransformation
        .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]
    
    val operator1 = transform1.getOperator

    assertTrue(operator1.isInstanceOf[AggregatingProcessingTimeWindowOperator[_, _]])

    val window2 = source
      .keyBy(0)
      .window(SlidingTimeWindows.of(
        Time.of(1, TimeUnit.SECONDS),
        Time.of(100, TimeUnit.MILLISECONDS)))
      .apply(new WindowFunction[(String, Int), (String, Int), Tuple, TimeWindow]() {
        def apply(
            key: Tuple,
            window: TimeWindow,
            values: java.lang.Iterable[(String, Int)],
            out: Collector[(String, Int)]) { }
      })

    val transform2 = window2.getJavaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator2 = transform2.getOperator

    assertTrue(operator2.isInstanceOf[AccumulatingProcessingTimeWindowOperator[_, _, _]])
  }

  @Test
  def testNonEvicting(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val reducer = new DummyReducer

    val window1 = source
      .keyBy(0)
      .window(SlidingTimeWindows.of(
        Time.of(1, TimeUnit.SECONDS),
        Time.of(100, TimeUnit.MILLISECONDS)))
      .trigger(CountTrigger.of(100))
      .reduce(reducer)

    val transform1 = window1.getJavaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator1 = transform1.getOperator

    assertTrue(operator1.isInstanceOf[WindowOperator[_, _, _, _]])
    val winOperator1 = operator1.asInstanceOf[WindowOperator[_, _, _, _]]
    assertTrue(winOperator1.getTrigger.isInstanceOf[CountTrigger[_]])
    assertTrue(winOperator1.getWindowAssigner.isInstanceOf[SlidingTimeWindows])
    assertTrue(
      winOperator1.getWindowBufferFactory.isInstanceOf[PreAggregatingHeapWindowBuffer.Factory[_]])


    val window2 = source
      .keyBy(0)
      .window(TumblingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
      .trigger(CountTrigger.of(100))
      .apply(new WindowFunction[(String, Int), (String, Int), Tuple, TimeWindow]() {
      def apply(
                    tuple: Tuple,
                    window: TimeWindow,
                    values: java.lang.Iterable[(String, Int)],
                    out: Collector[(String, Int)]) { }
    })

    val transform2 = window2.getJavaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator2 = transform2.getOperator

    assertTrue(operator2.isInstanceOf[WindowOperator[_, _, _, _]])
    val winOperator2 = operator2.asInstanceOf[WindowOperator[_, _, _, _]]
    assertTrue(winOperator2.getTrigger.isInstanceOf[CountTrigger[_]])
    assertTrue(winOperator2.getWindowAssigner.isInstanceOf[TumblingTimeWindows])
    assertTrue(winOperator2.getWindowBufferFactory.isInstanceOf[HeapWindowBuffer.Factory[_]])
  }

  @Test
  def testEvicting(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val reducer = new DummyReducer

    val window1 = source
      .keyBy(0)
      .window(SlidingTimeWindows.of(
        Time.of(1, TimeUnit.SECONDS),
        Time.of(100, TimeUnit.MILLISECONDS)))
      .evictor(TimeEvictor.of(Time.of(1, TimeUnit.SECONDS)))
      .reduce(reducer)

    val transform1 = window1.getJavaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator1 = transform1.getOperator

    assertTrue(operator1.isInstanceOf[EvictingWindowOperator[_, _, _, _]])
    val winOperator1 = operator1.asInstanceOf[EvictingWindowOperator[_, _, _, _]]
    assertTrue(winOperator1.getTrigger.isInstanceOf[ProcessingTimeTrigger])
    assertTrue(winOperator1.getEvictor.isInstanceOf[TimeEvictor[_]])
    assertTrue(winOperator1.getWindowAssigner.isInstanceOf[SlidingTimeWindows])
    assertTrue(winOperator1.getWindowBufferFactory.isInstanceOf[HeapWindowBuffer.Factory[_]])


    val window2 = source
      .keyBy(0)
      .window(TumblingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
      .trigger(CountTrigger.of(100))
      .evictor(CountEvictor.of(1000))
      .apply(new WindowFunction[(String, Int), (String, Int), Tuple, TimeWindow]() {
      def apply(
                    tuple: Tuple,
                    window: TimeWindow,
                    values: java.lang.Iterable[(String, Int)],
                    out: Collector[(String, Int)]) { }
    })

    val transform2 = window2.getJavaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator2 = transform2.getOperator

    assertTrue(operator2.isInstanceOf[EvictingWindowOperator[_, _, _, _]])
    val winOperator2 = operator2.asInstanceOf[EvictingWindowOperator[_, _, _, _]]
    assertTrue(winOperator2.getTrigger.isInstanceOf[CountTrigger[_]])
    assertTrue(winOperator2.getEvictor.isInstanceOf[CountEvictor[_]])
    assertTrue(winOperator2.getWindowAssigner.isInstanceOf[TumblingTimeWindows])
    assertTrue(winOperator2.getWindowBufferFactory.isInstanceOf[HeapWindowBuffer.Factory[_]])
  }

  @Test
  def testPreReduce(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.fromElements(("hello", 1), ("hello", 2))

    val reducer = new DummyReducer

    val window1 = source
      .keyBy(0)
      .window(SlidingTimeWindows.of(
        Time.of(1, TimeUnit.SECONDS),
        Time.of(100, TimeUnit.MILLISECONDS)))
      .trigger(CountTrigger.of(100))
      .apply(reducer, new WindowFunction[(String, Int), (String, Int), Tuple, TimeWindow]() {
        def apply(
                   tuple: Tuple,
                   window: TimeWindow,
                   values: java.lang.Iterable[(String, Int)],
                   out: Collector[(String, Int)]) { }
      })

    val transform1 = window1.getJavaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator1 = transform1.getOperator

    assertTrue(operator1.isInstanceOf[WindowOperator[_, _, _, _]])
    val winOperator1 = operator1.asInstanceOf[WindowOperator[_, _, _, _]]
    assertTrue(winOperator1.getTrigger.isInstanceOf[CountTrigger[_]])
    assertTrue(winOperator1.getWindowAssigner.isInstanceOf[SlidingTimeWindows])
    assertTrue(
      winOperator1.getWindowBufferFactory.isInstanceOf[PreAggregatingHeapWindowBuffer.Factory[_]])


    val window2 = source
      .keyBy(0)
      .window(TumblingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
      .trigger(CountTrigger.of(100))
      .apply(reducer, new WindowFunction[(String, Int), (String, Int), Tuple, TimeWindow]() {
        def apply(
                   tuple: Tuple,
                   window: TimeWindow,
                   values: java.lang.Iterable[(String, Int)],
                   out: Collector[(String, Int)]) { }
      })

    val transform2 = window2.getJavaStream.getTransformation
      .asInstanceOf[OneInputTransformation[(String, Int), (String, Int)]]

    val operator2 = transform2.getOperator

    assertTrue(operator2.isInstanceOf[WindowOperator[_, _, _, _]])
    val winOperator2 = operator2.asInstanceOf[WindowOperator[_, _, _, _]]
    assertTrue(winOperator2.getTrigger.isInstanceOf[CountTrigger[_]])
    assertTrue(winOperator2.getWindowAssigner.isInstanceOf[TumblingTimeWindows])
    assertTrue(
      winOperator2.getWindowBufferFactory.isInstanceOf[PreAggregatingHeapWindowBuffer.Factory[_]])
  }
}
