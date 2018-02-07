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

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.util.Collector
import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * ITCase for the [[org.apache.flink.api.common.state.BroadcastState]].
  */
class BroadcastStateITCase extends AbstractTestBase {

  @Test
  @throws[Exception]
  def testConnectWithBroadcastTranslation(): Unit = {

    val timerTimestamp = 100000L

    val DESCRIPTOR = new MapStateDescriptor[Long, String](
      "broadcast-state",
      BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]],
      BasicTypeInfo.STRING_TYPE_INFO)

    val expected = Map[Long, String](
      0L -> "test:0",
      1L -> "test:1",
      2L -> "test:2",
      3L -> "test:3",
      4L -> "test:4",
      5L -> "test:5")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val srcOne = env.generateSequence(0L, 5L)
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[Long]() {

        override def extractTimestamp(element: Long, previousElementTimestamp: Long): Long =
          element

        override def checkAndGetNextWatermark(lastElement: Long, extractedTimestamp: Long) =
          new Watermark(extractedTimestamp)

      }).keyBy((value: Long) => value)

    val srcTwo = env.fromCollection(expected.values.toSeq)
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[String]() {

        override def extractTimestamp(element: String, previousElementTimestamp: Long): Long =
          element.split(":")(1).toLong

        override def checkAndGetNextWatermark(lastElement: String, extractedTimestamp: Long) =
          new Watermark(extractedTimestamp)
      })

    val broadcast = srcTwo.broadcast(DESCRIPTOR)
    // the timestamp should be high enough to trigger the timer after all the elements arrive.
    val output = srcOne.connect(broadcast).process(
      new KeyedBroadcastProcessFunction[Long, Long, String, String]() {

        @throws[Exception]
        override def processElement(
            value: Long,
            ctx: KeyedBroadcastProcessFunction[Long, Long, String, String]#KeyedReadOnlyContext,
            out: Collector[String]): Unit = {

          ctx.timerService.registerEventTimeTimer(timerTimestamp)
        }

        @throws[Exception]
        override def processBroadcastElement(
            value: String,
            ctx: KeyedBroadcastProcessFunction[Long, Long, String, String]#KeyedContext,
            out: Collector[String]): Unit = {

          val key = value.split(":")(1).toLong
          ctx.getBroadcastState(DESCRIPTOR).put(key, value)
        }

        @throws[Exception]
        override def onTimer(
            timestamp: Long,
            ctx: KeyedBroadcastProcessFunction[Long, Long, String, String]#OnTimerContext,
            out: Collector[String]): Unit = {

          var counter = 0
          import scala.collection.JavaConversions._
          for (entry <- ctx.getBroadcastState(DESCRIPTOR).immutableEntries()) {
            val v = expected.get(entry.getKey).get
            assertEquals(v, entry.getValue)
            counter += 1
          }
          assertEquals(expected.size, counter)
        }
      })

    output.addSink(new DiscardingSink[String])
    env.execute
  }
}
