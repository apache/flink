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

package org.apache.flink.streaming.scala.examples.windowing

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * An example of grouped stream windowing in session windows with session timeout of 3 msec.
 * A source fetches elements with key, timestamp, and count.
 */
object SessionWindowing {

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val fileOutput = params.has("output")

    val input = List(
      ("a", 1L, 1),
      ("b", 1L, 1),
      ("b", 3L, 1),
      ("b", 5L, 1),
      ("c", 6L, 1),
      // We expect to detect the session "a" earlier than this point (the old
      // functionality can only detect here when the next starts)
      ("a", 10L, 1),
      // We expect to detect session "b" and "c" at this point as well
      ("c", 11L, 1)
    )

    val source: DataStream[(String, Long, Int)] = env.addSource(
      new SourceFunction[(String, Long, Int)]() {

        override def run(ctx: SourceContext[(String, Long, Int)]): Unit = {
          input.foreach(value => {
            ctx.collectWithTimestamp(value, value._2)
            ctx.emitWatermark(new Watermark(value._2 - 1))
          })
          ctx.emitWatermark(new Watermark(Long.MaxValue))
        }

        override def cancel(): Unit = {}

      })

    // We create sessions for each id with max timeout of 3 time units
    val aggregated: DataStream[(String, Long, Int)] = source
      .keyBy(_._1)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
      .sum(2)

    if (fileOutput) {
      aggregated.writeAsText(params.get("output"))
    } else {
      print("Printing result to stdout. Use --output to specify output path.")
      aggregated.print()
    }

    env.execute()
  }

}
