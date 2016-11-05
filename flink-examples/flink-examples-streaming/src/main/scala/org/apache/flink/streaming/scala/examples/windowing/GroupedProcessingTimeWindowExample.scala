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

import java.util.concurrent.TimeUnit.MILLISECONDS

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * An example of grouped stream windowing into sliding time windows.
 * This example uses [[RichParallelSourceFunction]] to generate a list of key-value pair.
 */
object GroupedProcessingTimeWindowExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val stream: DataStream[(Long, Long)] = env.addSource(new DataSource)

    stream
      .keyBy(0)
      .timeWindow(Time.of(2500, MILLISECONDS), Time.of(500, MILLISECONDS))
      .reduce((value1, value2) => (value1._1, value1._2 + value2._2))
      .addSink(new SinkFunction[(Long, Long)]() {
        override def invoke(in: (Long, Long)): Unit = {}
      })

    env.execute()
  }

  /**
   * Parallel data source that serves a list of key-value pair.
   */
  private class DataSource extends RichParallelSourceFunction[(Long, Long)] {
    @volatile private var running = true

    override def run(ctx: SourceContext[(Long, Long)]): Unit = {
      val startTime = System.currentTimeMillis()

      val numElements = 20000000
      val numKeys = 10000
      var value = 1L
      var count = 0L

      while (running && count < numElements) {

        ctx.collect((value, 1L))

        count += 1
        value += 1

        if (value > numKeys) {
          value = 1L
        }
      }

      val endTime = System.currentTimeMillis()
      println(s"Took ${endTime - startTime} msecs for ${numElements} values")
    }

    override def cancel(): Unit = running = false
  }
}
