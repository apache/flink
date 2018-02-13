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

package org.apache.flink.streaming.scala.examples.broadcast

import org.apache.flink.api.scala._
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.runtime.state.KeyedStateFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * Example illustrating the use of [[org.apache.flink.api.common.state.BroadcastState]].
  */
object BroadcastExample {

  def main(args: Array[String]): Unit = {

    val input = List(1, 2, 3, 4)

    val keyedInput = List[(Int, Int)](
      new Tuple2[Int, Int](1, 1),
      new Tuple2[Int, Int](1, 5),
      new Tuple2[Int, Int](2, 2),
      new Tuple2[Int, Int](2, 6),
      new Tuple2[Int, Int](3, 3),
      new Tuple2[Int, Int](3, 7),
      new Tuple2[Int, Int](4, 4),
      new Tuple2[Int, Int](4, 8)
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val mapStateDescriptor = new MapStateDescriptor[String, Integer](
      "Broadcast", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)

    val elementStream = env
      .fromCollection(keyedInput)
      .rebalance
      .map(value => value)
      .setParallelism(4)
      .keyBy(value => value._1)

    val broadcastStream = env
      .fromCollection(input)
      .flatMap((value: Int, out: Collector[Int]) => out.collect(value))
      .setParallelism(4)
      .broadcast(mapStateDescriptor)

    val output = elementStream
      .connect(broadcastStream)
      .process(new KeyedBroadcastProcessFunction[Int, (Int, Int), Int, String]() {

        private lazy val valueState = new ValueStateDescriptor[String](
          "any", BasicTypeInfo.STRING_TYPE_INFO)

        private lazy val mapStateDesc = new MapStateDescriptor[String, Integer](
          "Broadcast", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)

        @throws[Exception]
        override def processBroadcastElement(
            value: Int,
            ctx: KeyedBroadcastProcessFunction[Int, (Int, Int), Int, String]#KeyedContext,
            out: Collector[String])
          : Unit = {

          ctx.getBroadcastState(mapStateDesc).put(value + "", value)

          ctx.applyToKeyedState(valueState, new KeyedStateFunction[Int, ValueState[String]] {

            override def process(key: Int, state: ValueState[String]): Unit =
              out.collect("Broadcast side task#" +
                getRuntimeContext.getIndexOfThisSubtask + ": " + key + " " + state.value)
          })
        }

        @throws[Exception]
        override def processElement(
            value: (Int, Int),
            ctx: KeyedBroadcastProcessFunction[Int, (Int, Int), Int, String]#KeyedReadOnlyContext,
            out: Collector[String])
          : Unit = {

          val prev = getRuntimeContext.getState(valueState).value

          val str = new StringBuilder
          str.append("Value=").append(value).append(" Broadcast State=[")

          import scala.collection.JavaConversions._
          for (entry <- ctx.getBroadcastState(mapStateDesc).immutableEntries()) {
            str.append(entry.getKey).append("->").append(entry.getValue).append(" ")
          }
          str.append("]")

          getRuntimeContext.getState(valueState).update(str.toString)

          out.collect("BEFORE: " + prev + " " + "AFTER: " + str)

        }
      })

    output.print
    env.execute
  }
}
