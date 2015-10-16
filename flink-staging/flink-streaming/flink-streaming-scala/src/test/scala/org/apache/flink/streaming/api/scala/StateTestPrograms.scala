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

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
 * Test programs for stateful functions.
 */
object StateTestPrograms {

  def testStatefulFunctions(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    // test stateful map
    env.generateSequence(0, 10).setParallelism(1)
      .map { v => (1, v) }.setParallelism(1)
      .keyBy(_._1)
      .mapWithState((in, count: Option[Long]) =>
        count match {
          case Some(c) => (in._2 - c, Some(c + 1))
          case None => (in._2, Some(1L))
        }).setParallelism(1)
      
      .addSink(new RichSinkFunction[Long]() {
        var allZero = true
        override def invoke(in: Long) = {
          if (in != 0) allZero = false
        }
        override def close() = {
          assert(allZero)
        }
      })

    // test stateful flatmap
    env.fromElements((1, "First"), (2, "Second"), (1, "Hello world"))
      .keyBy(_._1)
      .flatMapWithState((w, s: Option[String]) =>
        s match {
          case Some(state) => (w._2.split(" ").toList.map(state + _), Some(w._2))
          case None => (List(w._2), Some(w._2))
        })
      .setParallelism(1)
      
      .addSink(new RichSinkFunction[String]() {
        val received = new util.HashSet[String]()
        override def invoke(in: String) = { received.add(in) }
        override def close() = {
          assert(received.size() == 4)
          assert(received.contains("First"))
          assert(received.contains("Second"))
          assert(received.contains("FirstHello"))
          assert(received.contains("Firstworld"))
        }
      }).setParallelism(1)

    // test stateful filter
    env.generateSequence(1, 10).keyBy(_ % 2).filterWithState((in, state: Option[Int]) =>
      state match {
        case Some(s) => (s < 2, Some(s + 1))
        case None => (true, Some(1))
      }).addSink(new RichSinkFunction[Long]() {
      var numOdd = 0
      var numEven = 0
      override def invoke(in: Long) = {
        if (in % 2 == 0) { numEven += 1 } else { numOdd += 1 }
      }
      override def close() = {
        assert(numOdd == 2)
        assert(numEven == 2)
      }
    }).setParallelism(1)

    env.execute("Stateful test")
  }

}
