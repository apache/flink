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

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.{Assert, Test}

/**
  * Unit test for [[org.apache.flink.streaming.api.scala.JoinedStreams]]
  */
class JoinedStreamsTest {
  private val env = StreamExecutionEnvironment.getExecutionEnvironment

  private val dataStream1 = env.fromElements("a1", "a2", "a3")
  private val dataStream2 = env.fromElements("a1", "a2")
  private val keySelector = (s: String) => s
  private val tsAssigner = TumblingEventTimeWindows.of(Time.milliseconds(1))

  @Test
  def testSetAllowedLateness(): Unit = {
    val lateness = Time.milliseconds(42)
    val withLateness = dataStream1.join(dataStream2)
      .where(keySelector)
      .equalTo(keySelector)
      .window(tsAssigner)
      .allowedLateness(lateness)
    Assert.assertEquals(lateness.toMilliseconds, withLateness.allowedLateness.toMilliseconds)
  }
}
