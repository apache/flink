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

package org.apache.flink.streaming.api.scala.windowing

import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.windowing.helper.{ Time => JavaTime }

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.clean
import org.apache.flink.streaming.api.windowing.helper.Timestamp

object Time {

  /**
   * Creates a helper representing a time trigger which triggers every given
   * length (slide size) or a time eviction which evicts all elements older
   * than length (window size) using System time.
   *
   */
  def of(windowSize: Long, timeUnit: TimeUnit): JavaTime[_] =
    JavaTime.of(windowSize, timeUnit)

  /**
   * Creates a helper representing a time trigger which triggers every given
   * length (slide size) or a time eviction which evicts all elements older
   * than length (window size) using a user defined timestamp extractor.
   *
   */
  def of[R](windowSize: Long, timestamp: R => Long, startTime: Long = 0): JavaTime[R] = {
    require(timestamp != null, "Timestamp must not be null.")
    val ts = new Timestamp[R] {
      val fun = clean(timestamp, true)
      override def getTimestamp(in: R) = fun(in)
    }
    JavaTime.of(windowSize, ts, startTime)
  }

}
