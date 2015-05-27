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

import org.apache.flink.streaming.api.windowing.helper.{ Delta => JavaDelta }
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.clean
import org.apache.flink.streaming.api.windowing.deltafunction.DeltaFunction

object Delta {

  /**
   * Creates a delta helper representing a delta trigger or eviction policy.
   * </br></br> This policy calculates a delta between the data point which
   * triggered last and the currently arrived data point. It triggers if the
   * delta is higher than a specified threshold. </br></br> In case it gets
   * used for eviction, this policy starts from the first element of the
   * buffer and removes all elements from the buffer which have a higher delta
   * then the threshold. As soon as there is an element with a lower delta,
   * the eviction stops.
   */
  def of[T](threshold: Double, deltaFunction: (T, T) => Double, initVal: T): JavaDelta[T] = {
    require(deltaFunction != null, "Delta function must not be null")
    val df = new DeltaFunction[T] {
      val cleanFun = clean(deltaFunction)
      override def getDelta(first: T, second: T) = cleanFun(first, second)
    }
    JavaDelta.of(threshold, df, initVal)
  }

}
