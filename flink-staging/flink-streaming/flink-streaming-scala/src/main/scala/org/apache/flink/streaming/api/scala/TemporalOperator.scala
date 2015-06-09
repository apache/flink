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

import org.apache.flink.api.scala.ClosureCleaner
import org.apache.flink.streaming.api.datastream.temporal.{ TemporalOperator => JTempOp }
import org.apache.flink.streaming.api.datastream.{ DataStream => JavaStream }
import org.apache.flink.streaming.api.datastream.temporal.TemporalWindow
import org.apache.flink.streaming.api.windowing.helper.Timestamp

abstract class TemporalOperator[I1, I2, OP <: TemporalWindow[OP]](
  i1: JavaStream[I1], i2: JavaStream[I2]) extends JTempOp[I1, I2, OP](i1, i2) {

  def onWindow(length: Long, ts1: I1 => Long, ts2: I2 => Long, startTime: Long = 0): OP = {
    val timeStamp1 = getTS(ts1)
    val timeStamp2 = getTS(ts2)
    onWindow(length, timeStamp1, timeStamp2, startTime)
  }

  def getTS[R](ts: R => Long): Timestamp[R] = {
    val cleanFun = clean(ts)
    new Timestamp[R] {
      def getTimestamp(in: R) = cleanFun(in)
    }
  }

  /**
   * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
   * is not disabled in the {@link org.apache.flink.api.common.ExecutionConfig}
   */
  private[flink] def clean[F <: AnyRef](f: F): F = {
    new StreamExecutionEnvironment(i1.getExecutionEnvironment).scalaClean(f)
  }

}
