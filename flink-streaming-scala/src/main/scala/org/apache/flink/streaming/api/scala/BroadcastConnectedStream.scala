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

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{BroadcastConnectedStream => JavaBCStream}
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedBroadcastProcessFunction}

class BroadcastConnectedStream[IN1, IN2](javaStream: JavaBCStream[IN1, IN2]) {

  /**
    * Assumes as inputs a [[org.apache.flink.streaming.api.datastream.BroadcastStream]] and a
    * [[KeyedStream]] and applies the given [[KeyedBroadcastProcessFunction]] on them, thereby
    * creating a transformed output stream.
    *
    * @param function The [[KeyedBroadcastProcessFunction]] applied to each element in the stream.
    * @tparam KS The type of the keys in the keyed stream.
    * @tparam OUT The type of the output elements.
    * @return The transformed [[DataStream]].
    */
  @PublicEvolving
  def process[KS, OUT: TypeInformation](
      function: KeyedBroadcastProcessFunction[KS, IN1, IN2, OUT])
    : DataStream[OUT] = {

    if (function == null) {
      throw new NullPointerException("KeyedBroadcastProcessFunction function must not be null.")
    }

    val outputTypeInfo : TypeInformation[OUT] = implicitly[TypeInformation[OUT]]
    asScalaStream(javaStream.process(function, outputTypeInfo))
  }

  /**
    * Assumes as inputs a [[org.apache.flink.streaming.api.datastream.BroadcastStream]]
    * and a non-keyed [[DataStream]] and applies the given
    * [[org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction]]
    * on them, thereby creating a transformed output stream.
    *
    * @param function The [[BroadcastProcessFunction]] applied to each element in the stream.
    * @tparam OUT The type of the output elements.
    * @return The transformed { @link DataStream}.
    */
  @PublicEvolving
  def process[OUT: TypeInformation](
      function: BroadcastProcessFunction[IN1, IN2, OUT])
    : DataStream[OUT] = {

    if (function == null) {
      throw new NullPointerException("BroadcastProcessFunction function must not be null.")
    }

    val outputTypeInfo : TypeInformation[OUT] = implicitly[TypeInformation[OUT]]
    asScalaStream(javaStream.process(function, outputTypeInfo))
  }

  /**
    * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
    * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]]
    */
  private[flink] def clean[F <: AnyRef](f: F) = {
    new StreamExecutionEnvironment(javaStream.getExecutionEnvironment).scalaClean(f)
  }
}
