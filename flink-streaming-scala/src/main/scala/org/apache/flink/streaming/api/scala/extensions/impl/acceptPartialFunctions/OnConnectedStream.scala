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
package org.apache.flink.streaming.api.scala.extensions.impl.acceptPartialFunctions

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream}

/**
  * Wraps a connected data stream, allowing to use anonymous partial functions to
  * perform extraction of items in a tuple, case class instance or collection
  *
  * @param stream The wrapped data stream
  * @tparam IN1 The type of the data stream items coming from the first connection
  * @tparam IN2 The type of the data stream items coming from the second connection
  */
class OnConnectedStream[IN1, IN2](stream: ConnectedStreams[IN1, IN2]) {

  /**
    * Applies a CoMap transformation on the connected streams.
    *
    * The transformation consists of two separate functions, where
    * the first one is called for each element of the first connected stream,
    * and the second one is called for each element of the second connected stream.
    *
    * @param map1 Function called per element of the first input.
    * @param map2 Function called per element of the second input.
    * @return The resulting data stream.
    */
  @PublicEvolving
  def mapWith[R: TypeInformation](map1: IN1 => R, map2: IN2 => R): DataStream[R] =
    stream.map(map1, map2)

  /**
    * Applies a CoFlatMap transformation on the connected streams.
    *
    * The transformation consists of two separate functions, where
    * the first one is called for each element of the first connected stream,
    * and the second one is called for each element of the second connected stream.
    *
    * @param flatMap1 Function called per element of the first input.
    * @param flatMap2 Function called per element of the second input.
    * @return The resulting data stream.
    */
  @PublicEvolving
  def flatMapWith[R: TypeInformation](
      flatMap1: IN1 => TraversableOnce[R], flatMap2: IN2 => TraversableOnce[R]): DataStream[R] =
    stream.flatMap(flatMap1, flatMap2)

  /**
    * Keys the two connected streams together. After this operation, all
    * elements with the same key from both streams will be sent to the
    * same parallel instance of the transformation functions.
    *
    * @param key1 The first stream's key function
    * @param key2 The second stream's key function
    * @return The key-grouped connected streams
    */
  @PublicEvolving
  def keyingBy[K1: TypeInformation, K2: TypeInformation](key1: IN1 => K1, key2: IN2 => K2):
      ConnectedStreams[IN1, IN2] =
    stream.keyBy(key1, key2)

}
