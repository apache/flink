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
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream}

/**
 * Wraps a keyed data stream, allowing to use anonymous partial functions to perform extraction of
 * items in a tuple, case class instance or collection
 *
 * @param stream
 *   The wrapped data stream
 * @tparam T
 *   The type of the data stream items
 * @tparam K
 *   The type of key
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
class OnKeyedStream[T, K](stream: KeyedStream[T, K]) {

  /**
   * Applies a reducer `fun` to the stream
   *
   * @param fun
   *   The reducing function to be applied on the keyed stream
   * @return
   *   A data set of Ts
   */
  @PublicEvolving
  def reduceWith(fun: (T, T) => T): DataStream[T] =
    stream.reduce(fun)
}
