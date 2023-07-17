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
 * Wraps a data stream, allowing to use anonymous partial functions to perform extraction of items
 * in a tuple, case class instance or collection
 *
 * @param stream
 *   The wrapped data stream
 * @tparam T
 *   The type of the data stream items
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
class OnDataStream[T](stream: DataStream[T]) {

  /**
   * Applies a function `fun` to each item of the stream
   *
   * @param fun
   *   The function to be applied to each item
   * @tparam R
   *   The type of the items in the returned stream
   * @return
   *   A dataset of R
   */
  @PublicEvolving
  def mapWith[R: TypeInformation](fun: T => R): DataStream[R] =
    stream.map(fun)

  /**
   * Applies a function `fun` to each item of the stream, producing a collection of items that will
   * be flattened in the resulting stream
   *
   * @param fun
   *   The function to be applied to each item
   * @tparam R
   *   The type of the items in the returned stream
   * @return
   *   A dataset of R
   */
  @PublicEvolving
  def flatMapWith[R: TypeInformation](fun: T => TraversableOnce[R]): DataStream[R] =
    stream.flatMap(fun)

  /**
   * Applies a predicate `fun` to each item of the stream, keeping only those for which the
   * predicate holds
   *
   * @param fun
   *   The predicate to be tested on each item
   * @return
   *   A dataset of R
   */
  @PublicEvolving
  def filterWith(fun: T => Boolean): DataStream[T] =
    stream.filter(fun)

  /**
   * Keys the items according to a keying function `fun`
   *
   * @param fun
   *   The keying function
   * @tparam K
   *   The type of the key, for which type information must be known
   * @return
   *   A stream of Ts keyed by Ks
   */
  @PublicEvolving
  def keyingBy[K: TypeInformation](fun: T => K): KeyedStream[T, K] =
    stream.keyBy(fun)

}
