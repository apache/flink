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
import org.apache.flink.streaming.api.scala.{DataStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector

/**
 * Wraps a joined data stream, allowing to use anonymous partial functions to perform extraction of
 * items in a tuple, case class instance or collection
 *
 * @param stream
 *   The wrapped data stream
 * @tparam T
 *   The type of the data stream items from the right input of the join
 * @tparam K
 *   The type of key
 * @tparam W
 *   The type of the window
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
class OnWindowedStream[T, K, W <: Window](stream: WindowedStream[T, K, W]) {

  /**
   * Applies a reduce function to the window. The window function is called for each evaluation of
   * the window for each key individually. The output of the reduce function is interpreted as a
   * regular non-windowed stream.
   *
   * This window will try and pre-aggregate data as much as the window policies permit. For
   * example,tumbling time windows can perfectly pre-aggregate the data, meaning that only one
   * element per key is stored. Sliding time windows will pre-aggregate on the granularity of the
   * slide interval, so a few elements are stored per key (one per slide interval). Custom windows
   * may not be able to pre-aggregate, or may need to store extra values in an aggregation tree.
   *
   * @param function
   *   The reduce function.
   * @return
   *   The data stream that is the result of applying the reduce function to the window.
   */
  @PublicEvolving
  def reduceWith(function: (T, T) => T): DataStream[T] =
    stream.reduce(function)

}
