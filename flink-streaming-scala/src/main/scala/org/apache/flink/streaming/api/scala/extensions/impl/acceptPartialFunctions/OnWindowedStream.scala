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
  * Wraps a joined data stream, allowing to use anonymous partial functions to
  * perform extraction of items in a tuple, case class instance or collection
  *
  * @param stream The wrapped data stream
  * @tparam T The type of the data stream items from the right input of the join
  * @tparam K The type of key
  * @tparam W The type of the window
  */
class OnWindowedStream[T, K, W <: Window](stream: WindowedStream[T, K, W]) {

  /**
    * Applies a reduce function to the window. The window function is called for each evaluation
    * of the window for each key individually. The output of the reduce function is interpreted
    * as a regular non-windowed stream.
    *
    * This window will try and pre-aggregate data as much as the window policies permit.
    * For example,tumbling time windows can perfectly pre-aggregate the data, meaning that only one
    * element per key is stored. Sliding time windows will pre-aggregate on the granularity of the
    * slide interval, so a few elements are stored per key (one per slide interval).
    * Custom windows may not be able to pre-aggregate, or may need to store extra values in an
    * aggregation tree.
    *
    * @param function The reduce function.
    * @return The data stream that is the result of applying the reduce function to the window.
    */
  @PublicEvolving
  def reduceWith(function: (T, T) => T) =
    stream.reduce(function)

  /**
    * Applies the given fold function to each window. The window function is called for each
    * evaluation of the window for each key individually. The output of the reduce function is
    * interpreted as a regular non-windowed stream.
    *
    * @param function The fold function.
    * @return The data stream that is the result of applying the fold function to the window.
    */
  @PublicEvolving
  def foldWith[R: TypeInformation](initialValue: R)(function: (R, T) => R) =
    stream.fold(initialValue)(function)

  /**
    * Applies the given window function to each window. The window function is called for each
    * evaluation of the window for each key individually. The output of the window function is
    * interpreted as a regular non-windowed stream.
    *
    * Arriving data is incrementally aggregated using the given fold function.
    *
    * @param initialValue The initial value of the fold
    * @param foldFunction The fold function that is used for incremental aggregation
    * @param windowFunction The window function.
    * @return The data stream that is the result of applying the window function to the window.
    */
  @PublicEvolving
  def applyWith[ACC: TypeInformation, R: TypeInformation](
      initialValue: ACC)(
      foldFunction: (ACC, T) => ACC,
      windowFunction: (K, W, Stream[ACC]) => TraversableOnce[R])
    : DataStream[R] =
    stream.fold(initialValue, foldFunction, {
      (key: K, window: W, items: Iterable[ACC], out: Collector[R]) =>
        windowFunction(key, window, items.toStream).foreach(out.collect)
    })

}
